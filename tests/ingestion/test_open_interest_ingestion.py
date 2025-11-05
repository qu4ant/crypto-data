"""
Tests for open interest ingestion workflow.

Tests the async download and import workflow for open interest data:
- _download_single_day()
- _download_symbol_open_interest_async()
- _process_metrics_results()
- ingest_binance_async() with 'open_interest' data_type
"""

import pytest
import asyncio
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch, Mock
from datetime import datetime

from crypto_data.ingestion import (
    _download_single_day,
    _download_symbol_open_interest_async,
    _process_metrics_results,
    initialize_ingestion_stats
)


# =============================================================================
# Tests for _download_single_day()
# =============================================================================

class TestDownloadSingleDay:
    """Test _download_single_day() function."""

    @pytest.mark.asyncio
    async def test_download_single_day_success(self):
        """Test successful single day download."""
        # Mock client
        mock_client = AsyncMock()
        mock_client.download_metrics = AsyncMock(return_value=True)

        # Setup
        temp_path = Path(tempfile.mkdtemp())
        progress_info = {'total': 1}

        try:
            result = await _download_single_day(
                client=mock_client,
                symbol='BTCUSDT',
                date='2025-11-02',
                temp_path=temp_path,
                progress_info=progress_info
            )

            # Verify result format
            assert result['success'] is True
            assert result['symbol'] == 'BTCUSDT'
            assert result['data_type'] == 'open_interest'
            assert result['date'] == '2025-11-02'
            assert result['file_path'] is not None
            assert result['error'] is None

            # Verify download was called
            mock_client.download_metrics.assert_called_once()

        finally:
            # Cleanup
            import shutil
            if temp_path.exists():
                shutil.rmtree(temp_path)

    @pytest.mark.asyncio
    async def test_download_single_day_not_found(self):
        """Test 404 creates error='not_found' result."""
        # Mock client that returns False (404)
        mock_client = AsyncMock()
        mock_client.download_metrics = AsyncMock(return_value=False)

        temp_path = Path(tempfile.mkdtemp())
        progress_info = {'total': 1}

        try:
            result = await _download_single_day(
                client=mock_client,
                symbol='BTCUSDT',
                date='2025-11-02',
                temp_path=temp_path,
                progress_info=progress_info
            )

            # Verify result
            assert result['success'] is False
            assert result['error'] == 'not_found'
            assert result['file_path'] is None

        finally:
            import shutil
            if temp_path.exists():
                shutil.rmtree(temp_path)

    @pytest.mark.asyncio
    async def test_download_single_day_exception(self):
        """Test exception handling."""
        # Mock client that raises exception
        mock_client = AsyncMock()
        mock_client.download_metrics = AsyncMock(
            side_effect=Exception("Network error")
        )

        temp_path = Path(tempfile.mkdtemp())
        progress_info = {'total': 1}

        try:
            result = await _download_single_day(
                client=mock_client,
                symbol='BTCUSDT',
                date='2025-11-02',
                temp_path=temp_path,
                progress_info=progress_info
            )

            # Verify error result
            assert result['success'] is False
            assert result['error'] == 'Network error'

        finally:
            import shutil
            if temp_path.exists():
                shutil.rmtree(temp_path)


# =============================================================================
# Tests for _download_symbol_open_interest_async()
# =============================================================================

class TestDownloadSymbolOpenInterestAsync:
    """Test _download_symbol_open_interest_async() function."""

    @pytest.mark.asyncio
    async def test_download_symbol_open_interest_parallel(self):
        """Test multiple days are downloaded in parallel."""
        days = ['2025-11-01', '2025-11-02', '2025-11-03']
        temp_path = Path(tempfile.mkdtemp())

        try:
            with patch('crypto_data.ingestion.BinanceDataVisionClientAsync') as mock_client_class:
                # Mock client instance
                mock_client = AsyncMock()
                mock_client.__aenter__ = AsyncMock(return_value=mock_client)
                mock_client.__aexit__ = AsyncMock(return_value=None)
                mock_client.download_metrics = AsyncMock(return_value=True)
                mock_client_class.return_value = mock_client

                results = await _download_symbol_open_interest_async(
                    symbol='BTCUSDT',
                    days=days,
                    temp_path=temp_path,
                    max_concurrent=20
                )

                # Verify we got results for all days
                assert len(results) == 3
                assert all(r['success'] for r in results)

        finally:
            import shutil
            if temp_path.exists():
                shutil.rmtree(temp_path)

    @pytest.mark.asyncio
    async def test_download_symbol_open_interest_empty_days(self):
        """Test empty day list is handled correctly."""
        temp_path = Path(tempfile.mkdtemp())

        try:
            results = await _download_symbol_open_interest_async(
                symbol='BTCUSDT',
                days=[],
                temp_path=temp_path,
                max_concurrent=20
            )

            # Should return empty list
            assert results == []

        finally:
            import shutil
            if temp_path.exists():
                shutil.rmtree(temp_path)

    @pytest.mark.asyncio
    async def test_download_symbol_open_interest_exception_handling(self):
        """Test exception is converted to error result."""
        days = ['2025-11-01']
        temp_path = Path(tempfile.mkdtemp())

        try:
            with patch('crypto_data.ingestion._download_single_day') as mock_download:
                # Mock download that raises exception
                mock_download.return_value = Exception("Network error")

                results = await _download_symbol_open_interest_async(
                    symbol='BTCUSDT',
                    days=days,
                    temp_path=temp_path,
                    max_concurrent=20
                )

                # Should have error result
                assert len(results) == 1
                assert results[0]['success'] is False
                assert 'error' in results[0]

        finally:
            import shutil
            if temp_path.exists():
                shutil.rmtree(temp_path)


# =============================================================================
# Tests for _process_metrics_results()
# =============================================================================

class TestProcessMetricsResults:
    """Test _process_metrics_results() function."""

    def test_process_metrics_results_success(self):
        """Test successful import flow."""
        with tempfile.TemporaryDirectory() as tmpdir:
            temp_file = Path(tmpdir) / "test.csv"
            temp_file.touch()

            results = [
                {
                    'success': True,
                    'symbol': 'BTCUSDT',
                    'data_type': 'open_interest',
                    'date': '2025-11-02',
                    'file_path': temp_file,
                    'error': None
                }
            ]

            mock_conn = MagicMock()
            stats = initialize_ingestion_stats()

            with patch('crypto_data.ingestion.import_metrics_to_duckdb') as mock_import:
                _process_metrics_results(results, mock_conn, stats, 'BTCUSDT')

                # Verify import was called
                mock_import.assert_called_once_with(
                    conn=mock_conn,
                    file_path=temp_file,
                    symbol='BTCUSDT',
                    exchange='binance'
                )

                # Verify stats updated
                assert stats['downloaded'] == 1
                assert stats['failed'] == 0

    def test_process_metrics_results_not_found(self):
        """Test not_found updates stats correctly."""
        results = [
            {
                'success': False,
                'symbol': 'BTCUSDT',
                'data_type': 'open_interest',
                'date': '2025-11-02',
                'file_path': None,
                'error': 'not_found'
            }
        ]

        mock_conn = MagicMock()
        stats = initialize_ingestion_stats()

        _process_metrics_results(results, mock_conn, stats, 'BTCUSDT')

        # Verify stats
        assert stats['not_found'] == 1
        assert stats['downloaded'] == 0
        assert stats['failed'] == 0

    def test_process_metrics_results_import_failure(self):
        """Test import error handling."""
        with tempfile.TemporaryDirectory() as tmpdir:
            temp_file = Path(tmpdir) / "test.csv"
            temp_file.touch()

            results = [
                {
                    'success': True,
                    'symbol': 'BTCUSDT',
                    'data_type': 'open_interest',
                    'date': '2025-11-02',
                    'file_path': temp_file,
                    'error': None
                }
            ]

            mock_conn = MagicMock()
            stats = initialize_ingestion_stats()

            with patch('crypto_data.ingestion.import_metrics_to_duckdb') as mock_import:
                # Make import raise exception
                mock_import.side_effect = Exception("Import failed")

                _process_metrics_results(results, mock_conn, stats, 'BTCUSDT')

                # Verify stats
                assert stats['failed'] == 1
                assert stats['downloaded'] == 0

    def test_process_metrics_results_cleanup(self):
        """Test temp file deletion."""
        with tempfile.TemporaryDirectory() as tmpdir:
            temp_file = Path(tmpdir) / "test.csv"
            temp_file.write_text("test content")

            assert temp_file.exists()

            results = [
                {
                    'success': True,
                    'symbol': 'BTCUSDT',
                    'data_type': 'open_interest',
                    'date': '2025-11-02',
                    'file_path': temp_file,
                    'error': None
                }
            ]

            mock_conn = MagicMock()
            stats = initialize_ingestion_stats()

            with patch('crypto_data.ingestion.import_metrics_to_duckdb'):
                _process_metrics_results(results, mock_conn, stats, 'BTCUSDT')

                # Verify file was deleted
                assert not temp_file.exists()

    def test_process_metrics_results_stats_update(self):
        """Test stats dict is updated correctly."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create temp files
            temp_file1 = Path(tmpdir) / "test1.csv"
            temp_file1.touch()
            temp_file2 = Path(tmpdir) / "test2.csv"
            temp_file2.touch()

            results = [
                {
                    'success': True,
                    'symbol': 'BTCUSDT',
                    'data_type': 'open_interest',
                    'date': '2025-11-02',
                    'file_path': temp_file1,
                    'error': None
                },
                {
                    'success': False,
                    'symbol': 'BTCUSDT',
                    'data_type': 'open_interest',
                    'date': '2025-11-03',
                    'file_path': None,
                    'error': 'not_found'
                },
                {
                    'success': True,
                    'symbol': 'BTCUSDT',
                    'data_type': 'open_interest',
                    'date': '2025-11-04',
                    'file_path': temp_file2,
                    'error': None
                }
            ]

            mock_conn = MagicMock()
            stats = initialize_ingestion_stats()

            with patch('crypto_data.ingestion.import_metrics_to_duckdb'):
                _process_metrics_results(results, mock_conn, stats, 'BTCUSDT')

                # Verify stats
                assert stats['downloaded'] == 2
                assert stats['not_found'] == 1
                assert stats['failed'] == 0


# =============================================================================
# Integration Test: ingest_binance_async() with open_interest
# =============================================================================

class TestIngestBinanceAsyncWithOpenInterest:
    """Test end-to-end open interest ingestion with ingest_binance_async()."""

    @pytest.mark.asyncio
    async def test_ingest_binance_async_with_open_interest(self):
        """Test end-to-end ingestion with 'open_interest' data_type."""
        from crypto_data import ingest_binance_async

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"

            # Mock all the download and import functions
            with patch('crypto_data.ingestion.asyncio.run') as mock_run, \
                 patch('crypto_data.ingestion.CryptoDatabase') as mock_db:

                # Mock database
                mock_conn = MagicMock()
                mock_db_instance = MagicMock()
                mock_db_instance.conn = mock_conn
                mock_db_instance.__enter__ = Mock(return_value=mock_db_instance)
                mock_db_instance.__exit__ = Mock(return_value=None)
                mock_db.return_value = mock_db_instance

                # Mock asyncio.run to return empty results
                mock_run.return_value = []

                # Run ingestion with open_interest data_type
                ingest_binance_async(
                    db_path=str(db_path),
                    symbols=['BTCUSDT'],
                    data_types=['open_interest'],
                    start_date='2025-11-01',
                    end_date='2025-11-02',
                    max_concurrent_metrics=20
                )

                # Verify database was created
                mock_db.assert_called()

                # Verify transaction was used
                assert mock_conn.execute.called

    def test_open_interest_with_1000prefix_retry(self):
        """Test that open_interest triggers 1000-prefix retry on all 404s."""
        from crypto_data import ingest_binance_async

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"

            with patch('crypto_data.ingestion.asyncio.run') as mock_run, \
                 patch('crypto_data.ingestion.CryptoDatabase') as mock_db, \
                 patch('crypto_data.ingestion._ticker_mappings', {}):

                # Mock database
                mock_conn = MagicMock()
                mock_db_instance = MagicMock()
                mock_db_instance.conn = mock_conn
                mock_db.return_value = mock_db_instance

                # Mock asyncio.run to return all 404s on first call, success on retry
                mock_run.side_effect = [
                    # First call: all 404s
                    [{
                        'success': False,
                        'error': 'not_found',
                        'symbol': 'PEPEUSDT',
                        'data_type': 'open_interest',
                        'date': '2025-11-01',
                        'file_path': None
                    }],
                    # Retry with 1000-prefix: success
                    [{
                        'success': True,
                        'error': None,
                        'symbol': '1000PEPEUSDT',
                        'data_type': 'open_interest',
                        'date': '2025-11-01',
                        'file_path': Path('/tmp/test.csv')
                    }]
                ]

                # Run ingestion with open_interest
                ingest_binance_async(
                    db_path=str(db_path),
                    symbols=['PEPEUSDT'],
                    data_types=['open_interest'],
                    start_date='2025-11-01',
                    end_date='2025-11-01',
                    max_concurrent_metrics=20
                )

                # Verify asyncio.run was called twice (original + retry)
                assert mock_run.call_count == 2

    def test_open_interest_cache_persists(self):
        """Test that 1000-prefix mapping is cached across symbols."""
        from crypto_data import ingest_binance_async
        from crypto_data.ingestion import _ticker_mappings

        # Clear and pre-populate cache
        _ticker_mappings.clear()
        _ticker_mappings['PEPEUSDT'] = '1000PEPEUSDT'

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"

            with patch('crypto_data.ingestion.asyncio.run') as mock_run, \
                 patch('crypto_data.ingestion.CryptoDatabase') as mock_db, \
                 patch('crypto_data.ingestion._ticker_mappings', _ticker_mappings):

                # Mock database
                mock_conn = MagicMock()
                mock_db_instance = MagicMock()
                mock_db_instance.conn = mock_conn
                mock_db.return_value = mock_db_instance

                # Mock success (using cached 1000-prefix)
                mock_run.return_value = [{
                    'success': True,
                    'error': None,
                    'symbol': '1000PEPEUSDT',
                    'data_type': 'open_interest',
                    'date': '2025-11-01',
                    'file_path': Path('/tmp/test.csv')
                }]

                # Run ingestion with open_interest
                ingest_binance_async(
                    db_path=str(db_path),
                    symbols=['PEPEUSDT'],
                    data_types=['open_interest'],
                    start_date='2025-11-01',
                    end_date='2025-11-01',
                    max_concurrent_metrics=20
                )

                # Verify asyncio.run was called only once (used cached mapping, no retry)
                assert mock_run.call_count == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
