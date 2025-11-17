"""
Tests for funding rates ingestion workflow.

Tests the async download and import workflow for funding rates data:
- _download_single_month_funding_rates()
- _download_symbol_funding_rates_async()
- _process_funding_rates_results()
- ingest_binance_async() with 'funding_rates' data_type
"""

import pytest
import tempfile
import asyncio
from pathlib import Path
from unittest.mock import MagicMock, patch, AsyncMock
from datetime import datetime

from crypto_data.ingestion import (
    _process_funding_rates_results,
    initialize_ingestion_stats,
    _download_single_month_funding_rates,
    _download_symbol_funding_rates_async
)


# =============================================================================
# Tests for _process_funding_rates_results()
# =============================================================================

class TestProcessFundingRatesResults:
    """Test _process_funding_rates_results() function."""

    def test_process_funding_rates_results_success(self):
        """Test successful import flow."""
        with tempfile.TemporaryDirectory() as tmpdir:
            temp_file = Path(tmpdir) / "test.zip"
            temp_file.touch()

            results = [
                {
                    'success': True,
                    'symbol': 'BTCUSDT',
                    'data_type': 'funding_rates',
                    'month': '2024-12',
                    'file_path': temp_file,
                    'error': None
                }
            ]

            mock_conn = MagicMock()
            stats = initialize_ingestion_stats()

            with patch('crypto_data.ingestion.import_funding_rates_to_duckdb') as mock_import:
                _process_funding_rates_results(results, mock_conn, stats, 'BTCUSDT')

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

    def test_process_funding_rates_results_not_found(self):
        """Test not_found updates stats correctly."""
        results = [
            {
                'success': False,
                'symbol': 'BTCUSDT',
                'data_type': 'funding_rates',
                'month': '2024-12',
                'file_path': None,
                'error': 'not_found'
            }
        ]

        mock_conn = MagicMock()
        stats = initialize_ingestion_stats()

        _process_funding_rates_results(results, mock_conn, stats, 'BTCUSDT')

        # Verify stats
        assert stats['not_found'] == 1
        assert stats['downloaded'] == 0
        assert stats['failed'] == 0

    def test_process_funding_rates_results_import_failure(self):
        """Test import error handling."""
        with tempfile.TemporaryDirectory() as tmpdir:
            temp_file = Path(tmpdir) / "test.zip"
            temp_file.touch()

            results = [
                {
                    'success': True,
                    'symbol': 'BTCUSDT',
                    'data_type': 'funding_rates',
                    'month': '2024-12',
                    'file_path': temp_file,
                    'error': None
                }
            ]

            mock_conn = MagicMock()
            stats = initialize_ingestion_stats()

            with patch('crypto_data.ingestion.import_funding_rates_to_duckdb') as mock_import:
                # Make import raise exception
                mock_import.side_effect = Exception("Import failed")

                _process_funding_rates_results(results, mock_conn, stats, 'BTCUSDT')

                # Verify stats
                assert stats['failed'] == 1
                assert stats['downloaded'] == 0

    def test_process_funding_rates_results_cleanup(self):
        """Test temp file deletion."""
        with tempfile.TemporaryDirectory() as tmpdir:
            temp_file = Path(tmpdir) / "test.zip"
            temp_file.write_text("test content")

            assert temp_file.exists()

            results = [
                {
                    'success': True,
                    'symbol': 'BTCUSDT',
                    'data_type': 'funding_rates',
                    'month': '2024-12',
                    'file_path': temp_file,
                    'error': None
                }
            ]

            mock_conn = MagicMock()
            stats = initialize_ingestion_stats()

            with patch('crypto_data.ingestion.import_funding_rates_to_duckdb'):
                _process_funding_rates_results(results, mock_conn, stats, 'BTCUSDT')

                # Verify file was deleted
                assert not temp_file.exists()

    def test_process_funding_rates_results_stats_update(self):
        """Test stats dict is updated correctly."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create temp files
            temp_file1 = Path(tmpdir) / "test1.zip"
            temp_file1.touch()
            temp_file2 = Path(tmpdir) / "test2.zip"
            temp_file2.touch()

            results = [
                {
                    'success': True,
                    'symbol': 'BTCUSDT',
                    'data_type': 'funding_rates',
                    'month': '2024-11',
                    'file_path': temp_file1,
                    'error': None
                },
                {
                    'success': False,
                    'symbol': 'BTCUSDT',
                    'data_type': 'funding_rates',
                    'month': '2024-10',
                    'file_path': None,
                    'error': 'not_found'
                },
                {
                    'success': True,
                    'symbol': 'BTCUSDT',
                    'data_type': 'funding_rates',
                    'month': '2024-12',
                    'file_path': temp_file2,
                    'error': None
                }
            ]

            mock_conn = MagicMock()
            stats = initialize_ingestion_stats()

            with patch('crypto_data.ingestion.import_funding_rates_to_duckdb'):
                _process_funding_rates_results(results, mock_conn, stats, 'BTCUSDT')

                # Verify stats
                assert stats['downloaded'] == 2
                assert stats['not_found'] == 1
                assert stats['failed'] == 0


# =============================================================================
# Tests for 1000-prefix auto-discovery
# =============================================================================

class TestFundingRates1000PrefixRetry:
    """Test 1000-prefix auto-discovery for funding rates."""

    def test_funding_rates_with_1000prefix_retry(self):
        """Test that funding_rates triggers 1000-prefix retry on all 404s."""
        from crypto_data import ingest_binance_async

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"

            with patch('crypto_data.ingestion.asyncio.run') as mock_run, \
                 patch('crypto_data.ingestion.CryptoDatabase') as mock_db, \
                 patch('crypto_data.ingestion._ticker_mappings', {}), \
                 patch('crypto_data.ingestion.generate_month_list', return_value=['2024-01']):

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
                        'data_type': 'funding_rates',
                        'month': '2024-01',
                        'file_path': None
                    }],
                    # Retry with 1000-prefix: success
                    [{
                        'success': True,
                        'error': None,
                        'symbol': '1000PEPEUSDT',
                        'data_type': 'funding_rates',
                        'month': '2024-01',
                        'file_path': Path('/tmp/test.zip')
                    }]
                ]

                # Run ingestion with funding_rates
                ingest_binance_async(
                    db_path=str(db_path),
                    symbols=['PEPEUSDT'],
                    data_types=['funding_rates'],
                    start_date='2024-01-01',
                    end_date='2024-01-31'
                )

                # Verify asyncio.run was called twice (original + retry)
                assert mock_run.call_count == 2

    def test_funding_rates_cache_persists(self):
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
                 patch('crypto_data.ingestion.generate_month_list', return_value=['2024-01']):

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
                    'data_type': 'funding_rates',
                    'month': '2024-01',
                    'file_path': Path('/tmp/test.zip')
                }]

                # Run ingestion with funding_rates
                ingest_binance_async(
                    db_path=str(db_path),
                    symbols=['PEPEUSDT'],
                    data_types=['funding_rates'],
                    start_date='2024-01-01',
                    end_date='2024-01-31'
                )

                # Verify asyncio.run was called only once (used cached mapping, no retry)
                assert mock_run.call_count == 1


# =============================================================================
# Tests for _download_single_month_funding_rates() - Async Function
# =============================================================================

class TestDownloadSingleMonthFundingRates:
    """Test _download_single_month_funding_rates() async function (improves coverage)."""

    @pytest.mark.asyncio
    async def test_download_single_month_success(self):
        """Test successful download of funding rates for single month."""
        with tempfile.TemporaryDirectory() as tmpdir:
            temp_path = Path(tmpdir)
            mock_client = AsyncMock()
            mock_client.download_funding_rates = AsyncMock(return_value=True)

            result = await _download_single_month_funding_rates(
                client=mock_client,
                symbol='BTCUSDT',
                month='2024-01',
                temp_path=temp_path,
                progress_info={'total': 1, 'completed': 0}
            )

            # Verify result structure
            assert result['success'] is True
            assert result['symbol'] == 'BTCUSDT'
            assert result['data_type'] == 'funding_rates'
            assert result['month'] == '2024-01'
            assert result['file_path'] is not None
            assert result['error'] is None

            # Verify download was called
            mock_client.download_funding_rates.assert_called_once()

    @pytest.mark.asyncio
    async def test_download_single_month_not_found(self):
        """Test 404 not found handling."""
        with tempfile.TemporaryDirectory() as tmpdir:
            temp_path = Path(tmpdir)
            mock_client = AsyncMock()
            mock_client.download_funding_rates = AsyncMock(return_value=False)

            result = await _download_single_month_funding_rates(
                client=mock_client,
                symbol='BTCUSDT',
                month='2024-01',
                temp_path=temp_path,
                progress_info={'total': 1, 'completed': 0}
            )

            # Verify result structure
            assert result['success'] is False
            assert result['symbol'] == 'BTCUSDT'
            assert result['error'] == 'not_found'
            assert result['file_path'] is None

    @pytest.mark.asyncio
    async def test_download_single_month_exception(self):
        """Test exception handling during download."""
        with tempfile.TemporaryDirectory() as tmpdir:
            temp_path = Path(tmpdir)
            mock_client = AsyncMock()
            mock_client.download_funding_rates = AsyncMock(
                side_effect=Exception("Network error")
            )

            result = await _download_single_month_funding_rates(
                client=mock_client,
                symbol='BTCUSDT',
                month='2024-01',
                temp_path=temp_path,
                progress_info={'total': 1, 'completed': 0}
            )

            # Verify error handling
            assert result['success'] is False
            assert result['symbol'] == 'BTCUSDT'
            assert result['error'] == 'Network error'
            assert result['file_path'] is None


# =============================================================================
# Tests for _download_symbol_funding_rates_async() - Parallel Downloads
# =============================================================================

class TestDownloadSymbolFundingRatesAsync:
    """Test _download_symbol_funding_rates_async() parallel download function."""

    # TODO: Fix this test - signature mismatch with actual function
    # @pytest.mark.asyncio
    # async def test_download_funding_rates_parallel(self):
    #     """Test parallel downloads for multiple months."""
    #     pass

    @pytest.mark.asyncio
    async def test_download_funding_rates_skip_existing(self):
        """Test skip_existing logic for funding rates."""
        with tempfile.TemporaryDirectory() as tmpdir:
            temp_path = Path(tmpdir)
            months = ['2024-01', '2024-02']

            # Mock existing data check (first month exists, second doesn't)
            def mock_data_exists(conn, symbol, month, data_type, interval=None, exchange='binance'):
                return month == '2024-01'  # First month exists

            with patch('crypto_data.ingestion.data_exists', side_effect=mock_data_exists):
                mock_client = AsyncMock()
                mock_conn = MagicMock()

                stats = initialize_ingestion_stats()

                results = await _download_symbol_funding_rates_async(
                    symbol='BTCUSDT',
                    months=months,
                    temp_path=temp_path,
                    conn=mock_conn,
                    skip_existing=True,
                    stats=stats,
                    max_concurrent=50
                )

                # Should only download second month (first skipped)
                assert len(results) == 1
                assert stats['skipped'] == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
