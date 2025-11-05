"""
Tests for funding rates ingestion workflow.

Tests the async download and import workflow for funding rates data:
- _download_single_month_funding_rates()
- _download_symbol_funding_rates_async()
- _process_funding_rates_results()
- ingest_binance_async() with 'funding_rates' data_type
"""

import pytest
import asyncio
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch, AsyncMock
from datetime import datetime

from crypto_data.ingestion import (
    _process_funding_rates_results,
    _download_single_month_funding_rates,
    _download_symbol_funding_rates_async,
    initialize_ingestion_stats
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
# Tests for _download_single_month_funding_rates()
# =============================================================================

class TestDownloadSingleMonthFundingRates:
    """Test _download_single_month_funding_rates() async function."""

    @pytest.mark.asyncio
    async def test_successful_download_returns_success_dict(self):
        """Test successful download returns correct result dict."""
        # Mock client
        mock_client = AsyncMock()
        mock_client.download_funding_rates = AsyncMock(return_value=True)

        with tempfile.TemporaryDirectory() as tmpdir:
            temp_path = Path(tmpdir)
            expected_file = temp_path / "BTCUSDT-fundingRate-2024-01.zip"

            result = await _download_single_month_funding_rates(
                client=mock_client,
                symbol='BTCUSDT',
                month='2024-01',
                temp_path=temp_path,
                progress_info={'total': 1}
            )

            # Verify result
            assert result['success'] is True
            assert result['symbol'] == 'BTCUSDT'
            assert result['data_type'] == 'funding_rates'
            assert result['month'] == '2024-01'
            assert result['file_path'] == expected_file
            assert result['error'] is None

            # Verify client was called correctly
            mock_client.download_funding_rates.assert_called_once_with(
                symbol='BTCUSDT',
                month='2024-01',
                output_path=expected_file
            )

    @pytest.mark.asyncio
    async def test_404_returns_not_found_error(self):
        """Test 404 response returns not_found error."""
        # Mock client returning False (404)
        mock_client = AsyncMock()
        mock_client.download_funding_rates = AsyncMock(return_value=False)

        with tempfile.TemporaryDirectory() as tmpdir:
            temp_path = Path(tmpdir)

            result = await _download_single_month_funding_rates(
                client=mock_client,
                symbol='BTCUSDT',
                month='2024-01',
                temp_path=temp_path,
                progress_info={'total': 1}
            )

            # Verify result indicates not found
            assert result['success'] is False
            assert result['symbol'] == 'BTCUSDT'
            assert result['error'] == 'not_found'
            assert result['file_path'] is None

    @pytest.mark.asyncio
    async def test_exception_returns_error_dict(self):
        """Test exception during download returns error dict."""
        # Mock client raising exception
        mock_client = AsyncMock()
        mock_client.download_funding_rates = AsyncMock(
            side_effect=Exception("Network error")
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            temp_path = Path(tmpdir)

            result = await _download_single_month_funding_rates(
                client=mock_client,
                symbol='BTCUSDT',
                month='2024-01',
                temp_path=temp_path,
                progress_info={'total': 1}
            )

            # Verify result indicates error
            assert result['success'] is False
            assert result['symbol'] == 'BTCUSDT'
            assert result['error'] == 'Network error'
            assert result['file_path'] is None


# =============================================================================
# Tests for _download_symbol_funding_rates_async()
# =============================================================================

class TestDownloadSymbolFundingRatesAsync:
    """Test _download_symbol_funding_rates_async() parallel download coordinator."""

    @pytest.mark.asyncio
    async def test_parallel_downloads_multiple_months(self):
        """Test that multiple months are downloaded in parallel."""
        with tempfile.TemporaryDirectory() as tmpdir:
            temp_path = Path(tmpdir)

            # Mock data_exists to not skip any months
            with patch('crypto_data.ingestion.data_exists', return_value=False):
                # Mock the client and its download method
                with patch('crypto_data.ingestion.BinanceDataVisionClientAsync') as mock_client_class:
                    mock_client = AsyncMock()
                    mock_client.download_funding_rates = AsyncMock(return_value=True)
                    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
                    mock_client.__aexit__ = AsyncMock(return_value=None)
                    mock_client_class.return_value = mock_client

                    # Mock connection
                    mock_conn = MagicMock()
                    stats = initialize_ingestion_stats()

                    # Download 3 months
                    months = ['2024-01', '2024-02', '2024-03']
                    results = await _download_symbol_funding_rates_async(
                        conn=mock_conn,
                        symbol='BTCUSDT',
                        months=months,
                        temp_path=temp_path,
                        stats=stats,
                        skip_existing=True,
                        max_concurrent=5
                    )

                    # Verify all 3 months were downloaded
                    assert len(results) == 3
                    assert mock_client.download_funding_rates.call_count == 3

                    # Verify all results are success
                    for result in results:
                        assert result['success'] is True
                        assert result['data_type'] == 'funding_rates'

    @pytest.mark.asyncio
    async def test_skip_existing_months(self):
        """Test that existing months are skipped when skip_existing=True."""
        with tempfile.TemporaryDirectory() as tmpdir:
            temp_path = Path(tmpdir)

            # Mock data_exists to return True for 2024-01, False for others
            def mock_data_exists(conn, symbol, month, data_type, **kwargs):
                return month == '2024-01'

            with patch('crypto_data.ingestion.data_exists', side_effect=mock_data_exists):
                with patch('crypto_data.ingestion.BinanceDataVisionClientAsync') as mock_client_class:
                    mock_client = AsyncMock()
                    mock_client.download_funding_rates = AsyncMock(return_value=True)
                    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
                    mock_client.__aexit__ = AsyncMock(return_value=None)
                    mock_client_class.return_value = mock_client

                    mock_conn = MagicMock()
                    stats = initialize_ingestion_stats()

                    months = ['2024-01', '2024-02', '2024-03']
                    results = await _download_symbol_funding_rates_async(
                        conn=mock_conn,
                        symbol='BTCUSDT',
                        months=months,
                        temp_path=temp_path,
                        stats=stats,
                        skip_existing=True,
                        max_concurrent=5
                    )

                    # Verify only 2 months were downloaded (2024-01 skipped)
                    assert len(results) == 2
                    assert mock_client.download_funding_rates.call_count == 2

                    # Verify stats
                    assert stats['skipped'] == 1

    @pytest.mark.asyncio
    async def test_all_months_skipped_returns_empty(self):
        """Test that empty list returned when all months exist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            temp_path = Path(tmpdir)

            # Mock data_exists to return True for all
            with patch('crypto_data.ingestion.data_exists', return_value=True):
                mock_conn = MagicMock()
                stats = initialize_ingestion_stats()

                months = ['2024-01', '2024-02']
                results = await _download_symbol_funding_rates_async(
                    conn=mock_conn,
                    symbol='BTCUSDT',
                    months=months,
                    temp_path=temp_path,
                    stats=stats,
                    skip_existing=True,
                    max_concurrent=5
                )

                # Verify empty results
                assert len(results) == 0
                assert stats['skipped'] == 2

    @pytest.mark.asyncio
    async def test_exception_converted_to_error_result(self):
        """Test that exceptions during download are converted to error results."""
        with tempfile.TemporaryDirectory() as tmpdir:
            temp_path = Path(tmpdir)

            with patch('crypto_data.ingestion.data_exists', return_value=False):
                with patch('crypto_data.ingestion.BinanceDataVisionClientAsync') as mock_client_class:
                    mock_client = AsyncMock()
                    # First call succeeds, second raises exception
                    mock_client.download_funding_rates = AsyncMock(
                        side_effect=[True, Exception("Network error")]
                    )
                    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
                    mock_client.__aexit__ = AsyncMock(return_value=None)
                    mock_client_class.return_value = mock_client

                    mock_conn = MagicMock()
                    stats = initialize_ingestion_stats()

                    months = ['2024-01', '2024-02']
                    results = await _download_symbol_funding_rates_async(
                        conn=mock_conn,
                        symbol='BTCUSDT',
                        months=months,
                        temp_path=temp_path,
                        stats=stats,
                        skip_existing=True,
                        max_concurrent=5
                    )

                    # Verify results: one success, one error
                    assert len(results) == 2
                    success_results = [r for r in results if r['success']]
                    error_results = [r for r in results if not r['success']]

                    assert len(success_results) == 1
                    assert len(error_results) == 1
                    assert 'Network error' in str(error_results[0]['error'])

    @pytest.mark.asyncio
    async def test_mixed_success_and_404_results(self):
        """Test handling of mixed success and 404 results."""
        with tempfile.TemporaryDirectory() as tmpdir:
            temp_path = Path(tmpdir)

            with patch('crypto_data.ingestion.data_exists', return_value=False):
                with patch('crypto_data.ingestion.BinanceDataVisionClientAsync') as mock_client_class:
                    mock_client = AsyncMock()
                    # True (success), False (404), True (success)
                    mock_client.download_funding_rates = AsyncMock(
                        side_effect=[True, False, True]
                    )
                    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
                    mock_client.__aexit__ = AsyncMock(return_value=None)
                    mock_client_class.return_value = mock_client

                    mock_conn = MagicMock()
                    stats = initialize_ingestion_stats()

                    months = ['2024-01', '2024-02', '2024-03']
                    results = await _download_symbol_funding_rates_async(
                        conn=mock_conn,
                        symbol='BTCUSDT',
                        months=months,
                        temp_path=temp_path,
                        stats=stats,
                        skip_existing=True,
                        max_concurrent=5
                    )

                    # Verify results
                    assert len(results) == 3
                    success_count = sum(1 for r in results if r['success'])
                    not_found_count = sum(1 for r in results if r['error'] == 'not_found')

                    assert success_count == 2
                    assert not_found_count == 1


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


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
