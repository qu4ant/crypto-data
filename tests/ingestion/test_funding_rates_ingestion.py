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
from pathlib import Path
from unittest.mock import MagicMock, patch
from datetime import datetime

from crypto_data.ingestion import (
    _process_funding_rates_results,
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
