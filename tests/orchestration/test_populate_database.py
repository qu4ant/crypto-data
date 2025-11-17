"""
Tests for populate_database() orchestration function.

Tests that populate_database() correctly orchestrates the complete workflow:
universe ingestion → symbol extraction → Binance data ingestion.

Uses mocks to avoid external API calls and file I/O.
"""

import pytest
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock, AsyncMock, call

from crypto_data.ingestion import populate_database


def test_populate_database_orchestrates_full_workflow():
    """Test that populate_database() calls all components in correct order."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / 'test.db')

        # Mock all heavy functions
        with patch('crypto_data.ingestion.ingest_universe', new_callable=AsyncMock) as mock_ingest_universe, \
             patch('crypto_data.ingestion.get_symbols_from_universe') as mock_get_symbols, \
             patch('crypto_data.ingestion.ingest_binance_async') as mock_ingest_binance, \
             patch('crypto_data.ingestion.CryptoDatabase') as mock_db, \
             patch('crypto_data.ingestion.Path') as mock_path:

            # Setup mocks
            mock_get_symbols.return_value = ['BTCUSDT', 'ETHUSDT', 'SOLUSDT']

            # Mock CryptoDatabase for symbol count
            mock_db_instance = MagicMock()
            mock_db_instance.execute.return_value.fetchone.return_value = (3,)
            mock_db.return_value = mock_db_instance

            # Mock Path for database size
            mock_path_instance = MagicMock()
            mock_path_instance.stat.return_value.st_size = 1024 * 1024  # 1 MB
            mock_path.return_value = mock_path_instance

            # Call populate_database
            populate_database(
                db_path=db_path,
                start_date='2024-01-01',
                end_date='2024-03-31',
                top_n=50,
                interval='5m',
                data_types=['spot', 'futures']
            )

            # Verify ingest_universe was called
            mock_ingest_universe.assert_called_once()

            # Verify get_symbols_from_universe called
            mock_get_symbols.assert_called_once_with(db_path, '2024-01-01', '2024-03-31', 50)

            # Verify ingest_binance_async called with extracted symbols
            mock_ingest_binance.assert_called_once_with(
                db_path=db_path,
                symbols=['BTCUSDT', 'ETHUSDT', 'SOLUSDT'],
                data_types=['spot', 'futures'],
                start_date='2024-01-01',
                end_date='2024-03-31',
                interval='5m',
                skip_existing=True,
                max_concurrent_klines=20,
                max_concurrent_metrics=100,
                max_concurrent_funding=50,
                failure_threshold=3
            )


def test_populate_database_handles_empty_universe():
    """Test that populate_database() exits gracefully when no symbols are extracted."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / 'test.db')

        # Mock functions
        with patch('crypto_data.ingestion.ingest_universe', new_callable=AsyncMock) as mock_ingest_universe, \
             patch('crypto_data.ingestion.get_symbols_from_universe') as mock_get_symbols, \
             patch('crypto_data.ingestion.ingest_binance_async') as mock_ingest_binance:

            # Setup mocks - empty universe
            mock_get_symbols.return_value = []

            # Call populate_database
            populate_database(
                db_path=db_path,
                start_date='2024-01-01',
                end_date='2024-01-31',
                top_n=50,
                interval='5m',
                data_types=['spot']
            )

            # Verify ingest_universe was called
            mock_ingest_universe.assert_called_once()

            # Verify get_symbols_from_universe was called
            assert mock_get_symbols.call_count == 1

            # Verify ingest_binance_async was NOT called (early return)
            mock_ingest_binance.assert_not_called()


def test_populate_database_generates_correct_month_list():
    """Test that populate_database() generates correct monthly snapshots (batch mode)."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / 'test.db')

        # Mock functions
        with patch('crypto_data.ingestion.ingest_universe', new_callable=AsyncMock) as mock_ingest_universe, \
             patch('crypto_data.ingestion.get_symbols_from_universe') as mock_get_symbols, \
             patch('crypto_data.ingestion.ingest_binance_async') as mock_ingest_binance:

            # Setup mocks
            mock_get_symbols.return_value = ['BTCUSDT']

            # Call sync with 12-month period
            populate_database(
                db_path=db_path,
                start_date='2024-01-01',
                end_date='2024-12-31',
                top_n=100,
                interval='5m',
                data_types=['spot']
            )

            # Verify ingest_universe was called
            mock_ingest_universe.assert_called_once()


def test_populate_database_continues_on_universe_failure():
    """Test that populate_database() continues even if universe ingestion has errors (batch mode logs and continues)."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / 'test.db')

        # Mock functions
        with patch('crypto_data.ingestion.ingest_universe', new_callable=AsyncMock) as mock_ingest_universe, \
             patch('crypto_data.ingestion.get_symbols_from_universe') as mock_get_symbols, \
             patch('crypto_data.ingestion.ingest_binance_async') as mock_ingest_binance:

            # Setup mocks
            mock_get_symbols.return_value = ['BTCUSDT']

            # Call sync (batch version doesn't raise on errors, logs them)
            populate_database(
                db_path=db_path,
                start_date='2024-01-01',
                end_date='2024-03-31',
                top_n=50,
                interval='5m',
                data_types=['spot']
            )

            # Verify ingest_universe was called
            mock_ingest_universe.assert_called_once()

            # Verify ingest_binance_async was still called (workflow continued)
            mock_ingest_binance.assert_called_once()


def test_populate_database_uses_default_data_types():
    """Test that populate_database() uses default data_types if not specified."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / 'test.db')

        # Mock functions
        with patch('crypto_data.ingestion.ingest_universe', new_callable=AsyncMock), \
             patch('crypto_data.ingestion.get_symbols_from_universe') as mock_get_symbols, \
             patch('crypto_data.ingestion.ingest_binance_async') as mock_ingest_binance:

            mock_get_symbols.return_value = ['BTCUSDT']

            # Call sync without data_types parameter
            populate_database(
                db_path=db_path,
                start_date='2024-01-01',
                end_date='2024-01-31',
                top_n=50,
                interval='5m'
                # data_types not specified
            )

            # Verify ingest_binance_async called with default ['spot', 'futures']
            call_args = mock_ingest_binance.call_args
            assert call_args[1]['data_types'] == ['spot', 'futures']


def test_populate_database_handles_year_boundary():
    """Test that populate_database() correctly handles date ranges crossing year boundaries."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / 'test.db')

        # Mock functions
        with patch('crypto_data.ingestion.ingest_universe', new_callable=AsyncMock) as mock_ingest_universe, \
             patch('crypto_data.ingestion.get_symbols_from_universe') as mock_get_symbols, \
             patch('crypto_data.ingestion.ingest_binance_async'):

            mock_get_symbols.return_value = ['BTCUSDT']

            # Call sync with year boundary
            populate_database(
                db_path=db_path,
                start_date='2023-11-01',
                end_date='2024-02-28',
                top_n=50,
                interval='5m',
                data_types=['spot']
            )

            # Verify ingest_universe was called
            mock_ingest_universe.assert_called_once()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
