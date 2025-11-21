"""
Tests for error handling improvements.

Tests:
- Database connection cleanup on exception
- Thread-safe ticker mappings cache
- Date validation
"""

import pytest
from crypto_data.enums import DataType, Interval
import asyncio
import threading
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock
from datetime import datetime

from crypto_data.database import CryptoDatabase
from crypto_data.ingestion import (
    ingest_universe,
    ingest_binance_async,
    populate_database,
    _ticker_mappings,
    _ticker_mappings_lock,
    _validate_and_parse_dates
)


# =============================================================================
# DATE VALIDATION TESTS
# =============================================================================

class TestDateValidation:
    """Test date validation for ingestion functions."""

    def test_valid_dates(self):
        """Test that valid dates parse correctly."""
        start, end = _validate_and_parse_dates('2024-01-01', '2024-12-31')
        assert start == datetime(2024, 1, 1)
        assert end == datetime(2024, 12, 31)

    def test_same_date(self):
        """Test that same start and end date is valid."""
        start, end = _validate_and_parse_dates('2024-06-15', '2024-06-15')
        assert start == end == datetime(2024, 6, 15)

    def test_invalid_start_date_format(self):
        """Test that invalid start date format raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            _validate_and_parse_dates('2024/01/01', '2024-12-31')
        assert "Invalid start_date format" in str(exc_info.value)
        assert "2024/01/01" in str(exc_info.value)

    def test_invalid_end_date_format(self):
        """Test that invalid end date format raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            _validate_and_parse_dates('2024-01-01', '31-12-2024')
        assert "Invalid end_date format" in str(exc_info.value)
        assert "31-12-2024" in str(exc_info.value)

    def test_invalid_date_values(self):
        """Test that invalid date values raise ValueError."""
        with pytest.raises(ValueError) as exc_info:
            _validate_and_parse_dates('2024-13-01', '2024-12-31')
        assert "Invalid start_date format" in str(exc_info.value)

        with pytest.raises(ValueError) as exc_info:
            _validate_and_parse_dates('2024-01-01', '2024-02-30')
        assert "Invalid end_date format" in str(exc_info.value)

    def test_start_after_end(self):
        """Test that start_date after end_date raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            _validate_and_parse_dates('2024-12-31', '2024-01-01')
        assert "must be before or equal to" in str(exc_info.value)
        assert "2024-12-31" in str(exc_info.value)
        assert "2024-01-01" in str(exc_info.value)

    def test_malformed_date_strings(self):
        """Test that malformed date strings raise ValueError."""
        invalid_dates = [
            ('', '2024-12-31'),
            ('2024-01-01', ''),
            ('not-a-date', '2024-12-31'),
            ('2024-01-01', 'invalid'),
        ]
        for start_date, end_date in invalid_dates:
            with pytest.raises(ValueError):
                _validate_and_parse_dates(start_date, end_date)


# =============================================================================
# DATABASE CONNECTION CLEANUP TESTS
# =============================================================================

class TestDatabaseConnectionCleanup:
    """Test that database connections are properly closed on exceptions."""

    def test_ingest_universe_closes_db_on_exception(self, tmp_path):
        """Test that ingest_universe closes database even when exception occurs."""
        db_path = str(tmp_path / "test.db")

        # Create database with proper schema
        db = CryptoDatabase(db_path)
        db.close()

        # Mock the CoinMarketCapClient to raise an exception during processing
        with patch('crypto_data.ingestion.CoinMarketCapClient') as mock_client_class:
            mock_client = MagicMock()
            mock_client_class.return_value = mock_client

            # Mock fetch to raise an exception after database is opened
            async def mock_fetch(*args, **kwargs):
                raise RuntimeError("Simulated API error")

            mock_client.fetch_historical_snapshot.side_effect = mock_fetch

            # Try to ingest - should handle the exception gracefully
            result = asyncio.run(ingest_universe(
                db_path=db_path,
                months=['2024-01'],
                top_n=10,
                exclude_tags=[],
                exclude_symbols=[]
            ))

            # Verify the result has the expected structure (even if fetch failed)
            assert 'by_tag' in result
            assert 'by_symbol' in result

        # Verify database can be opened again (wasn't left locked)
        # This is the key test - if finally block didn't run, database would be locked
        db = CryptoDatabase(db_path)
        assert db.conn is not None
        db.close()


# =============================================================================
# THREAD-SAFE TICKER MAPPINGS TESTS
# =============================================================================

class TestThreadSafeTickerMappings:
    """Test that ticker mappings cache is thread-safe."""

    def setup_method(self):
        """Clear ticker mappings before each test."""
        with _ticker_mappings_lock:
            _ticker_mappings.clear()

    def teardown_method(self):
        """Clear ticker mappings after each test."""
        with _ticker_mappings_lock:
            _ticker_mappings.clear()

    def test_concurrent_reads(self):
        """Test that concurrent reads from ticker mappings don't cause issues."""
        # Pre-populate the cache
        with _ticker_mappings_lock:
            _ticker_mappings['PEPE'] = '1000PEPE'
            _ticker_mappings['SHIB'] = '1000SHIB'

        results = []
        errors = []

        def read_mapping(symbol):
            try:
                with _ticker_mappings_lock:
                    value = _ticker_mappings.get(symbol, symbol)
                results.append(value)
            except Exception as e:
                errors.append(e)

        # Create multiple threads reading concurrently
        threads = []
        for _ in range(100):
            t1 = threading.Thread(target=read_mapping, args=('PEPE',))
            t2 = threading.Thread(target=read_mapping, args=('SHIB',))
            threads.extend([t1, t2])

        # Start all threads
        for t in threads:
            t.start()

        # Wait for completion
        for t in threads:
            t.join()

        # Verify no errors occurred
        assert len(errors) == 0, f"Errors occurred: {errors}"
        assert len(results) == 200  # 100 threads x 2 symbols

    def test_concurrent_writes(self):
        """Test that concurrent writes to ticker mappings are safe."""
        errors = []

        def write_mapping(symbol, value):
            try:
                with _ticker_mappings_lock:
                    _ticker_mappings[symbol] = value
            except Exception as e:
                errors.append(e)

        # Create multiple threads writing concurrently
        threads = []
        for i in range(50):
            t = threading.Thread(target=write_mapping, args=(f'SYMBOL{i}', f'1000SYMBOL{i}'))
            threads.append(t)

        # Start all threads
        for t in threads:
            t.start()

        # Wait for completion
        for t in threads:
            t.join()

        # Verify no errors and all writes succeeded
        assert len(errors) == 0, f"Errors occurred: {errors}"
        with _ticker_mappings_lock:
            assert len(_ticker_mappings) == 50

    def test_concurrent_read_write(self):
        """Test concurrent reads and writes don't cause race conditions."""
        with _ticker_mappings_lock:
            _ticker_mappings['EXISTING'] = '1000EXISTING'

        errors = []
        read_results = []

        def read_mapping():
            try:
                with _ticker_mappings_lock:
                    value = _ticker_mappings.get('EXISTING', 'DEFAULT')
                read_results.append(value)
            except Exception as e:
                errors.append(e)

        def write_mapping(symbol, value):
            try:
                with _ticker_mappings_lock:
                    _ticker_mappings[symbol] = value
            except Exception as e:
                errors.append(e)

        # Mix of read and write threads
        threads = []
        for i in range(25):
            t1 = threading.Thread(target=read_mapping)
            t2 = threading.Thread(target=write_mapping, args=(f'NEW{i}', f'1000NEW{i}'))
            threads.extend([t1, t2])

        # Start all threads
        for t in threads:
            t.start()

        # Wait for completion
        for t in threads:
            t.join()

        # Verify no errors
        assert len(errors) == 0, f"Errors occurred: {errors}"

        # Verify reads got consistent results
        assert len(read_results) == 25
        assert all(r == '1000EXISTING' for r in read_results)

        # Verify writes succeeded
        with _ticker_mappings_lock:
            assert 'EXISTING' in _ticker_mappings
            assert len(_ticker_mappings) == 26  # 1 existing + 25 new


# =============================================================================
# INTEGRATION TESTS WITH DATE VALIDATION
# =============================================================================

class TestDateValidationIntegration:
    """Test that date validation is properly integrated into main functions."""

    def test_ingest_binance_async_validates_dates(self, tmp_path):
        """Test that ingest_binance_async validates dates."""
        db_path = str(tmp_path / "test.db")
        db = CryptoDatabase(db_path)
        db.close()

        # Invalid date format
        with pytest.raises(ValueError) as exc_info:
            ingest_binance_async(
                db_path=db_path,
                symbols=['BTCUSDT'],
                start_date='2024/01/01',  # Invalid format
                end_date='2024-12-31',
                interval=Interval.MIN_5,
                data_types=[DataType.SPOT]
            )
        assert "Invalid start_date format" in str(exc_info.value)

        # Start after end
        with pytest.raises(ValueError) as exc_info:
            ingest_binance_async(
                db_path=db_path,
                symbols=['BTCUSDT'],
                start_date='2024-12-31',
                end_date='2024-01-01',
                interval=Interval.MIN_5,
                data_types=[DataType.SPOT]
            )
        assert "must be before or equal to" in str(exc_info.value)

    def test_populate_database_validates_dates(self, tmp_path):
        """Test that populate_database function validates dates."""
        db_path = str(tmp_path / "test.db")

        # Invalid date format
        with pytest.raises(ValueError) as exc_info:
            populate_database(
                db_path=db_path,
                start_date='invalid-date',
                end_date='2024-12-31',
                top_n=10
            )
        assert "Invalid start_date format" in str(exc_info.value)

        # Start after end
        with pytest.raises(ValueError) as exc_info:
            populate_database(
                db_path=db_path,
                start_date='2024-12-31',
                end_date='2024-01-01',
                top_n=10
            )
        assert "must be before or equal to" in str(exc_info.value)


# =============================================================================
# ADDITIONAL ERROR HANDLING TESTS (Coverage Improvements)
# =============================================================================

class TestDownloadFailureLogging:
    """Test error logging for download failures (improves coverage lines 106-107, 165-166)."""

    def test_metrics_download_failure_logged(self):
        """Test that metrics download failures are logged correctly."""
        from crypto_data.ingestion import _process_metrics_results, initialize_ingestion_stats

        results = [
            {
                'success': False,
                'symbol': 'BTCUSDT',
                'data_type': 'open_interest',
                'date': '2024-01-01',
                'file_path': None,
                'error': 'Connection timeout'  # Non-404 error
            }
        ]

        mock_conn = MagicMock()
        stats = initialize_ingestion_stats()

        # This should hit the error logging path (lines 106-107)
        _process_metrics_results(results, mock_conn, stats, 'BTCUSDT')

        # Verify stats updated for failed download
        assert stats['failed'] == 1
        assert stats['downloaded'] == 0
        assert stats['not_found'] == 0

    def test_funding_rates_download_failure_logged(self):
        """Test that funding rates download failures are logged correctly."""
        from crypto_data.ingestion import _process_funding_rates_results, initialize_ingestion_stats

        results = [
            {
                'success': False,
                'symbol': 'BTCUSDT',
                'data_type': 'funding_rates',
                'month': '2024-01',
                'file_path': None,
                'error': 'Network error'  # Non-404 error
            }
        ]

        mock_conn = MagicMock()
        stats = initialize_ingestion_stats()

        # This should hit the error logging path (lines 165-166)
        _process_funding_rates_results(results, mock_conn, stats, 'BTCUSDT')

        # Verify stats updated for failed download
        assert stats['failed'] == 1
        assert stats['downloaded'] == 0
        assert stats['not_found'] == 0

    def test_mixed_results_logging(self):
        """Test logging when there are mixed success/failure results."""
        from crypto_data.ingestion import _process_metrics_results, initialize_ingestion_stats
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as tmpdir:
            temp_file = Path(tmpdir) / "test.zip"
            temp_file.touch()

            results = [
                {
                    'success': True,
                    'symbol': 'BTCUSDT',
                    'data_type': 'open_interest',
                    'date': '2024-01-01',
                    'file_path': temp_file,
                    'error': None
                },
                {
                    'success': False,
                    'symbol': 'BTCUSDT',
                    'data_type': 'open_interest',
                    'date': '2024-01-02',
                    'file_path': None,
                    'error': 'not_found'
                },
                {
                    'success': False,
                    'symbol': 'BTCUSDT',
                    'data_type': 'open_interest',
                    'date': '2024-01-03',
                    'file_path': None,
                    'error': 'Connection error'
                }
            ]

            mock_conn = MagicMock()
            stats = initialize_ingestion_stats()

            with patch('crypto_data.ingestion.import_metrics_to_duckdb'):
                _process_metrics_results(results, mock_conn, stats, 'BTCUSDT')

            # Verify all error types counted correctly
            assert stats['downloaded'] == 1
            assert stats['not_found'] == 1
            assert stats['failed'] == 1
