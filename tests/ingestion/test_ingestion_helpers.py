"""
Tests for ingestion_helpers module.

Tests the shared helper functions used by both universe and binance ingestion:
- initialize_ingestion_stats()
- process_download_results()
- query_data_availability()
- log_ingestion_summary()
"""

import pytest
import tempfile
from pathlib import Path
from datetime import date
from unittest.mock import MagicMock, patch, call

from crypto_data.utils.ingestion_helpers import (
    initialize_ingestion_stats,
    process_download_results,
    query_data_availability,
    log_ingestion_summary
)


# =============================================================================
# Tests for initialize_ingestion_stats()
# =============================================================================

def test_initialize_ingestion_stats_returns_correct_structure():
    """Test that initialize_ingestion_stats returns dict with correct keys."""
    stats = initialize_ingestion_stats()

    assert isinstance(stats, dict)
    assert set(stats.keys()) == {'downloaded', 'skipped', 'failed', 'not_found'}
    assert all(isinstance(v, int) for v in stats.values())


def test_initialize_ingestion_stats_all_zeros():
    """Test that all stats are initialized to zero."""
    stats = initialize_ingestion_stats()

    assert stats['downloaded'] == 0
    assert stats['skipped'] == 0
    assert stats['failed'] == 0
    assert stats['not_found'] == 0


def test_initialize_ingestion_stats_is_mutable():
    """Test that returned dict can be modified (for stats tracking)."""
    stats = initialize_ingestion_stats()

    stats['downloaded'] += 5
    stats['failed'] += 2

    assert stats['downloaded'] == 5
    assert stats['failed'] == 2


# =============================================================================
# Tests for process_download_results()
# =============================================================================

def test_process_download_results_successful_import():
    """Test processing successful download results."""
    with tempfile.TemporaryDirectory() as tmpdir:
        temp_file = Path(tmpdir) / "test.zip"
        temp_file.touch()

        results = [
            {
                'success': True,
                'symbol': 'BTCUSDT',
                'data_type': 'spot',
                'month': '2024-01',
                'file_path': temp_file,
                'error': None
            }
        ]

        mock_conn = MagicMock()
        stats = initialize_ingestion_stats()

        with patch('crypto_data.utils.ingestion_helpers.import_to_duckdb') as mock_import:
            process_download_results(results, mock_conn, stats, '5m', 'BTCUSDT')

            # Verify import was called
            mock_import.assert_called_once_with(
                conn=mock_conn,
                file_path=temp_file,
                symbol='BTCUSDT',
                data_type='spot',
                interval='5m',
                original_symbol='BTCUSDT'
            )

            # Verify stats updated
            assert stats['downloaded'] == 1
            assert stats['failed'] == 0

            # Verify temp file deleted
            assert not temp_file.exists()


def test_process_download_results_handles_not_found():
    """Test processing results with not_found errors."""
    results = [
        {
            'success': False,
            'symbol': 'BTCUSDT',
            'data_type': 'spot',
            'month': '2024-01',
            'file_path': None,
            'error': 'not_found'
        }
    ]

    mock_conn = MagicMock()
    stats = initialize_ingestion_stats()

    with patch('crypto_data.utils.ingestion_helpers.import_to_duckdb') as mock_import:
        process_download_results(results, mock_conn, stats, '5m', 'BTCUSDT')

        # Verify import NOT called
        mock_import.assert_not_called()

        # Verify stats updated
        assert stats['not_found'] == 1
        assert stats['downloaded'] == 0
        assert stats['failed'] == 0


def test_process_download_results_handles_other_errors():
    """Test processing results with generic errors."""
    results = [
        {
            'success': False,
            'symbol': 'BTCUSDT',
            'data_type': 'spot',
            'month': '2024-01',
            'file_path': None,
            'error': 'Network timeout'
        }
    ]

    mock_conn = MagicMock()
    stats = initialize_ingestion_stats()

    with patch('crypto_data.utils.ingestion_helpers.import_to_duckdb') as mock_import:
        process_download_results(results, mock_conn, stats, '5m', 'BTCUSDT')

        # Verify import NOT called
        mock_import.assert_not_called()

        # Verify stats updated
        assert stats['failed'] == 1
        assert stats['not_found'] == 0
        assert stats['downloaded'] == 0


def test_process_download_results_import_failure():
    """Test that import failures are counted as failed."""
    with tempfile.TemporaryDirectory() as tmpdir:
        temp_file = Path(tmpdir) / "test.zip"
        temp_file.touch()

        results = [
            {
                'success': True,
                'symbol': 'BTCUSDT',
                'data_type': 'spot',
                'month': '2024-01',
                'file_path': temp_file,
                'error': None
            }
        ]

        mock_conn = MagicMock()
        stats = initialize_ingestion_stats()

        with patch('crypto_data.utils.ingestion_helpers.import_to_duckdb') as mock_import:
            mock_import.side_effect = Exception("Import failed")

            process_download_results(results, mock_conn, stats, '5m', 'BTCUSDT')

            # Verify stats updated
            assert stats['failed'] == 1
            assert stats['downloaded'] == 0

            # Note: temp file is NOT deleted on import failure (happens in success block)
            # This is by design - failed imports don't clean up files for debugging
            assert temp_file.exists()


def test_process_download_results_multiple_results():
    """Test processing mixed success/failure results."""
    with tempfile.TemporaryDirectory() as tmpdir:
        temp_file1 = Path(tmpdir) / "test1.zip"
        temp_file2 = Path(tmpdir) / "test2.zip"
        temp_file1.touch()
        temp_file2.touch()

        results = [
            {
                'success': True,
                'symbol': 'BTCUSDT',
                'data_type': 'spot',
                'month': '2024-01',
                'file_path': temp_file1,
                'error': None
            },
            {
                'success': False,
                'symbol': 'BTCUSDT',
                'data_type': 'spot',
                'month': '2024-02',
                'file_path': None,
                'error': 'not_found'
            },
            {
                'success': True,
                'symbol': 'BTCUSDT',
                'data_type': 'spot',
                'month': '2024-03',
                'file_path': temp_file2,
                'error': None
            }
        ]

        mock_conn = MagicMock()
        stats = initialize_ingestion_stats()

        with patch('crypto_data.utils.ingestion_helpers.import_to_duckdb'):
            process_download_results(results, mock_conn, stats, '5m', 'BTCUSDT')

            # Verify stats
            assert stats['downloaded'] == 2
            assert stats['not_found'] == 1
            assert stats['failed'] == 0


def test_process_download_results_uses_original_symbol():
    """Test that original_symbol is passed to import (for 1000-prefix normalization)."""
    with tempfile.TemporaryDirectory() as tmpdir:
        temp_file = Path(tmpdir) / "test.zip"
        temp_file.touch()

        results = [
            {
                'success': True,
                'symbol': '1000PEPEUSDT',  # Download symbol
                'data_type': 'futures',
                'month': '2024-01',
                'file_path': temp_file,
                'error': None
            }
        ]

        mock_conn = MagicMock()
        stats = initialize_ingestion_stats()

        with patch('crypto_data.utils.ingestion_helpers.import_to_duckdb') as mock_import:
            process_download_results(results, mock_conn, stats, '5m', 'PEPEUSDT')  # Original symbol

            # Verify original_symbol passed correctly
            mock_import.assert_called_once()
            assert mock_import.call_args[1]['original_symbol'] == 'PEPEUSDT'
            assert mock_import.call_args[1]['symbol'] == '1000PEPEUSDT'


# =============================================================================
# Tests for query_data_availability()
# =============================================================================

def test_query_data_availability_returns_correct_format():
    """Test that query returns list of tuples with correct structure."""
    mock_conn = MagicMock()
    mock_conn.execute.return_value.fetchall.return_value = [
        ('BTCUSDT', 'spot', date(2024, 1, 1), date(2024, 12, 31)),
        ('BTCUSDT', 'futures', date(2024, 1, 1), date(2024, 12, 31))
    ]

    result = query_data_availability(mock_conn, ['BTCUSDT'], '5m')

    assert isinstance(result, list)
    assert len(result) == 2
    assert all(isinstance(row, tuple) for row in result)
    assert all(len(row) == 4 for row in result)


def test_query_data_availability_executes_correct_query():
    """Test that correct SQL query is executed."""
    mock_conn = MagicMock()
    mock_conn.execute.return_value.fetchall.return_value = []

    query_data_availability(mock_conn, ['BTCUSDT', 'ETHUSDT'], '5m')

    # Verify query was executed
    assert mock_conn.execute.called

    # Verify query contains expected parts
    query = mock_conn.execute.call_args[0][0]
    assert 'binance_spot' in query
    assert 'binance_futures' in query
    assert 'UNION ALL' in query
    assert 'MIN(DATE(timestamp))' in query
    assert 'MAX(DATE(timestamp))' in query


def test_query_data_availability_with_multiple_symbols():
    """Test query with multiple symbols uses correct placeholders."""
    mock_conn = MagicMock()
    mock_conn.execute.return_value.fetchall.return_value = []

    symbols = ['BTCUSDT', 'ETHUSDT', 'SOLUSDT']
    query_data_availability(mock_conn, symbols, '5m')

    # Verify placeholders: (symbols + interval) * 2 queries
    # = (3 + 1) * 2 = 8 placeholders total
    query = mock_conn.execute.call_args[0][0]
    assert query.count('?') == (len(symbols) + 1) * 2

    # Verify parameters passed correctly
    params = mock_conn.execute.call_args[0][1]
    assert params == symbols + ['5m'] + symbols + ['5m']


def test_query_data_availability_empty_result():
    """Test handling of empty result (no data)."""
    mock_conn = MagicMock()
    mock_conn.execute.return_value.fetchall.return_value = []

    result = query_data_availability(mock_conn, ['BTCUSDT'], '5m')

    assert result == []


# =============================================================================
# Tests for log_ingestion_summary()
# =============================================================================

def test_log_ingestion_summary_logs_basic_stats():
    """Test that basic stats are logged."""
    stats = {
        'downloaded': 100,
        'skipped': 20,
        'failed': 5,
        'not_found': 10
    }

    with patch('crypto_data.utils.ingestion_helpers.logger') as mock_logger:
        log_ingestion_summary(stats, 'test.db', show_availability=False)

        # Verify summary logged
        assert any('Downloaded: 100' in str(call) for call in mock_logger.info.call_args_list)
        assert any('Skipped (existing): 20' in str(call) for call in mock_logger.info.call_args_list)
        assert any('Failed: 5' in str(call) for call in mock_logger.info.call_args_list)
        assert any('Not found: 10' in str(call) for call in mock_logger.info.call_args_list)


def test_log_ingestion_summary_logs_not_found_explanation():
    """Test that 'not found' explanation is logged when not_found > 0."""
    stats = {
        'downloaded': 100,
        'skipped': 0,
        'failed': 0,
        'not_found': 10
    }

    with patch('crypto_data.utils.ingestion_helpers.logger') as mock_logger:
        log_ingestion_summary(stats, 'test.db', show_availability=False)

        # Verify explanation logged
        assert any("doesn't exist in Binance Data Vision" in str(call) for call in mock_logger.info.call_args_list)
        assert any('Symbol delisted' in str(call) for call in mock_logger.info.call_args_list)


def test_log_ingestion_summary_skips_explanation_when_zero():
    """Test that 'not found' explanation is NOT logged when not_found = 0."""
    stats = {
        'downloaded': 100,
        'skipped': 0,
        'failed': 0,
        'not_found': 0
    }

    with patch('crypto_data.utils.ingestion_helpers.logger') as mock_logger:
        log_ingestion_summary(stats, 'test.db', show_availability=False)

        # Verify explanation NOT logged
        assert not any("doesn't exist in Binance Data Vision" in str(call) for call in mock_logger.info.call_args_list)


def test_log_ingestion_summary_logs_database_size():
    """Test that database file size is logged."""
    stats = initialize_ingestion_stats()

    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmpfile:
        db_path = tmpfile.name
        tmpfile.write(b'0' * 1024 * 1024)  # 1 MB

    try:
        with patch('crypto_data.utils.ingestion_helpers.logger') as mock_logger:
            log_ingestion_summary(stats, db_path, show_availability=False)

            # Verify database size logged
            assert any('Database size:' in str(call) for call in mock_logger.info.call_args_list)
    finally:
        Path(db_path).unlink()


def test_log_ingestion_summary_with_availability():
    """Test that availability is queried when show_availability=True."""
    stats = initialize_ingestion_stats()

    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmpfile:
        db_path = tmpfile.name

    try:
        with patch('crypto_data.database.CryptoDatabase') as mock_db, \
             patch('crypto_data.utils.ingestion_helpers.query_data_availability') as mock_query, \
             patch('crypto_data.utils.ingestion_helpers.logger'):

            mock_db_instance = MagicMock()
            mock_db.return_value = mock_db_instance
            mock_query.return_value = [
                ('BTCUSDT', 'spot', date(2024, 1, 1), date(2024, 12, 31))
            ]

            log_ingestion_summary(
                stats, db_path,
                symbols=['BTCUSDT'],
                start_date='2024-01-01',
                end_date='2024-12-31',
                interval='5m',
                show_availability=True
            )

            # Verify query was called
            mock_query.assert_called_once_with(
                mock_db_instance.conn,
                ['BTCUSDT'],
                '5m'
            )
    finally:
        Path(db_path).unlink()


def test_log_ingestion_summary_skips_availability_when_false():
    """Test that availability is NOT queried when show_availability=False."""
    stats = initialize_ingestion_stats()

    with patch('crypto_data.database.CryptoDatabase') as mock_db, \
         patch('crypto_data.utils.ingestion_helpers.query_data_availability') as mock_query, \
         patch('crypto_data.utils.ingestion_helpers.logger'):

        log_ingestion_summary(stats, 'test.db', show_availability=False)

        # Verify query NOT called
        mock_query.assert_not_called()


def test_log_ingestion_summary_handles_db_error():
    """Test that database errors are handled gracefully."""
    stats = initialize_ingestion_stats()

    with patch('crypto_data.database.CryptoDatabase') as mock_db, \
         patch('crypto_data.utils.ingestion_helpers.logger') as mock_logger:

        mock_db.side_effect = Exception("Database error")

        # Should not raise
        log_ingestion_summary(
            stats, 'nonexistent.db',
            symbols=['BTCUSDT'],
            start_date='2024-01-01',
            end_date='2024-12-31',
            interval='5m',
            show_availability=True
        )

        # Verify basic stats still logged
        assert any('Downloaded:' in str(call) for call in mock_logger.info.call_args_list)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
