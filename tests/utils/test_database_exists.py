"""
Tests for data_exists function in utils/database.py

Tests the month completion logic that determines whether to skip re-downloading.
"""

import pytest
import duckdb
from datetime import datetime
from crypto_data.utils.database import data_exists


@pytest.fixture
def temp_db(tmp_path):
    """Create a temporary database with spot and futures tables."""
    db_path = tmp_path / "test_exists.db"
    conn = duckdb.connect(str(db_path))

    # Create tables
    conn.execute("""
        CREATE TABLE binance_spot (
            symbol VARCHAR,
            interval VARCHAR,
            timestamp TIMESTAMP,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume DOUBLE,
            quote_volume DOUBLE,
            trades_count INTEGER,
            taker_buy_base_volume DOUBLE,
            taker_buy_quote_volume DOUBLE,
            PRIMARY KEY (symbol, interval, timestamp)
        )
    """)

    conn.execute("""
        CREATE TABLE binance_futures (
            symbol VARCHAR,
            interval VARCHAR,
            timestamp TIMESTAMP,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume DOUBLE,
            quote_volume DOUBLE,
            trades_count INTEGER,
            taker_buy_base_volume DOUBLE,
            taker_buy_quote_volume DOUBLE,
            PRIMARY KEY (symbol, interval, timestamp)
        )
    """)

    yield conn
    conn.close()


def test_complete_data_returns_true(temp_db):
    """Month with complete data (max timestamp > day 24) should return True."""
    # Insert data with max timestamp on day 28 of January 2024
    temp_db.execute("""
        INSERT INTO binance_spot VALUES
        ('BTCUSDT', '5m', '2024-01-01 00:00:00', 45000, 45100, 44900, 45050, 100, 4500000, 1000, 50, 2250000),
        ('BTCUSDT', '5m', '2024-01-15 12:00:00', 46000, 46100, 45900, 46050, 100, 4600000, 1000, 50, 2300000),
        ('BTCUSDT', '5m', '2024-01-28 23:55:00', 47000, 47100, 46900, 47050, 100, 4700000, 1000, 50, 2350000)
    """)

    result = data_exists(temp_db, 'BTCUSDT', '2024-01', 'spot', '5m')
    assert result is True


def test_partial_data_returns_false(temp_db):
    """Month with partial data (max timestamp < day 24) should return False."""
    # Insert data with max timestamp on day 15 of January 2024
    temp_db.execute("""
        INSERT INTO binance_spot VALUES
        ('BTCUSDT', '5m', '2024-01-01 00:00:00', 45000, 45100, 44900, 45050, 100, 4500000, 1000, 50, 2250000),
        ('BTCUSDT', '5m', '2024-01-10 12:00:00', 46000, 46100, 45900, 46050, 100, 4600000, 1000, 50, 2300000),
        ('BTCUSDT', '5m', '2024-01-15 23:55:00', 47000, 47100, 46900, 47050, 100, 4700000, 1000, 50, 2350000)
    """)

    result = data_exists(temp_db, 'BTCUSDT', '2024-01', 'spot', '5m')
    assert result is False


def test_no_data_returns_false(temp_db):
    """Month with no data should return False."""
    result = data_exists(temp_db, 'BTCUSDT', '2024-01', 'spot', '5m')
    assert result is False


def test_exactly_day_24_returns_true(temp_db):
    """Month with max timestamp exactly on day 24 should return True (boundary test)."""
    # Insert data with max timestamp exactly on day 24 at 00:00:00
    temp_db.execute("""
        INSERT INTO binance_spot VALUES
        ('BTCUSDT', '5m', '2024-01-01 00:00:00', 45000, 45100, 44900, 45050, 100, 4500000, 1000, 50, 2250000),
        ('BTCUSDT', '5m', '2024-01-24 00:00:00', 47000, 47100, 46900, 47050, 100, 4700000, 1000, 50, 2350000)
    """)

    result = data_exists(temp_db, 'BTCUSDT', '2024-01', 'spot', '5m')
    assert result is True


def test_december_month_boundary(temp_db):
    """December should correctly roll over to next year for end date."""
    # Insert data for December 2024 with max timestamp on day 28
    temp_db.execute("""
        INSERT INTO binance_spot VALUES
        ('BTCUSDT', '5m', '2024-12-01 00:00:00', 45000, 45100, 44900, 45050, 100, 4500000, 1000, 50, 2250000),
        ('BTCUSDT', '5m', '2024-12-28 23:55:00', 47000, 47100, 46900, 47050, 100, 4700000, 1000, 50, 2350000)
    """)

    result = data_exists(temp_db, 'BTCUSDT', '2024-12', 'spot', '5m')
    assert result is True


def test_january_data_not_counted_in_december(temp_db):
    """January data should not be counted when checking December."""
    # Insert data in December (partial) and January (complete)
    temp_db.execute("""
        INSERT INTO binance_spot VALUES
        ('BTCUSDT', '5m', '2024-12-01 00:00:00', 45000, 45100, 44900, 45050, 100, 4500000, 1000, 50, 2250000),
        ('BTCUSDT', '5m', '2024-12-15 12:00:00', 46000, 46100, 45900, 46050, 100, 4600000, 1000, 50, 2300000),
        ('BTCUSDT', '5m', '2025-01-28 23:55:00', 47000, 47100, 46900, 47050, 100, 4700000, 1000, 50, 2350000)
    """)

    # December check should return False (max is day 15)
    result = data_exists(temp_db, 'BTCUSDT', '2024-12', 'spot', '5m')
    assert result is False


def test_different_intervals_are_separate(temp_db):
    """Data for one interval should not affect another interval check."""
    # Insert complete data for 5m interval
    temp_db.execute("""
        INSERT INTO binance_spot VALUES
        ('BTCUSDT', '5m', '2024-01-28 00:00:00', 45000, 45100, 44900, 45050, 100, 4500000, 1000, 50, 2250000)
    """)

    # Insert partial data for 1h interval
    temp_db.execute("""
        INSERT INTO binance_spot VALUES
        ('BTCUSDT', '1h', '2024-01-15 00:00:00', 45000, 45100, 44900, 45050, 100, 4500000, 1000, 50, 2250000)
    """)

    # 5m should exist (complete)
    assert data_exists(temp_db, 'BTCUSDT', '2024-01', 'spot', '5m') is True

    # 1h should not exist (partial)
    assert data_exists(temp_db, 'BTCUSDT', '2024-01', 'spot', '1h') is False


def test_different_data_types_are_separate(temp_db):
    """Spot and futures data should be checked separately."""
    # Insert complete data for spot
    temp_db.execute("""
        INSERT INTO binance_spot VALUES
        ('BTCUSDT', '5m', '2024-01-28 00:00:00', 45000, 45100, 44900, 45050, 100, 4500000, 1000, 50, 2250000)
    """)

    # Insert partial data for futures
    temp_db.execute("""
        INSERT INTO binance_futures VALUES
        ('BTCUSDT', '5m', '2024-01-15 00:00:00', 45000, 45100, 44900, 45050, 100, 4500000, 1000, 50, 2250000)
    """)

    # Spot should exist (complete)
    assert data_exists(temp_db, 'BTCUSDT', '2024-01', 'spot', '5m') is True

    # Futures should not exist (partial)
    assert data_exists(temp_db, 'BTCUSDT', '2024-01', 'futures', '5m') is False


def test_different_symbols_are_separate(temp_db):
    """Data for one symbol should not affect another symbol check."""
    # Insert complete data for BTCUSDT
    temp_db.execute("""
        INSERT INTO binance_spot VALUES
        ('BTCUSDT', '5m', '2024-01-28 00:00:00', 45000, 45100, 44900, 45050, 100, 4500000, 1000, 50, 2250000)
    """)

    # Check ETHUSDT (no data)
    assert data_exists(temp_db, 'ETHUSDT', '2024-01', 'spot', '5m') is False


def test_year_boundary_forward(temp_db):
    """January 2025 should correctly parse month boundaries."""
    # Insert data for January 2025 with max timestamp on day 28
    temp_db.execute("""
        INSERT INTO binance_spot VALUES
        ('BTCUSDT', '5m', '2025-01-01 00:00:00', 45000, 45100, 44900, 45050, 100, 4500000, 1000, 50, 2250000),
        ('BTCUSDT', '5m', '2025-01-28 23:55:00', 47000, 47100, 46900, 47050, 100, 4700000, 1000, 50, 2350000)
    """)

    result = data_exists(temp_db, 'BTCUSDT', '2025-01', 'spot', '5m')
    assert result is True


def test_last_day_of_month_is_complete(temp_db):
    """Data on last day of month (31st) should be considered complete."""
    # Insert data for January with max timestamp on day 31
    temp_db.execute("""
        INSERT INTO binance_spot VALUES
        ('BTCUSDT', '5m', '2024-01-31 23:55:00', 47000, 47100, 46900, 47050, 100, 4700000, 1000, 50, 2350000)
    """)

    result = data_exists(temp_db, 'BTCUSDT', '2024-01', 'spot', '5m')
    assert result is True


def test_february_short_month(temp_db):
    """February (28/29 days) should work correctly with day 24 threshold."""
    # Insert data for February 2024 (leap year) with max timestamp on day 27
    temp_db.execute("""
        INSERT INTO binance_spot VALUES
        ('BTCUSDT', '5m', '2024-02-01 00:00:00', 45000, 45100, 44900, 45050, 100, 4500000, 1000, 50, 2250000),
        ('BTCUSDT', '5m', '2024-02-27 23:55:00', 47000, 47100, 46900, 47050, 100, 4700000, 1000, 50, 2350000)
    """)

    result = data_exists(temp_db, 'BTCUSDT', '2024-02', 'spot', '5m')
    assert result is True
