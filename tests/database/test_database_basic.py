"""
Basic smoke tests for DuckDB database functionality.
"""

import pytest
import tempfile
from pathlib import Path

from crypto_data import CryptoDatabase


def test_database_creation():
    """Test that database can be created and tables exist."""
    with tempfile.NamedTemporaryFile(suffix='.db', delete=True) as f:
        db_path = f.name
    # File is deleted when context exits, leaving just the path

    try:
        # Create database
        db = CryptoDatabase(db_path)

        # Verify tables exist
        tables = db.execute("SHOW TABLES").fetchall()
        table_names = [row[0] for row in tables]

        assert 'binance_spot' in table_names
        assert 'binance_futures' in table_names
        assert 'crypto_universe' in table_names

        # Verify binance_spot schema
        spot_schema = db.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'binance_spot'
        """).fetchall()

        spot_columns = [row[0] for row in spot_schema]

        assert 'symbol' in spot_columns
        assert 'timestamp' in spot_columns
        assert 'open' in spot_columns
        assert 'high' in spot_columns
        assert 'low' in spot_columns
        assert 'close' in spot_columns
        assert 'volume' in spot_columns

        # Verify crypto_universe schema
        universe_schema = db.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'crypto_universe'
        """).fetchall()

        universe_columns = [row[0] for row in universe_schema]

        assert 'date' in universe_columns
        assert 'symbol' in universe_columns
        assert 'rank' in universe_columns
        assert 'market_cap' in universe_columns
        assert 'categories' in universe_columns

        db.close()

    finally:
        # Cleanup
        if Path(db_path).exists():
            Path(db_path).unlink()


def test_database_stats_empty():
    """Test that stats work on empty database."""
    with tempfile.NamedTemporaryFile(suffix='.db', delete=True) as f:
        db_path = f.name
    # File is deleted when context exits, leaving just the path

    try:
        db = CryptoDatabase(db_path)
        stats = db.get_table_stats()

        # Should have entries for all tables (v3.0.0: universe is in DuckDB)
        assert 'binance_spot' in stats
        assert 'binance_futures' in stats
        assert 'crypto_universe' in stats

        # All should be empty
        assert stats['binance_spot']['records'] == 0
        assert stats['binance_futures']['records'] == 0
        assert stats['crypto_universe']['records'] == 0

        db.close()

    finally:
        if Path(db_path).exists():
            Path(db_path).unlink()


def test_database_context_manager():
    """Test database can be used as context manager."""
    with tempfile.NamedTemporaryFile(suffix='.db', delete=True) as f:
        db_path = f.name
    # File is deleted when context exits, leaving just the path

    try:
        with CryptoDatabase(db_path) as db:
            tables = db.execute("SHOW TABLES").fetchall()
            assert len(tables) >= 3  # binance_spot, binance_futures, crypto_universe

    finally:
        if Path(db_path).exists():
            Path(db_path).unlink()


def test_get_table_stats_with_spot_data():
    """Test get_table_stats() with actual OHLCV spot data."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / 'test.db'

        try:
            db = CryptoDatabase(str(db_path))

            # Insert test data into binance_spot
            db.conn.execute("""
                INSERT INTO binance_spot
                (symbol, interval, timestamp, open, high, low, close, volume,
                 quote_volume, trades_count, taker_buy_base_volume, taker_buy_quote_volume)
                VALUES
                    ('BTCUSDT', '5m', '2024-01-01 00:00:00', 42000, 42100, 41900, 42050, 10.5,
                     441000, 150, 5.2, 218400),
                    ('BTCUSDT', '5m', '2024-01-01 00:05:00', 42050, 42200, 42000, 42150, 12.3,
                     518000, 180, 6.1, 257000),
                    ('ETHUSDT', '5m', '2024-01-01 00:00:00', 2200, 2210, 2195, 2205, 50.2,
                     110500, 220, 25.1, 55300)
            """)

            # Get stats
            stats = db.get_table_stats()

            # Verify spot stats
            assert 'binance_spot' in stats
            spot_stats = stats['binance_spot']
            assert spot_stats['records'] == 3
            assert spot_stats['symbols'] == 2  # BTC and ETH
            assert spot_stats['min_date'] is not None
            assert spot_stats['max_date'] is not None

            db.close()

        finally:
            if db_path.exists():
                db_path.unlink()


def test_get_table_stats_with_universe_data():
    """Test get_table_stats() with crypto_universe data."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / 'test.db'

        try:
            db = CryptoDatabase(str(db_path))

            # Insert test data into crypto_universe
            db.conn.execute("""
                INSERT INTO crypto_universe
                (date, symbol, rank, market_cap, categories)
                VALUES
                    ('2024-01-01', 'BTC', 1, 800000000000, NULL),
                    ('2024-01-01', 'ETH', 2, 400000000000, NULL),
                    ('2024-02-01', 'BTC', 1, 850000000000, NULL),
                    ('2024-02-01', 'ETH', 2, 420000000000, NULL)
            """)

            # Get stats
            stats = db.get_table_stats()

            # Verify universe stats
            assert 'crypto_universe' in stats
            universe_stats = stats['crypto_universe']
            assert universe_stats['records'] == 4
            assert universe_stats['symbols'] == 2  # BTC and ETH
            assert universe_stats['min_date'] is not None
            assert universe_stats['max_date'] is not None

            db.close()

        finally:
            if db_path.exists():
                db_path.unlink()


def test_execute_query_with_results():
    """Test execute() method with queries that return results."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / 'test.db'

        try:
            db = CryptoDatabase(str(db_path))

            # Insert test data
            db.conn.execute("""
                INSERT INTO crypto_universe
                (date, symbol, rank, market_cap, categories)
                VALUES
                    ('2024-01-01', 'BTC', 1, 800000000000, NULL),
                    ('2024-01-01', 'ETH', 2, 400000000000, NULL)
            """)

            # Query using execute() method
            result = db.execute("""
                SELECT symbol, rank, market_cap
                FROM crypto_universe
                WHERE date = '2024-01-01'
                ORDER BY rank
            """).fetchall()

            # Verify results
            assert len(result) == 2
            assert result[0][0] == 'BTC'  # First symbol
            assert result[0][1] == 1  # Rank
            assert result[1][0] == 'ETH'  # Second symbol
            assert result[1][1] == 2  # Rank

            db.close()

        finally:
            if db_path.exists():
                db_path.unlink()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
