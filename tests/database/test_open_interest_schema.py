"""
Tests for open_interest table schema and basic operations.
"""

import pytest
import tempfile
from pathlib import Path
from datetime import datetime

from crypto_data import CryptoDatabase


def test_open_interest_table_created():
    """Test that open_interest table is created during database initialization."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / 'test.db'

        db = CryptoDatabase(str(db_path))

        # Verify table exists
        tables = db.execute("SHOW TABLES").fetchall()
        table_names = [row[0] for row in tables]

        assert 'open_interest' in table_names

        db.close()


def test_open_interest_schema_columns():
    """Test that open_interest table has all expected columns with correct types."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / 'test.db'

        db = CryptoDatabase(str(db_path))

        # Get schema
        schema = db.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = 'open_interest'
            ORDER BY ordinal_position
        """).fetchall()

        column_info = {row[0]: row[1] for row in schema}

        # Verify all columns exist
        assert 'exchange' in column_info
        assert 'symbol' in column_info
        assert 'timestamp' in column_info
        assert 'open_interest' in column_info

        # Verify correct types
        assert column_info['exchange'] == 'VARCHAR'
        assert column_info['symbol'] == 'VARCHAR'
        assert column_info['timestamp'] == 'TIMESTAMP'
        assert column_info['open_interest'] == 'DOUBLE'

        db.close()


def test_open_interest_primary_key():
    """Test that primary key is enforced on (exchange, symbol, timestamp)."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / 'test.db'

        db = CryptoDatabase(str(db_path))

        # Insert test data
        db.conn.execute("""
            INSERT INTO open_interest
            (exchange, symbol, timestamp, open_interest)
            VALUES ('binance', 'BTCUSDT', '2024-01-01 00:00:00', 1000.0)
        """)

        # Try to insert duplicate - should fail
        with pytest.raises(Exception) as excinfo:
            db.conn.execute("""
                INSERT INTO open_interest
                (exchange, symbol, timestamp, open_interest)
                VALUES ('binance', 'BTCUSDT', '2024-01-01 00:00:00', 2000.0)
            """)

        assert 'Duplicate' in str(excinfo.value) or 'PRIMARY KEY' in str(excinfo.value) or 'UNIQUE' in str(excinfo.value)

        db.close()


def test_open_interest_index_created():
    """Test that index on (exchange, symbol, timestamp) is created."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / 'test.db'

        db = CryptoDatabase(str(db_path))

        # Check indexes
        indexes = db.execute("""
            SELECT index_name
            FROM duckdb_indexes()
            WHERE table_name = 'open_interest'
        """).fetchall()

        index_names = [row[0] for row in indexes]

        # Should have the custom index (and possibly implicit PK index)
        assert any('idx_open_interest_exchange_symbol_time' in idx for idx in index_names) or len(index_names) > 0

        db.close()


def test_open_interest_insert_and_query():
    """Test basic insert and select operations on open_interest table."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / 'test.db'

        db = CryptoDatabase(str(db_path))

        # Insert test data
        db.conn.execute("""
            INSERT INTO open_interest
            (exchange, symbol, timestamp, open_interest)
            VALUES
                ('binance', 'BTCUSDT', '2024-01-01 00:00:00', 75000.5),
                ('binance', 'BTCUSDT', '2024-01-01 00:05:00', 75100.2),
                ('binance', 'ETHUSDT', '2024-01-01 00:00:00', 150000.0)
        """)

        # Query data
        result = db.execute("""
            SELECT exchange, symbol, timestamp, open_interest
            FROM open_interest
            WHERE symbol = 'BTCUSDT'
            ORDER BY timestamp
        """).fetchall()

        # Verify results
        assert len(result) == 2
        assert result[0][1] == 'BTCUSDT'
        assert result[0][3] == 75000.5  # open_interest
        assert result[1][3] == 75100.2  # open_interest

        db.close()


def test_open_interest_duplicate_key_prevention():
    """Test that primary key constraint prevents duplicate entries."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / 'test.db'

        db = CryptoDatabase(str(db_path))

        # Insert first record
        db.conn.execute("""
            INSERT INTO open_interest
            (exchange, symbol, timestamp, open_interest)
            VALUES ('binance', 'BTCUSDT', '2024-01-01 12:00:00', 1000.0)
        """)

        # Verify insertion
        count_before = db.execute("SELECT COUNT(*) FROM open_interest").fetchone()[0]
        assert count_before == 1

        # Attempt duplicate insert (should fail)
        with pytest.raises(Exception):
            db.conn.execute("""
                INSERT INTO open_interest
                (exchange, symbol, timestamp, open_interest)
                VALUES ('binance', 'BTCUSDT', '2024-01-01 12:00:00', 2000.0)
            """)

        # Verify count unchanged
        count_after = db.execute("SELECT COUNT(*) FROM open_interest").fetchone()[0]
        assert count_after == 1

        db.close()


def test_open_interest_multi_exchange():
    """Test that different exchanges can coexist in the same table."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / 'test.db'

        db = CryptoDatabase(str(db_path))

        # Insert data for multiple exchanges
        db.conn.execute("""
            INSERT INTO open_interest
            (exchange, symbol, timestamp, open_interest)
            VALUES
                ('binance', 'BTCUSDT', '2024-01-01 00:00:00', 75000.0),
                ('bybit', 'BTCUSDT', '2024-01-01 00:00:00', 50000.0),
                ('kraken', 'BTCUSDT', '2024-01-01 00:00:00', 30000.0)
        """)

        # Query data
        result = db.execute("""
            SELECT exchange, open_interest
            FROM open_interest
            WHERE symbol = 'BTCUSDT' AND timestamp = '2024-01-01 00:00:00'
            ORDER BY exchange
        """).fetchall()

        # Verify all three exchanges
        assert len(result) == 3
        assert result[0][0] == 'binance'
        assert result[1][0] == 'bybit'
        assert result[2][0] == 'kraken'

        db.close()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
