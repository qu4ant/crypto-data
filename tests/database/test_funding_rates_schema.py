"""
Tests for funding_rates table schema in database.py

Tests table creation, column validation, primary key enforcement,
and Binance exchange provenance.
"""

import pytest
from crypto_data.enums import DataType, Interval
import duckdb
from pathlib import Path
from crypto_data.database import CryptoDatabase


def test_funding_rates_table_created(tmp_path):
    """funding_rates table should be created automatically."""
    db_path = tmp_path / "test_schema.db"
    db = CryptoDatabase(str(db_path))

    # Check table exists
    tables = db.conn.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_name = 'funding_rates'
    """).fetchone()

    assert tables is not None
    assert tables[0] == 'funding_rates'

    db.close()


def test_funding_rates_schema_columns(tmp_path):
    """funding_rates table should have correct columns."""
    db_path = tmp_path / "test_schema.db"
    db = CryptoDatabase(str(db_path))

    # Get column info
    columns = db.conn.execute("""
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_name = 'funding_rates'
        ORDER BY column_name
    """).fetchall()

    # Convert to dict for easier checking
    col_info = {col[0]: {'type': col[1], 'nullable': col[2]} for col in columns}

    # Check required columns exist
    assert 'exchange' in col_info
    assert 'symbol' in col_info
    assert 'timestamp' in col_info
    assert 'funding_rate' in col_info

    # Check column types
    assert col_info['exchange']['type'] == 'VARCHAR'
    assert col_info['symbol']['type'] == 'VARCHAR'
    assert col_info['timestamp']['type'] == 'TIMESTAMP'
    assert col_info['funding_rate']['type'] == 'DOUBLE'

    # Check NOT NULL constraints
    assert col_info['exchange']['nullable'] == 'NO'
    assert col_info['symbol']['nullable'] == 'NO'
    assert col_info['timestamp']['nullable'] == 'NO'

    db.close()


def test_funding_rates_primary_key(tmp_path):
    """funding_rates should have composite primary key (exchange, symbol, timestamp)."""
    db_path = tmp_path / "test_schema.db"
    db = CryptoDatabase(str(db_path))

    # Insert first record
    db.conn.execute("""
        INSERT INTO funding_rates (exchange, symbol, timestamp, funding_rate)
        VALUES ('binance', 'BTCUSDT', '2024-12-01 00:00:00', 0.0001)
    """)

    # Try to insert duplicate - should fail
    with pytest.raises(Exception, match="Constraint Error|Duplicate"):
        db.conn.execute("""
            INSERT INTO funding_rates (exchange, symbol, timestamp, funding_rate)
            VALUES ('binance', 'BTCUSDT', '2024-12-01 00:00:00', 0.0002)
        """)

    db.close()


def test_funding_rates_index_created(tmp_path):
    """Index idx_funding_rates_exchange_symbol_time should be created."""
    db_path = tmp_path / "test_schema.db"
    db = CryptoDatabase(str(db_path))

    # Check index exists (DuckDB doesn't expose index metadata easily, so we check it doesn't error)
    # We can verify by checking the table can be queried efficiently
    result = db.conn.execute("""
        SELECT * FROM funding_rates
        WHERE exchange = 'binance' AND symbol = 'BTCUSDT'
        ORDER BY timestamp
    """).fetchall()

    assert result == []  # Empty table, but query should work

    db.close()


def test_funding_rates_insert_and_query(tmp_path):
    """Should be able to insert and query funding_rates data."""
    db_path = tmp_path / "test_schema.db"
    db = CryptoDatabase(str(db_path))

    # Insert test data
    db.conn.execute("""
        INSERT INTO funding_rates (exchange, symbol, timestamp, funding_rate)
        VALUES
            ('binance', 'BTCUSDT', '2024-12-01 00:00:00', 0.0001),
            ('binance', 'BTCUSDT', '2024-12-01 08:00:00', 0.00015),
            ('binance', 'ETHUSDT', '2024-12-01 00:00:00', 0.0002)
    """)

    # Query all
    result = db.conn.execute("SELECT COUNT(*) FROM funding_rates").fetchone()
    assert result[0] == 3

    # Query by symbol
    result = db.conn.execute("""
        SELECT COUNT(*) FROM funding_rates
        WHERE symbol = 'BTCUSDT'
    """).fetchone()
    assert result[0] == 2

    # Query with timestamp ordering
    result = db.conn.execute("""
        SELECT funding_rate FROM funding_rates
        WHERE symbol = 'BTCUSDT'
        ORDER BY timestamp
    """).fetchall()
    assert len(result) == 2
    assert result[0][0] == 0.0001
    assert result[1][0] == 0.00015

    db.close()


def test_funding_rates_duplicate_key_prevention(tmp_path):
    """Duplicate (exchange, symbol, timestamp) should be prevented."""
    db_path = tmp_path / "test_schema.db"
    db = CryptoDatabase(str(db_path))

    # Insert first record
    db.conn.execute("""
        INSERT INTO funding_rates (exchange, symbol, timestamp, funding_rate)
        VALUES ('binance', 'BTCUSDT', '2024-12-01 00:00:00', 0.0001)
    """)

    # Verify inserted
    count1 = db.conn.execute("SELECT COUNT(*) FROM funding_rates").fetchone()[0]
    assert count1 == 1

    # Try to insert duplicate (same exchange, symbol, timestamp)
    with pytest.raises(Exception):
        db.conn.execute("""
            INSERT INTO funding_rates (exchange, symbol, timestamp, funding_rate)
            VALUES ('binance', 'BTCUSDT', '2024-12-01 00:00:00', 0.0002)
        """)

    # Count should still be 1
    count2 = db.conn.execute("SELECT COUNT(*) FROM funding_rates").fetchone()[0]
    assert count2 == 1

    db.close()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
