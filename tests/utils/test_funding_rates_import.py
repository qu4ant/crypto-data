"""
Tests for import_funding_rates_to_duckdb function in utils/database.py

Tests ZIP extraction, CSV parsing, timestamp conversion (milliseconds),
column validation, and error handling.
"""

import pytest
from crypto_data.enums import DataType, Interval
import duckdb
import zipfile
from pathlib import Path
from crypto_data.utils.database import import_funding_rates_to_duckdb


@pytest.fixture
def temp_db(tmp_path):
    """Create a temporary database with funding_rates table."""
    db_path = tmp_path / "test_funding_rates_import.db"
    conn = duckdb.connect(str(db_path))

    # Create funding_rates table
    conn.execute("""
        CREATE TABLE funding_rates (
            exchange VARCHAR NOT NULL,
            symbol VARCHAR NOT NULL,
            timestamp TIMESTAMP NOT NULL,
            funding_rate DOUBLE,
            PRIMARY KEY (exchange, symbol, timestamp)
        )
    """)

    yield conn
    conn.close()


def test_successful_funding_rates_import(tmp_path, temp_db):
    """Basic import with valid ZIP containing CSV should succeed."""
    # Create valid funding rates CSV (millisecond timestamps)
    csv_content = """calc_time,funding_interval_hours,last_funding_rate
1733011200000,8,0.00037409
1733040000000,8,0.00027213
1733068800000,8,0.00033601"""

    # Create ZIP file containing CSV
    zip_path = tmp_path / "BTCUSDT-fundingRate-2024-12.zip"
    with zipfile.ZipFile(zip_path, 'w') as zf:
        zf.writestr("BTCUSDT-fundingRate-2024-12.csv", csv_content)

    # Import
    import_funding_rates_to_duckdb(temp_db, zip_path, 'BTCUSDT', exchange='binance')

    # Verify data was imported
    result = temp_db.execute("SELECT COUNT(*) FROM funding_rates").fetchone()
    assert result[0] == 3

    # Verify data content
    row = temp_db.execute("""
        SELECT symbol, funding_rate
        FROM funding_rates
        ORDER BY timestamp
        LIMIT 1
    """).fetchone()

    assert row[0] == 'BTCUSDT'
    assert abs(row[1] - 0.00037409) < 0.0000001


def test_funding_rates_csv_validation(tmp_path, temp_db):
    """Missing required columns should raise ValueError."""
    # Create CSV with missing required column
    csv_content = """calc_time,funding_interval_hours
1733011200000,8"""

    # Create ZIP file containing invalid CSV
    zip_path = tmp_path / "invalid.zip"
    with zipfile.ZipFile(zip_path, 'w') as zf:
        zf.writestr("invalid.csv", csv_content)

    # Should raise ValueError
    with pytest.raises(ValueError, match="CSV missing required columns"):
        import_funding_rates_to_duckdb(temp_db, zip_path, 'BTCUSDT')


def test_funding_rates_timestamp_conversion(tmp_path, temp_db):
    """Millisecond timestamps should be converted to datetime."""
    csv_content = """calc_time,funding_interval_hours,last_funding_rate
1733011200000,8,0.00037409"""

    zip_path = tmp_path / "timestamp_test.zip"
    with zipfile.ZipFile(zip_path, 'w') as zf:
        zf.writestr("timestamp_test.csv", csv_content)

    # Import
    import_funding_rates_to_duckdb(temp_db, zip_path, 'BTCUSDT')

    # Verify timestamp is proper datetime
    result = temp_db.execute("""
        SELECT timestamp, TYPEOF(timestamp)
        FROM funding_rates
    """).fetchone()

    assert result[1] == 'TIMESTAMP'
    # Verify timestamp value (1733011200000 ms = 2024-12-01 00:00:00 UTC)
    assert str(result[0]) == '2024-12-01 00:00:00'


def test_funding_rates_column_renaming(tmp_path, temp_db):
    """last_funding_rate should be renamed to funding_rate."""
    csv_content = """calc_time,funding_interval_hours,last_funding_rate
1733011200000,8,0.00037409"""

    zip_path = tmp_path / "rename_test.zip"
    with zipfile.ZipFile(zip_path, 'w') as zf:
        zf.writestr("rename_test.csv", csv_content)

    # Import
    import_funding_rates_to_duckdb(temp_db, zip_path, 'BTCUSDT')

    # Verify column exists with correct name
    result = temp_db.execute("""
        SELECT funding_rate
        FROM funding_rates
    """).fetchone()

    assert abs(result[0] - 0.00037409) < 0.0000001


def test_funding_rates_duplicate_removal(tmp_path, temp_db, caplog):
    """Duplicates by (exchange, symbol, timestamp) should be removed."""
    # Create CSV with duplicate timestamps
    csv_content = """calc_time,funding_interval_hours,last_funding_rate
1733011200000,8,0.00037409
1733040000000,8,0.00027213
1733011200000,8,0.00037409"""

    zip_path = tmp_path / "duplicates.zip"
    with zipfile.ZipFile(zip_path, 'w') as zf:
        zf.writestr("duplicates.csv", csv_content)

    # Import
    with caplog.at_level('DEBUG'):
        import_funding_rates_to_duckdb(temp_db, zip_path, 'BTCUSDT')

    # Check that duplicate removal was logged
    assert any("Removed 1 duplicate" in record.message for record in caplog.records)

    # Verify only 2 rows were inserted
    result = temp_db.execute("SELECT COUNT(*) FROM funding_rates").fetchone()
    assert result[0] == 2


def test_funding_rates_duplicate_key_handling(tmp_path, temp_db, caplog):
    """Duplicate insert should be handled silently."""
    csv_content = """calc_time,funding_interval_hours,last_funding_rate
1733011200000,8,0.00037409"""

    zip_path = tmp_path / "duplicate_key.zip"
    with zipfile.ZipFile(zip_path, 'w') as zf:
        zf.writestr("duplicate_key.csv", csv_content)

    # First import
    import_funding_rates_to_duckdb(temp_db, zip_path, 'BTCUSDT')

    # Verify first import
    count1 = temp_db.execute("SELECT COUNT(*) FROM funding_rates").fetchone()[0]
    assert count1 == 1

    # Second import (same data) - should not raise, should be skipped
    with caplog.at_level('DEBUG'):
        import_funding_rates_to_duckdb(temp_db, zip_path, 'BTCUSDT')

    # Verify no duplicate was inserted
    count2 = temp_db.execute("SELECT COUNT(*) FROM funding_rates").fetchone()[0]
    assert count2 == 1

    # Check log for skip message
    assert any("Skipped duplicate data" in record.message for record in caplog.records)


def test_funding_rates_adds_exchange_column(tmp_path, temp_db):
    """Exchange='binance' should be added to each row."""
    csv_content = """calc_time,funding_interval_hours,last_funding_rate
1733011200000,8,0.00037409"""

    zip_path = tmp_path / "exchange_test.zip"
    with zipfile.ZipFile(zip_path, 'w') as zf:
        zf.writestr("exchange_test.csv", csv_content)

    # Import
    import_funding_rates_to_duckdb(temp_db, zip_path, 'BTCUSDT', exchange='binance')

    # Verify exchange column
    result = temp_db.execute("SELECT exchange FROM funding_rates").fetchone()
    assert result[0] == 'binance'


def test_funding_rates_with_different_exchange(tmp_path, temp_db):
    """Custom exchange parameter should be used."""
    csv_content = """calc_time,funding_interval_hours,last_funding_rate
1733011200000,8,0.00037409"""

    zip_path = tmp_path / "bybit_test.zip"
    with zipfile.ZipFile(zip_path, 'w') as zf:
        zf.writestr("bybit_test.csv", csv_content)

    # Import with custom exchange
    import_funding_rates_to_duckdb(temp_db, zip_path, 'BTCUSDT', exchange='bybit')

    # Verify exchange column
    result = temp_db.execute("SELECT exchange FROM funding_rates").fetchone()
    assert result[0] == 'bybit'


def test_malformed_csv_raises_exception(tmp_path, temp_db):
    """Malformed CSV should raise an exception."""
    # Create invalid CSV (missing header row)
    csv_content = """1733011200000,8,0.00037409"""

    zip_path = tmp_path / "malformed.zip"
    with zipfile.ZipFile(zip_path, 'w') as zf:
        zf.writestr("malformed.csv", csv_content)

    # Should raise ValueError
    with pytest.raises(ValueError):
        import_funding_rates_to_duckdb(temp_db, zip_path, 'BTCUSDT')


def test_import_with_transaction(tmp_path, temp_db):
    """Import within transaction should commit or rollback atomically."""
    csv_content = """calc_time,funding_interval_hours,last_funding_rate
1733011200000,8,0.00037409"""

    zip_path = tmp_path / "transaction_test.zip"
    with zipfile.ZipFile(zip_path, 'w') as zf:
        zf.writestr("transaction_test.csv", csv_content)

    # Import within transaction
    temp_db.execute("BEGIN TRANSACTION")
    import_funding_rates_to_duckdb(temp_db, zip_path, 'BTCUSDT')
    temp_db.execute("COMMIT")

    # Verify data was committed
    result = temp_db.execute("SELECT COUNT(*) FROM funding_rates").fetchone()
    assert result[0] == 1


def test_import_multiple_symbols(tmp_path, temp_db):
    """Importing different symbols should work correctly."""
    # Create CSV for BTCUSDT
    btc_content = """calc_time,funding_interval_hours,last_funding_rate
1733011200000,8,0.00037409"""

    btc_zip = tmp_path / "BTCUSDT.zip"
    with zipfile.ZipFile(btc_zip, 'w') as zf:
        zf.writestr("BTCUSDT.csv", btc_content)

    # Create CSV for ETHUSDT
    eth_content = """calc_time,funding_interval_hours,last_funding_rate
1733011200000,8,0.00025000"""

    eth_zip = tmp_path / "ETHUSDT.zip"
    with zipfile.ZipFile(eth_zip, 'w') as zf:
        zf.writestr("ETHUSDT.csv", eth_content)

    # Import both
    import_funding_rates_to_duckdb(temp_db, btc_zip, 'BTCUSDT')
    import_funding_rates_to_duckdb(temp_db, eth_zip, 'ETHUSDT')

    # Verify both symbols exist
    result = temp_db.execute("""
        SELECT symbol, COUNT(*)
        FROM funding_rates
        GROUP BY symbol
        ORDER BY symbol
    """).fetchall()

    assert len(result) == 2
    assert result[0][0] == 'BTCUSDT'
    assert result[1][0] == 'ETHUSDT'


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
