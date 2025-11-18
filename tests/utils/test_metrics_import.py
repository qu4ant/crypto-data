"""
Tests for import_metrics_to_duckdb function in utils/database.py

Tests ZIP extraction, CSV parsing, timestamp conversion, column validation, and error handling.
"""

import pytest
import duckdb
import zipfile
from pathlib import Path
from crypto_data.utils.database import import_metrics_to_duckdb


@pytest.fixture
def temp_db(tmp_path):
    """Create a temporary database with open_interest table."""
    db_path = tmp_path / "test_metrics_import.db"
    conn = duckdb.connect(str(db_path))

    # Create open_interest table
    conn.execute("""
        CREATE TABLE open_interest (
            exchange VARCHAR NOT NULL,
            symbol VARCHAR NOT NULL,
            timestamp TIMESTAMP NOT NULL,
            open_interest DOUBLE,
            PRIMARY KEY (exchange, symbol, timestamp)
        )
    """)

    yield conn
    conn.close()


def test_successful_metrics_import(tmp_path, temp_db):
    """Basic import with valid ZIP containing CSV should succeed."""
    # Create valid metrics CSV
    csv_content = """create_time,symbol,sum_open_interest,sum_open_interest_value,count_toptrader_long_short_ratio,sum_toptrader_long_short_ratio,count_long_short_ratio,sum_taker_long_short_vol_ratio
2025-11-02 00:05:00,BTCUSDT,76643.1910000000000000,8430743345.6809000000000000,2.05109210,1.90989100,1.87546479,0.76060100
2025-11-02 00:10:00,BTCUSDT,76650.8170000000000000,8431092392.8673270000000000,2.05208950,1.90908400,1.87749490,0.97024300
2025-11-02 00:15:00,BTCUSDT,76659.2820000000000000,8432390699.2206000000000000,2.05106087,1.90890700,1.87632740,1.26739400"""

    # Create ZIP file containing CSV
    zip_path = tmp_path / "BTCUSDT-metrics-2025-11-02.zip"
    with zipfile.ZipFile(zip_path, 'w') as zf:
        zf.writestr("BTCUSDT-metrics-2025-11-02.csv", csv_content)

    # Import
    import_metrics_to_duckdb(temp_db, zip_path, 'BTCUSDT', exchange='binance')

    # Verify data was imported
    result = temp_db.execute("SELECT COUNT(*) FROM open_interest").fetchone()
    assert result[0] == 3

    # Verify data content
    row = temp_db.execute("""
        SELECT symbol, open_interest
        FROM open_interest
        ORDER BY timestamp
        LIMIT 1
    """).fetchone()

    assert row[0] == 'BTCUSDT'
    assert abs(row[1] - 76643.191) < 0.01


def test_metrics_csv_validation(tmp_path, temp_db):
    """Missing required columns should raise ValueError."""
    # Create CSV with missing required column (missing create_time)
    csv_content = """symbol,sum_open_interest
BTCUSDT,76643.19"""

    # Create ZIP file containing invalid CSV
    zip_path = tmp_path / "invalid.zip"
    with zipfile.ZipFile(zip_path, 'w') as zf:
        zf.writestr("invalid.csv", csv_content)

    # Should raise ValueError
    with pytest.raises(ValueError, match="CSV missing required columns"):
        import_metrics_to_duckdb(temp_db, zip_path, 'BTCUSDT')


def test_metrics_timestamp_conversion(tmp_path, temp_db):
    """String timestamps should be converted to datetime."""
    csv_content = """create_time,symbol,sum_open_interest,sum_open_interest_value,count_toptrader_long_short_ratio,sum_toptrader_long_short_ratio,count_long_short_ratio,sum_taker_long_short_vol_ratio
2025-11-02 00:05:00,BTCUSDT,76643.19,8430743345.68,2.05,1.91,1.88,0.76"""

    zip_path = tmp_path / "timestamp_test.zip"
    with zipfile.ZipFile(zip_path, 'w') as zf:
        zf.writestr("timestamp_test.csv", csv_content)

    # Import
    import_metrics_to_duckdb(temp_db, zip_path, 'BTCUSDT')

    # Verify timestamp is proper datetime
    result = temp_db.execute("""
        SELECT timestamp, TYPEOF(timestamp)
        FROM open_interest
    """).fetchone()

    assert result[1] == 'TIMESTAMP'
    # Verify timestamp value
    assert str(result[0]) == '2025-11-02 00:05:00'


def test_metrics_column_renaming(tmp_path, temp_db):
    """sum_open_interest should be renamed to open_interest."""
    csv_content = """create_time,symbol,sum_open_interest,sum_open_interest_value,count_toptrader_long_short_ratio,sum_toptrader_long_short_ratio,count_long_short_ratio,sum_taker_long_short_vol_ratio
2025-11-02 00:05:00,BTCUSDT,76643.19,8430743345.68,2.05,1.91,1.88,0.76"""

    zip_path = tmp_path / "rename_test.zip"
    with zipfile.ZipFile(zip_path, 'w') as zf:
        zf.writestr("rename_test.csv", csv_content)

    # Import
    import_metrics_to_duckdb(temp_db, zip_path, 'BTCUSDT')

    # Verify column exists with correct name
    result = temp_db.execute("""
        SELECT open_interest
        FROM open_interest
    """).fetchone()

    assert result[0] == 76643.19


def test_metrics_duplicate_removal(tmp_path, temp_db, caplog):
    """Duplicates by (exchange, symbol, timestamp) should be removed."""
    # Create CSV with duplicate timestamps
    csv_content = """create_time,symbol,sum_open_interest,sum_open_interest_value,count_toptrader_long_short_ratio,sum_toptrader_long_short_ratio,count_long_short_ratio,sum_taker_long_short_vol_ratio
2025-11-02 00:05:00,BTCUSDT,76643.19,8430743345.68,2.05,1.91,1.88,0.76
2025-11-02 00:10:00,BTCUSDT,76650.82,8431092392.87,2.05,1.91,1.88,0.97
2025-11-02 00:05:00,BTCUSDT,76643.19,8430743345.68,2.05,1.91,1.88,0.76"""

    zip_path = tmp_path / "duplicates.zip"
    with zipfile.ZipFile(zip_path, 'w') as zf:
        zf.writestr("duplicates.csv", csv_content)

    # Import
    with caplog.at_level('DEBUG'):
        import_metrics_to_duckdb(temp_db, zip_path, 'BTCUSDT')

    # Check that duplicate removal was logged
    assert any("Removed 1 duplicate" in record.message for record in caplog.records)

    # Verify only 2 rows were inserted
    result = temp_db.execute("SELECT COUNT(*) FROM open_interest").fetchone()
    assert result[0] == 2


def test_metrics_duplicate_key_handling(tmp_path, temp_db, caplog):
    """Duplicate insert should be handled silently."""
    csv_content = """create_time,symbol,sum_open_interest,sum_open_interest_value,count_toptrader_long_short_ratio,sum_toptrader_long_short_ratio,count_long_short_ratio,sum_taker_long_short_vol_ratio
2025-11-02 00:05:00,BTCUSDT,76643.19,8430743345.68,2.05,1.91,1.88,0.76"""

    zip_path = tmp_path / "duplicate_key.zip"
    with zipfile.ZipFile(zip_path, 'w') as zf:
        zf.writestr("duplicate_key.csv", csv_content)

    # First import
    import_metrics_to_duckdb(temp_db, zip_path, 'BTCUSDT')

    # Verify first import
    count1 = temp_db.execute("SELECT COUNT(*) FROM open_interest").fetchone()[0]
    assert count1 == 1

    # Second import (same data) - should not raise, should be skipped
    with caplog.at_level('DEBUG'):
        import_metrics_to_duckdb(temp_db, zip_path, 'BTCUSDT')

    # Verify no duplicate was inserted
    count2 = temp_db.execute("SELECT COUNT(*) FROM open_interest").fetchone()[0]
    assert count2 == 1

    # Check log for skip message
    assert any("Skipped duplicate data" in record.message for record in caplog.records)


def test_metrics_adds_exchange_column(tmp_path, temp_db):
    """Exchange='binance' should be added to each row."""
    csv_content = """create_time,symbol,sum_open_interest,sum_open_interest_value,count_toptrader_long_short_ratio,sum_toptrader_long_short_ratio,count_long_short_ratio,sum_taker_long_short_vol_ratio
2025-11-02 00:05:00,BTCUSDT,76643.19,8430743345.68,2.05,1.91,1.88,0.76"""

    zip_path = tmp_path / "exchange_test.zip"
    with zipfile.ZipFile(zip_path, 'w') as zf:
        zf.writestr("exchange_test.csv", csv_content)

    # Import
    import_metrics_to_duckdb(temp_db, zip_path, 'BTCUSDT', exchange='binance')

    # Verify exchange column
    result = temp_db.execute("SELECT exchange FROM open_interest").fetchone()
    assert result[0] == 'binance'


def test_metrics_with_different_exchange(tmp_path, temp_db):
    """Custom exchange parameter should be used."""
    csv_content = """create_time,symbol,sum_open_interest,sum_open_interest_value,count_toptrader_long_short_ratio,sum_toptrader_long_short_ratio,count_long_short_ratio,sum_taker_long_short_vol_ratio
2025-11-02 00:05:00,BTCUSDT,76643.19,8430743345.68,2.05,1.91,1.88,0.76"""

    zip_path = tmp_path / "bybit_test.zip"
    with zipfile.ZipFile(zip_path, 'w') as zf:
        zf.writestr("bybit_test.csv", csv_content)

    # Import with custom exchange
    import_metrics_to_duckdb(temp_db, zip_path, 'BTCUSDT', exchange='bybit')

    # Verify exchange column
    result = temp_db.execute("SELECT exchange FROM open_interest").fetchone()
    assert result[0] == 'bybit'


def test_malformed_csv_raises_exception(tmp_path, temp_db):
    """Malformed CSV should raise an exception."""
    # Create invalid CSV (missing header row)
    csv_content = """2025-11-02 00:05:00,BTCUSDT,76643.19,8430743345.68,2.05,1.91,1.88,0.76"""

    zip_path = tmp_path / "malformed.zip"
    with zipfile.ZipFile(zip_path, 'w') as zf:
        zf.writestr("malformed.csv", csv_content)

    # Should raise ValueError
    with pytest.raises(ValueError):
        import_metrics_to_duckdb(temp_db, zip_path, 'BTCUSDT')


def test_import_with_transaction(tmp_path, temp_db):
    """Import within transaction should commit or rollback atomically."""
    csv_content = """create_time,symbol,sum_open_interest,sum_open_interest_value,count_toptrader_long_short_ratio,sum_toptrader_long_short_ratio,count_long_short_ratio,sum_taker_long_short_vol_ratio
2025-11-02 00:05:00,BTCUSDT,76643.19,8430743345.68,2.05,1.91,1.88,0.76"""

    zip_path = tmp_path / "transaction_test.zip"
    with zipfile.ZipFile(zip_path, 'w') as zf:
        zf.writestr("transaction_test.csv", csv_content)

    # Import within transaction
    temp_db.execute("BEGIN TRANSACTION")
    import_metrics_to_duckdb(temp_db, zip_path, 'BTCUSDT')
    temp_db.execute("COMMIT")

    # Verify data was committed
    result = temp_db.execute("SELECT COUNT(*) FROM open_interest").fetchone()
    assert result[0] == 1


def test_import_multiple_symbols(tmp_path, temp_db):
    """Importing different symbols should work correctly."""
    # Create CSV for BTCUSDT
    btc_content = """create_time,symbol,sum_open_interest,sum_open_interest_value,count_toptrader_long_short_ratio,sum_toptrader_long_short_ratio,count_long_short_ratio,sum_taker_long_short_vol_ratio
2025-11-02 00:05:00,BTCUSDT,76643.19,8430743345.68,2.05,1.91,1.88,0.76"""

    btc_zip = tmp_path / "BTCUSDT.zip"
    with zipfile.ZipFile(btc_zip, 'w') as zf:
        zf.writestr("BTCUSDT.csv", btc_content)

    # Create CSV for ETHUSDT
    eth_content = """create_time,symbol,sum_open_interest,sum_open_interest_value,count_toptrader_long_short_ratio,sum_toptrader_long_short_ratio,count_long_short_ratio,sum_taker_long_short_vol_ratio
2025-11-02 00:05:00,ETHUSDT,150000.0,450000000.0,1.95,1.85,1.75,0.85"""

    eth_zip = tmp_path / "ETHUSDT.zip"
    with zipfile.ZipFile(eth_zip, 'w') as zf:
        zf.writestr("ETHUSDT.csv", eth_content)

    # Import both
    import_metrics_to_duckdb(temp_db, btc_zip, 'BTCUSDT')
    import_metrics_to_duckdb(temp_db, eth_zip, 'ETHUSDT')

    # Verify both symbols exist
    result = temp_db.execute("""
        SELECT symbol, COUNT(*)
        FROM open_interest
        GROUP BY symbol
        ORDER BY symbol
    """).fetchall()

    assert len(result) == 2
    assert result[0][0] == 'BTCUSDT'
    assert result[1][0] == 'ETHUSDT'


def test_metrics_zero_value_filtering(tmp_path, temp_db, caplog):
    """Rows with zero values in open_interest should be filtered out."""
    # Create CSV with mix of valid and zero-value data
    csv_content = """create_time,symbol,sum_open_interest,sum_open_interest_value,count_toptrader_long_short_ratio,sum_toptrader_long_short_ratio,count_long_short_ratio,sum_taker_long_short_vol_ratio
2025-11-02 00:05:00,BTCUSDT,76643.19,8430743345.68,2.05,1.91,1.88,0.76
2025-11-02 00:10:00,BTCUSDT,0,8431092392.87,2.05,1.91,1.88,0.97
2025-11-02 00:15:00,BTCUSDT,76659.28,8432000000.00,2.05,1.91,1.88,1.27
2025-11-02 00:20:00,BTCUSDT,0,1000000.00,2.05,1.91,1.88,0.85
2025-11-02 00:25:00,BTCUSDT,76670.45,8432500000.12,2.05,1.91,1.88,0.92"""

    zip_path = tmp_path / "zero_filter_test.zip"
    with zipfile.ZipFile(zip_path, 'w') as zf:
        zf.writestr("zero_filter_test.csv", csv_content)

    # Import
    with caplog.at_level('DEBUG'):
        import_metrics_to_duckdb(temp_db, zip_path, 'BTCUSDT')

    # Check that filtering was logged (2 rows with zero open_interest)
    assert any("Filtered out 2 rows with zero values" in record.message for record in caplog.records)

    # Verify only 3 valid rows were inserted (rows without zero open_interest)
    result = temp_db.execute("SELECT COUNT(*) FROM open_interest").fetchone()
    assert result[0] == 3

    # Verify the valid rows were kept
    rows = temp_db.execute("""
        SELECT open_interest
        FROM open_interest
        ORDER BY timestamp
    """).fetchall()

    assert len(rows) == 3
    assert abs(rows[0][0] - 76643.19) < 0.01
    assert abs(rows[1][0] - 76659.28) < 0.01
    assert abs(rows[2][0] - 76670.45) < 0.01


def test_metrics_all_zero_values(tmp_path, temp_db, caplog):
    """All rows with zero open_interest should result in no imports."""
    csv_content = """create_time,symbol,sum_open_interest,sum_open_interest_value,count_toptrader_long_short_ratio,sum_toptrader_long_short_ratio,count_long_short_ratio,sum_taker_long_short_vol_ratio
2025-11-02 00:05:00,BTCUSDT,0,8430743345.68,2.05,1.91,1.88,0.76
2025-11-02 00:10:00,BTCUSDT,0,8431092392.87,2.05,1.91,1.88,0.97
2025-11-02 00:15:00,BTCUSDT,0,8432000000.00,2.05,1.91,1.88,1.27"""

    zip_path = tmp_path / "all_zeros.zip"
    with zipfile.ZipFile(zip_path, 'w') as zf:
        zf.writestr("all_zeros.csv", csv_content)

    # Import
    with caplog.at_level('DEBUG'):
        import_metrics_to_duckdb(temp_db, zip_path, 'BTCUSDT')

    # Check that all rows were filtered
    assert any("Filtered out 3 rows with zero values" in record.message for record in caplog.records)

    # Verify no rows were inserted
    result = temp_db.execute("SELECT COUNT(*) FROM open_interest").fetchone()
    assert result[0] == 0


def test_metrics_1000prefix_normalization(tmp_path, temp_db):
    """CSV with 1000-prefix symbol should be normalized to base symbol."""
    # Create CSV where symbol column contains 1000PEPEUSDT
    # This simulates downloading from Binance with 1000-prefix
    csv_content = """create_time,symbol,sum_open_interest,sum_open_interest_value,count_toptrader_long_short_ratio,sum_toptrader_long_short_ratio,count_long_short_ratio,sum_taker_long_short_vol_ratio
2025-11-02 00:05:00,1000PEPEUSDT,5000000.0,150000000.0,2.05,1.91,1.88,0.76
2025-11-02 00:10:00,1000PEPEUSDT,5100000.0,153000000.0,2.05,1.91,1.88,0.97"""

    zip_path = tmp_path / "1000PEPEUSDT-metrics-2025-11-02.zip"
    with zipfile.ZipFile(zip_path, 'w') as zf:
        zf.writestr("1000PEPEUSDT-metrics-2025-11-02.csv", csv_content)

    # Import with normalized symbol (PEPEUSDT, not 1000PEPEUSDT)
    import_metrics_to_duckdb(temp_db, zip_path, 'PEPEUSDT', exchange='binance')

    # Verify data was imported with normalized symbol
    result = temp_db.execute("""
        SELECT symbol, COUNT(*)
        FROM open_interest
        WHERE symbol = 'PEPEUSDT'
        GROUP BY symbol
    """).fetchone()

    assert result is not None, "No data found for PEPEUSDT"
    assert result[0] == 'PEPEUSDT', "Symbol should be normalized to PEPEUSDT"
    assert result[1] == 2, "Should have 2 rows"

    # Verify no 1000-prefix symbol exists in database
    result_1000 = temp_db.execute("""
        SELECT COUNT(*)
        FROM open_interest
        WHERE symbol = '1000PEPEUSDT'
    """).fetchone()

    assert result_1000[0] == 0, "No 1000PEPEUSDT symbol should exist in database"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
