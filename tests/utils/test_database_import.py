"""
Tests for import_to_duckdb function in utils/database.py

Tests edge cases including malformed ZIPs, duplicate timestamps, and column harmonization.
"""

import pytest
import duckdb
import zipfile
from pathlib import Path
from crypto_data.utils.database import import_to_duckdb


@pytest.fixture
def temp_db(tmp_path):
    """Create a temporary database with spot and futures tables."""
    db_path = tmp_path / "test_import.db"
    conn = duckdb.connect(str(db_path))

    # Create tables
    conn.execute("""
        CREATE TABLE spot (
            exchange VARCHAR,
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
            PRIMARY KEY (exchange, symbol, interval, timestamp)
        )
    """)

    conn.execute("""
        CREATE TABLE futures (
            exchange VARCHAR,
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
            PRIMARY KEY (exchange, symbol, interval, timestamp)
        )
    """)

    yield conn
    conn.close()


def test_zip_without_csv_raises_error(tmp_path, temp_db):
    """ZIP file without CSV should raise ValueError."""
    # Create a ZIP with a text file instead of CSV
    zip_path = tmp_path / "no_csv.zip"
    with zipfile.ZipFile(zip_path, 'w') as zf:
        zf.writestr("readme.txt", "This is not a CSV file")

    with pytest.raises(ValueError, match="No CSV file found in ZIP"):
        import_to_duckdb(temp_db, zip_path, 'BTCUSDT', 'spot', '5m')


def test_csv_with_duplicates_logs_removal(tmp_path, temp_db, caplog):
    """CSV with duplicate timestamps should log removal count."""
    # Create CSV with duplicate timestamps
    csv_content = """1704067200000,45000,45100,44900,45050,100,1704067499999,4500000,1000,50,2250000,0
1704067500000,45100,45200,45000,45150,110,1704067799999,4960000,1100,55,2480000,0
1704067500000,45100,45200,45000,45150,110,1704067799999,4960000,1100,55,2480000,0
1704067800000,45200,45300,45100,45250,120,1704068099999,5430000,1200,60,2715000,0"""

    # Create ZIP
    zip_path = tmp_path / "duplicates.zip"
    with zipfile.ZipFile(zip_path, 'w') as zf:
        zf.writestr("BTCUSDT-5m-2024-01.csv", csv_content)

    # Import
    with caplog.at_level('DEBUG'):
        import_to_duckdb(temp_db, zip_path, 'BTCUSDT', 'spot', '5m')

    # Check that duplicate removal was logged
    assert any("Removed 1 duplicate" in record.message for record in caplog.records)

    # Verify only 3 rows were inserted (1 duplicate removed)
    result = temp_db.execute("SELECT COUNT(*) FROM spot").fetchone()
    assert result[0] == 3


def test_csv_with_count_column_normalized(tmp_path, temp_db):
    """CSV with 'count' column should be normalized to 'trades_count'."""
    # Create CSV with header using 'count' instead of 'trades_count'
    csv_content = """open_time,open,high,low,close,volume,close_time,quote_volume,count,taker_buy_base_volume,taker_buy_quote_volume,ignore
1704067200000,45000,45100,44900,45050,100,1704067499999,4500000,1000,50,2250000,0
1704067500000,45100,45200,45000,45150,110,1704067799999,4960000,1100,55,2480000,0"""

    # Create ZIP
    zip_path = tmp_path / "count_column.zip"
    with zipfile.ZipFile(zip_path, 'w') as zf:
        zf.writestr("BTCUSDT-5m-2024-01.csv", csv_content)

    # Import
    import_to_duckdb(temp_db, zip_path, 'BTCUSDT', 'spot', '5m')

    # Verify data was imported with trades_count column
    result = temp_db.execute("SELECT trades_count FROM spot ORDER BY timestamp LIMIT 1").fetchone()
    assert result[0] == 1000


def test_csv_with_taker_buy_volume_normalized(tmp_path, temp_db):
    """CSV with 'taker_buy_volume' column should be normalized to 'taker_buy_base_volume'."""
    # Create CSV with header using 'taker_buy_volume' instead of 'taker_buy_base_volume'
    csv_content = """open_time,open,high,low,close,volume,close_time,quote_volume,trades_count,taker_buy_volume,taker_buy_quote_volume,ignore
1704067200000,45000,45100,44900,45050,100,1704067499999,4500000,1000,50,2250000,0
1704067500000,45100,45200,45000,45150,110,1704067799999,4960000,1100,55,2480000,0"""

    # Create ZIP
    zip_path = tmp_path / "taker_buy_volume.zip"
    with zipfile.ZipFile(zip_path, 'w') as zf:
        zf.writestr("BTCUSDT-5m-2024-01.csv", csv_content)

    # Import
    import_to_duckdb(temp_db, zip_path, 'BTCUSDT', 'spot', '5m')

    # Verify data was imported with taker_buy_base_volume column
    result = temp_db.execute("SELECT taker_buy_base_volume FROM spot ORDER BY timestamp LIMIT 1").fetchone()
    assert result[0] == 50


def test_import_with_original_symbol(tmp_path, temp_db):
    """Import with original_symbol should store original symbol, not download symbol."""
    # Create CSV
    csv_content = """1704067200000,45000,45100,44900,45050,100,1704067499999,4500000,1000,50,2250000,0
1704067500000,45100,45200,45000,45150,110,1704067799999,4960000,1100,55,2480000,0"""

    # Create ZIP
    zip_path = tmp_path / "1000prefix.zip"
    with zipfile.ZipFile(zip_path, 'w') as zf:
        zf.writestr("1000PEPEUSDT-5m-2024-01.csv", csv_content)

    # Import with original_symbol (simulating 1000-prefix normalization)
    import_to_duckdb(temp_db, zip_path, '1000PEPEUSDT', 'spot', '5m', original_symbol='PEPEUSDT')

    # Verify symbol stored is PEPEUSDT, not 1000PEPEUSDT
    result = temp_db.execute("SELECT DISTINCT symbol FROM spot").fetchone()
    assert result[0] == 'PEPEUSDT'


def test_import_malformed_csv_raises_exception(tmp_path, temp_db):
    """Malformed CSV that pandas cannot parse should raise exception."""
    # Create CSV with invalid data
    csv_content = """1704067200000,45000,INVALID,44900,45050,100,1704067499999,4500000,1000,50,2250000,0"""

    # Create ZIP
    zip_path = tmp_path / "malformed.zip"
    with zipfile.ZipFile(zip_path, 'w') as zf:
        zf.writestr("BTCUSDT-5m-2024-01.csv", csv_content)

    # Import should raise exception (pandas will fail to parse)
    with pytest.raises(Exception):
        import_to_duckdb(temp_db, zip_path, 'BTCUSDT', 'spot', '5m')


def test_import_cleans_up_extracted_csv(tmp_path, temp_db):
    """Extracted CSV file should be deleted after import."""
    # Create CSV
    csv_content = """1704067200000,45000,45100,44900,45050,100,1704067499999,4500000,1000,50,2250000,0"""

    # Create ZIP
    zip_path = tmp_path / "cleanup.zip"
    csv_name = "BTCUSDT-5m-2024-01.csv"
    with zipfile.ZipFile(zip_path, 'w') as zf:
        zf.writestr(csv_name, csv_content)

    # Import
    import_to_duckdb(temp_db, zip_path, 'BTCUSDT', 'spot', '5m')

    # Verify extracted CSV was deleted
    extracted_csv = tmp_path / csv_name
    assert not extracted_csv.exists()


def test_import_cleans_up_csv_even_on_error(tmp_path, temp_db):
    """Extracted CSV should be deleted even if import fails."""
    # Create CSV with data that will fail import (missing columns)
    csv_content = """1704067200000,45000"""

    # Create ZIP
    zip_path = tmp_path / "cleanup_error.zip"
    csv_name = "BTCUSDT-5m-2024-01.csv"
    with zipfile.ZipFile(zip_path, 'w') as zf:
        zf.writestr(csv_name, csv_content)

    # Import should fail but still cleanup
    extracted_csv = tmp_path / csv_name
    try:
        import_to_duckdb(temp_db, zip_path, 'BTCUSDT', 'spot', '5m')
    except Exception:
        pass  # Expected to fail

    # Verify extracted CSV was deleted even after error
    assert not extracted_csv.exists()


def test_import_with_header_detection(tmp_path, temp_db):
    """CSV with header should be detected and parsed correctly."""
    # Create CSV with header
    csv_content = """open_time,open,high,low,close,volume,close_time,quote_volume,trades_count,taker_buy_base_volume,taker_buy_quote_volume,ignore
1704067200000,45000,45100,44900,45050,100,1704067499999,4500000,1000,50,2250000,0
1704067500000,45100,45200,45000,45150,110,1704067799999,4960000,1100,55,2480000,0"""

    # Create ZIP
    zip_path = tmp_path / "with_header.zip"
    with zipfile.ZipFile(zip_path, 'w') as zf:
        zf.writestr("BTCUSDT-5m-2024-01.csv", csv_content)

    # Import
    import_to_duckdb(temp_db, zip_path, 'BTCUSDT', 'spot', '5m')

    # Verify 2 rows were imported (header not counted)
    result = temp_db.execute("SELECT COUNT(*) FROM spot").fetchone()
    assert result[0] == 2


def test_import_without_header(tmp_path, temp_db):
    """CSV without header should be parsed with default column names."""
    # Create CSV without header
    csv_content = """1704067200000,45000,45100,44900,45050,100,1704067499999,4500000,1000,50,2250000,0
1704067500000,45100,45200,45000,45150,110,1704067799999,4960000,1100,55,2480000,0"""

    # Create ZIP
    zip_path = tmp_path / "no_header.zip"
    with zipfile.ZipFile(zip_path, 'w') as zf:
        zf.writestr("BTCUSDT-5m-2024-01.csv", csv_content)

    # Import
    import_to_duckdb(temp_db, zip_path, 'BTCUSDT', 'spot', '5m')

    # Verify 2 rows were imported
    result = temp_db.execute("SELECT COUNT(*) FROM spot").fetchone()
    assert result[0] == 2


def test_import_adds_symbol_and_interval_columns(tmp_path, temp_db):
    """Import should add symbol and interval columns to the data."""
    # Create CSV
    csv_content = """1704067200000,45000,45100,44900,45050,100,1704067499999,4500000,1000,50,2250000,0"""

    # Create ZIP
    zip_path = tmp_path / "columns.zip"
    with zipfile.ZipFile(zip_path, 'w') as zf:
        zf.writestr("BTCUSDT-5m-2024-01.csv", csv_content)

    # Import
    import_to_duckdb(temp_db, zip_path, 'BTCUSDT', 'spot', '5m')

    # Verify symbol and interval columns were added
    result = temp_db.execute("SELECT symbol, interval FROM spot").fetchone()
    assert result[0] == 'BTCUSDT'
    assert result[1] == '5m'
