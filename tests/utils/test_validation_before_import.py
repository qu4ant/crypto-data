"""
Tests for pre-import data validation in utils/database.py

Tests that Pandera validation runs BEFORE import and rejects invalid data.
"""

import pytest
import duckdb
import zipfile
from pathlib import Path
from crypto_data.utils.database import (
    import_to_duckdb,
    import_metrics_to_duckdb,
    import_funding_rates_to_duckdb
)


@pytest.fixture
def temp_db(tmp_path):
    """Create a temporary database with all required tables."""
    db_path = tmp_path / "test_validation.db"
    conn = duckdb.connect(str(db_path))

    # Create spot table
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

    # Create futures table
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

    # Create open_interest table
    conn.execute("""
        CREATE TABLE open_interest (
            exchange VARCHAR,
            symbol VARCHAR,
            timestamp TIMESTAMP,
            open_interest DOUBLE,
            PRIMARY KEY (exchange, symbol, timestamp)
        )
    """)

    # Create funding_rates table
    conn.execute("""
        CREATE TABLE funding_rates (
            exchange VARCHAR,
            symbol VARCHAR,
            timestamp TIMESTAMP,
            funding_rate DOUBLE,
            PRIMARY KEY (exchange, symbol, timestamp)
        )
    """)

    yield conn
    conn.close()


# =============================================================================
# OHLCV Validation Tests
# =============================================================================

def test_valid_ohlcv_data_passes_validation(tmp_path, temp_db):
    """Valid OHLCV data should pass validation and be imported."""
    # Create valid CSV (OHLC relationships correct, all positive)
    csv_content = """1704067200000,45000,45100,44900,45050,100,1704067499999,4500000,1000,50,2250000,0
1704067500000,45100,45200,45000,45150,110,1704067799999,4960000,1100,55,2480000,0
1704067800000,45200,45300,45100,45250,120,1704068099999,5430000,1200,60,2715000,0"""

    # Create ZIP
    zip_path = tmp_path / "valid.zip"
    with zipfile.ZipFile(zip_path, 'w') as zf:
        zf.writestr("BTCUSDT-5m-2024-01.csv", csv_content)

    # Import should succeed
    temp_db.execute("BEGIN TRANSACTION")
    try:
        import_to_duckdb(temp_db, zip_path, 'BTCUSDT', 'spot', '5m')
        temp_db.execute("COMMIT")
    except Exception as e:
        temp_db.execute("ROLLBACK")
        raise

    # Verify data was imported
    result = temp_db.execute("SELECT COUNT(*) FROM spot").fetchone()
    assert result[0] == 3, "Valid data should be imported"


def test_invalid_ohlc_high_less_than_low_rejects_data(tmp_path, temp_db):
    """OHLCV data with high < low should fail validation."""
    # Create invalid CSV (high < low violates OHLC relationship)
    csv_content = """1704067200000,45000,44900,45100,45050,100,1704067499999,4500000,1000,50,2250000,0"""
    #                                      ^^^^^ high  ^^^^^ low (INVALID: high < low)

    # Create ZIP
    zip_path = tmp_path / "invalid_ohlc.zip"
    with zipfile.ZipFile(zip_path, 'w') as zf:
        zf.writestr("BTCUSDT-5m-2024-01.csv", csv_content)

    # Import should fail with ValueError
    temp_db.execute("BEGIN TRANSACTION")
    with pytest.raises(ValueError, match="Data validation failed.*OHLCV"):
        import_to_duckdb(temp_db, zip_path, 'BTCUSDT', 'spot', '5m')
    temp_db.execute("ROLLBACK")

    # Verify NO data was imported (transaction rollback)
    result = temp_db.execute("SELECT COUNT(*) FROM spot").fetchone()
    assert result[0] == 0, "Invalid data should NOT be imported"


def test_invalid_negative_price_rejects_data(tmp_path, temp_db):
    """OHLCV data with negative prices should fail validation."""
    # Create invalid CSV (negative close price)
    csv_content = """1704067200000,45000,45100,44900,-45050,100,1704067499999,4500000,1000,50,2250000,0"""
    #                                                  ^^^^^^ negative close (INVALID)

    # Create ZIP
    zip_path = tmp_path / "negative_price.zip"
    with zipfile.ZipFile(zip_path, 'w') as zf:
        zf.writestr("BTCUSDT-5m-2024-01.csv", csv_content)

    # Import should fail
    temp_db.execute("BEGIN TRANSACTION")
    with pytest.raises(ValueError, match="Data validation failed"):
        import_to_duckdb(temp_db, zip_path, 'BTCUSDT', 'spot', '5m')
    temp_db.execute("ROLLBACK")

    # Verify NO data was imported
    result = temp_db.execute("SELECT COUNT(*) FROM spot").fetchone()
    assert result[0] == 0


def test_invalid_negative_volume_rejects_data(tmp_path, temp_db):
    """OHLCV data with negative volume should fail validation."""
    # Create invalid CSV (negative volume)
    csv_content = """1704067200000,45000,45100,44900,45050,-100,1704067499999,4500000,1000,50,2250000,0"""
    #                                                          ^^^^ negative volume (INVALID)

    # Create ZIP
    zip_path = tmp_path / "negative_volume.zip"
    with zipfile.ZipFile(zip_path, 'w') as zf:
        zf.writestr("BTCUSDT-5m-2024-01.csv", csv_content)

    # Import should fail
    temp_db.execute("BEGIN TRANSACTION")
    with pytest.raises(ValueError, match="Data validation failed"):
        import_to_duckdb(temp_db, zip_path, 'BTCUSDT', 'spot', '5m')
    temp_db.execute("ROLLBACK")

    # Verify NO data was imported
    result = temp_db.execute("SELECT COUNT(*) FROM spot").fetchone()
    assert result[0] == 0


def test_invalid_open_greater_than_high_rejects_data(tmp_path, temp_db):
    """OHLCV data with open > high should fail validation."""
    # Create invalid CSV (open > high violates OHLC relationship)
    csv_content = """1704067200000,45200,45100,44900,45050,100,1704067499999,4500000,1000,50,2250000,0"""
    #                            ^^^^^ open  ^^^^^ high (INVALID: open > high)

    # Create ZIP
    zip_path = tmp_path / "open_gt_high.zip"
    with zipfile.ZipFile(zip_path, 'w') as zf:
        zf.writestr("BTCUSDT-5m-2024-01.csv", csv_content)

    # Import should fail
    temp_db.execute("BEGIN TRANSACTION")
    with pytest.raises(ValueError, match="Data validation failed"):
        import_to_duckdb(temp_db, zip_path, 'BTCUSDT', 'spot', '5m')
    temp_db.execute("ROLLBACK")

    # Verify NO data was imported
    result = temp_db.execute("SELECT COUNT(*) FROM spot").fetchone()
    assert result[0] == 0


# =============================================================================
# Open Interest Validation Tests
# =============================================================================

def test_valid_open_interest_passes_validation(tmp_path, temp_db):
    """Valid open interest data should pass validation."""
    # Create valid CSV
    csv_content = """create_time,symbol,sum_open_interest,sum_open_interest_value,count_toptrader_long_short_ratio,sum_toptrader_long_short_ratio,count_long_short_ratio,sum_taker_long_short_vol_ratio
2024-01-01 00:00:00,BTCUSDT,1000000.5,45000000000,0.5,0.5,0.5,0.5
2024-01-01 00:05:00,BTCUSDT,1000100.0,45005000000,0.5,0.5,0.5,0.5"""

    # Create ZIP
    zip_path = tmp_path / "valid_metrics.zip"
    with zipfile.ZipFile(zip_path, 'w') as zf:
        zf.writestr("BTCUSDT-metrics-2024-01-01.csv", csv_content)

    # Import should succeed
    temp_db.execute("BEGIN TRANSACTION")
    try:
        import_metrics_to_duckdb(temp_db, zip_path, 'BTCUSDT')
        temp_db.execute("COMMIT")
    except Exception as e:
        temp_db.execute("ROLLBACK")
        raise

    # Verify data was imported
    result = temp_db.execute("SELECT COUNT(*) FROM open_interest").fetchone()
    assert result[0] == 2


def test_invalid_negative_open_interest_rejects_data(tmp_path, temp_db):
    """Open interest data with negative values should fail validation."""
    # Create invalid CSV (negative open interest)
    csv_content = """create_time,symbol,sum_open_interest,sum_open_interest_value,count_toptrader_long_short_ratio,sum_toptrader_long_short_ratio,count_long_short_ratio,sum_taker_long_short_vol_ratio
2024-01-01 00:00:00,BTCUSDT,-1000000.5,45000000000,0.5,0.5,0.5,0.5"""

    # Create ZIP
    zip_path = tmp_path / "negative_oi.zip"
    with zipfile.ZipFile(zip_path, 'w') as zf:
        zf.writestr("BTCUSDT-metrics-2024-01-01.csv", csv_content)

    # Import should fail
    temp_db.execute("BEGIN TRANSACTION")
    with pytest.raises(ValueError, match="Data validation failed.*open interest"):
        import_metrics_to_duckdb(temp_db, zip_path, 'BTCUSDT')
    temp_db.execute("ROLLBACK")

    # Verify NO data was imported
    result = temp_db.execute("SELECT COUNT(*) FROM open_interest").fetchone()
    assert result[0] == 0


# =============================================================================
# Funding Rates Validation Tests
# =============================================================================

def test_valid_funding_rates_pass_validation(tmp_path, temp_db):
    """Valid funding rate data should pass validation."""
    # Create valid CSV (reasonable funding rates: ±0.1%)
    csv_content = """calc_time,last_funding_rate,mark_price
1704067200000,0.0001,45000
1704153600000,0.00008,45100
1704240000000,-0.00005,45050"""

    # Create ZIP
    zip_path = tmp_path / "valid_funding.zip"
    with zipfile.ZipFile(zip_path, 'w') as zf:
        zf.writestr("BTCUSDT-fundingRate-2024-01.csv", csv_content)

    # Import should succeed
    temp_db.execute("BEGIN TRANSACTION")
    try:
        import_funding_rates_to_duckdb(temp_db, zip_path, 'BTCUSDT')
        temp_db.execute("COMMIT")
    except Exception as e:
        temp_db.execute("ROLLBACK")
        raise

    # Verify data was imported
    result = temp_db.execute("SELECT COUNT(*) FROM funding_rates").fetchone()
    assert result[0] == 3


# Note: Funding rates validation might not reject extreme values in non-strict mode
# The schema warns about extreme values but doesn't fail by default
# This is acceptable as funding rates can legitimately be extreme during market stress


# =============================================================================
# Transaction Rollback Tests
# =============================================================================

def test_validation_failure_triggers_transaction_rollback(tmp_path, temp_db):
    """When validation fails, transaction should rollback and leave DB unchanged."""
    # Insert valid data first
    csv_valid = """1704067200000,45000,45100,44900,45050,100,1704067499999,4500000,1000,50,2250000,0"""
    zip_valid = tmp_path / "valid_first.zip"
    with zipfile.ZipFile(zip_valid, 'w') as zf:
        zf.writestr("BTCUSDT-5m-2024-01.csv", csv_valid)

    temp_db.execute("BEGIN TRANSACTION")
    import_to_duckdb(temp_db, zip_valid, 'BTCUSDT', 'spot', '5m')
    temp_db.execute("COMMIT")

    # Verify 1 row inserted
    result = temp_db.execute("SELECT COUNT(*) FROM spot").fetchone()
    assert result[0] == 1

    # Now try to import invalid data
    csv_invalid = """1704067500000,45000,44900,45100,45050,100,1704067799999,4500000,1000,50,2250000,0"""
    zip_invalid = tmp_path / "invalid_second.zip"
    with zipfile.ZipFile(zip_invalid, 'w') as zf:
        zf.writestr("BTCUSDT-5m-2024-01.csv", csv_invalid)

    # Import should fail
    temp_db.execute("BEGIN TRANSACTION")
    with pytest.raises(ValueError):
        import_to_duckdb(temp_db, zip_invalid, 'ETHUSDT', 'spot', '5m')
    temp_db.execute("ROLLBACK")

    # Verify STILL only 1 row (rollback worked)
    result = temp_db.execute("SELECT COUNT(*) FROM spot").fetchone()
    assert result[0] == 1, "Transaction rollback should preserve original data"


def test_validation_logs_clear_error_message(tmp_path, temp_db, caplog):
    """Validation failure should log clear, helpful error message."""
    # Create invalid CSV
    csv_content = """1704067200000,45000,44900,45100,45050,100,1704067499999,4500000,1000,50,2250000,0"""

    # Create ZIP
    zip_path = tmp_path / "invalid_logged.zip"
    with zipfile.ZipFile(zip_path, 'w') as zf:
        zf.writestr("BTCUSDT-5m-2024-01.csv", csv_content)

    # Import should fail and log error
    temp_db.execute("BEGIN TRANSACTION")
    with caplog.at_level('ERROR'):
        with pytest.raises(ValueError):
            import_to_duckdb(temp_db, zip_path, 'BTCUSDT', 'spot', '5m')
    temp_db.execute("ROLLBACK")

    # Check error message contains helpful information
    error_logs = [r.message for r in caplog.records if r.levelname == 'ERROR']
    assert any("Data validation FAILED" in msg for msg in error_logs)
    assert any("File rejected" in msg for msg in error_logs)
    assert any("Suggestion" in msg for msg in error_logs), "Should provide helpful suggestion"
