"""
Tests for DataImporter

Tests the generic data importer with different strategies (klines, open_interest,
funding_rates) using in-memory DuckDB and test ZIP files.
"""

import tempfile
import zipfile
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from crypto_data.core.importer import DataImporter
from crypto_data.enums import DataType, Interval
from crypto_data.strategies.klines import KlinesStrategy
from crypto_data.strategies.open_interest import OpenInterestStrategy
from crypto_data.strategies.funding_rates import FundingRatesStrategy


# -----------------------------------------------------------------------------
# Fixtures
# -----------------------------------------------------------------------------

@pytest.fixture
def db_conn_spot():
    """Create in-memory DuckDB with spot table schema."""
    conn = duckdb.connect(':memory:')
    conn.execute("""
        CREATE TABLE spot (
            exchange VARCHAR NOT NULL,
            symbol VARCHAR NOT NULL,
            interval VARCHAR NOT NULL,
            timestamp TIMESTAMP NOT NULL,
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


@pytest.fixture
def db_conn_futures():
    """Create in-memory DuckDB with futures table schema."""
    conn = duckdb.connect(':memory:')
    conn.execute("""
        CREATE TABLE futures (
            exchange VARCHAR NOT NULL,
            symbol VARCHAR NOT NULL,
            interval VARCHAR NOT NULL,
            timestamp TIMESTAMP NOT NULL,
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


@pytest.fixture
def db_conn_open_interest():
    """Create in-memory DuckDB with open_interest table schema."""
    conn = duckdb.connect(':memory:')
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


@pytest.fixture
def db_conn_funding_rates():
    """Create in-memory DuckDB with funding_rates table schema."""
    conn = duckdb.connect(':memory:')
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


@pytest.fixture
def klines_zip(tmp_path):
    """Create a test ZIP file with klines CSV data (no header)."""
    csv_content = """1704067199999,42000.0,42100.0,41900.0,42050.0,100.5,1704067199999,4215000.0,500,50.2,2107500.0,0
1704067499999,42050.0,42150.0,41950.0,42100.0,110.3,1704067499999,4640000.0,520,55.1,2320000.0,0
1704067799999,42100.0,42200.0,42000.0,42150.0,95.8,1704067799999,4037000.0,480,47.9,2018500.0,0"""

    csv_path = tmp_path / "BTCUSDT-5m-2024-01.csv"
    csv_path.write_text(csv_content)

    zip_path = tmp_path / "BTCUSDT-5m-2024-01.zip"
    with zipfile.ZipFile(zip_path, 'w') as zf:
        zf.write(csv_path, csv_path.name)

    csv_path.unlink()  # Clean up CSV, keep only ZIP
    return zip_path


@pytest.fixture
def klines_zip_with_header(tmp_path):
    """Create a test ZIP file with klines CSV data (with header)."""
    # Note: using 'count' (renamed to trades_count) and taker_buy_base_volume
    csv_content = """open_time,open,high,low,close,volume,close_time,quote_volume,count,taker_buy_base_volume,taker_buy_quote_volume,ignore
1704067199999,42000.0,42100.0,41900.0,42050.0,100.5,1704067199999,4215000.0,500,50.2,2107500.0,0
1704067499999,42050.0,42150.0,41950.0,42100.0,110.3,1704067499999,4640000.0,520,55.1,2320000.0,0"""

    csv_path = tmp_path / "BTCUSDT-5m-2024-01.csv"
    csv_path.write_text(csv_content)

    zip_path = tmp_path / "BTCUSDT-header-5m-2024-01.zip"
    with zipfile.ZipFile(zip_path, 'w') as zf:
        zf.write(csv_path, csv_path.name)

    csv_path.unlink()
    return zip_path


@pytest.fixture
def open_interest_zip(tmp_path):
    """Create a test ZIP file with open interest CSV data."""
    csv_content = """create_time,symbol,sum_open_interest,sum_open_interest_value,count_toptrader_long_short_ratio,sum_toptrader_long_short_ratio,count_long_short_ratio,sum_taker_long_short_vol_ratio
2024-01-01 00:00:00,BTCUSDT,15000.5,630000000.0,1.2,1.15,0.95,1.1
2024-01-01 01:00:00,BTCUSDT,15100.2,635000000.0,1.18,1.12,0.97,1.08
2024-01-01 02:00:00,BTCUSDT,14900.8,625000000.0,1.22,1.18,0.93,1.12"""

    csv_path = tmp_path / "BTCUSDT-metrics-2024-01-01.csv"
    csv_path.write_text(csv_content)

    zip_path = tmp_path / "BTCUSDT-metrics-2024-01-01.zip"
    with zipfile.ZipFile(zip_path, 'w') as zf:
        zf.write(csv_path, csv_path.name)

    csv_path.unlink()
    return zip_path


@pytest.fixture
def open_interest_zip_with_zeros(tmp_path):
    """Create a test ZIP file with open interest CSV data including zeros."""
    csv_content = """create_time,symbol,sum_open_interest,sum_open_interest_value,count_toptrader_long_short_ratio,sum_toptrader_long_short_ratio,count_long_short_ratio,sum_taker_long_short_vol_ratio
2024-01-01 00:00:00,BTCUSDT,15000.5,630000000.0,1.2,1.15,0.95,1.1
2024-01-01 01:00:00,BTCUSDT,0,0,1.18,1.12,0.97,1.08
2024-01-01 02:00:00,BTCUSDT,14900.8,625000000.0,1.22,1.18,0.93,1.12"""

    csv_path = tmp_path / "BTCUSDT-metrics-2024-01-02.csv"
    csv_path.write_text(csv_content)

    zip_path = tmp_path / "BTCUSDT-metrics-2024-01-02.zip"
    with zipfile.ZipFile(zip_path, 'w') as zf:
        zf.write(csv_path, csv_path.name)

    csv_path.unlink()
    return zip_path


@pytest.fixture
def funding_rates_zip(tmp_path):
    """Create a test ZIP file with funding rates CSV data."""
    # Timestamps are in milliseconds
    csv_content = """calc_time,last_funding_rate,symbol
1704067200000,0.0001,BTCUSDT
1704096000000,0.00015,BTCUSDT
1704124800000,-0.0002,BTCUSDT"""

    csv_path = tmp_path / "BTCUSDT-fundingRate-2024-01.csv"
    csv_path.write_text(csv_content)

    zip_path = tmp_path / "BTCUSDT-fundingRate-2024-01.zip"
    with zipfile.ZipFile(zip_path, 'w') as zf:
        zf.write(csv_path, csv_path.name)

    csv_path.unlink()
    return zip_path


# -----------------------------------------------------------------------------
# TestDataImporterKlines
# -----------------------------------------------------------------------------

class TestDataImporterKlines:
    """Test DataImporter with KlinesStrategy."""

    def test_import_spot_klines_no_header(self, db_conn_spot, klines_zip):
        """Test importing spot klines CSV without header."""
        strategy = KlinesStrategy(DataType.SPOT, Interval.MIN_5)
        importer = DataImporter(strategy)

        db_conn_spot.execute("BEGIN TRANSACTION")
        rows = importer.import_file(db_conn_spot, klines_zip, 'BTCUSDT')
        db_conn_spot.execute("COMMIT")

        assert rows == 3

        # Verify data was inserted
        result = db_conn_spot.execute(
            "SELECT COUNT(*) FROM spot WHERE symbol = 'BTCUSDT'"
        ).fetchone()
        assert result[0] == 3

        # Verify exchange column
        result = db_conn_spot.execute(
            "SELECT DISTINCT exchange FROM spot"
        ).fetchone()
        assert result[0] == 'binance'

        # Verify interval column
        result = db_conn_spot.execute(
            "SELECT DISTINCT interval FROM spot"
        ).fetchone()
        assert result[0] == '5m'

    def test_import_spot_klines_with_header(self, db_conn_spot, klines_zip_with_header):
        """Test importing spot klines CSV with header (auto-detect)."""
        strategy = KlinesStrategy(DataType.SPOT, Interval.MIN_5)
        importer = DataImporter(strategy)

        db_conn_spot.execute("BEGIN TRANSACTION")
        rows = importer.import_file(db_conn_spot, klines_zip_with_header, 'BTCUSDT')
        db_conn_spot.execute("COMMIT")

        assert rows == 2

        # Verify data integrity
        result = db_conn_spot.execute(
            "SELECT open, high, low, close FROM spot ORDER BY timestamp LIMIT 1"
        ).fetchone()
        assert result[0] == 42000.0  # open
        assert result[1] == 42100.0  # high
        assert result[2] == 41900.0  # low
        assert result[3] == 42050.0  # close

    def test_import_futures_klines(self, db_conn_futures, klines_zip):
        """Test importing futures klines."""
        strategy = KlinesStrategy(DataType.FUTURES, Interval.MIN_5)
        importer = DataImporter(strategy)

        db_conn_futures.execute("BEGIN TRANSACTION")
        rows = importer.import_file(db_conn_futures, klines_zip, 'BTCUSDT')
        db_conn_futures.execute("COMMIT")

        assert rows == 3

        result = db_conn_futures.execute(
            "SELECT COUNT(*) FROM futures"
        ).fetchone()
        assert result[0] == 3

    def test_duplicate_import_returns_zero(self, db_conn_spot, klines_zip):
        """Test that importing duplicate data returns 0."""
        strategy = KlinesStrategy(DataType.SPOT, Interval.MIN_5)
        importer = DataImporter(strategy)

        # First import
        db_conn_spot.execute("BEGIN TRANSACTION")
        rows1 = importer.import_file(db_conn_spot, klines_zip, 'BTCUSDT')
        db_conn_spot.execute("COMMIT")
        assert rows1 == 3

        # Second import - should return 0 (duplicate)
        db_conn_spot.execute("BEGIN TRANSACTION")
        rows2 = importer.import_file(db_conn_spot, klines_zip, 'BTCUSDT')
        db_conn_spot.execute("COMMIT")
        assert rows2 == 0

    def test_custom_exchange(self, db_conn_spot, klines_zip):
        """Test importing with custom exchange name."""
        strategy = KlinesStrategy(DataType.SPOT, Interval.MIN_5)
        importer = DataImporter(strategy)

        db_conn_spot.execute("BEGIN TRANSACTION")
        importer.import_file(db_conn_spot, klines_zip, 'BTCUSDT', exchange='bybit')
        db_conn_spot.execute("COMMIT")

        result = db_conn_spot.execute(
            "SELECT DISTINCT exchange FROM spot"
        ).fetchone()
        assert result[0] == 'bybit'

    def test_csv_cleanup_after_import(self, db_conn_spot, klines_zip):
        """Test that CSV file is cleaned up after import."""
        strategy = KlinesStrategy(DataType.SPOT, Interval.MIN_5)
        importer = DataImporter(strategy)

        # Get the expected CSV path
        csv_path = klines_zip.parent / "BTCUSDT-5m-2024-01.csv"

        db_conn_spot.execute("BEGIN TRANSACTION")
        importer.import_file(db_conn_spot, klines_zip, 'BTCUSDT')
        db_conn_spot.execute("COMMIT")

        # CSV should be deleted
        assert not csv_path.exists()


# -----------------------------------------------------------------------------
# TestDataImporterOpenInterest
# -----------------------------------------------------------------------------

class TestDataImporterOpenInterest:
    """Test DataImporter with OpenInterestStrategy."""

    def test_import_open_interest(self, db_conn_open_interest, open_interest_zip):
        """Test importing open interest data."""
        strategy = OpenInterestStrategy()
        importer = DataImporter(strategy)

        db_conn_open_interest.execute("BEGIN TRANSACTION")
        rows = importer.import_file(db_conn_open_interest, open_interest_zip, 'BTCUSDT')
        db_conn_open_interest.execute("COMMIT")

        assert rows == 3

        # Verify data
        result = db_conn_open_interest.execute(
            "SELECT COUNT(*) FROM open_interest WHERE symbol = 'BTCUSDT'"
        ).fetchone()
        assert result[0] == 3

        # Verify open_interest values
        result = db_conn_open_interest.execute(
            "SELECT open_interest FROM open_interest ORDER BY timestamp LIMIT 1"
        ).fetchone()
        assert result[0] == 15000.5

    def test_zero_values_filtered(self, db_conn_open_interest, open_interest_zip_with_zeros):
        """Test that rows with zero open interest are filtered out."""
        strategy = OpenInterestStrategy()
        importer = DataImporter(strategy)

        db_conn_open_interest.execute("BEGIN TRANSACTION")
        rows = importer.import_file(db_conn_open_interest, open_interest_zip_with_zeros, 'BTCUSDT')
        db_conn_open_interest.execute("COMMIT")

        # Only 2 rows (the one with 0 is filtered)
        assert rows == 2

    def test_symbol_override(self, db_conn_open_interest, open_interest_zip):
        """Test that symbol is overridden (for 1000-prefix normalization)."""
        strategy = OpenInterestStrategy()
        importer = DataImporter(strategy)

        # Import with a different symbol than what's in the CSV
        db_conn_open_interest.execute("BEGIN TRANSACTION")
        rows = importer.import_file(db_conn_open_interest, open_interest_zip, 'PEPEUSDT')
        db_conn_open_interest.execute("COMMIT")

        # Symbol should be PEPEUSDT, not BTCUSDT from CSV
        result = db_conn_open_interest.execute(
            "SELECT DISTINCT symbol FROM open_interest"
        ).fetchone()
        assert result[0] == 'PEPEUSDT'


# -----------------------------------------------------------------------------
# TestDataImporterFundingRates
# -----------------------------------------------------------------------------

class TestDataImporterFundingRates:
    """Test DataImporter with FundingRatesStrategy."""

    def test_import_funding_rates(self, db_conn_funding_rates, funding_rates_zip):
        """Test importing funding rate data."""
        strategy = FundingRatesStrategy()
        importer = DataImporter(strategy)

        db_conn_funding_rates.execute("BEGIN TRANSACTION")
        rows = importer.import_file(db_conn_funding_rates, funding_rates_zip, 'BTCUSDT')
        db_conn_funding_rates.execute("COMMIT")

        assert rows == 3

        # Verify data
        result = db_conn_funding_rates.execute(
            "SELECT COUNT(*) FROM funding_rates WHERE symbol = 'BTCUSDT'"
        ).fetchone()
        assert result[0] == 3

        # Verify funding_rate values (including negative)
        result = db_conn_funding_rates.execute(
            "SELECT funding_rate FROM funding_rates ORDER BY timestamp"
        ).fetchall()
        assert result[0][0] == 0.0001
        assert result[1][0] == 0.00015
        assert result[2][0] == -0.0002

    def test_timestamp_conversion(self, db_conn_funding_rates, funding_rates_zip):
        """Test that millisecond timestamps are converted correctly."""
        strategy = FundingRatesStrategy()
        importer = DataImporter(strategy)

        db_conn_funding_rates.execute("BEGIN TRANSACTION")
        importer.import_file(db_conn_funding_rates, funding_rates_zip, 'BTCUSDT')
        db_conn_funding_rates.execute("COMMIT")

        # Check first timestamp (1704067200000 ms = 2024-01-01 00:00:00)
        result = db_conn_funding_rates.execute(
            "SELECT timestamp FROM funding_rates ORDER BY timestamp LIMIT 1"
        ).fetchone()
        assert str(result[0]).startswith('2024-01-01')


# -----------------------------------------------------------------------------
# TestDataImporterEdgeCases
# -----------------------------------------------------------------------------

class TestDataImporterEdgeCases:
    """Test edge cases and error handling."""

    def test_no_csv_in_zip_raises_error(self, tmp_path, db_conn_spot):
        """Test that importing a ZIP without CSV raises ValueError."""
        # Create a ZIP with no CSV
        zip_path = tmp_path / "empty.zip"
        with zipfile.ZipFile(zip_path, 'w') as zf:
            zf.writestr("readme.txt", "no csv here")

        strategy = KlinesStrategy(DataType.SPOT, Interval.MIN_5)
        importer = DataImporter(strategy)

        db_conn_spot.execute("BEGIN TRANSACTION")
        with pytest.raises(ValueError, match="No CSV file found"):
            importer.import_file(db_conn_spot, zip_path, 'BTCUSDT')
        db_conn_spot.execute("ROLLBACK")

    def test_strategy_property(self):
        """Test that importer exposes its strategy."""
        strategy = KlinesStrategy(DataType.SPOT, Interval.MIN_5)
        importer = DataImporter(strategy)

        assert importer.strategy is strategy
        assert importer.strategy.table_name == 'spot'
