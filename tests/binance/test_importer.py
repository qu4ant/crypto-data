"""
Tests for BinanceDuckDBImporter

Tests the generic data importer with different strategies (klines, open_interest,
funding_rates) using in-memory DuckDB and test ZIP files.
"""

import zipfile
from datetime import datetime

import duckdb
import pytest

from crypto_data.binance_datasets.funding_rates import BinanceFundingRatesDataset
from crypto_data.binance_datasets.klines import BinanceKlinesDataset
from crypto_data.binance_datasets.open_interest import BinanceOpenInterestDataset
from crypto_data.binance_importer import BinanceDuckDBImporter
from crypto_data.enums import DataType, Interval

# -----------------------------------------------------------------------------
# Fixtures
# -----------------------------------------------------------------------------


@pytest.fixture
def db_conn_spot():
    """Create in-memory DuckDB with spot table schema."""
    conn = duckdb.connect(":memory:")
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
    conn = duckdb.connect(":memory:")
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
    conn = duckdb.connect(":memory:")
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
    conn = duckdb.connect(":memory:")
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
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.write(csv_path, csv_path.name)

    csv_path.unlink()  # Clean up CSV, keep only ZIP
    return zip_path


@pytest.fixture
def klines_zip_with_duplicate_rows(tmp_path):
    """Create a kline ZIP whose CSV contains duplicate primary-key rows."""
    csv_content = """1704067199999,42000.0,42100.0,41900.0,42050.0,100.5,1704067199999,4215000.0,500,50.2,2107500.0,0
1704067199999,42000.0,42100.0,41900.0,42050.0,100.5,1704067199999,4215000.0,500,50.2,2107500.0,0"""

    csv_path = tmp_path / "BTCUSDT-5m-duplicate-2024-01.csv"
    csv_path.write_text(csv_content)

    zip_path = tmp_path / "BTCUSDT-5m-duplicate-2024-01.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.write(csv_path, csv_path.name)

    csv_path.unlink()
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
    with zipfile.ZipFile(zip_path, "w") as zf:
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
    with zipfile.ZipFile(zip_path, "w") as zf:
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
    with zipfile.ZipFile(zip_path, "w") as zf:
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
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.write(csv_path, csv_path.name)

    csv_path.unlink()
    return zip_path


# -----------------------------------------------------------------------------
# TestBinanceDuckDBImporterKlines
# -----------------------------------------------------------------------------


class TestBinanceDuckDBImporterKlines:
    """Test BinanceDuckDBImporter with BinanceKlinesDataset."""

    def test_import_spot_klines_no_header(self, db_conn_spot, klines_zip):
        """Test importing spot klines CSV without header."""
        dataset = BinanceKlinesDataset(DataType.SPOT, Interval.MIN_5)
        importer = BinanceDuckDBImporter(dataset)

        db_conn_spot.execute("BEGIN TRANSACTION")
        rows = importer.import_file(db_conn_spot, klines_zip, "BTCUSDT")
        db_conn_spot.execute("COMMIT")

        assert rows == 3

        # Verify data was inserted
        result = db_conn_spot.execute(
            "SELECT COUNT(*) FROM spot WHERE symbol = 'BTCUSDT'"
        ).fetchone()
        assert result[0] == 3

        # Verify exchange column
        result = db_conn_spot.execute("SELECT DISTINCT exchange FROM spot").fetchone()
        assert result[0] == "binance"

        # Verify interval column
        result = db_conn_spot.execute("SELECT DISTINCT interval FROM spot").fetchone()
        assert result[0] == "5m"

    def test_duplicate_csv_rows_are_reported(self, db_conn_spot, klines_zip_with_duplicate_rows):
        """Duplicate rows dropped before insert are exposed as import anomalies."""
        dataset = BinanceKlinesDataset(DataType.SPOT, Interval.MIN_5)
        importer = BinanceDuckDBImporter(dataset)

        db_conn_spot.execute("BEGIN TRANSACTION")
        rows = importer.import_file(db_conn_spot, klines_zip_with_duplicate_rows, "BTCUSDT")
        db_conn_spot.execute("COMMIT")

        assert rows == 1
        assert importer.last_import_anomalies[0]["check_name"] == "import_dropped_duplicate_rows"
        assert importer.last_import_anomalies[0]["count"] == 1

    def test_import_spot_klines_with_header(self, db_conn_spot, klines_zip_with_header):
        """Test importing spot klines CSV with header (auto-detect)."""
        dataset = BinanceKlinesDataset(DataType.SPOT, Interval.MIN_5)
        importer = BinanceDuckDBImporter(dataset)

        db_conn_spot.execute("BEGIN TRANSACTION")
        rows = importer.import_file(db_conn_spot, klines_zip_with_header, "BTCUSDT")
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
        dataset = BinanceKlinesDataset(DataType.FUTURES, Interval.MIN_5)
        importer = BinanceDuckDBImporter(dataset)

        db_conn_futures.execute("BEGIN TRANSACTION")
        rows = importer.import_file(db_conn_futures, klines_zip, "BTCUSDT")
        db_conn_futures.execute("COMMIT")

        assert rows == 3

        result = db_conn_futures.execute("SELECT COUNT(*) FROM futures").fetchone()
        assert result[0] == 3

    def test_duplicate_import_returns_zero(self, db_conn_spot, klines_zip):
        """Test that importing duplicate data returns 0."""
        dataset = BinanceKlinesDataset(DataType.SPOT, Interval.MIN_5)
        importer = BinanceDuckDBImporter(dataset)

        # First import
        db_conn_spot.execute("BEGIN TRANSACTION")
        rows1 = importer.import_file(db_conn_spot, klines_zip, "BTCUSDT")
        db_conn_spot.execute("COMMIT")
        assert rows1 == 3

        # Second import - should return 0 (duplicate)
        db_conn_spot.execute("BEGIN TRANSACTION")
        rows2 = importer.import_file(db_conn_spot, klines_zip, "BTCUSDT")
        db_conn_spot.execute("COMMIT")
        assert rows2 == 0

    def test_partial_duplicate_import_inserts_missing_rows(self, db_conn_spot, klines_zip):
        """Test that re-importing a partial period inserts only missing rows."""
        dataset = BinanceKlinesDataset(DataType.SPOT, Interval.MIN_5)
        importer = BinanceDuckDBImporter(dataset)

        db_conn_spot.execute("""
            INSERT INTO spot VALUES (
                'binance', 'BTCUSDT', '5m', '2024-01-01 00:00:00',
                42000.0, 42100.0, 41900.0, 42050.0, 100.5,
                4215000.0, 500, 50.2, 2107500.0
            )
        """)

        db_conn_spot.execute("BEGIN TRANSACTION")
        rows = importer.import_file(db_conn_spot, klines_zip, "BTCUSDT")
        db_conn_spot.execute("COMMIT")

        assert rows == 2

        result = db_conn_spot.execute(
            "SELECT COUNT(*) FROM spot WHERE symbol = 'BTCUSDT'"
        ).fetchone()
        assert result[0] == 3

    def test_daily_replace_refreshes_existing_rows(self, db_conn_spot, klines_zip):
        """Daily refresh should delete the day window before inserting validated rows."""
        dataset = BinanceKlinesDataset(DataType.SPOT, Interval.MIN_5)
        importer = BinanceDuckDBImporter(dataset)

        db_conn_spot.execute("""
            INSERT INTO spot VALUES (
                'binance', 'BTCUSDT', '5m', '2024-01-01 00:05:00',
                1.0, 2.0, 0.5, 1.5, 10.0,
                15.0, 1, 5.0, 7.5
            )
        """)

        db_conn_spot.execute("BEGIN TRANSACTION")
        rows = importer.import_file(
            db_conn_spot,
            klines_zip,
            "BTCUSDT",
            period="2024-01-01",
            replace_existing=True,
        )
        db_conn_spot.execute("COMMIT")

        assert rows == 3

        count = db_conn_spot.execute(
            "SELECT COUNT(*) FROM spot WHERE symbol = 'BTCUSDT'"
        ).fetchone()[0]
        refreshed_open = db_conn_spot.execute("""
            SELECT open FROM spot
            WHERE symbol = 'BTCUSDT' AND timestamp = '2024-01-01 00:05:00'
        """).fetchone()[0]

        assert count == 3
        assert refreshed_open == 42050.0

    def test_import_klines_filters_to_requested_close_window(self, db_conn_spot, klines_zip):
        """Kline imports should not insert candles outside the requested date window."""
        dataset = BinanceKlinesDataset(DataType.SPOT, Interval.MIN_5)
        importer = BinanceDuckDBImporter(dataset)

        db_conn_spot.execute("BEGIN TRANSACTION")
        rows = importer.import_file(
            db_conn_spot,
            klines_zip,
            "BTCUSDT",
            window_start=datetime(2024, 1, 1, 0, 0),
            window_end=datetime(2024, 1, 1, 0, 5),
        )
        db_conn_spot.execute("COMMIT")

        assert rows == 1

        timestamps = db_conn_spot.execute(
            "SELECT timestamp FROM spot WHERE symbol = 'BTCUSDT' ORDER BY timestamp"
        ).fetchall()
        assert timestamps == [(datetime(2024, 1, 1, 0, 5),)]

    def test_csv_cleanup_after_import(self, db_conn_spot, klines_zip):
        """Test that CSV file is cleaned up after import."""
        dataset = BinanceKlinesDataset(DataType.SPOT, Interval.MIN_5)
        importer = BinanceDuckDBImporter(dataset)

        # Get the expected CSV path
        csv_path = klines_zip.parent / "BTCUSDT-5m-2024-01.csv"

        db_conn_spot.execute("BEGIN TRANSACTION")
        importer.import_file(db_conn_spot, klines_zip, "BTCUSDT")
        db_conn_spot.execute("COMMIT")

        # CSV should be deleted
        assert not csv_path.exists()


# -----------------------------------------------------------------------------
# TestBinanceDuckDBImporterOpenInterest
# -----------------------------------------------------------------------------


class TestBinanceDuckDBImporterOpenInterest:
    """Test BinanceDuckDBImporter with BinanceOpenInterestDataset."""

    def test_import_open_interest(self, db_conn_open_interest, open_interest_zip):
        """Test importing open interest data."""
        dataset = BinanceOpenInterestDataset()
        importer = BinanceDuckDBImporter(dataset)

        db_conn_open_interest.execute("BEGIN TRANSACTION")
        rows = importer.import_file(db_conn_open_interest, open_interest_zip, "BTCUSDT")
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
        dataset = BinanceOpenInterestDataset()
        importer = BinanceDuckDBImporter(dataset)

        db_conn_open_interest.execute("BEGIN TRANSACTION")
        rows = importer.import_file(db_conn_open_interest, open_interest_zip_with_zeros, "BTCUSDT")
        db_conn_open_interest.execute("COMMIT")

        # Only 2 rows (the one with 0 is filtered)
        assert rows == 2
        assert (
            importer.last_import_anomalies[0]["check_name"]
            == "import_dropped_zero_open_interest_rows"
        )
        assert importer.last_import_anomalies[0]["count"] == 1

    def test_symbol_override(self, db_conn_open_interest, open_interest_zip):
        """Test that symbol is overridden (for 1000-prefix normalization)."""
        dataset = BinanceOpenInterestDataset()
        importer = BinanceDuckDBImporter(dataset)

        # Import with a different symbol than what's in the CSV
        db_conn_open_interest.execute("BEGIN TRANSACTION")
        rows = importer.import_file(db_conn_open_interest, open_interest_zip, "PEPEUSDT")
        db_conn_open_interest.execute("COMMIT")

        # Symbol should be PEPEUSDT, not BTCUSDT from CSV
        result = db_conn_open_interest.execute(
            "SELECT DISTINCT symbol FROM open_interest"
        ).fetchone()
        assert result[0] == "PEPEUSDT"


# -----------------------------------------------------------------------------
# TestBinanceDuckDBImporterFundingRates
# -----------------------------------------------------------------------------


class TestBinanceDuckDBImporterFundingRates:
    """Test BinanceDuckDBImporter with BinanceFundingRatesDataset."""

    def test_import_funding_rates(self, db_conn_funding_rates, funding_rates_zip):
        """Test importing funding rate data."""
        dataset = BinanceFundingRatesDataset()
        importer = BinanceDuckDBImporter(dataset)

        db_conn_funding_rates.execute("BEGIN TRANSACTION")
        rows = importer.import_file(db_conn_funding_rates, funding_rates_zip, "BTCUSDT")
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
        dataset = BinanceFundingRatesDataset()
        importer = BinanceDuckDBImporter(dataset)

        db_conn_funding_rates.execute("BEGIN TRANSACTION")
        importer.import_file(db_conn_funding_rates, funding_rates_zip, "BTCUSDT")
        db_conn_funding_rates.execute("COMMIT")

        # Check first timestamp (1704067200000 ms = 2024-01-01 00:00:00)
        result = db_conn_funding_rates.execute(
            "SELECT timestamp FROM funding_rates ORDER BY timestamp LIMIT 1"
        ).fetchone()
        assert str(result[0]).startswith("2024-01-01")


# -----------------------------------------------------------------------------
# TestBinanceDuckDBImporterEdgeCases
# -----------------------------------------------------------------------------


class TestBinanceDuckDBImporterEdgeCases:
    """Test edge cases and error handling."""

    def test_no_csv_in_zip_raises_error(self, tmp_path, db_conn_spot):
        """Test that importing a ZIP without CSV raises ValueError."""
        # Create a ZIP with no CSV
        zip_path = tmp_path / "empty.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("readme.txt", "no csv here")

        dataset = BinanceKlinesDataset(DataType.SPOT, Interval.MIN_5)
        importer = BinanceDuckDBImporter(dataset)

        db_conn_spot.execute("BEGIN TRANSACTION")
        with pytest.raises(ValueError, match="No CSV file found"):
            importer.import_file(db_conn_spot, zip_path, "BTCUSDT")
        db_conn_spot.execute("ROLLBACK")

    def test_dataset_property(self):
        """Test that importer exposes its dataset."""
        dataset = BinanceKlinesDataset(DataType.SPOT, Interval.MIN_5)
        importer = BinanceDuckDBImporter(dataset)

        assert importer.dataset is dataset
        assert importer.dataset.table_name == "spot"
