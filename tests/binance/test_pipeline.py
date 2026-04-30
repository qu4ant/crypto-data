"""
Tests for the Binance pipeline module.

Focuses on unit testing helper functions.
Full integration tests would require mocking network calls.
"""

from datetime import datetime

import duckdb
import pytest

from crypto_data.binance_datasets.base import Period
from crypto_data.binance_pipeline import _period_exists_in_db, _prune_klines_outside_date_range
from crypto_data.enums import DataType, Interval
from crypto_data.utils.dates import parse_date_range


class TestParseDateRange:
    """Tests for parse_date_range helper function."""

    def test_valid_dates(self):
        """Test parsing valid date strings."""
        start, end = parse_date_range("2024-01-01", "2024-12-31")

        assert start == datetime(2024, 1, 1)
        assert end == datetime(2024, 12, 31)

    def test_valid_dates_same_day(self):
        """Test parsing when start and end are the same day."""
        start, end = parse_date_range("2024-06-15", "2024-06-15")

        assert start == datetime(2024, 6, 15)
        assert end == datetime(2024, 6, 15)

    def test_invalid_start_date_raises(self):
        """Test that invalid start_date format raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            parse_date_range("01-01-2024", "2024-12-31")

        assert "Invalid start_date format" in str(exc_info.value)
        assert "01-01-2024" in str(exc_info.value)
        assert "expected YYYY-MM-DD" in str(exc_info.value)

    def test_invalid_end_date_raises(self):
        """Test that invalid end_date format raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            parse_date_range("2024-01-01", "12/31/2024")

        assert "Invalid end_date format" in str(exc_info.value)
        assert "12/31/2024" in str(exc_info.value)
        assert "expected YYYY-MM-DD" in str(exc_info.value)

    def test_start_after_end_raises(self):
        """Test that start_date after end_date raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            parse_date_range("2024-12-31", "2024-01-01")

        assert "cannot be after" in str(exc_info.value)
        assert "2024-12-31" in str(exc_info.value)
        assert "2024-01-01" in str(exc_info.value)

    def test_invalid_date_values_raises(self):
        """Test that invalid date values (e.g., month 13) raise ValueError."""
        with pytest.raises(ValueError) as exc_info:
            parse_date_range("2024-13-01", "2024-12-31")

        assert "Invalid start_date format" in str(exc_info.value)

    def test_empty_string_raises(self):
        """Test that empty strings raise ValueError."""
        with pytest.raises(ValueError) as exc_info:
            parse_date_range("", "2024-12-31")

        assert "Invalid start_date format" in str(exc_info.value)

    def test_none_values_raise(self):
        """Test that None values raise appropriate errors."""
        with pytest.raises((ValueError, TypeError)):
            parse_date_range(None, "2024-12-31")

        with pytest.raises((ValueError, TypeError)):
            parse_date_range("2024-01-01", None)


class TestPeriodExistsInDb:
    """Tests for _period_exists_in_db completeness logic."""

    @pytest.fixture
    def db_conn(self):
        conn = duckdb.connect(":memory:")
        conn.execute("""
            CREATE TABLE spot (
                exchange VARCHAR,
                symbol VARCHAR,
                interval VARCHAR,
                timestamp TIMESTAMP
            )
        """)
        conn.execute("""
            CREATE TABLE open_interest (
                exchange VARCHAR,
                symbol VARCHAR,
                timestamp TIMESTAMP
            )
        """)
        conn.execute("""
            CREATE TABLE funding_rates (
                exchange VARCHAR,
                symbol VARCHAR,
                timestamp TIMESTAMP
            )
        """)
        try:
            yield conn
        finally:
            conn.close()

    def test_partial_contiguous_klines_period_is_incomplete(self, db_conn):
        period = Period("2024-01", is_monthly=True)
        # A contiguous sub-range in the month is not enough for full coverage.
        db_conn.execute("""
            INSERT INTO spot VALUES
            ('binance', 'BTCUSDT', '5m', '2024-01-01 00:05:00'),
            ('binance', 'BTCUSDT', '5m', '2024-01-01 00:10:00'),
            ('binance', 'BTCUSDT', '5m', '2024-01-01 00:15:00')
        """)

        assert not _period_exists_in_db(
            conn=db_conn,
            table="spot",
            symbol="BTCUSDT",
            interval="5m",
            period=period,
        )

    def test_full_klines_period_is_complete(self, db_conn):
        period = Period("2024-01", is_monthly=True)
        # Daily close keys for a full January monthly window:
        # 2024-01-02 ... 2024-02-01 (31 rows, contiguous).
        db_conn.execute("""
            INSERT INTO spot
            SELECT
                'binance' AS exchange,
                'BTCUSDT' AS symbol,
                '1d' AS interval,
                ts AS timestamp
            FROM generate_series(
                TIMESTAMP '2024-01-02 00:00:00',
                TIMESTAMP '2024-02-01 00:00:00',
                INTERVAL 1 DAY
            ) AS t(ts)
        """)

        assert _period_exists_in_db(
            conn=db_conn,
            table="spot",
            symbol="BTCUSDT",
            interval="1d",
            period=period,
        )

    def test_prune_klines_removes_stale_rows_outside_requested_window(self, db_conn):
        db_conn.execute("""
            INSERT INTO spot VALUES
            ('binance', 'BTCUSDT', '4h', '2024-01-01 00:00:00'),
            ('binance', 'BTCUSDT', '4h', '2024-01-01 04:00:00'),
            ('binance', 'BTCUSDT', '4h', '2024-02-01 00:00:00'),
            ('binance', 'BTCUSDT', '4h', '2024-02-01 04:00:00'),
            ('binance', 'ETHUSDT', '4h', '2024-02-01 04:00:00')
        """)

        _prune_klines_outside_date_range(
            conn=db_conn,
            symbols=["BTCUSDT"],
            data_types=[DataType.SPOT],
            interval=Interval.HOUR_4,
            start=datetime(2024, 1, 1),
            end=datetime(2024, 1, 31),
        )

        btc_timestamps = db_conn.execute("""
            SELECT timestamp
            FROM spot
            WHERE symbol = 'BTCUSDT'
            ORDER BY timestamp
        """).fetchall()
        eth_count = db_conn.execute(
            "SELECT COUNT(*) FROM spot WHERE symbol = 'ETHUSDT'"
        ).fetchone()[0]

        assert btc_timestamps == [
            (datetime(2024, 1, 1, 4, 0),),
            (datetime(2024, 2, 1, 0, 0),),
        ]
        assert eth_count == 1

    def test_missing_period_end_close_is_incomplete(self, db_conn):
        period = Period("2024-01", is_monthly=True)
        # Missing the final Jan 31 candle close at 2024-02-01 00:00:00.
        db_conn.execute("""
            INSERT INTO spot
            SELECT
                'binance' AS exchange,
                'BTCUSDT' AS symbol,
                '1d' AS interval,
                ts AS timestamp
            FROM generate_series(
                TIMESTAMP '2024-01-02 00:00:00',
                TIMESTAMP '2024-01-31 00:00:00',
                INTERVAL 1 DAY
            ) AS t(ts)
        """)

        assert not _period_exists_in_db(
            conn=db_conn,
            table="spot",
            symbol="BTCUSDT",
            interval="1d",
            period=period,
        )

    def test_gapped_klines_period_is_incomplete(self, db_conn):
        period = Period("2024-01", is_monthly=True)
        # Gap at 00:10.
        db_conn.execute("""
            INSERT INTO spot VALUES
            ('binance', 'BTCUSDT', '5m', '2024-01-01 00:05:00'),
            ('binance', 'BTCUSDT', '5m', '2024-01-01 00:15:00')
        """)

        assert not _period_exists_in_db(
            conn=db_conn,
            table="spot",
            symbol="BTCUSDT",
            interval="5m",
            period=period,
        )

    def test_open_interest_period_requires_complete_daily_coverage(self, db_conn):
        period = Period("2024-01-01", is_monthly=False)
        db_conn.execute("""
            INSERT INTO open_interest VALUES
            ('binance', 'BTCUSDT', '2024-01-01 00:00:00'),
            ('binance', 'BTCUSDT', '2024-01-01 01:00:00'),
            ('binance', 'BTCUSDT', '2024-01-01 02:00:00'),
            ('binance', 'BTCUSDT', '2024-01-01 03:00:00'),
            ('binance', 'BTCUSDT', '2024-01-01 04:00:00'),
            ('binance', 'BTCUSDT', '2024-01-01 05:00:00'),
            ('binance', 'BTCUSDT', '2024-01-01 06:00:00'),
            ('binance', 'BTCUSDT', '2024-01-01 07:00:00'),
            ('binance', 'BTCUSDT', '2024-01-01 08:00:00'),
            ('binance', 'BTCUSDT', '2024-01-01 09:00:00'),
            ('binance', 'BTCUSDT', '2024-01-01 10:00:00'),
            ('binance', 'BTCUSDT', '2024-01-01 11:00:00'),
            ('binance', 'BTCUSDT', '2024-01-01 12:00:00'),
            ('binance', 'BTCUSDT', '2024-01-01 13:00:00'),
            ('binance', 'BTCUSDT', '2024-01-01 14:00:00'),
            ('binance', 'BTCUSDT', '2024-01-01 15:00:00'),
            ('binance', 'BTCUSDT', '2024-01-01 16:00:00'),
            ('binance', 'BTCUSDT', '2024-01-01 17:00:00'),
            ('binance', 'BTCUSDT', '2024-01-01 18:00:00'),
            ('binance', 'BTCUSDT', '2024-01-01 19:00:00'),
            ('binance', 'BTCUSDT', '2024-01-01 20:00:00'),
            ('binance', 'BTCUSDT', '2024-01-01 21:00:00'),
            ('binance', 'BTCUSDT', '2024-01-01 22:00:00'),
            ('binance', 'BTCUSDT', '2024-01-01 23:00:00')
        """)

        assert _period_exists_in_db(
            conn=db_conn,
            table="open_interest",
            symbol="BTCUSDT",
            interval=None,
            period=period,
        )

    def test_open_interest_single_row_period_is_incomplete(self, db_conn):
        period = Period("2024-01-01", is_monthly=False)
        db_conn.execute("""
            INSERT INTO open_interest VALUES
            ('binance', 'BTCUSDT', '2024-01-01 00:00:00')
        """)

        assert not _period_exists_in_db(
            conn=db_conn,
            table="open_interest",
            symbol="BTCUSDT",
            interval=None,
            period=period,
        )

    def test_funding_rates_period_requires_complete_monthly_coverage(self, db_conn):
        period = Period("2024-01", is_monthly=True)
        db_conn.execute("""
            INSERT INTO funding_rates VALUES
            ('binance', 'BTCUSDT', '2024-01-01 00:00:00'),
            ('binance', 'BTCUSDT', '2024-01-01 08:00:00'),
            ('binance', 'BTCUSDT', '2024-01-01 16:00:00'),
            ('binance', 'BTCUSDT', '2024-01-02 00:00:00'),
            ('binance', 'BTCUSDT', '2024-01-31 08:00:00'),
            ('binance', 'BTCUSDT', '2024-01-31 16:00:00')
        """)

        # This has period-edge rows but a large internal gap, so it should be
        # considered incomplete and re-downloaded.
        assert not _period_exists_in_db(
            conn=db_conn,
            table="funding_rates",
            symbol="BTCUSDT",
            interval=None,
            period=period,
        )

        db_conn.execute("DELETE FROM funding_rates")
        db_conn.execute("""
            INSERT INTO funding_rates
            SELECT
                'binance' AS exchange,
                'BTCUSDT' AS symbol,
                ts AS timestamp
            FROM generate_series(
                TIMESTAMP '2024-01-01 00:00:00',
                TIMESTAMP '2024-01-31 16:00:00',
                INTERVAL 8 HOUR
            ) AS t(ts)
        """)

        assert _period_exists_in_db(
            conn=db_conn,
            table="funding_rates",
            symbol="BTCUSDT",
            interval=None,
            period=period,
        )
