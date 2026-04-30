"""
Tests for the Binance pipeline module.

Focuses on unit testing helper functions.
Full integration tests would require mocking network calls.
"""

from datetime import datetime
from types import SimpleNamespace
from unittest.mock import patch

import duckdb
import pytest

from crypto_data.binance_datasets.base import DownloadResult, Period
from crypto_data.binance_pipeline import (
    _period_exists_in_db,
    _process_results,
    _prune_klines_outside_date_range,
    update_binance_market_data,
)
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


class TestProcessResults:
    """Tests for download result import handling."""

    def test_import_exception_rolls_back_and_propagates(self, tmp_path):
        conn = duckdb.connect(":memory:")
        conn.execute("CREATE TABLE imported (symbol VARCHAR, period VARCHAR)")
        file_path = tmp_path / "BTCUSDT-2024-01.zip"
        file_path.write_text("placeholder")
        stats = {"downloaded": 0, "skipped": 0, "failed": 0, "not_found": 0}

        class FailingImporter:
            dataset = SimpleNamespace(table_name="open_interest")

            def import_file(self, conn, file_path, symbol, **kwargs):
                conn.execute("INSERT INTO imported VALUES (?, ?)", [symbol, kwargs["period"]])
                raise RuntimeError("import transaction failed")

        with pytest.raises(RuntimeError, match="import transaction failed"):
            _process_results(
                [
                    DownloadResult(
                        success=True,
                        symbol="BTCUSDT",
                        data_type=DataType.OPEN_INTEREST,
                        period="2024-01",
                        file_path=file_path,
                    )
                ],
                FailingImporter(),
                conn,
                stats,
                "BTCUSDT",
            )

        assert conn.execute("SELECT COUNT(*) FROM imported").fetchone()[0] == 0
        assert not file_path.exists()
        assert stats["downloaded"] == 0
        assert stats["failed"] == 0
        conn.close()

    def test_failed_download_results_update_stats_without_raising(self):
        stats = {"downloaded": 0, "skipped": 0, "failed": 0, "not_found": 0}

        _process_results(
            [
                DownloadResult(
                    success=False,
                    symbol="BTCUSDT",
                    data_type=DataType.OPEN_INTEREST,
                    period="2024-01",
                    error="not_found",
                ),
                DownloadResult(
                    success=False,
                    symbol="BTCUSDT",
                    data_type=DataType.OPEN_INTEREST,
                    period="2024-02",
                    error="network error",
                ),
            ],
            SimpleNamespace(dataset=SimpleNamespace(table_name="open_interest")),
            None,
            stats,
            "BTCUSDT",
        )

        assert stats["not_found"] == 1
        assert stats["failed"] == 1

    def test_successful_result_import_uses_effective_download_symbol(self, tmp_path):
        conn = duckdb.connect(":memory:")
        conn.execute("CREATE TABLE imported (symbol VARCHAR, period VARCHAR)")
        file_path = tmp_path / "1000PEPEUSDT-2024-01.zip"
        file_path.write_text("placeholder")
        stats = {"downloaded": 0, "skipped": 0, "failed": 0, "not_found": 0}

        class RecordingImporter:
            dataset = SimpleNamespace(table_name="open_interest")

            def import_file(self, conn, file_path, symbol, **kwargs):
                conn.execute("INSERT INTO imported VALUES (?, ?)", [symbol, kwargs["period"]])

        _process_results(
            [
                DownloadResult(
                    success=True,
                    symbol="1000PEPEUSDT",
                    data_type=DataType.OPEN_INTEREST,
                    period="2024-01",
                    file_path=file_path,
                )
            ],
            RecordingImporter(),
            conn,
            stats,
            "PEPEUSDT",
        )

        imported_symbol = conn.execute("SELECT symbol FROM imported").fetchone()[0]
        assert imported_symbol == "1000PEPEUSDT"
        assert stats["downloaded"] == 1
        assert not file_path.exists()
        conn.close()


def test_update_binance_market_data_propagates_ingestion_errors(tmp_path):
    def raise_from_ingestion(coro, caller_name):
        coro.close()
        raise RuntimeError("import transaction failed")

    with (
        patch("crypto_data.binance_pipeline.run_async_from_sync") as fake_async,
        patch("crypto_data.binance_pipeline.log_ingestion_summary") as fake_summary,
    ):
        fake_async.side_effect = raise_from_ingestion
        with pytest.raises(RuntimeError, match="import transaction failed"):
            update_binance_market_data(
                db_path=str(tmp_path / "test.db"),
                symbols=["BTCUSDT"],
                data_types=[DataType.OPEN_INTEREST],
                start_date="2024-01-01",
                end_date="2024-01-01",
            )

    fake_summary.assert_not_called()


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

    def test_open_interest_period_requires_complete_5m_daily_coverage(self, db_conn):
        period = Period("2024-01-01", is_monthly=False)
        db_conn.execute("""
            INSERT INTO open_interest
            SELECT
                'binance' AS exchange,
                'BTCUSDT' AS symbol,
                ts AS timestamp
            FROM generate_series(
                TIMESTAMP '2024-01-01 00:00:00',
                TIMESTAMP '2024-01-01 23:55:00',
                INTERVAL 5 MINUTE
            ) AS t(ts)
        """)

        assert _period_exists_in_db(
            conn=db_conn,
            table="open_interest",
            symbol="BTCUSDT",
            interval=None,
            period=period,
        )

    def test_open_interest_10_minute_gap_is_incomplete(self, db_conn):
        period = Period("2024-01-01", is_monthly=False)
        db_conn.execute("""
            INSERT INTO open_interest
            SELECT
                'binance' AS exchange,
                'BTCUSDT' AS symbol,
                ts AS timestamp
            FROM generate_series(
                TIMESTAMP '2024-01-01 00:00:00',
                TIMESTAMP '2024-01-01 23:55:00',
                INTERVAL 5 MINUTE
            ) AS t(ts)
            WHERE ts != TIMESTAMP '2024-01-01 00:05:00'
        """)

        assert not _period_exists_in_db(
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
