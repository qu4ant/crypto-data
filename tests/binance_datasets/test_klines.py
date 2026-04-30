"""
Tests for BinanceKlinesDataset (spot and futures OHLCV data).

Tests initialization, period generation, URL building, CSV parsing,
and schema validation.
"""

import logging
import tempfile
from datetime import datetime
from pathlib import Path

import pandas as pd
import pytest

from crypto_data.binance_datasets import BinanceKlinesDataset
from crypto_data.binance_datasets.base import Period
from crypto_data.enums import DataType, Interval
from crypto_data.schemas import OHLCV_SCHEMA


class TestBinanceKlinesDatasetInit:
    """Tests for BinanceKlinesDataset initialization."""

    def test_spot_dataset(self):
        """Create a spot klines dataset."""
        dataset = BinanceKlinesDataset(DataType.SPOT, Interval.MIN_5)

        assert dataset.data_type == DataType.SPOT
        assert dataset.table_name == "spot"
        assert dataset.interval == Interval.MIN_5

    def test_futures_dataset(self):
        """Create a futures klines dataset."""
        dataset = BinanceKlinesDataset(DataType.FUTURES, Interval.HOUR_1)

        assert dataset.data_type == DataType.FUTURES
        assert dataset.table_name == "futures"
        assert dataset.interval == Interval.HOUR_1

    def test_invalid_data_type_raises(self):
        """Raise ValueError for non-klines data types."""
        with pytest.raises(ValueError, match="only supports SPOT or FUTURES"):
            BinanceKlinesDataset(DataType.OPEN_INTEREST, Interval.MIN_5)

        with pytest.raises(ValueError, match="only supports SPOT or FUTURES"):
            BinanceKlinesDataset(DataType.FUNDING_RATES, Interval.MIN_5)

    def test_is_monthly_true(self):
        """is_monthly property returns True for klines dataset."""
        dataset = BinanceKlinesDataset(DataType.SPOT, Interval.MIN_5)
        assert dataset.is_monthly is True

    def test_default_max_concurrent_20(self):
        """default_max_concurrent returns 20 for klines dataset."""
        dataset = BinanceKlinesDataset(DataType.SPOT, Interval.MIN_5)
        assert dataset.default_max_concurrent == 20


class TestBinanceKlinesDatasetPeriods:
    """Tests for BinanceKlinesDataset period generation."""

    def test_generate_monthly_periods(self):
        """Generate monthly periods for a 3-month range."""
        dataset = BinanceKlinesDataset(DataType.SPOT, Interval.MIN_5)

        start = datetime(2024, 1, 15)
        end = datetime(2024, 3, 20)

        periods = dataset.generate_periods(start, end)

        assert len(periods) == 3
        assert periods[0].value == "2024-01"
        assert periods[0].is_monthly is True
        assert periods[1].value == "2024-02"
        assert periods[1].is_monthly is True
        assert periods[2].value == "2024-03"
        assert periods[2].is_monthly is True

    def test_generate_single_month(self):
        """Generate periods for same-month start/end."""
        dataset = BinanceKlinesDataset(DataType.FUTURES, Interval.HOUR_4)

        start = datetime(2024, 6, 1)
        end = datetime(2024, 6, 30)

        periods = dataset.generate_periods(start, end)

        assert len(periods) == 1
        assert periods[0].value == "2024-06"

    def test_generate_hybrid_periods_uses_daily_only_for_unfinished_month(self):
        """Use monthly files for completed months and daily files for the current month."""
        dataset = BinanceKlinesDataset(
            DataType.SPOT,
            Interval.MIN_5,
            as_of=datetime(2024, 3, 3),
        )

        periods = dataset.generate_periods(
            datetime(2024, 1, 15),
            datetime(2024, 3, 3),
        )

        monthly_periods = [period.value for period in periods if period.is_monthly]
        daily_periods = [period.value for period in periods if not period.is_monthly]

        assert monthly_periods == ["2024-01", "2024-02"]
        assert daily_periods[0] == "2024-03-01"
        assert daily_periods[-1] == "2024-03-02"

    def test_generate_hybrid_periods_for_current_month(self):
        """Generate daily files through the last complete day for the current month."""
        dataset = BinanceKlinesDataset(
            DataType.SPOT,
            Interval.MIN_5,
            as_of=datetime(2024, 3, 4),
        )

        periods = dataset.generate_periods(
            datetime(2024, 1, 15),
            datetime(2024, 3, 3),
        )

        monthly_periods = [period.value for period in periods if period.is_monthly]
        daily_periods = [period.value for period in periods if not period.is_monthly]

        assert monthly_periods == ["2024-01", "2024-02"]
        assert daily_periods == ["2024-03-01", "2024-03-02", "2024-03-03"]

    def test_recent_daily_periods_are_marked_for_refresh(self):
        """The last three available daily files are always refreshed."""
        dataset = BinanceKlinesDataset(
            DataType.SPOT,
            Interval.MIN_5,
            as_of=datetime(2024, 3, 6),
        )

        periods = dataset.generate_periods(
            datetime(2024, 3, 1),
            datetime(2024, 3, 5),
        )

        refresh_periods = [period.value for period in periods if period.replace_existing]

        assert refresh_periods == ["2024-03-03", "2024-03-04", "2024-03-05"]


class TestBinanceKlinesDatasetUrls:
    """Tests for BinanceKlinesDataset URL building."""

    def test_spot_url(self):
        """Build spot klines download URL."""
        dataset = BinanceKlinesDataset(DataType.SPOT, Interval.MIN_5)
        period = Period("2024-01")

        url = dataset.build_download_url(
            base_url="https://data.binance.vision/", symbol="BTCUSDT", period=period
        )

        expected = (
            "https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/5m/BTCUSDT-5m-2024-01.zip"
        )
        assert url == expected

    def test_futures_url(self):
        """Build futures klines download URL."""
        dataset = BinanceKlinesDataset(DataType.FUTURES, Interval.HOUR_1)
        period = Period("2024-06")

        url = dataset.build_download_url(
            base_url="https://data.binance.vision/", symbol="ETHUSDT", period=period
        )

        expected = (
            "https://data.binance.vision/"
            "data/futures/um/monthly/klines/ETHUSDT/1h/ETHUSDT-1h-2024-06.zip"
        )
        assert url == expected

    def test_url_with_explicit_interval(self):
        """Build URL with explicitly provided interval."""
        dataset = BinanceKlinesDataset(DataType.SPOT, Interval.MIN_5)
        period = Period("2024-01")

        url = dataset.build_download_url(
            base_url="https://data.binance.vision/",
            symbol="BTCUSDT",
            period=period,
            interval="1h",  # Override instance interval
        )

        expected = (
            "https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/1h/BTCUSDT-1h-2024-01.zip"
        )
        assert url == expected

    def test_spot_daily_url(self):
        """Build spot daily klines download URL."""
        dataset = BinanceKlinesDataset(DataType.SPOT, Interval.MIN_5)
        period = Period("2024-03-01", is_monthly=False)

        url = dataset.build_download_url(
            base_url="https://data.binance.vision/", symbol="BTCUSDT", period=period
        )

        expected = (
            "https://data.binance.vision/"
            "data/spot/daily/klines/BTCUSDT/5m/BTCUSDT-5m-2024-03-01.zip"
        )
        assert url == expected

    def test_futures_daily_url(self):
        """Build futures daily klines download URL."""
        dataset = BinanceKlinesDataset(DataType.FUTURES, Interval.HOUR_1)
        period = Period("2024-03-01", is_monthly=False)

        url = dataset.build_download_url(
            base_url="https://data.binance.vision/", symbol="ETHUSDT", period=period
        )

        expected = (
            "https://data.binance.vision/"
            "data/futures/um/daily/klines/ETHUSDT/1h/ETHUSDT-1h-2024-03-01.zip"
        )
        assert url == expected

    def test_temp_filename(self):
        """Build temporary filename for download."""
        dataset = BinanceKlinesDataset(DataType.SPOT, Interval.MIN_5)
        period = Period("2024-01")

        filename = dataset.build_temp_filename(symbol="BTCUSDT", period=period)

        assert filename == "BTCUSDT-spot-5m-2024-01.zip"

    def test_temp_filename_futures(self):
        """Build temporary filename for futures download."""
        dataset = BinanceKlinesDataset(DataType.FUTURES, Interval.HOUR_4)
        period = Period("2024-06")

        filename = dataset.build_temp_filename(symbol="ETHUSDT", period=period)

        assert filename == "ETHUSDT-futures-4h-2024-06.zip"


class TestBinanceKlinesDatasetCsvParsing:
    """Tests for BinanceKlinesDataset CSV parsing."""

    def test_parse_csv_without_header(self):
        """Parse CSV file without header (Binance format)."""
        dataset = BinanceKlinesDataset(DataType.SPOT, Interval.MIN_5)

        # Create a temporary CSV file without header
        # Binance klines format: open_time, open, high, low, close, volume,
        # close_time, quote_volume, trades_count, taker_buy_base, taker_buy_quote, ignore
        csv_content = (
            "1704067200000,42000.0,42100.0,41900.0,42050.0,100.5,"
            "1704067499999,4205000.0,500,50.2,2102500.0,0\n"
            "1704067500000,42050.0,42150.0,41950.0,42100.0,120.3,"
            "1704067799999,5063000.0,600,60.1,2531500.0,0\n"
        )

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(csv_content)
            temp_path = Path(f.name)

        try:
            df = dataset.parse_csv(temp_path, "BTCUSDT")

            # Check columns
            expected_cols = [
                "exchange",
                "symbol",
                "interval",
                "timestamp",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "quote_volume",
                "trades_count",
                "taker_buy_base_volume",
                "taker_buy_quote_volume",
            ]
            assert list(df.columns) == expected_cols

            # Check row count
            assert len(df) == 2

            # Check metadata columns
            assert df["exchange"].iloc[0] == "binance"
            assert df["symbol"].iloc[0] == "BTCUSDT"
            assert df["interval"].iloc[0] == "5m"

            # Check timestamp conversion (milliseconds)
            assert df["timestamp"].iloc[0] == pd.Timestamp("2024-01-01 00:05:00")

            # Check OHLCV values
            assert df["open"].iloc[0] == 42000.0
            assert df["high"].iloc[0] == 42100.0
            assert df["low"].iloc[0] == 41900.0
            assert df["close"].iloc[0] == 42050.0
            assert df["volume"].iloc[0] == 100.5
        finally:
            temp_path.unlink()

    def test_parse_csv_with_header(self):
        """Parse CSV file with header."""
        dataset = BinanceKlinesDataset(DataType.FUTURES, Interval.HOUR_1)

        csv_content = (
            "open_time,open,high,low,close,volume,close_time,quote_volume,"
            "trades_count,taker_buy_base_volume,taker_buy_quote_volume,ignore\n"
            "1704067200000,42000.0,42100.0,41900.0,42050.0,100.5,"
            "1704070799999,4205000.0,500,50.2,2102500.0,0\n"
        )

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(csv_content)
            temp_path = Path(f.name)

        try:
            df = dataset.parse_csv(temp_path, "ETHUSDT")

            assert len(df) == 1
            assert df["exchange"].iloc[0] == "binance"
            assert df["symbol"].iloc[0] == "ETHUSDT"
            assert df["interval"].iloc[0] == "1h"
        finally:
            temp_path.unlink()

    def test_parse_csv_microseconds(self):
        """Parse CSV with microsecond timestamps (>= 5e12)."""
        dataset = BinanceKlinesDataset(DataType.SPOT, Interval.MIN_5)

        # Microsecond timestamp (>= 5e12)
        csv_content = (
            "1704067200000000,42000.0,42100.0,41900.0,42050.0,100.5,"
            "1704067499999000,4205000.0,500,50.2,2102500.0,0\n"
        )

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(csv_content)
            temp_path = Path(f.name)

        try:
            df = dataset.parse_csv(temp_path, "BTCUSDT")

            # Should correctly convert microseconds
            assert df["timestamp"].iloc[0] == pd.Timestamp("2024-01-01 00:05:00")
        finally:
            temp_path.unlink()

    def test_parse_csv_drops_duplicates(self, caplog):
        """Parse CSV and drop duplicate timestamps."""
        dataset = BinanceKlinesDataset(DataType.SPOT, Interval.MIN_5)
        caplog.set_level(logging.WARNING)

        # Duplicate rows (same close_time)
        csv_content = (
            "1704067200000,42000.0,42100.0,41900.0,42050.0,100.5,"
            "1704067499999,4205000.0,500,50.2,2102500.0,0\n"
            "1704067200000,42000.0,42100.0,41900.0,42050.0,100.5,"
            "1704067499999,4205000.0,500,50.2,2102500.0,0\n"
        )

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(csv_content)
            temp_path = Path(f.name)

        try:
            df = dataset.parse_csv(temp_path, "BTCUSDT")

            # Should have only 1 row after deduplication
            assert len(df) == 1
            assert "Dropped 1 duplicate kline rows" in caplog.text
        finally:
            temp_path.unlink()


class TestBinanceKlinesDatasetSchema:
    """Tests for BinanceKlinesDataset schema."""

    def test_get_schema_returns_ohlcv_schema(self):
        """get_schema() returns OHLCV_SCHEMA."""
        dataset = BinanceKlinesDataset(DataType.SPOT, Interval.MIN_5)
        schema = dataset.get_schema()

        assert schema is OHLCV_SCHEMA
