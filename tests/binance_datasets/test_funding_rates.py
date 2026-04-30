"""
Tests for BinanceFundingRatesDataset (monthly funding rate data).

Tests initialization, period generation, URL building, CSV parsing,
and schema validation.
"""

import logging
import tempfile
from datetime import datetime
from pathlib import Path

import pandas as pd

from crypto_data.binance_datasets import BinanceFundingRatesDataset
from crypto_data.binance_datasets.base import Period
from crypto_data.enums import DataType
from crypto_data.schemas import FUNDING_RATES_SCHEMA


class TestBinanceFundingRatesDatasetInit:
    """Tests for BinanceFundingRatesDataset initialization."""

    def test_data_type(self):
        """data_type property returns FUNDING_RATES."""
        dataset = BinanceFundingRatesDataset()
        assert dataset.data_type == DataType.FUNDING_RATES

    def test_table_name(self):
        """table_name property returns 'funding_rates'."""
        dataset = BinanceFundingRatesDataset()
        assert dataset.table_name == "funding_rates"

    def test_is_monthly_true(self):
        """is_monthly property returns True for monthly data."""
        dataset = BinanceFundingRatesDataset()
        assert dataset.is_monthly is True

    def test_default_max_concurrent_50(self):
        """default_max_concurrent returns 50 for monthly metrics."""
        dataset = BinanceFundingRatesDataset()
        assert dataset.default_max_concurrent == 50


class TestBinanceFundingRatesDatasetPeriods:
    """Tests for BinanceFundingRatesDataset period generation."""

    def test_generate_monthly_periods(self):
        """Generate monthly periods for a 3-month range."""
        dataset = BinanceFundingRatesDataset()

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
        dataset = BinanceFundingRatesDataset()

        start = datetime(2024, 6, 1)
        end = datetime(2024, 6, 30)

        periods = dataset.generate_periods(start, end)

        assert len(periods) == 1
        assert periods[0].value == "2024-06"
        assert periods[0].is_monthly is True

    def test_generate_cross_year_periods(self):
        """Generate monthly periods crossing year boundary."""
        dataset = BinanceFundingRatesDataset()

        start = datetime(2023, 11, 15)
        end = datetime(2024, 2, 10)

        periods = dataset.generate_periods(start, end)

        assert len(periods) == 4
        assert periods[0].value == "2023-11"
        assert periods[1].value == "2023-12"
        assert periods[2].value == "2024-01"
        assert periods[3].value == "2024-02"


class TestBinanceFundingRatesDatasetUrls:
    """Tests for BinanceFundingRatesDataset URL building."""

    def test_funding_url(self):
        """Build funding rate download URL."""
        dataset = BinanceFundingRatesDataset()
        period = Period("2024-01", is_monthly=True)

        url = dataset.build_download_url(
            base_url="https://data.binance.vision/", symbol="BTCUSDT", period=period
        )

        expected = (
            "https://data.binance.vision/"
            "data/futures/um/monthly/fundingRate/BTCUSDT/BTCUSDT-fundingRate-2024-01.zip"
        )
        assert url == expected

    def test_url_ignores_interval(self):
        """URL building ignores interval parameter."""
        dataset = BinanceFundingRatesDataset()
        period = Period("2024-06", is_monthly=True)

        url = dataset.build_download_url(
            base_url="https://data.binance.vision/",
            symbol="ETHUSDT",
            period=period,
            interval="5m",  # Should be ignored
        )

        expected = (
            "https://data.binance.vision/"
            "data/futures/um/monthly/fundingRate/ETHUSDT/ETHUSDT-fundingRate-2024-06.zip"
        )
        assert url == expected

    def test_temp_filename(self):
        """Build temporary filename for download."""
        dataset = BinanceFundingRatesDataset()
        period = Period("2024-01", is_monthly=True)

        filename = dataset.build_temp_filename(symbol="BTCUSDT", period=period)

        assert filename == "BTCUSDT-fundingRate-2024-01.zip"

    def test_temp_filename_ignores_interval(self):
        """Temp filename ignores interval parameter."""
        dataset = BinanceFundingRatesDataset()
        period = Period("2024-06", is_monthly=True)

        filename = dataset.build_temp_filename(
            symbol="ETHUSDT",
            period=period,
            interval="1h",  # Should be ignored
        )

        assert filename == "ETHUSDT-fundingRate-2024-06.zip"


class TestBinanceFundingRatesDatasetCsvParsing:
    """Tests for BinanceFundingRatesDataset CSV parsing."""

    def test_parse_csv(self):
        """Parse funding rate CSV file with header and millisecond timestamps."""
        dataset = BinanceFundingRatesDataset()

        # Funding rate CSV format (always has header, calc_time in milliseconds)
        # 1705312800000 = 2024-01-15 10:00:00 UTC
        # 1705341600000 = 2024-01-15 18:00:00 UTC
        csv_content = (
            "symbol,calc_time,funding_interval_hours,last_funding_rate\n"
            "BTCUSDT,1705312800000,8,0.0001\n"
            "BTCUSDT,1705341600000,8,0.00015\n"
        )

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(csv_content)
            temp_path = Path(f.name)

        try:
            df = dataset.parse_csv(temp_path, "BTCUSDT")

            # Check columns
            expected_cols = ["exchange", "symbol", "timestamp", "funding_rate"]
            assert list(df.columns) == expected_cols

            # Check row count
            assert len(df) == 2

            # Check metadata columns
            assert df["exchange"].iloc[0] == "binance"
            assert df["symbol"].iloc[0] == "BTCUSDT"

            # Check timestamp conversion (milliseconds to datetime)
            assert df["timestamp"].iloc[0] == pd.Timestamp("2024-01-15 10:00:00")
            assert df["timestamp"].iloc[1] == pd.Timestamp("2024-01-15 18:00:00")

            # Check funding_rate values
            assert df["funding_rate"].iloc[0] == 0.0001
            assert df["funding_rate"].iloc[1] == 0.00015
        finally:
            temp_path.unlink()

    def test_parse_csv_symbol_override(self):
        """Parse CSV with symbol override (1000-prefix normalization)."""
        dataset = BinanceFundingRatesDataset()

        csv_content = (
            "symbol,calc_time,funding_interval_hours,last_funding_rate\n"
            "1000PEPEUSDT,1705312800000,8,0.0002\n"
        )

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(csv_content)
            temp_path = Path(f.name)

        try:
            # Pass normalized symbol (without 1000 prefix)
            df = dataset.parse_csv(temp_path, "PEPEUSDT")

            # Symbol should be overridden to normalized version
            assert df["symbol"].iloc[0] == "PEPEUSDT"
        finally:
            temp_path.unlink()

    def test_parse_csv_drops_duplicates(self, caplog):
        """Parse CSV and drop duplicate timestamps."""
        dataset = BinanceFundingRatesDataset()
        caplog.set_level(logging.WARNING)

        csv_content = (
            "symbol,calc_time,funding_interval_hours,last_funding_rate\n"
            "BTCUSDT,1705312800000,8,0.0001\n"
            "BTCUSDT,1705312800000,8,0.0001\n"
        )

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(csv_content)
            temp_path = Path(f.name)

        try:
            df = dataset.parse_csv(temp_path, "BTCUSDT")

            # Should have only 1 row after deduplication
            assert len(df) == 1
            assert "Dropped 1 duplicate funding rate rows" in caplog.text
        finally:
            temp_path.unlink()

    def test_parse_csv_negative_funding_rate(self):
        """Parse CSV with negative funding rates (shorts pay longs)."""
        dataset = BinanceFundingRatesDataset()

        csv_content = (
            "symbol,calc_time,funding_interval_hours,last_funding_rate\n"
            "BTCUSDT,1705312800000,8,-0.0003\n"
        )

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(csv_content)
            temp_path = Path(f.name)

        try:
            df = dataset.parse_csv(temp_path, "BTCUSDT")

            # Negative funding rates should be preserved
            assert df["funding_rate"].iloc[0] == -0.0003
        finally:
            temp_path.unlink()


class TestBinanceFundingRatesDatasetSchema:
    """Tests for BinanceFundingRatesDataset schema."""

    def test_get_schema_returns_funding_rates_schema(self):
        """get_schema() returns FUNDING_RATES_SCHEMA."""
        dataset = BinanceFundingRatesDataset()
        schema = dataset.get_schema()

        assert schema is FUNDING_RATES_SCHEMA
