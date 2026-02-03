"""
Tests for OpenInterestStrategy (daily open interest metrics).

Tests initialization, period generation, URL building, CSV parsing,
and schema validation.
"""

import tempfile
from datetime import datetime
from pathlib import Path

import pandas as pd
import pytest

from crypto_data.enums import DataType
from crypto_data.schemas import OPEN_INTEREST_SCHEMA
from crypto_data.strategies import OpenInterestStrategy
from crypto_data.strategies.base import Period


class TestOpenInterestStrategyInit:
    """Tests for OpenInterestStrategy initialization."""

    def test_data_type(self):
        """data_type property returns OPEN_INTEREST."""
        strategy = OpenInterestStrategy()
        assert strategy.data_type == DataType.OPEN_INTEREST

    def test_table_name(self):
        """table_name property returns 'open_interest'."""
        strategy = OpenInterestStrategy()
        assert strategy.table_name == 'open_interest'

    def test_is_monthly_false(self):
        """is_monthly property returns False for daily data."""
        strategy = OpenInterestStrategy()
        assert strategy.is_monthly is False

    def test_default_max_concurrent_100(self):
        """default_max_concurrent returns 100 for daily metrics."""
        strategy = OpenInterestStrategy()
        assert strategy.default_max_concurrent == 100

    def test_requires_interval_returns_false(self):
        """requires_interval() returns False for open interest strategy."""
        strategy = OpenInterestStrategy()
        assert strategy.requires_interval() is False


class TestOpenInterestStrategyPeriods:
    """Tests for OpenInterestStrategy period generation."""

    def test_generate_daily_periods(self):
        """Generate daily periods for a 3-day range."""
        strategy = OpenInterestStrategy()

        start = datetime(2024, 1, 15)
        end = datetime(2024, 1, 17)

        periods = strategy.generate_periods(start, end)

        assert len(periods) == 3
        assert periods[0].value == '2024-01-15'
        assert periods[0].is_monthly is False
        assert periods[1].value == '2024-01-16'
        assert periods[1].is_monthly is False
        assert periods[2].value == '2024-01-17'
        assert periods[2].is_monthly is False

    def test_generate_single_day(self):
        """Generate periods for same-day start/end."""
        strategy = OpenInterestStrategy()

        start = datetime(2024, 6, 15)
        end = datetime(2024, 6, 15)

        periods = strategy.generate_periods(start, end)

        assert len(periods) == 1
        assert periods[0].value == '2024-06-15'
        assert periods[0].is_monthly is False

    def test_generate_cross_month_periods(self):
        """Generate daily periods crossing month boundary."""
        strategy = OpenInterestStrategy()

        start = datetime(2024, 1, 30)
        end = datetime(2024, 2, 2)

        periods = strategy.generate_periods(start, end)

        assert len(periods) == 4
        assert periods[0].value == '2024-01-30'
        assert periods[1].value == '2024-01-31'
        assert periods[2].value == '2024-02-01'
        assert periods[3].value == '2024-02-02'


class TestOpenInterestStrategyUrls:
    """Tests for OpenInterestStrategy URL building."""

    def test_metrics_url(self):
        """Build metrics download URL."""
        strategy = OpenInterestStrategy()
        period = Period('2024-01-15', is_monthly=False)

        url = strategy.build_download_url(
            base_url='https://data.binance.vision/',
            symbol='BTCUSDT',
            period=period
        )

        expected = (
            'https://data.binance.vision/'
            'data/futures/um/daily/metrics/BTCUSDT/BTCUSDT-metrics-2024-01-15.zip'
        )
        assert url == expected

    def test_url_ignores_interval(self):
        """URL building ignores interval parameter."""
        strategy = OpenInterestStrategy()
        period = Period('2024-06-20', is_monthly=False)

        url = strategy.build_download_url(
            base_url='https://data.binance.vision/',
            symbol='ETHUSDT',
            period=period,
            interval='5m'  # Should be ignored
        )

        expected = (
            'https://data.binance.vision/'
            'data/futures/um/daily/metrics/ETHUSDT/ETHUSDT-metrics-2024-06-20.zip'
        )
        assert url == expected

    def test_temp_filename(self):
        """Build temporary filename for download."""
        strategy = OpenInterestStrategy()
        period = Period('2024-01-15', is_monthly=False)

        filename = strategy.build_temp_filename(
            symbol='BTCUSDT',
            period=period
        )

        assert filename == 'BTCUSDT-metrics-2024-01-15.zip'

    def test_temp_filename_ignores_interval(self):
        """Temp filename ignores interval parameter."""
        strategy = OpenInterestStrategy()
        period = Period('2024-06-20', is_monthly=False)

        filename = strategy.build_temp_filename(
            symbol='ETHUSDT',
            period=period,
            interval='1h'  # Should be ignored
        )

        assert filename == 'ETHUSDT-metrics-2024-06-20.zip'


class TestOpenInterestStrategyCsvParsing:
    """Tests for OpenInterestStrategy CSV parsing."""

    def test_parse_csv(self):
        """Parse metrics CSV file with header."""
        strategy = OpenInterestStrategy()

        # Metrics CSV format (always has header)
        csv_content = (
            "create_time,symbol,sum_open_interest,sum_open_interest_value,"
            "count_toptrader_long_short_ratio,sum_toptrader_long_short_ratio,"
            "count_long_short_ratio,sum_taker_long_short_vol_ratio\n"
            "2024-01-15 00:00:00,BTCUSDT,12345.67,123456789.0,"
            "1.5,2.0,1.2,0.8\n"
            "2024-01-15 04:00:00,BTCUSDT,12350.00,123500000.0,"
            "1.4,1.9,1.1,0.9\n"
        )

        with tempfile.NamedTemporaryFile(
            mode='w', suffix='.csv', delete=False
        ) as f:
            f.write(csv_content)
            temp_path = Path(f.name)

        try:
            df = strategy.parse_csv(temp_path, 'BTCUSDT', 'binance')

            # Check columns
            expected_cols = ['exchange', 'symbol', 'timestamp', 'open_interest']
            assert list(df.columns) == expected_cols

            # Check row count
            assert len(df) == 2

            # Check metadata columns
            assert df['exchange'].iloc[0] == 'binance'
            assert df['symbol'].iloc[0] == 'BTCUSDT'

            # Check timestamp conversion
            assert df['timestamp'].iloc[0] == pd.Timestamp('2024-01-15 00:00:00')
            assert df['timestamp'].iloc[1] == pd.Timestamp('2024-01-15 04:00:00')

            # Check open_interest values
            assert df['open_interest'].iloc[0] == 12345.67
            assert df['open_interest'].iloc[1] == 12350.00
        finally:
            temp_path.unlink()

    def test_filters_zero_values(self):
        """Parse CSV and filter out zero open_interest values."""
        strategy = OpenInterestStrategy()

        csv_content = (
            "create_time,symbol,sum_open_interest,sum_open_interest_value,"
            "count_toptrader_long_short_ratio,sum_toptrader_long_short_ratio,"
            "count_long_short_ratio,sum_taker_long_short_vol_ratio\n"
            "2024-01-15 00:00:00,BTCUSDT,12345.67,123456789.0,"
            "1.5,2.0,1.2,0.8\n"
            "2024-01-15 04:00:00,BTCUSDT,0,0.0,"
            "1.4,1.9,1.1,0.9\n"
            "2024-01-15 08:00:00,BTCUSDT,12360.00,123600000.0,"
            "1.3,1.8,1.0,0.85\n"
        )

        with tempfile.NamedTemporaryFile(
            mode='w', suffix='.csv', delete=False
        ) as f:
            f.write(csv_content)
            temp_path = Path(f.name)

        try:
            df = strategy.parse_csv(temp_path, 'BTCUSDT', 'binance')

            # Should filter out zero value row
            assert len(df) == 2
            assert df['open_interest'].iloc[0] == 12345.67
            assert df['open_interest'].iloc[1] == 12360.00
        finally:
            temp_path.unlink()

    def test_parse_csv_symbol_override(self):
        """Parse CSV with symbol override (1000-prefix normalization)."""
        strategy = OpenInterestStrategy()

        csv_content = (
            "create_time,symbol,sum_open_interest,sum_open_interest_value,"
            "count_toptrader_long_short_ratio,sum_toptrader_long_short_ratio,"
            "count_long_short_ratio,sum_taker_long_short_vol_ratio\n"
            "2024-01-15 00:00:00,1000PEPEUSDT,12345.67,123456789.0,"
            "1.5,2.0,1.2,0.8\n"
        )

        with tempfile.NamedTemporaryFile(
            mode='w', suffix='.csv', delete=False
        ) as f:
            f.write(csv_content)
            temp_path = Path(f.name)

        try:
            # Pass normalized symbol (without 1000 prefix)
            df = strategy.parse_csv(temp_path, 'PEPEUSDT', 'binance')

            # Symbol should be overridden to normalized version
            assert df['symbol'].iloc[0] == 'PEPEUSDT'
        finally:
            temp_path.unlink()

    def test_parse_csv_drops_duplicates(self):
        """Parse CSV and drop duplicate timestamps."""
        strategy = OpenInterestStrategy()

        csv_content = (
            "create_time,symbol,sum_open_interest,sum_open_interest_value,"
            "count_toptrader_long_short_ratio,sum_toptrader_long_short_ratio,"
            "count_long_short_ratio,sum_taker_long_short_vol_ratio\n"
            "2024-01-15 00:00:00,BTCUSDT,12345.67,123456789.0,"
            "1.5,2.0,1.2,0.8\n"
            "2024-01-15 00:00:00,BTCUSDT,12345.67,123456789.0,"
            "1.5,2.0,1.2,0.8\n"
        )

        with tempfile.NamedTemporaryFile(
            mode='w', suffix='.csv', delete=False
        ) as f:
            f.write(csv_content)
            temp_path = Path(f.name)

        try:
            df = strategy.parse_csv(temp_path, 'BTCUSDT', 'binance')

            # Should have only 1 row after deduplication
            assert len(df) == 1
        finally:
            temp_path.unlink()


class TestOpenInterestStrategySchema:
    """Tests for OpenInterestStrategy schema."""

    def test_get_schema_returns_open_interest_schema(self):
        """get_schema() returns OPEN_INTEREST_SCHEMA."""
        strategy = OpenInterestStrategy()
        schema = strategy.get_schema()

        assert schema is OPEN_INTEREST_SCHEMA
