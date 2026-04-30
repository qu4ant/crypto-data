"""
Test non-strict validation mode for all Pandera schemas.

Tests the strict=False parameter which returns errors instead of raising exceptions.
This improves code coverage for error handling paths in schema validation functions.
"""

import pytest
from crypto_data.enums import DataType, Interval
import pandas as pd
import pandera.pandas as pa
from datetime import datetime

from crypto_data.schemas import (
    validate_universe_dataframe,
    validate_ohlcv_dataframe,
    validate_open_interest_dataframe,
    validate_funding_rates_dataframe
)


def _v6_universe_row(**overrides) -> dict:
    """Single-row v6 universe payload with sensible defaults."""
    base = {
        'provider': 'coinmarketcap',
        'provider_id': 1,
        'date': pd.Timestamp('2024-01-01'),
        'symbol': 'BTC',
        'name': 'Bitcoin',
        'slug': 'bitcoin',
        'rank': 1,
        'market_cap': 1_000_000_000.0,
        'fully_diluted_market_cap': None,
        'circulating_supply': None,
        'max_supply': None,
        'tags': 'currency',
        'platform': None,
        'date_added': pd.NaT,
    }
    base.update(overrides)
    return base


class TestUniverseNonStrictValidation:
    """Test non-strict validation for universe schema."""

    def test_valid_data_passes_non_strict(self):
        """Valid data should pass even in non-strict mode."""
        df = pd.DataFrame([_v6_universe_row()])

        result = validate_universe_dataframe(df, strict=False)
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1

    def test_invalid_rank_returns_errors_non_strict(self):
        """Invalid rank should return SchemaErrors, not raise."""
        df = pd.DataFrame([_v6_universe_row(rank=0)])

        result = validate_universe_dataframe(df, strict=False)
        assert isinstance(result, pa.errors.SchemaErrors)
        assert hasattr(result, 'failure_cases')

    def test_negative_market_cap_returns_errors_non_strict(self):
        """Negative market cap should return SchemaErrors."""
        df = pd.DataFrame([_v6_universe_row(market_cap=-1000.0)])

        result = validate_universe_dataframe(df, strict=False)
        assert isinstance(result, pa.errors.SchemaErrors)

    def test_null_symbol_returns_errors_non_strict(self):
        """Null symbols should return SchemaErrors."""
        df = pd.DataFrame([_v6_universe_row(symbol=None)])

        result = validate_universe_dataframe(df, strict=False)
        assert isinstance(result, pa.errors.SchemaErrors)


class TestOHLCVNonStrictValidation:
    """Test non-strict validation for OHLCV schema."""

    def test_valid_ohlcv_passes_non_strict(self):
        """Valid OHLCV data should pass in non-strict mode."""
        df = pd.DataFrame({
            'exchange': ['binance'],
            'symbol': ['BTCUSDT'],
            'interval': [Interval.HOUR_1.value],
            'timestamp': [pd.Timestamp('2024-01-01')],
            'open': [40000.0],
            'high': [41000.0],
            'low': [39000.0],
            'close': [40500.0],
            'volume': [100.0],
            'quote_volume': [4000000.0],
            'trades_count': [1000],
            'taker_buy_base_volume': [50.0],
            'taker_buy_quote_volume': [2000000.0]
        })

        result = validate_ohlcv_dataframe(df, strict=False)
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1

    def test_high_less_than_low_returns_errors_non_strict(self):
        """OHLC violation (high < low) should return errors."""
        df = pd.DataFrame({
            'exchange': ['binance'],
            'symbol': ['BTCUSDT'],
            'interval': [Interval.HOUR_1.value],
            'timestamp': [pd.Timestamp('2024-01-01')],
            'open': [40000.0],
            'high': [38000.0],  # Invalid: high < low
            'low': [39000.0],
            'close': [40500.0],
            'volume': [100.0],
            'quote_volume': [4000000.0],
            'trades_count': [1000],
            'taker_buy_base_volume': [50.0],
            'taker_buy_quote_volume': [2000000.0]
        })

        result = validate_ohlcv_dataframe(df, strict=False)
        assert isinstance(result, pa.errors.SchemaErrors)

    def test_negative_volume_returns_errors_non_strict(self):
        """Negative volume should return errors."""
        df = pd.DataFrame({
            'exchange': ['binance'],
            'symbol': ['BTCUSDT'],
            'interval': [Interval.HOUR_1.value],
            'timestamp': [pd.Timestamp('2024-01-01')],
            'open': [40000.0],
            'high': [41000.0],
            'low': [39000.0],
            'close': [40500.0],
            'volume': [-100.0],  # Invalid: negative
            'quote_volume': [4000000.0],
            'trades_count': [1000],
            'taker_buy_volume': [50.0],
            'taker_buy_quote_volume': [2000000.0]
        })

        result = validate_ohlcv_dataframe(df, strict=False)
        assert isinstance(result, pa.errors.SchemaErrors)


class TestOpenInterestNonStrictValidation:
    """Test non-strict validation for open interest schema."""

    def test_valid_open_interest_passes_non_strict(self):
        """Valid open interest data should pass."""
        df = pd.DataFrame({
            'exchange': ['binance'],
            'symbol': ['BTCUSDT'],
            'timestamp': [pd.Timestamp('2024-01-01')],
            'open_interest': [1000000.0]
        })

        result = validate_open_interest_dataframe(df, strict=False)
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1

    def test_negative_open_interest_returns_errors_non_strict(self):
        """Negative open interest should return errors."""
        df = pd.DataFrame({
            'exchange': ['binance'],
            'symbol': ['BTCUSDT'],
            'timestamp': [pd.Timestamp('2024-01-01')],
            'open_interest': [-1000.0]  # Invalid: negative (also filtered during import)
        })

        result = validate_open_interest_dataframe(df, strict=False)
        assert isinstance(result, pa.errors.SchemaErrors)

    def test_null_open_interest_returns_errors_non_strict(self):
        """Null open interest should return errors."""
        df = pd.DataFrame({
            'exchange': ['binance'],
            'symbol': ['BTCUSDT'],
            'timestamp': [pd.Timestamp('2024-01-01')],
            'open_interest': [None]  # Invalid: null
        })

        result = validate_open_interest_dataframe(df, strict=False)
        assert isinstance(result, pa.errors.SchemaErrors)


class TestFundingRatesNonStrictValidation:
    """Test non-strict validation for funding rates schema."""

    def test_valid_funding_rates_pass_non_strict(self):
        """Valid funding rates data should pass."""
        df = pd.DataFrame({
            'exchange': ['binance'],
            'symbol': ['BTCUSDT'],
            'timestamp': [pd.Timestamp('2024-01-01')],
            'funding_rate': [0.0001]
        })

        result = validate_funding_rates_dataframe(df, strict=False)
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1

    def test_null_funding_rate_returns_errors_non_strict(self):
        """Null funding rate should return errors."""
        df = pd.DataFrame({
            'exchange': ['binance'],
            'symbol': ['BTCUSDT'],
            'timestamp': [pd.Timestamp('2024-01-01')],
            'funding_rate': [None]  # Invalid: null
        })

        result = validate_funding_rates_dataframe(df, strict=False)
        assert isinstance(result, pa.errors.SchemaErrors)

    def test_extreme_funding_rate_passes_non_strict(self):
        """Extreme funding rates should pass validation (warning only in statistical check)."""
        df = pd.DataFrame({
            'exchange': ['binance'],
            'symbol': ['BTCUSDT'],
            'timestamp': [pd.Timestamp('2024-01-01')],
            'funding_rate': [0.05]  # 5% - extreme but valid
        })

        result = validate_funding_rates_dataframe(df, strict=False)
        assert isinstance(result, pd.DataFrame)  # Should pass, warnings are separate


class TestErrorStructure:
    """Test that SchemaErrors have expected structure for debugging."""

    def test_schema_errors_have_failure_cases(self):
        """SchemaErrors should contain failure_cases DataFrame for debugging."""
        df = pd.DataFrame([_v6_universe_row(rank=0)])

        errors = validate_universe_dataframe(df, strict=False)
        assert isinstance(errors, pa.errors.SchemaErrors)
        assert hasattr(errors, 'failure_cases')
        assert isinstance(errors.failure_cases, pd.DataFrame)
        assert len(errors.failure_cases) > 0

    def test_schema_errors_are_informative(self):
        """SchemaErrors should contain helpful information for debugging."""
        df = pd.DataFrame({
            'exchange': ['binance'],
            'symbol': ['BTCUSDT'],
            'interval': [Interval.HOUR_1.value],
            'timestamp': [pd.Timestamp('2024-01-01')],
            'open': [40000.0],
            'high': [38000.0],  # Invalid: high < low
            'low': [39000.0],
            'close': [40500.0],
            'volume': [100.0],
            'quote_volume': [4000000.0],
            'trades_count': [1000],
            'taker_buy_base_volume': [50.0],
            'taker_buy_quote_volume': [2000000.0]
        })

        errors = validate_ohlcv_dataframe(df, strict=False)
        assert isinstance(errors, pa.errors.SchemaErrors)

        # Check that error message contains useful information
        error_str = str(errors)
        assert len(error_str) > 0  # Has some error description
