"""
Tests for OHLCV Schema

Tests Pandera schemas for OHLCV data validation including:
- Valid data passes
- Invalid data fails with correct errors
- OHLC relationship checks
- Statistical validation
"""

import pytest
import pandas as pd
import pandera as pa

from crypto_data.schemas import (
    OHLCV_SCHEMA,
    OHLCV_STATISTICAL_SCHEMA,
    SPOT_SCHEMA,
    FUTURES_SCHEMA,
    validate_ohlcv_dataframe,
    validate_ohlcv_statistical
)


@pytest.mark.schema
class TestOHLCVSchema:
    """Tests for OHLCV schema validation"""

    def test_valid_ohlcv_passes(self, valid_ohlcv_df):
        """Test that valid OHLCV data passes validation"""
        # Should not raise
        validated_df = OHLCV_SCHEMA.validate(valid_ohlcv_df)
        assert len(validated_df) == len(valid_ohlcv_df)

    def test_valid_ohlcv_validate_function(self, valid_ohlcv_df):
        """Test validate_ohlcv_dataframe function with valid data"""
        validated_df = validate_ohlcv_dataframe(valid_ohlcv_df, strict=True)
        assert len(validated_df) == len(valid_ohlcv_df)

    def test_high_less_than_low_fails(self, invalid_ohlcv_high_low):
        """Test that high < low violation is caught"""
        with pytest.raises(pa.errors.SchemaError):
            OHLCV_SCHEMA.validate(invalid_ohlcv_high_low)

    def test_negative_price_fails(self, invalid_ohlcv_negative_price):
        """Test that negative prices are caught"""
        with pytest.raises(pa.errors.SchemaError):
            OHLCV_SCHEMA.validate(invalid_ohlcv_negative_price)

    def test_null_price_fails(self):
        """Test that null prices are caught"""
        df = pd.DataFrame({
            'exchange': ['binance'],
            'symbol': ['BTCUSDT'],
            'interval': ['5m'],
            'timestamp': [pd.Timestamp('2024-01-01')],
            'open': [None],  # Null price
            'high': [50100.0],
            'low': [49900.0],
            'close': [50010.0],
            'volume': [100.5],
            'quote_volume': [5000000.0],
            'trades_count': [500],
            'taker_buy_base_volume': [50.2],
            'taker_buy_quote_volume': [2500000.0]
        })

        with pytest.raises(pa.errors.SchemaError):
            OHLCV_SCHEMA.validate(df)

    def test_invalid_exchange_fails(self, valid_ohlcv_df):
        """Test that invalid exchange name is caught (uppercase, special chars)"""
        df = valid_ohlcv_df.copy()
        df['exchange'] = 'INVALID!@#'  # Uppercase + special characters (not allowed)

        with pytest.raises(pa.errors.SchemaError):
            OHLCV_SCHEMA.validate(df)

    def test_invalid_interval_fails(self, valid_ohlcv_df):
        """Test that invalid interval is caught"""
        df = valid_ohlcv_df.copy()
        df['interval'] = '99m'  # Invalid interval

        with pytest.raises(pa.errors.SchemaError):
            OHLCV_SCHEMA.validate(df)

    def test_negative_volume_fails(self, valid_ohlcv_df):
        """Test that negative volume is caught"""
        df = valid_ohlcv_df.copy()
        df.loc[0, 'volume'] = -100.0

        with pytest.raises(pa.errors.SchemaError):
            OHLCV_SCHEMA.validate(df)

    def test_empty_dataframe_passes(self):
        """Test that empty DataFrame with correct schema passes"""
        df = pd.DataFrame({
            'exchange': pd.Series([], dtype=str),
            'symbol': pd.Series([], dtype=str),
            'interval': pd.Series([], dtype=str),
            'timestamp': pd.Series([], dtype='datetime64[ns]'),
            'open': pd.Series([], dtype=float),
            'high': pd.Series([], dtype=float),
            'low': pd.Series([], dtype=float),
            'close': pd.Series([], dtype=float),
            'volume': pd.Series([], dtype=float),
            'quote_volume': pd.Series([], dtype=float),
            'trades_count': pd.Series([], dtype='Int64'),
            'taker_buy_base_volume': pd.Series([], dtype=float),
            'taker_buy_quote_volume': pd.Series([], dtype=float)
        })

        # Should not raise
        OHLCV_SCHEMA.validate(df)


@pytest.mark.schema
class TestSpotFuturesSchemas:
    """Tests for Spot and Futures specific schemas"""

    def test_spot_schema_valid(self, valid_ohlcv_df):
        """Test that spot schema validates valid data"""
        validated_df = SPOT_SCHEMA.validate(valid_ohlcv_df)
        assert len(validated_df) == len(valid_ohlcv_df)

    def test_futures_schema_valid(self, valid_ohlcv_df):
        """Test that futures schema validates valid data"""
        validated_df = FUTURES_SCHEMA.validate(valid_ohlcv_df)
        assert len(validated_df) == len(valid_ohlcv_df)


@pytest.mark.schema
class TestOHLCVStatisticalValidation:
    """Tests for OHLCV statistical validation"""

    def test_valid_data_passes_statistical(self, valid_ohlcv_df):
        """Test that valid data passes statistical checks"""
        passed, warnings = validate_ohlcv_statistical(valid_ohlcv_df)
        # Should pass (no extreme outliers in sample data)
        assert passed is True
        assert len(warnings) == 0

    def test_extreme_price_jump_detected(self):
        """Test that extreme price jumps are detected"""
        # Need many normal points + one huge jump for z-score > 5 sigma
        # Create 50 incremental prices, then huge jump
        num_points = 51
        close_prices = list(range(50000, 50000 + 50)) + [5000000.0]  # 100x jump

        df = pd.DataFrame({
            'exchange': ['binance'] * num_points,
            'symbol': ['BTCUSDT'] * num_points,
            'interval': ['5m'] * num_points,
            'timestamp': pd.date_range('2024-01-01', periods=num_points, freq='5T'),
            'open': [50000.0] * num_points,
            'high': [50100.0] * num_points,
            'low': [49900.0] * num_points,
            'close': close_prices,
            'volume': [100.0] * num_points,
            'quote_volume': [5000000.0] * num_points,
            'trades_count': [500] * num_points,
            'taker_buy_base_volume': [50.0] * num_points,
            'taker_buy_quote_volume': [2500000.0] * num_points
        })

        passed, warnings = validate_ohlcv_statistical(df)
        # Should detect outlier (warning, not error)
        assert passed is False or len(warnings) > 0
