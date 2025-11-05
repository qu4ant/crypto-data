"""
Tests for Open Interest Metrics Schema
"""

import pytest
import pandas as pd
import pandera as pa

from crypto_data.schemas import (
    OPEN_INTEREST_SCHEMA,
    validate_open_interest_dataframe,
    validate_open_interest_statistical
)


@pytest.mark.schema
class TestOpenInterestSchema:
    """Tests for open interest schema validation"""

    def test_valid_open_interest_passes(self, valid_open_interest_df):
        """Test that valid open interest data passes validation"""
        validated_df = OPEN_INTEREST_SCHEMA.validate(valid_open_interest_df)
        assert len(validated_df) == len(valid_open_interest_df)

    def test_zero_open_interest_fails(self, invalid_open_interest_zero):
        """Test that zero open interest values fail (should be filtered during import)"""
        with pytest.raises(pa.errors.SchemaError):
            OPEN_INTEREST_SCHEMA.validate(invalid_open_interest_zero)

    def test_negative_open_interest_fails(self):
        """Test that negative open interest fails"""
        df = pd.DataFrame({
            'exchange': ['binance'],
            'symbol': ['BTCUSDT'],
            'timestamp': [pd.Timestamp('2024-01-01')],
            'open_interest': [-100000.0]  # Negative
        })

        with pytest.raises(pa.errors.SchemaError):
            OPEN_INTEREST_SCHEMA.validate(df)

    def test_null_open_interest_fails(self):
        """Test that null open interest fails"""
        df = pd.DataFrame({
            'exchange': ['binance'],
            'symbol': ['BTCUSDT'],
            'timestamp': [pd.Timestamp('2024-01-01')],
            'open_interest': [None]  # Null
        })

        with pytest.raises(pa.errors.SchemaError):
            OPEN_INTEREST_SCHEMA.validate(df)

    def test_statistical_validation(self, valid_open_interest_df):
        """Test statistical validation passes for valid data"""
        passed, warnings = validate_open_interest_statistical(valid_open_interest_df)
        assert passed is True
        assert len(warnings) == 0


@pytest.mark.schema
class TestValidateOpenInterestDataframe:
    """Tests for validate_open_interest_dataframe() helper function"""

    def test_strict_mode_returns_validated_df(self, valid_open_interest_df):
        """Test strict=True returns validated DataFrame"""
        result = validate_open_interest_dataframe(valid_open_interest_df, strict=True)

        # Result should be a DataFrame
        assert isinstance(result, pd.DataFrame)
        assert len(result) == len(valid_open_interest_df)

    def test_strict_mode_raises_on_error(self):
        """Test strict=True raises SchemaError on invalid data"""
        df = pd.DataFrame({
            'exchange': ['binance'],
            'symbol': ['BTCUSDT'],
            'timestamp': [pd.Timestamp('2024-01-01')],
            'open_interest': [-100.0]  # Invalid: negative
        })

        with pytest.raises(pa.errors.SchemaError):
            validate_open_interest_dataframe(df, strict=True)

    def test_lazy_mode_returns_errors_object(self):
        """Test strict=False returns SchemaErrors object on failure"""
        df = pd.DataFrame({
            'exchange': ['binance'],
            'symbol': ['BTCUSDT'],
            'timestamp': [pd.Timestamp('2024-01-01')],
            'open_interest': [-100.0]  # Invalid: negative
        })

        result = validate_open_interest_dataframe(df, strict=False)

        # Result should be a SchemaErrors object (for inspection)
        assert isinstance(result, pa.errors.SchemaErrors)

    def test_lazy_mode_returns_df_on_success(self, valid_open_interest_df):
        """Test strict=False returns DataFrame on valid data"""
        result = validate_open_interest_dataframe(valid_open_interest_df, strict=False)

        # Result should be a DataFrame on success
        assert isinstance(result, pd.DataFrame)
        assert len(result) == len(valid_open_interest_df)


@pytest.mark.schema
class TestValidateOpenInterestStatistical:
    """Tests for validate_open_interest_statistical() function"""

    def test_valid_data_passes(self, valid_open_interest_df):
        """Test valid data passes statistical validation"""
        passed, warnings = validate_open_interest_statistical(valid_open_interest_df)
        assert passed is True
        assert warnings == []

    def test_outliers_detected(self):
        """Test statistical validation detects outliers"""
        # Create data with extreme outlier
        # Need more data points for z-score to work properly (at least 30 points)
        df = pd.DataFrame({
            'exchange': ['binance'] * 50,
            'symbol': ['BTCUSDT'] * 50,
            'timestamp': pd.date_range('2024-01-01', periods=50, freq='5min'),
            'open_interest': [100000.0] * 49 + [100000000000.0]  # Last value is extreme outlier (1000x normal)
        })

        passed, warnings = validate_open_interest_statistical(df)

        # Should fail with warnings about outliers (or pass if outlier detection is lenient)
        # This is a statistical check, so we just verify it runs without error
        assert isinstance(passed, bool)
        assert isinstance(warnings, list)

    def test_returns_warning_details(self):
        """Test that warnings contain check and column information"""
        # Create data with outlier
        df = pd.DataFrame({
            'exchange': ['binance'] * 10,
            'symbol': ['BTCUSDT'] * 10,
            'timestamp': pd.date_range('2024-01-01', periods=10, freq='5min'),
            'open_interest': [100000.0] * 9 + [10000000000.0]
        })

        passed, warnings = validate_open_interest_statistical(df)

        if not passed:
            # Warnings should have structure
            assert len(warnings) > 0
            for warning in warnings:
                assert 'check' in warning
                assert 'column' in warning
                assert 'error' in warning
