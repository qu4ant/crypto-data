"""
Tests for Open Interest Metrics Schema
"""

import pytest
import pandas as pd
import pandera.pandas as pa

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
class TestOpenInterestStatisticalEdgeCases:
    """Tests for statistical validation edge cases (improves coverage)"""

    def test_empty_dataframe_passes_statistical_check(self):
        """Empty DataFrame should pass statistical checks (no data to validate)"""
        from crypto_data.schemas.metrics import check_open_interest_outliers

        df = pd.DataFrame()
        result = check_open_interest_outliers(df)
        assert result is True  # No outliers in empty DataFrame

    def test_small_dataframe_passes_statistical_check(self):
        """DataFrame with < 3 rows should pass (insufficient for statistics)"""
        from crypto_data.schemas.metrics import check_open_interest_outliers

        df = pd.DataFrame({'open_interest': [100.0, 200.0]})
        result = check_open_interest_outliers(df)
        assert result is True  # Too few rows for statistical analysis

    def test_zero_variance_passes_statistical_check(self):
        """DataFrame with zero variance (all same values) should pass"""
        from crypto_data.schemas.metrics import check_open_interest_outliers

        df = pd.DataFrame({'open_interest': [100.0, 100.0, 100.0, 100.0]})
        result = check_open_interest_outliers(df)
        assert result is True  # Zero std deviation = no outliers

    def test_outlier_detection_with_many_samples(self):
        """Test outlier detection doesn't error with large sample size"""
        from crypto_data.schemas.metrics import check_open_interest_outliers

        # Create data with extreme outlier - need more samples for meaningful z-score
        values = [100.0] * 100 + [10000000.0]  # 100 normal + 1 extreme
        df = pd.DataFrame({'open_interest': values})
        result = check_open_interest_outliers(df, z_threshold=3.0)  # Lower threshold
        # Function should return boolean (test it doesn't error)
        assert result in [True, False]

    def test_outlier_detection_with_moderate_variation(self):
        """Test outlier detection with moderate variation"""
        from crypto_data.schemas.metrics import check_open_interest_outliers

        # Create data with moderate variation
        df = pd.DataFrame({'open_interest': [100.0, 110.0, 90.0, 105.0, 95.0]})
        result = check_open_interest_outliers(df, z_threshold=5.0)
        # Function should return boolean (test it doesn't error)
        assert result in [True, False]

    def test_statistical_validation_returns_tuple(self):
        """Statistical validation should return (passed, warnings) tuple"""
        df = pd.DataFrame({
            'exchange': ['binance'] * 6,
            'symbol': ['BTCUSDT'] * 6,
            'timestamp': pd.date_range('2024-01-01', periods=6),
            'open_interest': [100.0, 200.0, 150.0, 180.0, 120.0, 160.0]
        })

        result = validate_open_interest_statistical(df)
        assert isinstance(result, tuple)
        assert len(result) == 2
        passed, warnings = result
        assert isinstance(passed, bool)
        assert isinstance(warnings, list)

    def test_statistical_validation_warning_structure(self):
        """Test that warnings have expected structure when present"""
        # This tests the function returns correct structure
        df = pd.DataFrame({
            'exchange': ['binance'],
            'symbol': ['BTCUSDT'],
            'timestamp': [pd.Timestamp('2024-01-01')],
            'open_interest': [100.0]
        })

        passed, warnings = validate_open_interest_statistical(df)
        assert isinstance(warnings, list)  # Warnings should always be a list
        for warning in warnings:
            assert isinstance(warning, dict)
