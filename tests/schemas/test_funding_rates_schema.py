"""
Tests for Funding Rates Schema
"""

import pandas as pd
import pandera.pandas as pa
import pytest

from crypto_data.schemas import (
    FUNDING_RATES_SCHEMA,
    validate_funding_rates_statistical,
)


@pytest.mark.schema
class TestFundingRatesSchema:
    """Tests for funding rates schema validation"""

    def test_valid_funding_rates_passes(self, valid_funding_rates_df):
        """Test that valid funding rates data passes validation"""
        validated_df = FUNDING_RATES_SCHEMA.validate(valid_funding_rates_df)
        assert len(validated_df) == len(valid_funding_rates_df)

    def test_null_funding_rate_fails(self):
        """Test that null funding rates fail"""
        df = pd.DataFrame(
            {
                "exchange": ["binance"],
                "symbol": ["BTCUSDT"],
                "timestamp": [pd.Timestamp("2024-01-01")],
                "funding_rate": [None],  # Null
            }
        )

        with pytest.raises(pa.errors.SchemaError):
            FUNDING_RATES_SCHEMA.validate(df)

    def test_extreme_funding_rate_passes_schema(self, funding_rates_extreme):
        """Test that extreme values pass main schema (they're rare but valid)"""
        # Should pass schema validation (extremes are valid)
        validated_df = FUNDING_RATES_SCHEMA.validate(funding_rates_extreme)
        assert len(validated_df) == len(funding_rates_extreme)

    def test_extreme_funding_rate_detected_statistically(self, funding_rates_extreme):
        """Test that extreme values are detected by statistical validation"""
        passed, warnings = validate_funding_rates_statistical(funding_rates_extreme)
        # Should detect extreme value (warning, not error)
        assert passed is False or len(warnings) > 0

    def test_statistical_validation_normal(self, valid_funding_rates_df):
        """Test statistical validation passes for normal data"""
        passed, warnings = validate_funding_rates_statistical(valid_funding_rates_df)
        assert passed is True
        assert len(warnings) == 0
