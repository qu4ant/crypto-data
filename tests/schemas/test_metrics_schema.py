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
