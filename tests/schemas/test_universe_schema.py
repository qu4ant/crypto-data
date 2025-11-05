"""
Tests for Crypto Universe Schema
"""

import pytest
import pandas as pd
import pandera as pa
from datetime import datetime

from crypto_data.schemas import (
    UNIVERSE_SCHEMA,
    validate_universe_dataframe
)


@pytest.mark.schema
class TestUniverseSchema:
    """Tests for crypto universe schema validation"""

    def test_valid_universe_passes(self, valid_universe_df):
        """Test that valid universe data passes validation"""
        validated_df = UNIVERSE_SCHEMA.validate(valid_universe_df)
        assert len(validated_df) == len(valid_universe_df)

    def test_duplicate_ranks_fails(self, invalid_universe_duplicate_ranks):
        """Test that duplicate ranks on same date fail"""
        with pytest.raises(pa.errors.SchemaError):
            UNIVERSE_SCHEMA.validate(invalid_universe_duplicate_ranks)

    def test_negative_market_cap_fails(self):
        """Test that negative market cap fails"""
        df = pd.DataFrame({
            'date': [datetime(2024, 1, 1)] * 3,
            'symbol': ['BTC', 'ETH', 'BNB'],
            'rank': [1, 2, 3],
            'market_cap': [800000000000.0, -400000000000.0, 80000000000.0],  # Negative
            'categories': ['currency', 'smart-contracts', 'exchange-token']
        })

        with pytest.raises(pa.errors.SchemaError):
            UNIVERSE_SCHEMA.validate(df)

    def test_null_symbol_fails(self):
        """Test that null symbol fails"""
        df = pd.DataFrame({
            'date': [datetime(2024, 1, 1)] * 3,
            'symbol': ['BTC', None, 'BNB'],  # Null
            'rank': [1, 2, 3],
            'market_cap': [800000000000.0, 400000000000.0, 80000000000.0],
            'categories': ['currency', 'smart-contracts', 'exchange-token']
        })

        with pytest.raises(pa.errors.SchemaError):
            UNIVERSE_SCHEMA.validate(df)

    def test_rank_less_than_one_fails(self):
        """Test that rank < 1 fails"""
        df = pd.DataFrame({
            'date': [datetime(2024, 1, 1)] * 3,
            'symbol': ['BTC', 'ETH', 'BNB'],
            'rank': [0, 1, 2],  # Rank 0 is invalid
            'market_cap': [800000000000.0, 400000000000.0, 80000000000.0],
            'categories': ['currency', 'smart-contracts', 'exchange-token']
        })

        with pytest.raises(pa.errors.SchemaError):
            UNIVERSE_SCHEMA.validate(df)


@pytest.mark.schema
class TestValidateUniverseDataframe:
    """Tests for validate_universe_dataframe() helper function"""

    def test_strict_mode_returns_validated_df(self, valid_universe_df):
        """Test strict=True returns validated DataFrame"""
        result = validate_universe_dataframe(valid_universe_df, strict=True)

        # Result should be a DataFrame
        assert isinstance(result, pd.DataFrame)
        assert len(result) == len(valid_universe_df)

    def test_strict_mode_raises_on_error(self):
        """Test strict=True raises SchemaError on invalid data"""
        df = pd.DataFrame({
            'date': [datetime(2024, 1, 1)] * 3,
            'symbol': ['BTC', 'ETH', 'BNB'],
            'rank': [1, 1, 2],  # Duplicate rank
            'market_cap': [800000000000.0, 400000000000.0, 80000000000.0],
            'categories': ['currency', 'smart-contracts', 'exchange-token']
        })

        with pytest.raises(pa.errors.SchemaError):
            validate_universe_dataframe(df, strict=True)

    def test_lazy_mode_returns_errors_object(self):
        """Test strict=False returns SchemaErrors object on failure"""
        df = pd.DataFrame({
            'date': [datetime(2024, 1, 1)] * 3,
            'symbol': ['BTC', 'ETH', 'BNB'],
            'rank': [1, 1, 2],  # Duplicate rank
            'market_cap': [800000000000.0, 400000000000.0, 80000000000.0],
            'categories': ['currency', 'smart-contracts', 'exchange-token']
        })

        result = validate_universe_dataframe(df, strict=False)

        # Result should be a SchemaErrors object (for inspection)
        assert isinstance(result, pa.errors.SchemaErrors)

    def test_lazy_mode_returns_df_on_success(self, valid_universe_df):
        """Test strict=False returns DataFrame on valid data"""
        result = validate_universe_dataframe(valid_universe_df, strict=False)

        # Result should be a DataFrame on success
        assert isinstance(result, pd.DataFrame)
        assert len(result) == len(valid_universe_df)
