"""
Tests for Crypto Universe Schema
"""

import pytest
import pandas as pd
import pandera.pandas as pa
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
