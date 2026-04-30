"""
Tests for Crypto Universe Schema
"""

import pandera.pandas as pa
import pytest

from crypto_data.schemas import UNIVERSE_SCHEMA


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

    def test_negative_market_cap_passes(self, valid_universe_df):
        """Market cap values are stored but not used as validation gates."""
        df = valid_universe_df.head(3).copy()
        df["market_cap"] = [800000000000.0, -400000000000.0, 80000000000.0]

        UNIVERSE_SCHEMA.validate(df)

    def test_null_symbol_fails(self, valid_universe_df):
        """Test that null symbol fails"""
        df = valid_universe_df.head(3).copy()
        df["symbol"] = ["BTC", None, "BNB"]

        with pytest.raises(pa.errors.SchemaError):
            UNIVERSE_SCHEMA.validate(df)

    def test_rank_less_than_one_fails(self, valid_universe_df):
        """Test that rank < 1 fails"""
        df = valid_universe_df.head(3).copy()
        df["rank"] = [0, 1, 2]

        with pytest.raises(pa.errors.SchemaError):
            UNIVERSE_SCHEMA.validate(df)
