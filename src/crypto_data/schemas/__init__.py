"""
Pandera Data Validation Schemas

This module provides Pandera schemas for validating cryptocurrency data:
- OHLCV data (spot/futures) with OHLC relationship checks
- Open interest metrics
- Funding rates
- Crypto universe rankings

All schemas include:
- Type validation
- Range checks (non-negative, non-null)
- Relationship checks (OHLC, rank consistency)
- Statistical validation (outliers, distributions)

Usage:
    from crypto_data.schemas import OHLCV_SCHEMA, validate_ohlcv_dataframe

    # Validate DataFrame
    validated_df = validate_ohlcv_dataframe(df)

    # Or use schema directly
    OHLCV_SCHEMA.validate(df)
"""

# Import all schemas and validation functions
# Import custom check functions (useful for testing)
from crypto_data.schemas.checks import (
    check_no_duplicate_ranks_per_date,
    check_ohlc_relationships,
    check_price_continuity,
    check_timestamp_monotonic,
    check_volume_outliers,
)
from crypto_data.schemas.funding_rates import (
    FUNDING_RATES_SCHEMA,
    FUNDING_RATES_STATISTICAL_SCHEMA,
    validate_funding_rates_dataframe,
    validate_funding_rates_statistical,
)
from crypto_data.schemas.metrics import (
    OPEN_INTEREST_SCHEMA,
    OPEN_INTEREST_STATISTICAL_SCHEMA,
    validate_open_interest_dataframe,
    validate_open_interest_statistical,
)
from crypto_data.schemas.ohlcv import (
    FUTURES_SCHEMA,
    OHLCV_SCHEMA,
    OHLCV_STATISTICAL_SCHEMA,
    SPOT_SCHEMA,
    validate_ohlcv_dataframe,
    validate_ohlcv_statistical,
)
from crypto_data.schemas.universe import UNIVERSE_SCHEMA, validate_universe_dataframe

# Public API
__all__ = [
    # OHLCV schemas
    "OHLCV_SCHEMA",
    "OHLCV_STATISTICAL_SCHEMA",
    "SPOT_SCHEMA",
    "FUTURES_SCHEMA",
    "validate_ohlcv_dataframe",
    "validate_ohlcv_statistical",
    # Open interest schemas
    "OPEN_INTEREST_SCHEMA",
    "OPEN_INTEREST_STATISTICAL_SCHEMA",
    "validate_open_interest_dataframe",
    "validate_open_interest_statistical",
    # Funding rates schemas
    "FUNDING_RATES_SCHEMA",
    "FUNDING_RATES_STATISTICAL_SCHEMA",
    "validate_funding_rates_dataframe",
    "validate_funding_rates_statistical",
    # Universe schemas
    "UNIVERSE_SCHEMA",
    "validate_universe_dataframe",
    # Check functions (for testing/custom validation)
    "check_ohlc_relationships",
    "check_price_continuity",
    "check_volume_outliers",
    "check_timestamp_monotonic",
    "check_no_duplicate_ranks_per_date",
]
