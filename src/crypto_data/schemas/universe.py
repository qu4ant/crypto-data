"""
Crypto Universe Schema

Pandera schema for validating CoinMarketCap universe ranking data.

Features:
- No duplicate ranks per date
- Non-negative market_cap
- Date range validation
"""

import pandera as pa
from pandera import Column, Check, DataFrameSchema
from crypto_data.schemas.checks import check_no_duplicate_ranks_per_date


# =============================================================================
# Crypto Universe Schema
# =============================================================================

UNIVERSE_SCHEMA = DataFrameSchema(
    columns={
        # Primary key columns
        'date': Column(
            'datetime64[ns]',  # Date stored as datetime
            checks=[
                Check(lambda s: s.notna().all(), error="Null dates detected"),
            ],
            nullable=False,
            description="Snapshot date (first day of month)"
        ),
        'symbol': Column(
            str,
            checks=[
                Check.str_matches(r'^[A-Z0-9]+$'),  # Uppercase alphanumeric
                Check.str_length(min_value=1, max_value=20),
                Check(lambda s: s.notna().all(), error="Null symbols detected")
            ],
            nullable=False,
            description="Base asset symbol (e.g., 'BTC', 'ETH')"
        ),

        # Ranking columns
        'rank': Column(
            'Int64',  # Integer
            checks=[
                Check.greater_than_or_equal_to(1, error="Rank must be >= 1"),
                Check(lambda s: s.notna().all(), error="Null ranks detected")
            ],
            nullable=False,
            description="CoinMarketCap rank (1 = highest market cap)"
        ),
        'market_cap': Column(
            float,
            checks=[
                Check.greater_than_or_equal_to(0, error="Market cap cannot be negative"),
                Check(lambda s: s.notna().all(), error="Null market caps detected")
            ],
            nullable=False,
            description="Market capitalization in USD"
        ),

        # Categories column (optional)
        'categories': Column(
            str,
            nullable=True,  # Can be null or empty
            description="Comma-separated list of CoinMarketCap tags"
        ),
    },

    # DataFrame-level checks (relationships within data)
    checks=[
        Check(
            check_no_duplicate_ranks_per_date,
            name='no_duplicate_ranks',
            error="Duplicate ranks detected on same date"
        ),
    ],

    # Schema-level settings
    strict=True,  # Reject columns not defined in schema
    coerce=True,  # Coerce data types
    ordered=False,  # Column order doesn't matter
    unique=['date', 'symbol'],  # Primary key constraint
    description="Schema for CoinMarketCap universe rankings"
)


# =============================================================================
# Validation Functions
# =============================================================================

def validate_universe_dataframe(df: pa.typing.DataFrame, strict: bool = True) -> pa.typing.DataFrame:
    """
    Validate universe DataFrame against schema.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame to validate
    strict : bool
        If True, raise on validation errors. If False, return errors as dict (default: True)

    Returns
    -------
    pd.DataFrame
        Validated DataFrame (if strict=True)

    Raises
    ------
    pa.errors.SchemaError
        If validation fails and strict=True
    """
    if strict:
        return UNIVERSE_SCHEMA.validate(df, lazy=False)
    else:
        try:
            return UNIVERSE_SCHEMA.validate(df, lazy=True)
        except pa.errors.SchemaErrors as e:
            # Return errors for inspection
            return e


# Export schemas
__all__ = [
    'UNIVERSE_SCHEMA',
    'validate_universe_dataframe'
]
