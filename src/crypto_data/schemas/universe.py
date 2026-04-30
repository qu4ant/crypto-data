"""
Crypto Universe Schema (v6.0.0)

Pandera schema for validating CoinMarketCap-enriched universe ranking data.

Identity is `(provider, provider_id, date)`. `symbol` is mutable / not identity.
"""

import pandas as pd
import pandera.pandas as pa
from pandera.pandas import Check, Column, DataFrameSchema

from crypto_data.schemas.checks import check_no_duplicate_ranks_per_date

UNIVERSE_SCHEMA = DataFrameSchema(
    columns={
        # Identity
        "provider": Column(
            str,
            checks=[Check.equal_to("coinmarketcap", error="provider must be 'coinmarketcap'")],
            nullable=False,
            description="Data provider identifier",
        ),
        "provider_id": Column(
            "Int64",
            checks=[Check.greater_than_or_equal_to(1, error="provider_id must be >= 1")],
            nullable=False,
            description="Provider-internal asset id (e.g., CMC id)",
        ),
        "date": Column(
            "datetime64[ns]",
            nullable=False,
            description="Snapshot date",
        ),
        # Display / linking
        "symbol": Column(
            str,
            checks=[
                Check.str_matches(r"^[A-Z0-9]+$"),
                Check.str_length(min_value=1, max_value=20),
            ],
            nullable=False,
            description="Base asset symbol (mutable, not identity)",
        ),
        "name": Column(
            str,
            checks=[Check.str_length(min_value=1, max_value=100)],
            nullable=False,
            description="Human-readable asset name",
        ),
        "slug": Column(str, nullable=True, description="URL-style identifier"),
        # Universe selection
        "rank": Column(
            "Int64",
            checks=[Check.greater_than_or_equal_to(1, error="rank must be >= 1")],
            nullable=False,
            description="Provider rank (1 = highest market cap)",
        ),
        "market_cap": Column(
            float,
            nullable=True,
            description="Market capitalization in USD",
        ),
        "fully_diluted_market_cap": Column(
            float,
            nullable=True,
            description="Fully diluted market cap in USD",
        ),
        "circulating_supply": Column(float, nullable=True),
        "max_supply": Column(float, nullable=True),
        # Metadata
        "tags": Column(str, nullable=True, description="Comma-separated CMC tags"),
        "platform": Column(str, nullable=True, description="Chain platform name; NULL for L1s"),
        "date_added": Column("datetime64[ns]", nullable=True, description="Listing date"),
    },
    checks=[
        Check(
            check_no_duplicate_ranks_per_date,
            name="no_duplicate_ranks",
            error="Duplicate ranks detected on same date",
        ),
    ],
    strict=True,
    coerce=True,
    ordered=False,
    unique=["provider", "provider_id", "date"],
    description="CMC enriched universe schema (v6.0.0)",
)


def validate_universe_dataframe(
    df: pd.DataFrame,
    *,
    strict: bool = True,
) -> pd.DataFrame | pa.errors.SchemaErrors:
    """Validate a universe DataFrame against UNIVERSE_SCHEMA.

    Args:
        df: DataFrame to validate.
        strict: If True, raise on first error. If False, accumulate errors lazily
            and return them as a SchemaErrors object instead of a DataFrame.

    Returns:
        The validated DataFrame on success, or a SchemaErrors object when
        strict=False and validation failed.
    """
    if strict:
        return UNIVERSE_SCHEMA.validate(df, lazy=False)
    try:
        return UNIVERSE_SCHEMA.validate(df, lazy=True)
    except pa.errors.SchemaErrors as e:
        return e


__all__ = ["UNIVERSE_SCHEMA", "validate_universe_dataframe"]
