"""
Funding Rates Schema

Pandera schema for validating funding rate data from Binance futures.

Features:
- Non-null checks
- Timestamp validation
- Distribution checks
"""

import numpy as np
import pandera.pandas as pa
from pandera.pandas import Check, Column, DataFrameSchema

# =============================================================================
# Funding Rates Schema
# =============================================================================

FUNDING_RATES_SCHEMA = DataFrameSchema(
    columns={
        # Primary key columns
        "exchange": Column(
            str,
            checks=[Check.isin(["binance"])],
            nullable=False,
            description="Exchange name (always 'binance')",
        ),
        "symbol": Column(
            str,
            checks=[
                Check.str_matches(r"^[A-Z0-9]+$"),  # Uppercase alphanumeric
                Check.str_length(min_value=3, max_value=20),
            ],
            nullable=False,
            description="Futures contract symbol (e.g., 'BTCUSDT')",
        ),
        "timestamp": Column(
            "datetime64[ns]",
            checks=[
                Check(lambda s: s.notna().all(), error="Null timestamps detected"),
            ],
            nullable=False,
            description="Funding rate calculation time",
        ),
        # Funding rate column
        "funding_rate": Column(
            float,
            checks=[
                Check(lambda s: s.notna().all(), error="Null funding rates detected"),
                Check(lambda s: ~np.isinf(s).any(), error="Infinite funding rate values detected"),
                # Typical funding rates are -1% to +1%, but extreme values CAN occur
                # We check but don't fail on extremes (they're rare but valid)
            ],
            nullable=False,
            description="Funding rate (positive = longs pay shorts, negative = shorts pay longs)",
        ),
    },
    # DataFrame-level checks (relationships between rows)
    checks=[
        Check(
            lambda df: ~df.duplicated(subset=["exchange", "symbol", "timestamp"]).any(),
            name="no_duplicate_timestamps",
            error="Duplicate timestamps detected for same exchange+symbol (violates PRIMARY KEY constraint)",
        )
    ],
    # Schema-level settings
    strict=True,  # Reject columns not defined in schema
    coerce=True,  # Coerce data types
    ordered=False,  # Column order doesn't matter
    description="Schema for funding rates from Binance Futures",
)


# Statistical validation schema (warnings only, not errors)
FUNDING_RATES_STATISTICAL_SCHEMA = DataFrameSchema(
    columns={
        "funding_rate": Column(float),
    },
    checks=[
        Check(
            lambda df: check_funding_rate_distribution(df),
            name="funding_rate_distribution",
            error="Funding rate distribution check failed",
            # This will be a warning in quality checks
        ),
    ],
    strict=False,  # Allow other columns
    description="Statistical validation for funding rates (warning level)",
)


# =============================================================================
# Statistical Check Functions
# =============================================================================


def check_funding_rate_distribution(df: pa.typing.DataFrame) -> bool:
    """
    Check funding rate distribution (mean close to zero, reasonable std).

    Funding rates should be roughly balanced (mean close to zero) over time.
    Large mean or std might indicate data issues.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with 'funding_rate' column

    Returns
    -------
    bool
        True if distribution looks reasonable (warning if False)
    """
    if df.empty or len(df) < 10:
        return True

    funding_rates = df["funding_rate"]

    # Calculate mean and std
    mean_rate = funding_rates.mean()
    std_rate = funding_rates.std()

    # Check if mean is reasonably close to zero (within 0.1% = 0.001)
    # and std is reasonable (< 1% = 0.01)
    mean_ok = abs(mean_rate) < 0.001
    std_ok = std_rate < 0.01

    # Both checks should pass
    return mean_ok and std_ok


# =============================================================================
# Validation Functions
# =============================================================================


def validate_funding_rates_dataframe(
    df: pa.typing.DataFrame, strict: bool = True
) -> pa.typing.DataFrame:
    """
    Validate funding rates DataFrame against schema.

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
        return FUNDING_RATES_SCHEMA.validate(df, lazy=False)
    try:
        return FUNDING_RATES_SCHEMA.validate(df, lazy=True)
    except pa.errors.SchemaErrors as e:
        # Return errors for inspection
        return e


def validate_funding_rates_statistical(df: pa.typing.DataFrame) -> tuple:
    """
    Run statistical validation checks on funding rates DataFrame (non-blocking).

    Returns warnings instead of raising errors. Use for data quality monitoring.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame to validate

    Returns
    -------
    tuple
        (passed: bool, warnings: list)
    """
    warnings = []

    try:
        FUNDING_RATES_STATISTICAL_SCHEMA.validate(df, lazy=True)
        return (True, [])
    except pa.errors.SchemaErrors as e:
        # Convert schema errors to warnings
        if hasattr(e, "failure_cases") and not e.failure_cases.empty:
            for _, row in e.failure_cases.iterrows():
                warnings.append(
                    {
                        "check": row.get("check", "N/A"),
                        "column": row.get("column", "DataFrame"),
                        "error": str(row.get("failure_case", "N/A")),
                    }
                )
        return (False, warnings)


# Export schemas
__all__ = [
    "FUNDING_RATES_SCHEMA",
    "FUNDING_RATES_STATISTICAL_SCHEMA",
    "validate_funding_rates_dataframe",
    "validate_funding_rates_statistical",
]
