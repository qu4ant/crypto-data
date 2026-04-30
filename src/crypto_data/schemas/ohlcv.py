"""
OHLCV Data Schema (Spot and Futures)

Pandera schemas for validating OHLCV (Open, High, Low, Close, Volume) data
from Binance spot and futures markets.

Features:
- OHLC relationship checks (high >= low, etc.)
- Non-negative price/volume validation
- Timestamp validation
- Statistical checks (price continuity, volume outliers)
- Separate schemas for spot vs futures (same structure, different semantics)
"""

import pandera.pandas as pa
from pandera.pandas import Check, Column, DataFrameSchema

from crypto_data.schemas.checks import (
    check_ohlc_relationships,
    check_price_continuity,
    check_volume_outliers,
)

# =============================================================================
# OHLCV Schema (Spot and Futures)
# =============================================================================

# Common schema for both spot and futures OHLCV data
OHLCV_SCHEMA = DataFrameSchema(
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
            description="Trading pair symbol (e.g., 'BTCUSDT')",
        ),
        "interval": Column(
            str,
            checks=[
                Check.isin(
                    [
                        "1m",
                        "3m",
                        "5m",
                        "15m",
                        "30m",
                        "1h",
                        "2h",
                        "4h",
                        "6h",
                        "8h",
                        "12h",
                        "1d",
                        "3d",
                        "1w",
                        "1M",
                    ]
                )
            ],
            nullable=False,
            description="Kline interval (e.g., '5m', '1h', '1d')",
        ),
        "timestamp": Column(
            "datetime64[ns]",
            checks=[
                Check(lambda s: s.notna().all(), error="Null timestamps detected"),
            ],
            nullable=False,
            description="Normalized candle close-time key (used as primary key)",
        ),
        # OHLC price columns
        "open": Column(
            float,
            checks=[
                Check.greater_than(0, error="Open price must be positive"),
                Check(lambda s: s.notna().all(), error="Null open prices detected"),
            ],
            nullable=False,
            description="Opening price",
        ),
        "high": Column(
            float,
            checks=[
                Check.greater_than(0, error="High price must be positive"),
                Check(lambda s: s.notna().all(), error="Null high prices detected"),
            ],
            nullable=False,
            description="Highest price",
        ),
        "low": Column(
            float,
            checks=[
                Check.greater_than(0, error="Low price must be positive"),
                Check(lambda s: s.notna().all(), error="Null low prices detected"),
            ],
            nullable=False,
            description="Lowest price",
        ),
        "close": Column(
            float,
            checks=[
                Check.greater_than(0, error="Close price must be positive"),
                Check(lambda s: s.notna().all(), error="Null close prices detected"),
            ],
            nullable=False,
            description="Closing price",
        ),
        # Volume columns
        "volume": Column(
            float,
            checks=[
                Check.greater_than_or_equal_to(0, error="Volume cannot be negative"),
                Check(lambda s: s.notna().all(), error="Null volume detected"),
            ],
            nullable=False,
            description="Base asset volume",
        ),
        "quote_volume": Column(
            float,
            checks=[
                Check.greater_than_or_equal_to(0, error="Quote volume cannot be negative"),
                Check(lambda s: s.notna().all(), error="Null quote volume detected"),
            ],
            nullable=False,
            description="Quote asset volume",
        ),
        # Optional columns (can be null)
        "trades_count": Column(
            "Int64",  # Nullable integer
            checks=[Check.greater_than_or_equal_to(0, error="Trades count cannot be negative")],
            nullable=True,
            description="Number of trades",
        ),
        "taker_buy_base_volume": Column(
            float,
            checks=[
                Check.greater_than_or_equal_to(0, error="Taker buy base volume cannot be negative")
            ],
            nullable=True,
            description="Taker buy base asset volume",
        ),
        "taker_buy_quote_volume": Column(
            float,
            checks=[
                Check.greater_than_or_equal_to(0, error="Taker buy quote volume cannot be negative")
            ],
            nullable=True,
            description="Taker buy quote asset volume",
        ),
    },
    # DataFrame-level checks (relationships between columns)
    checks=[
        Check(
            check_ohlc_relationships,
            name="ohlc_relationships",
            error="OHLC relationships violated (high must be >= low/open/close, low must be <= open/close)",
        ),
        Check(
            lambda df: ~df.duplicated(subset=["exchange", "symbol", "interval", "timestamp"]).any(),
            name="no_duplicate_primary_keys",
            error="Duplicate records detected for same exchange+symbol+interval+timestamp (violates PRIMARY KEY constraint)",
        ),
    ],
    # Schema-level settings
    strict=True,  # Reject columns not defined in schema
    coerce=True,  # Coerce data types
    ordered=False,  # Column order doesn't matter
    description="Schema for OHLCV data from Binance (spot/futures)",
)


# Statistical validation schema (warnings only, not errors)
# Use this for data quality monitoring but don't fail imports
OHLCV_STATISTICAL_SCHEMA = DataFrameSchema(
    columns={
        "close": Column(float),
        "volume": Column(float),
    },
    checks=[
        Check(
            lambda df: check_price_continuity(df, sigma=5.0),
            name="price_continuity",
            error="Extreme price jumps detected (>5 sigma)",
            # This will be a warning in quality checks, not a hard failure
        ),
        Check(
            lambda df: check_volume_outliers(df, iqr_multiplier=3.0),
            name="volume_outliers",
            error="Extreme volume outliers detected (>Q3 + 3*IQR)",
            # This will be a warning in quality checks, not a hard failure
        ),
    ],
    strict=False,  # Allow other columns
    description="Statistical validation for OHLCV data (warning level)",
)


# =============================================================================
# Validation Functions
# =============================================================================


def validate_ohlcv_dataframe(df: pa.typing.DataFrame, strict: bool = True) -> pa.typing.DataFrame:
    """
    Validate OHLCV DataFrame against schema.

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
        return OHLCV_SCHEMA.validate(df, lazy=False)
    try:
        return OHLCV_SCHEMA.validate(df, lazy=True)
    except pa.errors.SchemaErrors as e:
        # Return errors for inspection
        return e


def validate_ohlcv_statistical(df: pa.typing.DataFrame) -> tuple:
    """
    Run statistical validation checks on OHLCV DataFrame (non-blocking).

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
        OHLCV_STATISTICAL_SCHEMA.validate(df, lazy=True)
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


# =============================================================================
# Spot and Futures Schemas (same structure, different semantic meaning)
# =============================================================================

# Spot market schema (identical to OHLCV_SCHEMA)
# Just reference the same schema - they have identical validation rules
SPOT_SCHEMA = OHLCV_SCHEMA

# Futures market schema (identical to OHLCV_SCHEMA)
# Just reference the same schema - they have identical validation rules
FUTURES_SCHEMA = OHLCV_SCHEMA


# Export schemas
__all__ = [
    "FUTURES_SCHEMA",
    "OHLCV_SCHEMA",
    "OHLCV_STATISTICAL_SCHEMA",
    "SPOT_SCHEMA",
    "validate_ohlcv_dataframe",
    "validate_ohlcv_statistical",
]
