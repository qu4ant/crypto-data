"""
Open Interest Metrics Schema

Pandera schema for validating open interest metrics data from Binance futures.

Features:
- Non-negative open_interest validation
- Non-null checks
- Timestamp validation
- Statistical outlier detection
"""

import pandera.pandas as pa
from pandera.pandas import Column, Check, DataFrameSchema
import numpy as np


# =============================================================================
# Open Interest Schema
# =============================================================================

OPEN_INTEREST_SCHEMA = DataFrameSchema(
    columns={
        # Primary key columns
        'exchange': Column(
            str,
            checks=[
                Check.str_length(min_value=1, max_value=50),  # Any exchange name
                Check.str_matches(r'^[a-z0-9_-]+$')  # Lowercase alphanumeric + underscore/hyphen
            ],
            nullable=False,
            description="Exchange name (e.g., 'binance', 'bybit', 'kraken')"
        ),
        'symbol': Column(
            str,
            checks=[
                Check.str_matches(r'^[A-Z0-9]+$'),  # Uppercase alphanumeric
                Check.str_length(min_value=3, max_value=20)
            ],
            nullable=False,
            description="Futures contract symbol (e.g., 'BTCUSDT')"
        ),
        'timestamp': Column(
            'datetime64[ns]',
            checks=[
                Check(lambda s: s.notna().all(), error="Null timestamps detected"),
            ],
            nullable=False,
            description="Metric timestamp"
        ),

        # Metric columns
        'open_interest': Column(
            float,
            checks=[
                Check.greater_than(0, error="Open interest must be positive (zero values filtered during import)"),
                Check(lambda s: s.notna().all(), error="Null open interest detected"),
                Check(lambda s: ~np.isinf(s).any(), error="Infinite open interest values detected")
            ],
            nullable=False,
            description="Open interest value (sum of all open positions)"
        ),
    },

    # DataFrame-level checks (relationships between rows)
    checks=[
        Check(
            lambda df: ~df.duplicated(subset=['exchange', 'symbol', 'timestamp']).any(),
            name='no_duplicate_primary_keys',
            error="Duplicate records detected for same exchange+symbol+timestamp (violates PRIMARY KEY constraint)"
        )
    ],

    # Schema-level settings
    strict=True,  # Reject columns not defined in schema
    coerce=True,  # Coerce data types
    ordered=False,  # Column order doesn't matter
    description="Schema for open interest metrics from Binance Futures"
)


# Statistical validation schema (warnings only, not errors)
OPEN_INTEREST_STATISTICAL_SCHEMA = DataFrameSchema(
    columns={
        'open_interest': Column(float),
    },
    checks=[
        Check(
            lambda df: check_open_interest_outliers(df, z_threshold=5.0),
            name='open_interest_outliers',
            error="Extreme open interest outliers detected (>5 sigma)",
            # This will be a warning in quality checks, not a hard failure
        ),
    ],
    strict=False,  # Allow other columns
    description="Statistical validation for open interest data (warning level)"
)


# =============================================================================
# Statistical Check Functions
# =============================================================================

def check_open_interest_outliers(df: pa.typing.DataFrame, z_threshold: float = 5.0) -> bool:
    """
    Check for open interest outliers using Z-score method.

    Uses Z-score method:
    - Outliers are values with |Z-score| > threshold

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with 'open_interest' column
    z_threshold : float
        Z-score threshold for outlier detection (default: 5.0)

    Returns
    -------
    bool
        True if no extreme outliers detected (warning if False)
    """
    if df.empty or len(df) < 3:
        return True

    oi = df['open_interest']

    # Calculate z-scores
    mean_oi = oi.mean()
    std_oi = oi.std()

    if std_oi == 0:
        return True

    z_scores = np.abs((oi - mean_oi) / std_oi)
    outliers = (z_scores > z_threshold).sum()

    # Return False if outliers detected (will be a warning)
    return outliers == 0


# =============================================================================
# Validation Functions
# =============================================================================

def validate_open_interest_dataframe(df: pa.typing.DataFrame, strict: bool = True) -> pa.typing.DataFrame:
    """
    Validate open interest DataFrame against schema.

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
        return OPEN_INTEREST_SCHEMA.validate(df, lazy=False)
    else:
        try:
            return OPEN_INTEREST_SCHEMA.validate(df, lazy=True)
        except pa.errors.SchemaErrors as e:
            # Return errors for inspection
            return e


def validate_open_interest_statistical(df: pa.typing.DataFrame) -> tuple:
    """
    Run statistical validation checks on open interest DataFrame (non-blocking).

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
        OPEN_INTEREST_STATISTICAL_SCHEMA.validate(df, lazy=True)
        return (True, [])
    except pa.errors.SchemaErrors as e:
        # Convert schema errors to warnings
        if hasattr(e, 'failure_cases') and not e.failure_cases.empty:
            for _, row in e.failure_cases.iterrows():
                warnings.append({
                    'check': row.get('check', 'N/A'),
                    'column': row.get('column', 'DataFrame'),
                    'error': str(row.get('failure_case', 'N/A'))
                })
        return (False, warnings)


# Export schemas
__all__ = [
    'OPEN_INTEREST_SCHEMA',
    'OPEN_INTEREST_STATISTICAL_SCHEMA',
    'validate_open_interest_dataframe',
    'validate_open_interest_statistical'
]
