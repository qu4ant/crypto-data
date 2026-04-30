"""
Custom Pandera Check Functions

Reusable validation functions for crypto data schemas.
These checks are used across multiple schemas for consistency.
"""

import numpy as np
import pandas as pd


def check_ohlc_relationships(df: pd.DataFrame) -> bool:
    """
    Check OHLC relationship constraints.

    Rules:
    - high >= low (always)
    - high >= open
    - high >= close
    - low <= open
    - low <= close

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with 'open', 'high', 'low', 'close' columns

    Returns
    -------
    bool
        True if all relationships are valid
    """
    if df.empty:
        return True

    violations = []

    # Check high >= low
    if (df["high"] < df["low"]).any():
        violations.append("high < low")

    # Check high >= open
    if (df["high"] < df["open"]).any():
        violations.append("high < open")

    # Check high >= close
    if (df["high"] < df["close"]).any():
        violations.append("high < close")

    # Check low <= open
    if (df["low"] > df["open"]).any():
        violations.append("low > open")

    # Check low <= close
    if (df["low"] > df["close"]).any():
        violations.append("low > close")

    # Pandera reports the False result; violations list is for callers/debugging.
    return not violations


def check_price_continuity(df: pd.DataFrame, sigma: float = 5.0) -> bool:
    """
    Check if price returns are within N standard deviations (statistical outlier detection).

    Calculates log returns and checks if any exceed N*sigma threshold.
    This catches extreme price jumps that might indicate data errors.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with 'close' column
    sigma : float
        Number of standard deviations threshold (default: 5.0)

    Returns
    -------
    bool
        True if all returns are within N*sigma (warning if False)
    """
    if df.empty or len(df) < 2:
        return True

    # Calculate log returns
    returns = np.log(df["close"] / df["close"].shift(1))
    returns = returns.dropna()

    if len(returns) == 0:
        return True

    # Calculate mean and std
    mean_return = returns.mean()
    std_return = returns.std()

    if std_return == 0:
        return True

    # Check for outliers (beyond N sigma)
    z_scores = np.abs((returns - mean_return) / std_return)
    outliers = (z_scores > sigma).sum()

    # Return False if outliers detected (will be a warning, not error)
    return outliers == 0


def check_volume_outliers(df: pd.DataFrame, iqr_multiplier: float = 3.0) -> bool:
    """
    Check for volume outliers using IQR method.

    Uses Interquartile Range (IQR) method:
    - Outliers are values > Q3 + (IQR * multiplier) or < Q1 - (IQR * multiplier)
    - Default multiplier of 3.0 is more lenient than typical 1.5

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with 'volume' column
    iqr_multiplier : float
        IQR multiplier for outlier detection (default: 3.0)

    Returns
    -------
    bool
        True if no extreme volume outliers detected (warning if False)
    """
    if df.empty:
        return True

    # Filter out zero volumes (common for some periods)
    volumes = df["volume"][df["volume"] > 0]

    if len(volumes) < 4:  # Need at least 4 values for quartiles
        return True

    # Calculate quartiles and IQR
    q1 = volumes.quantile(0.25)
    q3 = volumes.quantile(0.75)
    iqr = q3 - q1

    if iqr == 0:
        return True

    # Define outlier bounds
    lower_bound = q1 - (iqr_multiplier * iqr)
    upper_bound = q3 + (iqr_multiplier * iqr)

    # Check for outliers
    outliers = ((volumes < lower_bound) | (volumes > upper_bound)).sum()

    # Return False if outliers detected (will be a warning)
    return outliers == 0


def check_timestamp_monotonic(df: pd.DataFrame) -> bool:
    """
    Check if timestamps are monotonically increasing (no time reversals).

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with 'timestamp' column

    Returns
    -------
    bool
        True if timestamps are strictly increasing
    """
    if df.empty or len(df) < 2:
        return True

    # Check if timestamps are strictly increasing
    return df["timestamp"].is_monotonic_increasing


def check_no_duplicate_ranks_per_date(df: pd.DataFrame) -> bool:
    """
    Check that no rank appears twice on the same date.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with 'date' and 'rank' columns

    Returns
    -------
    bool
        True if no duplicate ranks per date
    """
    if df.empty:
        return True

    # Check for duplicates in (date, rank) combinations
    duplicates = df.duplicated(subset=["date", "rank"]).sum()

    return duplicates == 0
