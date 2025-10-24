"""
Interval Validation for Crypto Data

Validates that database intervals match requested intervals to prevent data mixing.
Uses both filename pattern matching and database metadata for validation.
"""

import logging
import re
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


def extract_interval_from_filename(db_path: str) -> Optional[str]:
    """
    Extract interval from database filename.

    Parses filenames like:
    - crypto_5m.db → '5m'
    - crypto_1h.db → '1h'
    - crypto_4h.db → '4h'
    - data.db → None (doesn't match pattern)

    Parameters
    ----------
    db_path : str
        Path to database file

    Returns
    -------
    str or None
        Interval string if pattern matches, None otherwise

    Examples
    --------
    >>> extract_interval_from_filename('crypto_5m.db')
    '5m'
    >>> extract_interval_from_filename('/path/to/crypto_1h.db')
    '1h'
    >>> extract_interval_from_filename('data.db')
    None
    """
    filename = Path(db_path).stem  # Get filename without extension

    # Pattern: crypto_{interval} where interval is like 5m, 1h, 4h, 1d
    pattern = r'^crypto_([0-9]+[smhd])$'
    match = re.match(pattern, filename)

    if match:
        interval = match.group(1)
        logger.debug(f"Extracted interval '{interval}' from filename '{filename}'")
        return interval

    logger.debug(f"Filename '{filename}' doesn't match crypto_{{interval}} pattern")
    return None


def validate_interval_consistency(db_path: str, requested_interval: str, db):
    """
    Validate interval consistency between filename, database, and request.

    Performs two checks:
    1. Filename check: If filename matches crypto_{interval}.db pattern,
       validate it matches requested interval
    2. Database metadata check: If database has stored interval, validate
       it matches requested interval. If no stored interval, store it.

    Parameters
    ----------
    db_path : str
        Path to database file
    requested_interval : str
        Requested interval (e.g., '5m', '1h')
    db : CryptoDatabase
        Database instance with metadata methods

    Raises
    ------
    ValueError
        If interval mismatch is detected in filename or database

    Examples
    --------
    >>> db = CryptoDatabase('crypto_5m.db')
    >>> validate_interval_consistency('crypto_5m.db', '5m', db)  # OK
    >>> validate_interval_consistency('crypto_5m.db', '1h', db)  # ERROR
    """
    logger.info("Validating interval consistency...")

    # Check 1: Filename validation
    filename_interval = extract_interval_from_filename(db_path)

    if filename_interval:
        if filename_interval != requested_interval:
            raise ValueError(
                f"\n"
                f"❌ Interval mismatch detected!\n"
                f"\n"
                f"  Filename suggests: {filename_interval} (from {Path(db_path).name})\n"
                f"  Requested interval: {requested_interval}\n"
                f"\n"
                f"Solutions:\n"
                f"  • Use correct database: --db crypto_{requested_interval}.db --interval {requested_interval}\n"
                f"  • Or use correct interval: --db crypto_{filename_interval}.db --interval {filename_interval}\n"
                f"\n"
                f"Note: Each interval needs its own database file.\n"
            )
        logger.debug(f"✓ Filename check passed: {filename_interval} == {requested_interval}")
    else:
        logger.warning(
            f"⚠️  Filename '{Path(db_path).name}' doesn't follow crypto_{{interval}}.db pattern. "
            f"Recommend using: crypto_{requested_interval}.db"
        )

    # Check 2: Database metadata validation
    stored_interval = db.get_interval()

    if stored_interval is None:
        # First run - store the interval
        db.set_metadata('interval', requested_interval)
        logger.info(f"✓ Stored interval '{requested_interval}' in database metadata")
    elif stored_interval != requested_interval:
        raise ValueError(
            f"\n"
            f"❌ Database interval mismatch!\n"
            f"\n"
            f"  This database contains: {stored_interval} data\n"
            f"  Requested interval: {requested_interval}\n"
            f"\n"
            f"This database is dedicated to {stored_interval} data and cannot store {requested_interval} data.\n"
            f"\n"
            f"Solutions:\n"
            f"  • Use the correct database for {requested_interval} data:\n"
            f"    python scripts/populate_binance.py --db crypto_{requested_interval}.db --interval {requested_interval} ...\n"
            f"  • Or query the existing {stored_interval} data:\n"
            f"    python scripts/populate_binance.py --db {Path(db_path).name} --interval {stored_interval} ...\n"
        )
    else:
        logger.debug(f"✓ Metadata check passed: {stored_interval} == {requested_interval}")

    logger.info("✓ Interval validation passed")
