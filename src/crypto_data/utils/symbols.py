"""
Symbol Extraction Utilities

Provides symbol extraction utilities for the crypto-data package.
"""

import logging
from typing import List
import duckdb

logger = logging.getLogger(__name__)


def get_symbols_from_universe(
    db_path: str,
    start_date: str,
    end_date: str,
    top_n: int
) -> List[str]:
    """
    Extract unique symbols from universe in database (UNION strategy).

    Returns all symbols that appeared in top N at ANY point during the period.
    This avoids survivorship bias by including symbols that entered/exited
    the top N ranking during the time period.

    Parameters
    ----------
    db_path : str
        Path to database file
    start_date : str
        Start date (YYYY-MM-DD)
    end_date : str
        End date (YYYY-MM-DD)
    top_n : int
        Rank threshold (e.g., 50 for top 50)

    Returns
    -------
    List[str]
        List of symbols with USDT suffix (e.g., ['BTCUSDT', 'ETHUSDT', ...])
        Empty list if extraction fails or no data found

    Example
    -------
    >>> symbols = get_symbols_from_universe(
    ...     'crypto_data.db',
    ...     '2024-01-01',
    ...     '2024-12-31',
    ...     top_n=50
    ... )
    >>> # Returns ~60-70 unique symbols (captures entries/exits)
    >>> len(symbols)
    67
    """
    try:
        conn = duckdb.connect(db_path, read_only=True)

        # Get unique symbols from universe within date range and rank threshold
        result = conn.execute("""
            SELECT DISTINCT symbol
            FROM crypto_universe
            WHERE date >= ?
                AND date <= ?
                AND rank <= ?
            ORDER BY symbol
        """, [start_date, end_date, top_n]).fetchall()

        conn.close()

        # Convert to list and add USDT suffix
        symbols = [f"{row[0]}USDT" for row in result]

        logger.info(f"Extracted {len(symbols)} unique symbols from universe (top {top_n} across period)")
        logger.debug(f"Symbols: {', '.join(symbols[:10])}{'...' if len(symbols) > 10 else ''}")

        return symbols

    except Exception as e:
        logger.error(f"Failed to extract symbols from universe: {e}")
        logger.error(f"Make sure crypto_universe table in {db_path} contains data for {start_date} to {end_date}")
        logger.error(f"Run universe ingestion first to populate universe.")
        return []
