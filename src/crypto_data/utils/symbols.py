"""
Symbol Extraction Utilities

Provides symbol extraction utilities for the crypto-data package.
"""

import logging
from typing import List, Optional, Sequence
import duckdb

from crypto_data.universe_filters import (
    has_excluded_symbol,
    has_excluded_tag,
    resolve_exclude_symbols,
    resolve_exclude_tags,
)

logger = logging.getLogger(__name__)


def get_binance_symbols_from_universe(
    db_path: str,
    start_date: str,
    end_date: str,
    top_n: int,
    exclude_tags: Optional[Sequence[str]] = None,
    exclude_symbols: Optional[Sequence[str]] = None,
) -> List[str]:
    """
    Extract unique symbols from universe in database (UNION approach).

    Returns all symbols that appeared in top N at ANY point during the period.
    This avoids survivorship bias by including symbols that entered/exited
    the top N ranking during the time period.

    Important
    ---------
    This function is intended for download coverage / prefetching only.
    It returns a superset of symbols across the full date range so Binance
    data can be downloaded once without missing assets that enter later.
    It is NOT a point-in-time investable universe.

    For research or backtests, keep point-in-time membership by filtering
    against the ``crypto_universe`` table on each rebalance month/date after
    the data has been downloaded.

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
    exclude_tags : Sequence[str], optional
        CMC tags/tag families to exclude from symbol extraction. Defaults to
        stablecoins, wrapped assets, and tokenized assets.
    exclude_symbols : Sequence[str], optional
        Symbols to exclude from extraction. Defaults to [].

    Returns
    -------
    List[str]
        List of symbols with USDT suffix (e.g., ['BTCUSDT', 'ETHUSDT', ...])
        Empty list if extraction fails or no data found

    Example
    -------
    >>> symbols = get_binance_symbols_from_universe(
    ...     'crypto_data.db',
    ...     '2024-01-01',
    ...     '2024-12-31',
    ...     top_n=50
    ... )
    >>> # Returns ~60-70 unique symbols (captures entries/exits)
    >>> len(symbols)
    67
    """
    exclude_tags = resolve_exclude_tags(exclude_tags)
    exclude_symbols = resolve_exclude_symbols(exclude_symbols)

    try:
        with duckdb.connect(db_path, read_only=True) as conn:
            # Intentionally build a UNION across the full period.
            # This is a download-time superset, not a tradable PIT universe.
            result = conn.execute("""
                SELECT DISTINCT symbol, tags
                FROM crypto_universe
                WHERE date >= ?
                    AND date <= ?
                    AND rank <= ?
                ORDER BY symbol
            """, [start_date, end_date, top_n]).fetchall()

        # If any historical row for a symbol has an excluded tag, exclude the
        # symbol from the download superset. This protects re-runs over legacy
        # unfiltered universe tables where some snapshots may have stale/null tags.
        symbol_exclusions = {}
        for symbol, tags in result:
            symbol_exclusions.setdefault(symbol, False)
            if has_excluded_tag(tags, exclude_tags):
                symbol_exclusions[symbol] = True

        base_symbols = [
            symbol
            for symbol, excluded_by_tag in symbol_exclusions.items()
            if not excluded_by_tag and not has_excluded_symbol(symbol, exclude_symbols)
        ]
        symbols = [f"{symbol}USDT" for symbol in sorted(base_symbols)]

        logger.info(f"Extracted {len(symbols)} unique symbols from universe (top {top_n} across period)")
        logger.debug(f"Symbols: {', '.join(symbols[:10])}{'...' if len(symbols) > 10 else ''}")

        return symbols

    except Exception as e:
        logger.error(f"Failed to extract symbols from universe: {e}")
        logger.error(f"Make sure crypto_universe table in {db_path} contains data for {start_date} to {end_date}")
        logger.error(f"Run universe ingestion first to populate universe.")
        return []
