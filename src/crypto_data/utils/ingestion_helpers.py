"""
Ingestion Helpers - Shared utilities for Universe and Binance ingestion

This module contains helper functions used by both ingest_universe() and
ingest_binance_async() to reduce code duplication and improve maintainability.

Functions:
    - initialize_ingestion_stats(): Create standardized stats tracking dict
    - process_download_results(): Import downloads to DB, update stats, cleanup
    - query_data_availability(): Query first/last dates from OHLCV tables
    - log_ingestion_summary(): Log comprehensive ingestion summary with stats
"""

import logging
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from datetime import date

from crypto_data.utils.database import import_to_duckdb
from crypto_data.utils.formatting import format_file_size, format_availability_bar, format_availability_bar_daily

logger = logging.getLogger(__name__)


def initialize_ingestion_stats() -> Dict[str, int]:
    """
    Create standardized statistics tracking dictionary.

    Returns
    -------
    Dict[str, int]
        Statistics dictionary with keys: downloaded, skipped, failed, not_found
        All initialized to 0

    Example
    -------
    >>> stats = initialize_ingestion_stats()
    >>> stats['downloaded'] += 1
    >>> stats
    {'downloaded': 1, 'skipped': 0, 'failed': 0, 'not_found': 0}
    """
    return {
        'downloaded': 0,
        'skipped': 0,
        'failed': 0,
        'not_found': 0
    }


def process_download_results(
    results: List[Dict],
    conn,
    stats: Dict[str, int],
    interval: str,
    original_symbol: str
) -> None:
    """
    Process download results: import to DuckDB, update stats, cleanup temp files.

    This function handles the complete lifecycle of download results:
    1. Import successful downloads to DuckDB
    2. Update statistics (downloaded/failed/not_found)
    3. Delete temporary files after import
    4. Log progress at DEBUG level

    Used by both sync and async ingestion workflows to eliminate code duplication.

    Parameters
    ----------
    results : List[Dict]
        Download results with keys:
        - success: bool
        - symbol: str (download symbol, may be 1000-prefixed)
        - data_type: str ('spot' or 'futures')
        - month: str (YYYY-MM format)
        - file_path: Path or None
        - error: str or None ('not_found' for 404s)
    conn : duckdb.DuckDBPyConnection
        Database connection for imports
    stats : Dict[str, int]
        Statistics dictionary (modified in place)
        Keys: downloaded, skipped, failed, not_found
    interval : str
        Kline interval (e.g., '5m', '1h', '1d')
    original_symbol : str
        Original symbol to store in database (handles 1000-prefix normalization)
        Example: PEPEUSDT (even if downloaded as 1000PEPEUSDT)

    Side Effects
    ------------
    - Modifies stats dict in place
    - Imports data to DuckDB (via import_to_duckdb)
    - Deletes temporary files from disk
    - Logs import progress at DEBUG level

    Example
    -------
    >>> results = [
    ...     {'success': True, 'symbol': '1000PEPEUSDT', 'month': '2024-01', ...},
    ...     {'success': False, 'error': 'not_found', 'month': '2024-02', ...}
    ... ]
    >>> stats = initialize_ingestion_stats()
    >>> process_download_results(results, conn, stats, '5m', 'PEPEUSDT')
    >>> stats['downloaded']  # 1
    >>> stats['not_found']   # 1
    """
    for result in results:
        if result['success']:
            try:
                import_to_duckdb(
                    conn=conn,
                    file_path=result['file_path'],
                    symbol=result['symbol'],
                    data_type=result['data_type'],
                    interval=interval,
                    exchange='binance',
                    original_symbol=original_symbol  # Store with original symbol (e.g., PEPEUSDT)
                )
                stats['downloaded'] += 1
                logger.debug(f"    ✓ {result['month']} imported")

                # Delete temp file
                if result['file_path'].exists():
                    result['file_path'].unlink()

            except Exception as e:
                logger.error(f"    ✗ Import failed {result['month']}: {e}")
                stats['failed'] += 1

        else:
            # Download failed or not found
            if result['error'] == 'not_found':
                stats['not_found'] += 1
                logger.debug(f"    - {result['month']} not found")
            else:
                stats['failed'] += 1
                logger.debug(f"    ✗ {result['month']}: {result['error']}")


def query_data_availability(
    conn,
    symbols: List[str],
    interval: str
) -> List[Tuple[str, str, date, date]]:
    """
    Query availability summary from ALL data tables.

    Executes UNION query across spot, futures, open_interest, and funding_rates tables
    (filtered by exchange='binance') to get first/last dates for each symbol+data_type.

    Used by log_ingestion_summary() to display availability bars and coverage %.

    Parameters
    ----------
    conn : duckdb.DuckDBPyConnection
        Database connection
    symbols : List[str]
        List of symbols to query (e.g., ['BTCUSDT', 'ETHUSDT'])
    interval : str
        Kline interval (e.g., '5m') - only applies to spot/futures

    Returns
    -------
    List[Tuple[str, str, date, date]]
        [(symbol, data_type, first_date, last_date), ...]
        Sorted by (symbol, data_type)
        data_type in: 'spot', 'futures', 'open_interest', 'funding_rates'

    Example
    -------
    >>> results = query_data_availability(conn, ['BTCUSDT', 'ETHUSDT'], '5m')
    >>> # [('BTCUSDT', 'funding_rates', date(2024,1,1), date(2024,12,31)),
    >>> #  ('BTCUSDT', 'futures', date(2024,1,1), date(2024,12,31)),
    >>> #  ('BTCUSDT', 'open_interest', date(2024,1,1), date(2024,12,31)),
    >>> #  ('BTCUSDT', 'spot', date(2024,1,1), date(2024,12,31)),
    >>> #  ('ETHUSDT', 'spot', date(2024,2,1), date(2024,11,30))]
    """
    # Validate input
    if not symbols:
        return []

    placeholders = ','.join('?' * len(symbols))

    query = f"""
        SELECT
            symbol,
            'spot' as data_type,
            MIN(timestamp::DATE) as first_date,
            MAX(timestamp::DATE) as last_date
        FROM spot
        WHERE exchange = 'binance' AND symbol IN ({placeholders}) AND interval = ?
        GROUP BY symbol
        UNION ALL
        SELECT
            symbol,
            'futures' as data_type,
            MIN(timestamp::DATE) as first_date,
            MAX(timestamp::DATE) as last_date
        FROM futures
        WHERE exchange = 'binance' AND symbol IN ({placeholders}) AND interval = ?
        GROUP BY symbol
        UNION ALL
        SELECT
            symbol,
            'open_interest' as data_type,
            MIN(timestamp::DATE) as first_date,
            MAX(timestamp::DATE) as last_date
        FROM open_interest
        WHERE exchange = 'binance' AND symbol IN ({placeholders})
        GROUP BY symbol
        UNION ALL
        SELECT
            symbol,
            'funding_rates' as data_type,
            MIN(timestamp::DATE) as first_date,
            MAX(timestamp::DATE) as last_date
        FROM funding_rates
        WHERE exchange = 'binance' AND symbol IN ({placeholders})
        GROUP BY symbol
        ORDER BY symbol, data_type
    """

    # Parameters: symbols twice for spot+futures (with interval), symbols twice for OI+FR (no interval)
    result = conn.execute(
        query,
        symbols + [interval] + symbols + [interval] + symbols + symbols
    ).fetchall()
    return result


def log_ingestion_summary(
    stats: Dict[str, int],
    db_path: str,
    symbols: Optional[List[str]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    interval: Optional[str] = None,
    show_availability: bool = False
) -> None:
    """
    Log ingestion summary with statistics and optional availability visualization.

    Displays:
    - Download statistics (downloaded/skipped/failed/not_found)
    - Database file size
    - Symbol count (if database accessible)
    - Detailed availability bars (if show_availability=True)

    Parameters
    ----------
    stats : Dict[str, int]
        Statistics dictionary with counts: downloaded, skipped, failed, not_found
    db_path : str
        Path to DuckDB database file
    symbols : List[str], optional
        List of symbols processed (for availability query)
        Required if show_availability=True
    start_date, end_date : str, optional
        Date range in YYYY-MM-DD format (for availability bars)
        Required if show_availability=True
    interval : str, optional
        Interval used (e.g., '5m')
        Required if show_availability=True
    show_availability : bool
        If True, shows detailed availability bars (async mode)
        If False, shows simple stats only (sync mode)

    Logs
    ----
    - Summary statistics (always)
    - Notes about 'not found' reasons (if not_found > 0)
    - Availability bars with coverage % (if show_availability=True)
    - Database size (always)

    Example
    -------
    >>> stats = {'downloaded': 100, 'skipped': 20, 'failed': 5, 'not_found': 10}
    >>> log_ingestion_summary(stats, 'crypto_data.db', show_availability=False)
    # Logs: Downloaded: 100, Skipped: 20, Failed: 5, Not found: 10

    >>> log_ingestion_summary(
    ...     stats, 'crypto_data.db',
    ...     symbols=['BTCUSDT'], start_date='2024-01-01', end_date='2024-12-31',
    ...     interval='5m', show_availability=True
    ... )
    # Logs detailed availability bars for BTCUSDT
    """
    from crypto_data.database import CryptoDatabase

    # Log summary separator
    logger.info("=" * 60)
    logger.info("Ingestion Summary:")
    logger.info(f"  Downloaded: {stats['downloaded']}")
    logger.info(f"  Skipped (existing): {stats['skipped']}")
    logger.info(f"  Failed: {stats['failed']}")
    logger.info(f"  Not found: {stats['not_found']}")

    # Log detailed availability if requested
    if show_availability and symbols and start_date and end_date and interval:
        try:
            db = CryptoDatabase(db_path)
            conn = db.conn

            availability_result = query_data_availability(conn, symbols, interval)

            if availability_result:
                logger.info("")
                logger.info(f"Data Availability ({start_date} → {end_date}):")

                # Group results by symbol
                symbol_data = {}
                for symbol, data_type, first_date, last_date in availability_result:
                    if symbol not in symbol_data:
                        symbol_data[symbol] = {}
                    symbol_data[symbol][data_type] = (first_date, last_date)

                # ANSI color for warnings
                YELLOW = '\033[33m'
                RESET = '\033[0m'

                # All possible data types
                ALL_DATA_TYPES = ['spot', 'futures', 'open_interest', 'funding_rates']

                # Display hierarchically grouped by symbol
                for idx, symbol in enumerate(sorted(symbol_data.keys())):
                    # Symbol header with consistent indentation
                    separator = "  "
                    logger.info(f"{separator}───────────── {symbol} ─────────────")

                    # Detect missing data types for this symbol
                    available_types = set(symbol_data[symbol].keys())
                    missing_types = set(ALL_DATA_TYPES) - available_types

                    # Display each available data type with indentation
                    for data_type in ALL_DATA_TYPES:
                        if data_type in symbol_data[symbol]:
                            first_date, last_date = symbol_data[symbol][data_type]

                            # Use daily formatting for open_interest, monthly for others
                            if data_type == 'open_interest':
                                bar, pct, covered, total, dates = format_availability_bar_daily(
                                    first_date, last_date, start_date, end_date
                                )
                                coverage_str = f"{pct:3d}% ({covered:3d}/{total}d)"
                            else:
                                bar, pct, covered, total, dates = format_availability_bar(
                                    first_date, last_date, start_date, end_date
                                )
                                coverage_str = f"{pct:3d}% ({covered:2d}/{total}m)"

                            # Add warning if ANY data type is missing for this symbol
                            warning = ""
                            if missing_types:
                                missing_list = ', '.join(sorted(missing_types)).replace('_', ' ').upper()
                                warning = f" {YELLOW}⚠ MISSING: {missing_list}{RESET}"

                            # Only show warning on the first line for this symbol
                            if data_type == min(available_types):
                                logger.info(f"    {data_type:<15} {bar} {coverage_str} {dates}{warning}")
                            else:
                                logger.info(f"    {data_type:<15} {bar} {coverage_str} {dates}")

            db.close()

        except Exception as e:
            logger.warning(f"Could not query availability: {e}")

    # Log database file size
    try:
        db_size = Path(db_path).stat().st_size
        logger.info(f"Database size: {format_file_size(db_size)}")
    except Exception as e:
        logger.debug(f"Could not get database size: {e}")

    logger.info("=" * 60)
