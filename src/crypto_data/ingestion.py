"""
Unified Ingestion Module

Handles all data ingestion for CoinMarketCap and Binance (sync and async).

Functions:
    - ingest_universe(): CoinMarketCap universe rankings
    - ingest_binance_async(): Binance OHLCV data (async parallel downloads)
    - populate_database(): Complete workflow (universe → binance)

Internal helpers:
    - _fetch_snapshot(): Fetch CoinMarketCap snapshot
    - _download_single_month(): Download single month async
    - _download_symbol_data_type_async(): Download all months for symbol+data_type
"""

import logging
import tempfile
import asyncio
import threading
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Tuple, Set
import pandas as pd

from crypto_data.clients.coinmarketcap import CoinMarketCapClient
from crypto_data.clients.binance_vision_async import BinanceDataVisionClientAsync
from crypto_data.database import CryptoDatabase
from crypto_data.enums import DataType, Interval
from crypto_data.utils.dates import generate_month_list, generate_day_list
from crypto_data.utils.formatting import format_file_size
from crypto_data.utils.database import import_to_duckdb, import_metrics_to_duckdb, import_funding_rates_to_duckdb, data_exists
from crypto_data.utils.symbols import get_symbols_from_universe
from crypto_data.utils.ingestion_helpers import (
    initialize_ingestion_stats,
    process_download_results,
    log_ingestion_summary
)

logger = logging.getLogger(__name__)

# Cache for auto-discovered 1000-prefix ticker mappings (e.g., PEPEUSDT → 1000PEPEUSDT)
# Persists across multiple downloads in the same session to avoid repeated 404s
_ticker_mappings = {}
_ticker_mappings_lock = threading.Lock()


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def _process_metrics_results(
    results: List[Dict],
    conn,
    stats: Dict[str, int],
    symbol: str
) -> None:
    """
    Process open interest metrics download results: import to DuckDB, update stats, cleanup.

    Similar to process_download_results() but for metrics data (CSV instead of ZIP).

    Parameters
    ----------
    results : List[Dict]
        Download results with keys:
        - success: bool
        - symbol: str
        - data_type: 'open_interest'
        - date: str (YYYY-MM-DD format)
        - file_path: Path or None
        - error: str or None ('not_found' for 404s)
    conn : duckdb.DuckDBPyConnection
        Database connection for imports
    stats : Dict[str, int]
        Statistics dictionary (modified in place)
    symbol : str
        Symbol being processed
    """
    for result in results:
        if result['success']:
            try:
                import_metrics_to_duckdb(
                    conn=conn,
                    file_path=result['file_path'],
                    symbol=symbol,
                    exchange='binance'
                )
                stats['downloaded'] += 1
                logger.debug(f"    ✓ {result['date']} imported")

                # Delete temp file
                if result['file_path'].exists():
                    result['file_path'].unlink()

            except Exception as e:
                logger.error(f"    ✗ Import failed {result['date']}: {type(e).__name__}: {e}")
                logger.debug(f"    Error details: {repr(e)}")
                stats['failed'] += 1

        else:
            # Download failed or not found
            if result['error'] == 'not_found':
                stats['not_found'] += 1
                logger.debug(f"    - {result['date']} not found")
            else:
                stats['failed'] += 1
                logger.debug(f"    ✗ {result['date']}: {result['error']}")


def _process_funding_rates_results(
    results: List[Dict],
    conn,
    stats: Dict[str, int],
    symbol: str
) -> None:
    """
    Process funding rate download results: import to DuckDB, update stats, cleanup.

    Similar to process_download_results() but for funding rate data (monthly, not daily).

    Parameters
    ----------
    results : List[Dict]
        Download results with keys:
        - success: bool
        - symbol: str
        - data_type: 'funding_rates'
        - month: str (YYYY-MM format)
        - file_path: Path or None
        - error: str or None ('not_found' for 404s)
    conn : duckdb.DuckDBPyConnection
        Database connection for imports
    stats : Dict[str, int]
        Statistics dictionary (modified in place)
    symbol : str
        Symbol being processed
    """
    for result in results:
        if result['success']:
            try:
                import_funding_rates_to_duckdb(
                    conn=conn,
                    file_path=result['file_path'],
                    symbol=symbol,
                    exchange='binance'
                )
                stats['downloaded'] += 1
                logger.debug(f"    ✓ {result['month']} imported")

                # Delete temp file
                if result['file_path'].exists():
                    result['file_path'].unlink()

            except Exception as e:
                logger.error(f"    ✗ Import failed {result['month']}: {type(e).__name__}: {e}")
                logger.debug(f"    Error details: {repr(e)}")
                stats['failed'] += 1

        else:
            # Download failed or not found
            if result['error'] == 'not_found':
                stats['not_found'] += 1
                logger.debug(f"    - {result['month']} not found")
            else:
                stats['failed'] += 1
                logger.debug(f"    ✗ {result['month']}: {result['error']}")


def _validate_and_parse_dates(start_date: str, end_date: str) -> Tuple[datetime, datetime]:
    """
    Validate and parse date strings.

    Parameters
    ----------
    start_date : str
        Start date in YYYY-MM-DD format
    end_date : str
        End date in YYYY-MM-DD format

    Returns
    -------
    Tuple[datetime, datetime]
        Parsed start and end datetime objects

    Raises
    ------
    ValueError
        If dates are invalid or start_date is after end_date
    """
    try:
        start = datetime.strptime(start_date, "%Y-%m-%d")
    except ValueError as e:
        raise ValueError(f"Invalid start_date format. Expected YYYY-MM-DD, got '{start_date}': {e}")

    try:
        end = datetime.strptime(end_date, "%Y-%m-%d")
    except ValueError as e:
        raise ValueError(f"Invalid end_date format. Expected YYYY-MM-DD, got '{end_date}': {e}")

    if start > end:
        raise ValueError(f"start_date ({start_date}) must be before or equal to end_date ({end_date})")

    return start, end


# =============================================================================
# UNIVERSE INGESTION (CoinMarketCap)
# =============================================================================

async def _fetch_snapshot(
    client: CoinMarketCapClient,
    date: pd.Timestamp,
    top_n: int,
    excluded_tags: List[str],
    excluded_symbols: List[str],
    date_str: str
) -> Tuple[pd.DataFrame, Set[str], Set[str]]:
    """
    Fetch universe snapshot from CoinMarketCap API with historical rankings.

    Filters out coins that match any of the excluded tags (case-insensitive)
    or excluded symbols (case-insensitive).

    Parameters
    ----------
    client : CoinMarketCapClient
        Async CMC client (shared session)
    date : pd.Timestamp
        Date for the snapshot
    top_n : int
        Number of top coins to fetch
    excluded_tags : List[str]
        Tags to exclude
    excluded_symbols : List[str]
        Symbols to exclude
    date_str : str
        Date string in YYYY-MM-DD format

    Returns
    -------
    Tuple[pd.DataFrame, Set[str], Set[str]]
        - DataFrame with columns: date, symbol, rank, market_cap, categories
        - Set of symbols excluded by tags
        - Set of symbols excluded by symbol blacklist
    """
    # Fetch from CoinMarketCap historical listings API (async)
    coins = await client.get_historical_listings(date_str, top_n)

    # Convert excluded tags to lowercase once (for case-insensitive matching)
    excluded_tags_lower = [tag.lower() for tag in excluded_tags]

    # Convert excluded symbols to uppercase once (for case-insensitive matching)
    excluded_symbols_upper = [symbol.upper() for symbol in excluded_symbols]

    # Process each coin
    records = []
    excluded_by_tag: Set[str] = set()
    excluded_by_symbol: Set[str] = set()

    for coin in coins:
        symbol = coin.get('symbol', '').upper()
        rank = coin.get('cmcRank', 0)

        # Get tags (similar to CoinGecko categories)
        tags = coin.get('tags', [])
        tags_str = ','.join(tags) if tags else ''

        # Filter based on excluded tags (case-insensitive)
        if any(tag.lower() in excluded_tags_lower for tag in tags):
            logger.debug(f"  → Filtered {symbol} (excluded tag)")
            excluded_by_tag.add(symbol)
            continue

        # Filter based on excluded symbols (case-insensitive)
        if symbol in excluded_symbols_upper:
            logger.debug(f"  → Filtered {symbol} (blacklisted symbol)")
            excluded_by_symbol.add(symbol)
            continue

        # Get market cap from quotes
        market_cap = 0
        quotes = coin.get('quotes', [])
        if quotes and len(quotes) > 0:
            market_cap = quotes[0].get('marketCap', 0)

        record = {
            'date': date,
            'symbol': symbol,
            'rank': rank,
            'market_cap': market_cap,
            'categories': tags_str  # Store tags as comma-separated string
        }

        records.append(record)

    # Convert to DataFrame
    df = pd.DataFrame(records)

    return df, excluded_by_tag, excluded_by_symbol


async def ingest_universe(
    db_path: str,
    months: List[str],
    top_n: int = 100,
    exclude_tags: List[str] = [],
    exclude_symbols: List[str] = [],
    max_concurrent: int = 5
) -> Dict[str, Set[str]]:
    """
    Fetch universe snapshots for multiple months in parallel, write to DB sequentially.

    This function downloads all month snapshots concurrently (max_concurrent at a time)
    and then writes them sequentially to avoid database write conflicts.

    Parameters
    ----------
    db_path : str
        Path to DuckDB database file
    months : List[str]
        List of month strings in YYYY-MM format (e.g., ['2024-01', '2024-02'])
    top_n : int
        Number of top cryptocurrencies to fetch (default: 100)
    exclude_tags : List[str]
        List of CoinMarketCap tags to exclude (e.g., ['stablecoin', 'wrapped-tokens'])
    exclude_symbols : List[str]
        List of symbols to exclude (e.g., ['LUNA', 'FTT', 'UST'])
    max_concurrent : int
        Maximum number of concurrent API requests (default: 5)

    Example
    -------
    >>> import asyncio
    >>> asyncio.run(ingest_universe_batch_async(
    ...     db_path='crypto_data.db',
    ...     months=['2024-01', '2024-02', '2024-03'],
    ...     top_n=100,
    ...     exclude_tags=['stablecoin'],
    ...     exclude_symbols=['LUNA'],
    ...     max_concurrent=5
    ... ))
    """
    # Create async client with concurrency control
    async with CoinMarketCapClient(max_concurrent=max_concurrent) as client:
        # Build tasks for all months
        tasks = []
        for month in months:
            snapshot_date_str = f'{month}-01'
            snapshot_date = pd.Timestamp(snapshot_date_str)

            # Create task for fetching this month
            task = _fetch_snapshot(
                client=client,
                date=snapshot_date,
                top_n=top_n,
                excluded_tags=exclude_tags,
                excluded_symbols=exclude_symbols,
                date_str=snapshot_date_str
            )
            tasks.append((month, snapshot_date_str, task))

        # Download all months in parallel
        logger.info(f"Downloading {len(tasks)} months in parallel...")
        results = await asyncio.gather(*[task for _, _, task in tasks], return_exceptions=True)

    # Write to database sequentially (avoid write conflicts)
    db = CryptoDatabase(db_path)
    conn = db.conn

    success_count = 0
    fail_count = 0
    all_excluded_by_tag: Set[str] = set()
    all_excluded_by_symbol: Set[str] = set()

    try:
        for i, (month, date_str, _) in enumerate(tasks):
            result = results[i]

            # Handle exceptions
            if isinstance(result, Exception):
                logger.error(f"  ⚠ Warning: Failed {date_str}: {result}")
                fail_count += 1
                continue

            # result is a tuple: (DataFrame, excluded_by_tag, excluded_by_symbol)
            df_new, excluded_by_tag, excluded_by_symbol = result

            # Aggregate exclusions (UNION across months)
            all_excluded_by_tag.update(excluded_by_tag)
            all_excluded_by_symbol.update(excluded_by_symbol)

            # Delete existing data for this date, then insert new data (ATOMIC)
            # This ensures top_n changes work correctly (e.g., 100→25 removes the extra 75)
            committed = False
            try:
                conn.execute("BEGIN TRANSACTION")

                # Delete all existing records for this date
                conn.execute("DELETE FROM crypto_universe WHERE date = ?", [date_str])
                logger.debug(f"Deleted existing records for {date_str}")

                # Insert new data if we have any
                if len(df_new) > 0:
                    conn.execute("INSERT INTO crypto_universe SELECT * FROM df_new")
                    logger.debug(f"Inserted {len(df_new)} new records for {date_str}")
                else:
                    logger.debug(f"No data to insert for {date_str}")

                conn.execute("COMMIT")
                committed = True
                success_count += 1

            except Exception as e:
                if not committed:
                    conn.execute("ROLLBACK")
                    logger.debug(f"Transaction rolled back for {date_str}")
                logger.error(f"Failed to update universe for {date_str}: {e}")
                fail_count += 1
                # Continue with other months instead of raising
                continue

    finally:
        db.close()

    # Summary log
    logger.info(f"✓ Downloaded {success_count}/{len(tasks)} snapshots successfully" +
                (f" ({fail_count} failed)" if fail_count > 0 else ""))

    # Return exclusion summary
    return {
        'by_tag': all_excluded_by_tag,
        'by_symbol': all_excluded_by_symbol
    }


# =============================================================================
# BINANCE ASYNC INGESTION (Parallel Downloads)
# =============================================================================

async def _download_single_month(
    client: BinanceDataVisionClientAsync,
    symbol: str,
    data_type: DataType,
    month: str,
    interval: Interval,
    temp_path: Path,
    progress_info: Dict
) -> Dict:
    """
    Download a single month file asynchronously.

    Parameters
    ----------
    client : BinanceDataVisionClientAsync
        Async client for downloads
    symbol : str
        Symbol to download (e.g., 'BTCUSDT')
    data_type : DataType
        Data type (DataType.SPOT or DataType.FUTURES)
    month : str
        Month in YYYY-MM format
    interval : Interval
        Kline interval (e.g., Interval.MIN_5)
    temp_path : Path
        Temporary directory for downloads
    progress_info : dict
        Shared progress info (for logging)

    Returns
    -------
    dict
        {
            'success': bool,
            'symbol': str,
            'data_type': str,
            'month': str,
            'file_path': Path or None,
            'error': str or None
        }
    """
    temp_file = temp_path / f"{symbol}-{data_type}-{month}.zip"

    try:
        success = await client.download_klines(
            symbol=symbol,
            data_type=data_type,
            month=month,
            interval=interval,
            output_path=temp_file
        )

        if not success:
            return {
                'success': False,
                'symbol': symbol,
                'data_type': data_type,
                'month': month,
                'file_path': None,
                'error': 'not_found'
            }

        return {
            'success': True,
            'symbol': symbol,
            'data_type': data_type,
            'month': month,
            'file_path': temp_file,
            'error': None
        }

    except Exception as e:
        logger.error(f"Download error {symbol} {data_type} {month}: {e}")
        return {
            'success': False,
            'symbol': symbol,
            'data_type': data_type,
            'month': month,
            'file_path': None,
            'error': str(e)
        }


async def _download_single_day(
    client: BinanceDataVisionClientAsync,
    symbol: str,
    date: str,
    temp_path: Path,
    progress_info: Dict
) -> Dict:
    """
    Download a single day metrics file asynchronously.

    Parameters
    ----------
    client : BinanceDataVisionClientAsync
        Async client for downloads
    symbol : str
        Symbol to download (e.g., 'BTCUSDT')
    date : str
        Date in YYYY-MM-DD format
    temp_path : Path
        Temporary directory for downloads
    progress_info : dict
        Shared progress info (for logging)

    Returns
    -------
    dict
        {
            'success': bool,
            'symbol': str,
            'data_type': 'open_interest',
            'date': str,
            'file_path': Path or None,
            'error': str or None
        }
    """
    temp_file = temp_path / f"{symbol}-metrics-{date}.zip"

    try:
        success = await client.download_metrics(
            symbol=symbol,
            date=date,
            output_path=temp_file
        )

        if not success:
            return {
                'success': False,
                'symbol': symbol,
                'data_type': 'open_interest',
                'date': date,
                'file_path': None,
                'error': 'not_found'
            }

        return {
            'success': True,
            'symbol': symbol,
            'data_type': 'open_interest',
            'date': date,
            'file_path': temp_file,
            'error': None
        }

    except Exception as e:
        logger.error(f"Download error {symbol} open_interest {date}: {e}")
        return {
            'success': False,
            'symbol': symbol,
            'data_type': 'open_interest',
            'date': date,
            'file_path': None,
            'error': str(e)
        }


async def _download_symbol_open_interest_async(
    symbol: str,
    days: List[str],
    temp_path: Path,
    max_concurrent: int = 100
) -> List[Dict]:
    """
    Download all days of open interest metrics for a symbol in parallel.

    Parameters
    ----------
    symbol : str
        Symbol to download
    days : List[str]
        List of days to download (YYYY-MM-DD format)
    temp_path : Path
        Temporary directory
    max_concurrent : int
        Maximum concurrent downloads (default: 100, higher than klines due to daily granularity)

    Returns
    -------
    List[dict]
        List of download results
    """
    if not days:
        logger.info(f"  {symbol} open_interest: No days to download")
        return []

    # Download all days in parallel
    async with BinanceDataVisionClientAsync(max_concurrent=max_concurrent) as client:
        progress_info = {'total': len(days)}

        tasks = [
            _download_single_day(
                client=client,
                symbol=symbol,
                date=day,
                temp_path=temp_path,
                progress_info=progress_info
            )
            for day in days
        ]

        # Gather all results (return_exceptions=True to continue on errors)
        results = await asyncio.gather(*tasks, return_exceptions=True)

    # Convert exceptions to error results
    processed_results = []
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            day = days[i]
            logger.error(f"  Exception downloading {symbol} open_interest {day}: {result}")
            processed_results.append({
                'success': False,
                'symbol': symbol,
                'data_type': 'open_interest',
                'date': day,
                'file_path': None,
                'error': str(result)
            })
        else:
            processed_results.append(result)

    return processed_results


async def _download_single_month_funding_rates(
    client: BinanceDataVisionClientAsync,
    symbol: str,
    month: str,
    temp_path: Path,
    progress_info: Dict
) -> Dict:
    """
    Download a single month funding rates file asynchronously.

    Parameters
    ----------
    client : BinanceDataVisionClientAsync
        Async client for downloads
    symbol : str
        Symbol to download (e.g., 'BTCUSDT')
    month : str
        Month in YYYY-MM format
    temp_path : Path
        Temporary directory for downloads
    progress_info : dict
        Shared progress info (for logging)

    Returns
    -------
    dict
        {
            'success': bool,
            'symbol': str,
            'data_type': 'funding_rates',
            'month': str,
            'file_path': Path or None,
            'error': str or None
        }
    """
    temp_file = temp_path / f"{symbol}-fundingRate-{month}.zip"

    try:
        success = await client.download_funding_rates(
            symbol=symbol,
            month=month,
            output_path=temp_file
        )

        if not success:
            return {
                'success': False,
                'symbol': symbol,
                'data_type': 'funding_rates',
                'month': month,
                'file_path': None,
                'error': 'not_found'
            }

        return {
            'success': True,
            'symbol': symbol,
            'data_type': 'funding_rates',
            'month': month,
            'file_path': temp_file,
            'error': None
        }

    except Exception as e:
        logger.error(f"  Download error {month}: {e}")
        return {
            'success': False,
            'symbol': symbol,
            'data_type': 'funding_rates',
            'month': month,
            'file_path': None,
            'error': str(e)
        }


async def _download_symbol_funding_rates_async(
    symbol: str,
    months: List[str],
    temp_path: Path,
    conn,
    skip_existing: bool,
    stats: Dict,
    max_concurrent: int = 50
) -> List[Dict]:
    """
    Download all months of funding rates for a symbol in parallel.

    Follows the klines pattern (monthly downloads, not daily like open_interest).

    Parameters
    ----------
    symbol : str
        Symbol to download
    months : List[str]
        List of months to download (YYYY-MM format)
    temp_path : Path
        Temporary directory
    conn : duckdb.DuckDBPyConnection
        Database connection (for checking existing data)
    skip_existing : bool
        Skip if data already exists
    stats : dict
        Statistics dictionary (updated in place)
    max_concurrent : int
        Maximum concurrent downloads (default: 50, higher than klines due to monthly granularity)

    Returns
    -------
    List[dict]
        List of download results
    """
    # Build download queue for this symbol (same as klines pattern)
    months_to_download = []

    for month in months:
        # Skip if data exists (SAME LOGIC AS SPOT/FUTURES)
        if skip_existing and data_exists(conn, symbol, month, 'funding_rates', interval=None, exchange='binance'):
            logger.debug(f"  Skipped {symbol} funding_rates {month} (already exists)")
            stats['skipped'] += 1
            continue

        months_to_download.append(month)

    if not months_to_download:
        logger.info(f"  {symbol} funding_rates: All data already exists (skipped {len(months)} months)")
        return []

    # Download all months in parallel
    async with BinanceDataVisionClientAsync(max_concurrent=max_concurrent) as client:
        progress_info = {'total': len(months_to_download)}

        tasks = [
            _download_single_month_funding_rates(
                client=client,
                symbol=symbol,
                month=month,
                temp_path=temp_path,
                progress_info=progress_info
            )
            for month in months_to_download  # Use filtered list
        ]

        # Gather all results (return_exceptions=True to continue on errors)
        results = await asyncio.gather(*tasks, return_exceptions=True)

    # Convert exceptions to error results
    processed_results = []
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            month = months_to_download[i]  # Use filtered list
            logger.error(f"  Exception downloading {symbol} funding_rates {month}: {result}")
            processed_results.append({
                'success': False,
                'symbol': symbol,
                'data_type': 'funding_rates',
                'month': month,
                'file_path': None,
                'error': str(result)
            })
        else:
            processed_results.append(result)

    return processed_results


async def _download_symbol_data_type_async(
    symbol: str,
    data_type: DataType,
    months: List[str],
    interval: Interval,
    temp_path: Path,
    conn,
    skip_existing: bool,
    stats: Dict,
    max_concurrent: int = 20,
    failure_threshold: int = 2
) -> List[Dict]:
    """
    Download all months for a symbol+data_type in parallel.

    This is where the async magic happens - all months download concurrently.

    Parameters
    ----------
    symbol : str
        Symbol to download
    data_type : DataType
        Data type (DataType.SPOT or DataType.FUTURES)
    months : List[str]
        List of months to download
    interval : Interval
        Kline interval
    temp_path : Path
        Temporary directory
    conn : duckdb.DuckDBPyConnection
        Database connection (for checking existing data)
    skip_existing : bool
        Skip if data already exists
    stats : dict
        Statistics dictionary (updated in place)
    max_concurrent : int
        Maximum concurrent downloads (default: 20)
    failure_threshold : int
        Stop if last N months are all missing (likely delisted) (default: 3)

    Returns
    -------
    List[dict]
        List of download results
    """
    # Build download queue for this symbol+data_type
    months_to_download = []

    for month in months:
        # Skip if data exists
        if skip_existing and data_exists(conn, symbol, month, data_type, interval, exchange='binance'):
            logger.debug(f"  Skipped {symbol} {data_type} {month} (already exists)")
            stats['skipped'] += 1
            continue

        months_to_download.append(month)

    if not months_to_download:
        logger.info(f"  {symbol} {data_type}: All data already exists (skipped {len(months)} months)")
        return []

    # Download all months in parallel
    async with BinanceDataVisionClientAsync(max_concurrent=max_concurrent) as client:
        progress_info = {'total': len(months_to_download)}

        tasks = [
            _download_single_month(
                client=client,
                symbol=symbol,
                data_type=data_type,
                month=month,
                interval=interval,
                temp_path=temp_path,
                progress_info=progress_info
            )
            for month in months_to_download
        ]

        # Gather all results (return_exceptions=True to continue on errors)
        results = await asyncio.gather(*tasks, return_exceptions=True)

    # Convert exceptions to error results
    processed_results = []
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            month = months_to_download[i]
            logger.error(f"  Exception downloading {symbol} {data_type} {month}: {result}")
            processed_results.append({
                'success': False,
                'symbol': symbol,
                'data_type': data_type,
                'month': month,
                'file_path': None,
                'error': str(result)
            })
        else:
            processed_results.append(result)

    # =========================================================================
    # GAP DETECTION: Stop at first N consecutive missing months (delisting)
    # Ignores "leading failures" (404s before token launch)
    # =========================================================================
    if failure_threshold > 0 and len(processed_results) > 0:
        # Step 1: Find first successful download (token launch date)
        first_success_idx = -1
        for i, result in enumerate(processed_results):
            if result['success']:
                first_success_idx = i
                break

        # If no successful downloads at all, return empty (token doesn't exist in this period)
        if first_success_idx == -1:
            logger.debug(f"  No successful downloads found - token may not exist in this period")
            return processed_results  # Return all failures (will show as "No data available")

        # Step 2: Scan for gaps AFTER first success (ignore leading failures)
        consecutive_failures = 0
        gap_start_idx = -1
        last_successful_idx = first_success_idx

        for i in range(first_success_idx + 1, len(processed_results)):
            result = processed_results[i]

            if not result['success'] and result['error'] == 'not_found':
                # Failure AFTER token launch: increment counter
                consecutive_failures += 1

                # Mark gap start if this is the first failure in sequence
                if consecutive_failures == 1:
                    gap_start_idx = i

                # Check if we've hit the threshold
                if consecutive_failures >= failure_threshold:
                    # GAP DETECTED: Stop importing everything after this gap
                    months_filtered = len(processed_results) - gap_start_idx
                    leading_failures = first_success_idx  # Count of 404s before launch

                    last_good_month = months_to_download[last_successful_idx]
                    gap_start_month = months_to_download[gap_start_idx]
                    first_success_month = months_to_download[first_success_idx]

                    logger.warning(f"  ⚠ {symbol} {data_type} detected gap: {consecutive_failures}+ consecutive months missing starting at {gap_start_month}")
                    logger.warning(f"  ⚠ Token launched: {first_success_month}, last good month: {last_good_month}")
                    logger.warning(f"  ⚠ Stopping import - filtering out {months_filtered} months after gap")

                    # Keep only results BEFORE the gap (including leading failures for correct indexing)
                    processed_results = processed_results[:gap_start_idx]

                    # Update stats: count leading failures + gap as skipped
                    stats['skipped'] += months_filtered

                    break  # Stop scanning
            else:
                # Success: reset counter
                consecutive_failures = 0
                last_successful_idx = i

    return processed_results


def ingest_binance_async(
    db_path: str,
    symbols: List[str],
    data_types: List[DataType],
    start_date: str,
    end_date: str,
    interval: Interval = Interval.MIN_5,
    skip_existing: bool = True,
    max_concurrent_klines: int = 20,
    max_concurrent_metrics: int = 100,
    max_concurrent_funding: int = 50,
    failure_threshold: int = 3
):
    """
    Download Binance data asynchronously and import to DuckDB.

    PERFORMANCE: 5-10x faster than synchronous version.

    Strategy:
    - For each symbol + data_type: download ALL months in parallel (async)
    - Import to DuckDB as files arrive (sync, but fast)
    - Symbols processed sequentially (prevents rate-limiting)
    - Auto-detect gaps/delisting: if N+ consecutive months missing, stop at gap
    - Auto-discover 1000-prefix futures: if all futures fail, retry with 1000{SYMBOL}

    Process:
    1. For each symbol + data_type combination:
       a. Check ticker mapping cache for 1000-prefix (e.g., PEPEUSDT → 1000PEPEUSDT)
       b. Build list of months to download (skip existing)
       c. Download all months in parallel using asyncio.gather()
       d. If all futures downloads 404: auto-retry with 1000{SYMBOL}, cache if successful
       e. Detect gaps: scan chronologically, if N+ consecutive months missing → stop
       f. Import only files BEFORE first gap (filter out everything after)
       g. Clean up temp files
    2. Update availability metadata (using original symbol name for consistency)

    Parameters
    ----------
    db_path : str
        Path to DuckDB database file
    symbols : List[str]
        List of symbols to download (e.g., ['BTCUSDT', 'ETHUSDT'])
    data_types : List[DataType]
        List of data types to download (e.g., [DataType.SPOT, DataType.FUTURES])
    start_date : str
        Start date in YYYY-MM-DD format
    end_date : str
        End date in YYYY-MM-DD format
    interval : Interval
        Kline interval (default: Interval.MIN_5)
    skip_existing : bool
        Skip if data already exists in database (default: True)
    max_concurrent_klines : int
        Maximum concurrent downloads for spot/futures klines (default: 20)
        Recommended: 10-20 for optimal S3 performance with high-frequency data
    max_concurrent_metrics : int
        Maximum concurrent downloads for open_interest metrics (default: 100)
        Higher default due to daily granularity (365 files/year vs 105k for 5m klines)
    max_concurrent_funding : int
        Maximum concurrent downloads for funding_rates (default: 50)
        Higher default due to monthly granularity (12 files/year)
    failure_threshold : int
        Auto-detect gaps/delisting: stop at first N consecutive missing months (default: 3)
        Example: FTTUSDT with threshold=1 → stops after first missing month (2022-12)
        Set to 0 to disable gap detection (downloads all available data)

    Example
    -------
    >>> from crypto_data import ingest_binance_async, DataType, Interval
    >>> ingest_binance_async(
    ...     db_path='crypto_data.db',
    ...     symbols=['BTCUSDT', 'ETHUSDT', 'SOLUSDT'],
    ...     data_types=[DataType.SPOT, DataType.FUTURES, DataType.OPEN_INTEREST],
    ...     start_date='2024-01-01',
    ...     end_date='2024-12-31',
    ...     interval=Interval.MIN_5,
    ...     max_concurrent_klines=20,
    ...     max_concurrent_metrics=100
    ... )

    Notes
    -----
    - Requires aiohttp: pip install aiohttp
    - Uses async/await for downloads, sync for DuckDB imports
    - Symbols are processed sequentially to respect rate limits
    - Each symbol downloads all its months concurrently
    - Auto-detects 1000-prefix futures: PEPE/SHIB/BONK use 1000PEPEUSDT/1000SHIBUSDT/1000BONKUSDT
      Mappings cached in session to avoid repeated 404s
    - Auto-detects gaps/delisting: stops at FIRST N+ consecutive missing months
      Example: FTTUSDT with threshold=1
        → Downloads Jan-Nov 2022 (OK)
        → Detects gap at Dec 2022 (404)
        → STOPS: Does NOT import Sep 2023+ even though data exists
    """
    # Parse and validate dates
    start, end = _validate_and_parse_dates(start_date, end_date)

    # Generate month list
    months = generate_month_list(start, end)

    # Connect to database
    db = CryptoDatabase(db_path)
    conn = db.conn

    # Initialize statistics
    stats = initialize_ingestion_stats()

    # Create temp directory for downloads
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        # Process each symbol + data_type
        for symbol in symbols:
            for data_type in data_types:
                # === BRANCH: Open Interest (metrics data) ===
                if data_type == DataType.OPEN_INTEREST:
                    # Check if we have a cached ticker mapping (e.g., PEPEUSDT → 1000PEPEUSDT)
                    with _ticker_mappings_lock:
                        download_symbol = _ticker_mappings.get(symbol, symbol)

                    # Log if using cached mapping
                    if download_symbol != symbol:
                        logger.info(f"Downloading {symbol} {data_type} (using cached mapping: {download_symbol})...")
                    else:
                        logger.info(f"Downloading {symbol} {data_type}...")

                    # Generate day list instead of month list
                    days = generate_day_list(start, end)

                    # Download all days in parallel (async)
                    results = asyncio.run(
                        _download_symbol_open_interest_async(
                            symbol=download_symbol,
                            days=days,
                            temp_path=temp_path,
                            max_concurrent=max_concurrent_metrics
                        )
                    )

                    # Process results: import to DuckDB with transaction
                    # IMPORTANT: Always store with original symbol (user-requested), not download_symbol
                    committed = False
                    try:
                        conn.execute("BEGIN TRANSACTION")
                        _process_metrics_results(results, conn, stats, symbol)
                        conn.execute("COMMIT")
                        committed = True
                    except Exception as e:
                        if not committed:
                            conn.execute("ROLLBACK")
                            logger.debug(f"Transaction rolled back for {symbol} {data_type}")
                        logger.error(f"Failed to import {symbol} {data_type}: {e}")
                        # Don't raise - continue with other symbols

                    # Auto-retry with 1000-prefix if all downloads failed with 404
                    # Only retry if we haven't already used a cached mapping
                    if (download_symbol == symbol and  # Not using cached mapping already
                        len(results) > 0 and  # Must have actual results
                        all(not r['success'] and r['error'] == 'not_found' for r in results)):

                        # Try with 1000-prefix (e.g., PEPEUSDT → 1000PEPEUSDT)
                        prefixed_symbol = f"1000{symbol}"
                        logger.info(f"  → No data found, retrying with {prefixed_symbol}...")

                        # Retry download with prefixed symbol
                        retry_results = asyncio.run(
                            _download_symbol_open_interest_async(
                                symbol=prefixed_symbol,
                                days=days,
                                temp_path=temp_path,
                                max_concurrent=max_concurrent_metrics
                            )
                        )

                        # Check if retry was successful (at least one success)
                        if any(r['success'] for r in retry_results):
                            # Cache the mapping for future use (thread-safe)
                            with _ticker_mappings_lock:
                                _ticker_mappings[symbol] = prefixed_symbol
                            logger.info(f"  ✓ Auto-discovered mapping: {symbol} → {prefixed_symbol}")

                            # Process retry results with transaction
                            committed = False
                            try:
                                conn.execute("BEGIN TRANSACTION")
                                _process_metrics_results(retry_results, conn, stats, symbol)
                                conn.execute("COMMIT")
                                committed = True
                            except Exception as e:
                                if not committed:
                                    conn.execute("ROLLBACK")
                                    logger.debug(f"Transaction rolled back for {symbol} {data_type} retry")
                                logger.error(f"Failed to import {symbol} {data_type} retry: {e}")
                                # Don't raise - continue with other symbols

                            # Update symbol reference for availability metadata
                            download_symbol = prefixed_symbol
                        else:
                            logger.debug(f"  - Retry with {prefixed_symbol} also failed")

                    continue  # Skip to next data_type

                # === BRANCH: Funding Rates (futures only) ===
                if data_type == DataType.FUNDING_RATES:
                    # Check if we have a cached ticker mapping (e.g., PEPEUSDT → 1000PEPEUSDT)
                    with _ticker_mappings_lock:
                        download_symbol = _ticker_mappings.get(symbol, symbol)

                    # Log if using cached mapping
                    if download_symbol != symbol:
                        logger.info(f"Downloading {symbol} {data_type} (using cached mapping: {download_symbol})...")
                    else:
                        logger.info(f"Downloading {symbol} {data_type}...")

                    # Funding rates use monthly files (not daily like open_interest)
                    # Download all months in parallel (async)
                    results = asyncio.run(
                        _download_symbol_funding_rates_async(
                            symbol=download_symbol,
                            months=months,
                            temp_path=temp_path,
                            conn=conn,
                            skip_existing=skip_existing,
                            stats=stats,
                            max_concurrent=max_concurrent_funding
                        )
                    )

                    # Process results: import to DuckDB with transaction
                    # IMPORTANT: Always store with original symbol (user-requested), not download_symbol
                    committed = False
                    try:
                        conn.execute("BEGIN TRANSACTION")
                        _process_funding_rates_results(results, conn, stats, symbol)
                        conn.execute("COMMIT")
                        committed = True
                    except Exception as e:
                        if not committed:
                            conn.execute("ROLLBACK")
                            logger.debug(f"Transaction rolled back for {symbol} {data_type}")
                        logger.error(f"Failed to import {symbol} {data_type}: {e}")
                        # Don't raise - continue with other symbols

                    # Auto-retry with 1000-prefix if all downloads failed with 404
                    # Only retry if we haven't already used a cached mapping
                    if (download_symbol == symbol and  # Not using cached mapping already
                        len(results) > 0 and  # Must have actual results
                        all(not r['success'] and r['error'] == 'not_found' for r in results)):

                        # Try with 1000-prefix (e.g., PEPEUSDT → 1000PEPEUSDT)
                        prefixed_symbol = f"1000{symbol}"
                        logger.info(f"  → No data found, retrying with {prefixed_symbol}...")

                        # Retry download with prefixed symbol
                        retry_results = asyncio.run(
                            _download_symbol_funding_rates_async(
                                symbol=prefixed_symbol,
                                months=months,
                                temp_path=temp_path,
                                conn=conn,
                                skip_existing=skip_existing,
                                stats=stats,
                                max_concurrent=max_concurrent_funding
                            )
                        )

                        # Check if retry was successful (at least one success)
                        if any(r['success'] for r in retry_results):
                            # Cache the mapping for future use (thread-safe)
                            with _ticker_mappings_lock:
                                _ticker_mappings[symbol] = prefixed_symbol
                            logger.info(f"  ✓ Auto-discovered mapping: {symbol} → {prefixed_symbol}")

                            # Process retry results with transaction
                            committed = False
                            try:
                                conn.execute("BEGIN TRANSACTION")
                                _process_funding_rates_results(retry_results, conn, stats, symbol)
                                conn.execute("COMMIT")
                                committed = True
                            except Exception as e:
                                if not committed:
                                    conn.execute("ROLLBACK")
                                    logger.debug(f"Transaction rolled back for {symbol} {data_type} retry")
                                logger.error(f"Failed to import {symbol} {data_type} retry: {e}")
                                # Don't raise - continue with other symbols

                            # Update symbol reference for availability metadata
                            download_symbol = prefixed_symbol
                        else:
                            logger.debug(f"  - Retry with {prefixed_symbol} also failed")

                    continue  # Skip to next data_type

                # === BRANCH: Klines (spot/futures) ===
                # Check if we have a cached ticker mapping (e.g., PEPEUSDT → 1000PEPEUSDT)
                with _ticker_mappings_lock:
                    download_symbol = _ticker_mappings.get(symbol, symbol)

                # Log if using cached mapping
                if download_symbol != symbol:
                    logger.info(f"Downloading {symbol} {data_type} (using cached mapping: {download_symbol})...")
                else:
                    logger.info(f"Downloading {symbol} {data_type}...")

                # Download all months in parallel (async)
                results = asyncio.run(
                    _download_symbol_data_type_async(
                        symbol=download_symbol,
                        data_type=data_type,
                        months=months,
                        interval=interval,
                        temp_path=temp_path,
                        conn=conn,
                        skip_existing=skip_existing,
                        stats=stats,
                        max_concurrent=max_concurrent_klines,
                        failure_threshold=failure_threshold
                    )
                )

                # Process results: import to DuckDB using helper with transaction
                # IMPORTANT: Always store with original symbol (user-requested), not download_symbol
                # Transaction ensures atomicity: either ALL files imported or NONE
                committed = False
                try:
                    conn.execute("BEGIN TRANSACTION")
                    process_download_results(results, conn, stats, interval, symbol)
                    conn.execute("COMMIT")
                    committed = True
                except Exception as e:
                    if not committed:
                        conn.execute("ROLLBACK")
                        logger.debug(f"Transaction rolled back for {symbol} {data_type}")
                    logger.error(f"Failed to import {symbol} {data_type}: {e}")
                    # Don't raise - continue with other symbols

                # Auto-retry with 1000-prefix for futures if all downloads failed with 404
                # Only retry if we haven't already used a cached mapping
                if (data_type == DataType.FUTURES and
                    download_symbol == symbol and  # Not using cached mapping already
                    len(results) > 0 and  # Must have actual results (not skipped due to existing data)
                    all(not r['success'] and r['error'] == 'not_found' for r in results)):

                    # Try with 1000-prefix (e.g., PEPEUSDT → 1000PEPEUSDT)
                    prefixed_symbol = f"1000{symbol}"
                    logger.info(f"  → No data found, retrying with {prefixed_symbol}...")

                    # Retry download with prefixed symbol
                    retry_results = asyncio.run(
                        _download_symbol_data_type_async(
                            symbol=prefixed_symbol,
                            data_type=data_type,
                            months=months,
                            interval=interval,
                            temp_path=temp_path,
                            conn=conn,
                            skip_existing=skip_existing,
                            stats=stats,
                            max_concurrent=max_concurrent_klines,
                            failure_threshold=failure_threshold
                        )
                    )

                    # Check if retry was successful (at least one success)
                    if any(r['success'] for r in retry_results):
                        # Cache the mapping for future use (thread-safe)
                        with _ticker_mappings_lock:
                            _ticker_mappings[symbol] = prefixed_symbol
                        logger.info(f"  ✓ Auto-discovered mapping: {symbol} → {prefixed_symbol}")

                        # Process retry results using helper with transaction
                        committed = False
                        try:
                            conn.execute("BEGIN TRANSACTION")
                            process_download_results(retry_results, conn, stats, interval, symbol)
                            conn.execute("COMMIT")
                            committed = True
                        except Exception as e:
                            if not committed:
                                conn.execute("ROLLBACK")
                                logger.debug(f"Transaction rolled back for {symbol} {data_type} retry")
                            logger.error(f"Failed to import {symbol} {data_type} retry: {e}")
                            # Don't raise - continue with other symbols

                        # Update symbol reference for availability metadata
                        download_symbol = prefixed_symbol
                    else:
                        logger.debug(f"  - Retry with {prefixed_symbol} also failed")

    db.close()

    # Log comprehensive summary using helper
    log_ingestion_summary(
        stats=stats,
        db_path=db_path,
        symbols=symbols,
        start_date=start_date,
        end_date=end_date,
        interval=interval,
        show_availability=True  # Show detailed availability bars for async
    )


# =============================================================================
# UNIFIED DATABASE POPULATION FUNCTION
# =============================================================================

def populate_database(
    db_path: str,
    start_date: str,
    end_date: str,
    top_n: int,
    interval: Interval = Interval.MIN_5,
    data_types: List[DataType] = None,
    exclude_tags: List[str] = [],
    exclude_symbols: List[str] = []
):
    """
    Complete workflow: universe + binance in one call.

    Orchestrates the full ingestion pipeline:
    1. Ingest monthly universe snapshots
    2. Extract symbols from universe
    3. Download Binance OHLCV data (with auto-discovery of 1000-prefix futures)
    4. Display final summary (symbol count + database size)

    Parameters
    ----------
    db_path : str
        Path to DuckDB database file
    start_date : str
        Start date (YYYY-MM-DD)
    end_date : str
        End date (YYYY-MM-DD)
    top_n : int
        Number of top coins by market cap
    interval : Interval
        Kline interval (default: Interval.MIN_5)
    data_types : List[DataType], optional
        Data types to download (default: [DataType.SPOT, DataType.FUTURES])
        Options: DataType.SPOT, DataType.FUTURES, DataType.OPEN_INTEREST, DataType.FUNDING_RATES
    exclude_tags : List[str]
        List of CoinMarketCap tags to exclude (e.g., ['stablecoin', 'wrapped-tokens'])
        Default: [] (no exclusions)
    exclude_symbols : List[str]
        List of symbols to exclude (e.g., ['LUNA', 'FTT', 'UST'])
        Default: [] (no exclusions)

    Example
    -------
    >>> from crypto_data import populate_database, DataType, Interval
    >>> populate_database(
    ...     db_path='crypto_data.db',
    ...     start_date='2024-01-01',
    ...     end_date='2024-12-31',
    ...     top_n=100,
    ...     interval=Interval.MIN_5,
    ...     data_types=[DataType.SPOT, DataType.FUTURES, DataType.OPEN_INTEREST, DataType.FUNDING_RATES],
    ...     exclude_tags=['stablecoin', 'wrapped-tokens', 'privacy'],
    ...     exclude_symbols=['LUNA', 'FTT', 'UST']
    ... )
    """
    if data_types is None:
        data_types = [DataType.SPOT, DataType.FUTURES]

    logger.info("=" * 60)
    logger.info("Starting Database Population")
    logger.info("=" * 60)
    logger.info(f"  Database: {db_path}")
    logger.info(f"  Period: {start_date} → {end_date}")
    logger.info(f"  Top N: {top_n}")
    logger.info(f"  Interval: {interval}")
    logger.info(f"  Data types: {', '.join(data_types)}")
    logger.info("")

    # Parse and validate dates for monthly snapshots
    start, end = _validate_and_parse_dates(start_date, end_date)

    # Generate monthly snapshots
    months = generate_month_list(start, end)

    logger.info(f"Step 1/2: Ingesting Universe ({len(months)} monthly snapshots)")
    logger.info("-" * 60)

    # Ingest universe for all months in parallel (async)
    exclusions = asyncio.run(ingest_universe(
        db_path=db_path,
        months=months,
        top_n=top_n,
        exclude_tags=exclude_tags,
        exclude_symbols=exclude_symbols,
        max_concurrent=5
    ))

    logger.info("")
    logger.info(f"Step 2/2: Ingesting Binance Data")
    logger.info("-" * 60)

    # Extract symbols from universe
    symbols = get_symbols_from_universe(db_path, start_date, end_date, top_n)

    if not symbols:
        logger.error("No symbols extracted from universe! Cannot proceed with Binance ingestion.")
        logger.error("Make sure Step 1 (universe ingestion) completed successfully.")
        return

    logger.info("")

    # Ingest Binance (using parallel async version)
    ingest_binance_async(
        db_path=db_path,
        symbols=symbols,
        data_types=data_types,
        start_date=start_date,
        end_date=end_date,
        interval=interval,
        skip_existing=True,
        max_concurrent_klines=20,  # Optimal S3 performance for high-frequency data
        max_concurrent_metrics=100,  # Higher for daily open_interest (365 files/year)
        max_concurrent_funding=50,  # Higher for monthly funding_rates (12 files/year)
        failure_threshold=3  # Stop after 3 consecutive missing months
    )

    logger.info("")
    logger.info("=" * 60)
    logger.info("✓ Database Population Finished!")

    # Display exclusion summary (yellow by default due to "Excluded" keyword)
    if exclusions and (exclusions['by_tag'] or exclusions['by_symbol']):
        logger.info("")
        logger.info("Excluded assets:")

        if exclusions['by_tag']:
            tag_list = sorted(exclusions['by_tag'])
            logger.info(f"  By tag ({len(tag_list)}): {', '.join(tag_list)}")

        if exclusions['by_symbol']:
            symbol_list = sorted(exclusions['by_symbol'])
            logger.info(f"  By symbol ({len(symbol_list)}): {', '.join(symbol_list)}")

    # Log number of symbols with data
    try:
        db = CryptoDatabase(db_path)
        result = db.execute("""
            SELECT COUNT(DISTINCT symbol) FROM (
                SELECT symbol FROM spot WHERE exchange = 'binance'
                UNION
                SELECT symbol FROM futures WHERE exchange = 'binance'
            )
        """).fetchone()
        symbol_count = result[0] if result else 0
        logger.info(f"Symbols with data: {symbol_count}")
        db.close()
    except Exception as e:
        logger.debug(f"Could not count symbols: {e}")

    # Log database file size
    try:
        db_size = Path(db_path).stat().st_size
        logger.info(f"Database size: {format_file_size(db_size)} ({db_size:,} bytes)")
    except Exception as e:
        logger.debug(f"Could not get database size: {e}")

    logger.info("=" * 60)
