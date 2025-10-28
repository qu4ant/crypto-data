"""
Unified Ingestion Module

Handles all data ingestion for CoinMarketCap and Binance (sync and async).

Functions:
    - ingest_universe(): CoinMarketCap universe rankings
    - ingest_binance_async(): Binance OHLCV data (async parallel downloads)
    - sync(): Complete workflow (universe → binance)

Internal helpers:
    - _fetch_snapshot(): Fetch CoinMarketCap snapshot
    - _download_single_month(): Download single month async
    - _download_symbol_data_type_async(): Download all months for symbol+data_type
"""

import logging
import tempfile
import asyncio
from pathlib import Path
from datetime import datetime
from typing import List, Dict
import pandas as pd

from crypto_data.clients.coinmarketcap import CoinMarketCapClient
from crypto_data.clients.binance_vision_async import BinanceDataVisionClientAsync
from crypto_data.database import CryptoDatabase
from crypto_data.utils.config import load_universe_config
from crypto_data.utils.dates import generate_month_list
from crypto_data.utils.formatting import format_file_size
from crypto_data.utils.database import import_to_duckdb, data_exists
from crypto_data.utils.symbols import get_symbols_from_universe
from crypto_data.utils.ingestion_helpers import (
    initialize_ingestion_stats,
    process_download_results,
    log_ingestion_summary
)

logger = logging.getLogger(__name__)

# Project root for config loading
project_root = Path(__file__).parent.parent.parent

# Cache for auto-discovered 1000-prefix ticker mappings (e.g., PEPEUSDT → 1000PEPEUSDT)
# Persists across multiple downloads in the same session to avoid repeated 404s
_ticker_mappings = {}


# =============================================================================
# UNIVERSE INGESTION (CoinMarketCap)
# =============================================================================

def _fetch_snapshot(
    date: pd.Timestamp,
    top_n: int,
    excluded_tags: List[str],
    excluded_symbols: List[str],
    date_str: str
) -> pd.DataFrame:
    """
    Fetch universe snapshot from CoinMarketCap API with historical rankings.

    Filters out coins that match any of the excluded tags (case-insensitive)
    or excluded symbols (case-insensitive).

    Returns
    -------
    pd.DataFrame
        DataFrame with columns: date, symbol, rank, market_cap, categories
    """
    # Instantiate client and fetch from CoinMarketCap historical listings API
    client = CoinMarketCapClient()
    coins = client.get_historical_listings(date_str, top_n)
    logger.info(f"  → Fetched {len(coins)} coins from CoinMarketCap")

    # Convert excluded tags to lowercase once (for case-insensitive matching)
    excluded_tags_lower = [tag.lower() for tag in excluded_tags]

    # Convert excluded symbols to uppercase once (for case-insensitive matching)
    excluded_symbols_upper = [symbol.upper() for symbol in excluded_symbols]

    # Process each coin
    records = []
    filtered_count = 0

    for coin in coins:
        symbol = coin.get('symbol', '').upper()
        rank = coin.get('cmcRank', 0)

        # Get tags (similar to CoinGecko categories)
        tags = coin.get('tags', [])
        tags_str = ','.join(tags) if tags else ''

        # Filter based on excluded tags (case-insensitive)
        if any(tag.lower() in excluded_tags_lower for tag in tags):
            logger.debug(f"  → Filtered {symbol} (excluded tag)")
            filtered_count += 1
            continue

        # Filter based on excluded symbols (case-insensitive)
        if symbol in excluded_symbols_upper:
            logger.debug(f"  → Filtered {symbol} (blacklisted symbol)")
            filtered_count += 1
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

    logger.info(f"  → Kept {len(df)} coins, filtered {filtered_count}")

    return df


def ingest_universe(
    db_path: str,
    date: str,
    top_n: int = 100,
    rate_limit_delay: float = 0.0
):
    """
    Fetch universe snapshot from CoinMarketCap with historical rankings and save to DuckDB.

    Process:
    1. Fetch top N markets from CoinMarketCap historical API (1 API call)
    2. Filter based on excluded tags (stablecoins, wrapped tokens, etc.) and symbols (LUNA, FTT, etc.)
    3. Transform to DataFrame with metadata (rank, market_cap, categories)
    4. DELETE existing data for this date, INSERT new data (atomic transaction)

    Total API calls: 1 (CoinMarketCap only)

    Parameters
    ----------
    db_path : str
        Path to DuckDB database file
    date : str
        Date of the snapshot (YYYY-MM-DD format)
    top_n : int
        Number of top cryptocurrencies to fetch (default: 100)
    rate_limit_delay : float
        Delay between API calls in seconds (default: 0.0)
        With 0.0, relies on 429 retry logic for automatic throttling

    Example
    -------
    >>> ingest_universe(
    ...     db_path='crypto_data.db',
    ...     date='2022-01-01',
    ...     top_n=100
    ... )
    """
    logger.info(f"Starting CoinMarketCap universe ingestion")
    logger.info(f"  Database: {db_path}")
    logger.info(f"  Date: {date}")
    logger.info(f"  Top N: {top_n}")

    # Load universe config (excluded tags and symbols)
    excluded_tags, excluded_symbols = load_universe_config()
    if excluded_tags:
        logger.info(f"  Excluding tags: {', '.join(excluded_tags[:5])}{'...' if len(excluded_tags) > 5 else ''}")
    if excluded_symbols:
        logger.info(f"  Excluding symbols: {', '.join(excluded_symbols)}")

    # Parse date
    snapshot_date = pd.Timestamp(date)

    # Fetch from CoinMarketCap
    logger.info(f"Fetching historical rankings from CoinMarketCap API...")
    df_new = _fetch_snapshot(snapshot_date, top_n, excluded_tags, excluded_symbols, date)

    logger.info(f"Fetched {len(df_new)} coins")

    # Connect to database
    db = CryptoDatabase(db_path)
    conn = db.conn

    # Atomic transaction: DELETE + INSERT
    # DELETE removes previous snapshot for this date (start fresh)
    # Ensures consistency: each run is complete
    committed = False
    try:
        conn.execute("BEGIN TRANSACTION")

        conn.execute("DELETE FROM crypto_universe WHERE date = ?", [date])
        logger.debug(f"Deleted existing snapshot for {date}")

        # Only insert if we have data (empty DataFrame would cause DuckDB error)
        if len(df_new) > 0:
            conn.execute("INSERT INTO crypto_universe SELECT * FROM df_new")
            logger.debug(f"Inserted {len(df_new)} new records")

        conn.execute("COMMIT")
        committed = True

        logger.info(f"✓ Updated {len(df_new)} records for {date}")

    except Exception as e:
        # Only ROLLBACK if transaction was not committed
        if not committed:
            conn.execute("ROLLBACK")
            logger.debug("Transaction rolled back")
        logger.error(f"Failed to update universe: {e}")
        raise
    finally:
        db.close()

    logger.info("CoinMarketCap universe ingestion complete")


# =============================================================================
# BINANCE ASYNC INGESTION (Parallel Downloads)
# =============================================================================

async def _download_single_month(
    client: BinanceDataVisionClientAsync,
    symbol: str,
    data_type: str,
    month: str,
    interval: str,
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
    data_type : str
        Data type ('spot' or 'futures')
    month : str
        Month in YYYY-MM format
    interval : str
        Kline interval (e.g., '5m')
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


async def _download_symbol_data_type_async(
    symbol: str,
    data_type: str,
    months: List[str],
    interval: str,
    temp_path: Path,
    conn,
    skip_existing: bool,
    stats: Dict,
    max_concurrent: int = 20,
    failure_threshold: int = 3
) -> List[Dict]:
    """
    Download all months for a symbol+data_type in parallel.

    This is where the async magic happens - all months download concurrently.

    Parameters
    ----------
    symbol : str
        Symbol to download
    data_type : str
        Data type ('spot' or 'futures')
    months : List[str]
        List of months to download
    interval : str
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
        if skip_existing and data_exists(conn, symbol, month, data_type, interval):
            logger.debug(f"  Skipped {symbol} {data_type} {month} (already exists)")
            stats['skipped'] += 1
            continue

        months_to_download.append(month)

    if not months_to_download:
        logger.info(f"  {symbol} {data_type}: All data already exists (skipped {len(months)} months)")
        return []

    logger.info(f"  {symbol} {data_type}: Downloading {len(months_to_download)} months in parallel...")

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
    data_types: List[str],
    start_date: str,
    end_date: str,
    interval: str = '5m',
    skip_existing: bool = True,
    max_concurrent: int = 20,
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
    data_types : List[str]
        List of data types to download ('spot', 'futures')
    start_date : str
        Start date in YYYY-MM-DD format
    end_date : str
        End date in YYYY-MM-DD format
    interval : str
        Kline interval (default: '5m')
    skip_existing : bool
        Skip if data already exists in database (default: True)
    max_concurrent : int
        Maximum concurrent downloads per symbol (default: 20)
        Recommended: 10-20 for optimal S3 performance
    failure_threshold : int
        Auto-detect gaps/delisting: stop at first N consecutive missing months (default: 3)
        Example: FTTUSDT with threshold=1 → stops after first missing month (2022-12)
        Set to 0 to disable gap detection (downloads all available data)

    Example
    -------
    >>> ingest_binance_async(
    ...     db_path='crypto_data.db',
    ...     symbols=['BTCUSDT', 'ETHUSDT', 'SOLUSDT'],
    ...     data_types=['spot', 'futures'],
    ...     start_date='2024-01-01',
    ...     end_date='2024-12-31',
    ...     max_concurrent=20
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
    logger.info(f"Starting Async Binance Ingestion")
    logger.info(f"  Database: {db_path}")
    logger.info(f"  Symbols: {len(symbols)} symbols")
    logger.info(f"  Data types: {data_types}")
    logger.info(f"  Date range: {start_date} to {end_date}")
    logger.info(f"  Interval: {interval}")
    logger.info(f"  Max concurrent downloads per symbol: {max_concurrent}")
    logger.info(f"  Gap detection threshold: {failure_threshold} consecutive months (0 = disabled)")

    # Parse dates
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    # Generate month list
    months = generate_month_list(start, end)
    logger.info(f"  Months to process: {len(months)}")

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
                # Check if we have a cached ticker mapping (e.g., PEPEUSDT → 1000PEPEUSDT)
                download_symbol = _ticker_mappings.get(symbol, symbol)

                # Log if using cached mapping
                if download_symbol != symbol:
                    logger.info(f"Processing {symbol} {data_type} (using cached mapping: {download_symbol})...")
                else:
                    logger.info(f"Processing {symbol} {data_type}...")

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
                        max_concurrent=max_concurrent,
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
                if (data_type == 'futures' and
                    download_symbol == symbol and  # Not using cached mapping already
                    all(not r['success'] and r['error'] == 'not_found' for r in results)):

                    # Try with 1000-prefix (e.g., PEPEUSDT → 1000PEPEUSDT)
                    prefixed_symbol = f"1000{symbol}"
                    logger.info(f"  → All futures 404s, retrying with {prefixed_symbol}...")

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
                            max_concurrent=max_concurrent,
                            failure_threshold=failure_threshold
                        )
                    )

                    # Check if retry was successful (at least one success)
                    if any(r['success'] for r in retry_results):
                        # Cache the mapping for future use
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
# UNIFIED SYNC FUNCTION
# =============================================================================

def sync(
    db_path: str,
    start_date: str,
    end_date: str,
    top_n: int,
    interval: str = '5m',
    data_types: List[str] = None
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
    interval : str
        Kline interval (default: '5m')
    data_types : List[str], optional
        Data types to download (default: ['spot', 'futures'])

    Example
    -------
    >>> sync(
    ...     db_path='crypto_data.db',
    ...     start_date='2024-01-01',
    ...     end_date='2024-12-31',
    ...     top_n=100,
    ...     interval='5m',
    ...     data_types=['spot', 'futures']
    ... )
    """
    if data_types is None:
        data_types = ['spot', 'futures']

    logger.info("=" * 60)
    logger.info("Starting Complete Sync")
    logger.info("=" * 60)
    logger.info(f"  Database: {db_path}")
    logger.info(f"  Period: {start_date} → {end_date}")
    logger.info(f"  Top N: {top_n}")
    logger.info(f"  Interval: {interval}")
    logger.info(f"  Data types: {', '.join(data_types)}")
    logger.info("")

    # Parse dates for monthly snapshots
    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')

    # Generate monthly snapshots
    months = generate_month_list(start, end)

    logger.info(f"Step 1/2: Ingesting Universe ({len(months)} monthly snapshots)")
    logger.info("-" * 60)

    # Ingest universe for each month
    for i, month in enumerate(months, 1):
        snapshot_date = f'{month}-01'
        logger.info(f"  [{i}/{len(months)}] Universe snapshot: {snapshot_date}")
        try:
            ingest_universe(db_path=db_path, date=snapshot_date, top_n=top_n)
        except Exception as e:
            logger.error(f"  ⚠ Warning: Failed {snapshot_date}: {e}")

    logger.info("")
    logger.info(f"Step 2/2: Ingesting Binance Data")
    logger.info("-" * 60)

    # Extract symbols from universe
    symbols = get_symbols_from_universe(db_path, start_date, end_date, top_n)

    if not symbols:
        logger.error("No symbols extracted from universe! Cannot proceed with Binance ingestion.")
        logger.error("Make sure Step 1 (universe ingestion) completed successfully.")
        return

    logger.info(f"  Extracted {len(symbols)} symbols from universe")
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
        max_concurrent=20,  # Optimal S3 performance
        failure_threshold=3  # Stop after 3 consecutive missing months
    )

    logger.info("")
    logger.info("=" * 60)
    logger.info("✓ Complete Sync Finished!")

    # Log number of symbols with data
    try:
        db = CryptoDatabase(db_path)
        result = db.execute("""
            SELECT COUNT(DISTINCT symbol) FROM (
                SELECT symbol FROM binance_spot
                UNION
                SELECT symbol FROM binance_futures
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
