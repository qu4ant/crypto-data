"""
Unified Ingestion Module

Handles CoinMarketCap universe ingestion and database population workflow.

Functions:
    - ingest_universe(): CoinMarketCap universe rankings (async parallel downloads)
    - populate_database(): Complete workflow (universe → binance)

Note: Binance data ingestion is handled by crypto_data.core.ingest_binance_async()
"""

import logging
import asyncio
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Tuple, Set
import pandas as pd

from crypto_data.clients.coinmarketcap import CoinMarketCapClient
from crypto_data.database import CryptoDatabase
from crypto_data.enums import DataType, Interval
from crypto_data.utils.dates import generate_month_list
from crypto_data.utils.formatting import format_file_size
from crypto_data.utils.symbols import get_symbols_from_universe

logger = logging.getLogger(__name__)


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

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
    >>> asyncio.run(ingest_universe(
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
    db = None
    success_count = 0
    fail_count = 0
    all_excluded_by_tag: Set[str] = set()
    all_excluded_by_symbol: Set[str] = set()

    try:
        db = CryptoDatabase(db_path)
        conn = db.conn
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
                    try:
                        conn.execute("ROLLBACK")
                    except Exception as rollback_error:
                        logger.debug(f"Rollback not needed or failed (expected if BEGIN failed): {rollback_error}")
                    logger.debug(f"Transaction rolled back for {date_str}")
                logger.error(f"Failed to update universe for {date_str}: {e}")
                fail_count += 1
                # Continue with other months instead of raising
                continue

    finally:
        if db:
            db.close()

    # Check if ALL fetches failed
    if fail_count == len(tasks) and len(tasks) > 0:
        raise RuntimeError(
            f"All {fail_count} universe fetches failed. Check network connectivity "
            f"and CoinMarketCap API status."
        )

    # Summary log
    logger.info(f"✓ Downloaded {success_count}/{len(tasks)} snapshots successfully" +
                (f" ({fail_count} failed)" if fail_count > 0 else ""))

    # Return exclusion summary
    return {
        'by_tag': all_excluded_by_tag,
        'by_symbol': all_excluded_by_symbol
    }


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
    # Import ingest_binance_async from core (strategy pattern implementation)
    from crypto_data.core import ingest_binance_async

    if data_types is None:
        data_types = [DataType.SPOT, DataType.FUTURES]

    logger.info("=" * 60)
    logger.info("Starting Database Population")
    logger.info("=" * 60)
    logger.info(f"  Database: {db_path}")
    logger.info(f"  Period: {start_date} → {end_date}")
    logger.info(f"  Top N: {top_n}")
    logger.info(f"  Interval: {interval}")
    logger.info(f"  Data types: {', '.join(dt.value for dt in data_types)}")
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
        raise RuntimeError(
            "No symbols extracted from universe. Cannot proceed with Binance ingestion. "
            "Check that Step 1 (universe ingestion) completed successfully and that your "
            "exclude_tags/exclude_symbols aren't filtering everything out."
        )

    logger.info("")

    # Ingest Binance (using strategy pattern implementation)
    ingest_binance_async(
        db_path=db_path,
        symbols=symbols,
        data_types=data_types,
        start_date=start_date,
        end_date=end_date,
        interval=interval,
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
        logger.warning(f"Could not count symbols: {e}")

    # Log database file size
    try:
        db_size = Path(db_path).stat().st_size
        logger.info(f"Database size: {format_file_size(db_size)} ({db_size:,} bytes)")
    except Exception as e:
        logger.warning(f"Could not determine database size: {e}")

    logger.info("=" * 60)
