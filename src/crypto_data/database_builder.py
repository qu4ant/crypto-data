"""
Database builder for CoinMarketCap universe and Binance market data.

Handles CoinMarketCap universe ingestion and full Binance DuckDB population.

Functions:
    - update_coinmarketcap_universe(): CoinMarketCap rankings (async parallel downloads)
    - create_binance_database(): Complete workflow (universe -> binance)

Note: Binance data ingestion is handled by crypto_data.binance_pipeline.
"""

import asyncio
import logging
from pathlib import Path

import duckdb
import pandas as pd
import pandera.pandas as pa

from crypto_data.clients.coinmarketcap import CoinMarketCapClient
from crypto_data.database import CryptoDatabase
from crypto_data.enums import DataType, Interval
from crypto_data.schemas import UNIVERSE_SCHEMA
from crypto_data.tables import UNIVERSE_COLUMNS
from crypto_data.universe_filters import (
    has_excluded_symbol,
    has_excluded_tag,
    resolve_exclude_symbols,
    resolve_exclude_tags,
)
from crypto_data.utils.dates import Frequency, generate_date_list, parse_date_range
from crypto_data.utils.formatting import format_file_size
from crypto_data.utils.runtime import run_async_from_sync
from crypto_data.utils.symbols import get_binance_symbols_from_universe

logger = logging.getLogger(__name__)

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

# =============================================================================
# UNIVERSE INGESTION (CoinMarketCap)
# =============================================================================


def _get_existing_universe_dates(db_path: str) -> set[str]:
    """
    Return existing universe snapshot dates as YYYY-MM-DD strings.

    If the database/table is not available yet, returns an empty set.
    """
    if db_path != ":memory:" and not Path(db_path).exists():
        return set()

    with duckdb.connect(db_path, read_only=True) as conn:
        table_exists = conn.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_name = 'crypto_universe'
            LIMIT 1
            """
        ).fetchone()
        if table_exists is None:
            return set()

        rows = conn.execute(
            "SELECT DISTINCT strftime(date, '%Y-%m-%d') FROM crypto_universe"
        ).fetchall()
    return {row[0] for row in rows if row and row[0]}


async def _fetch_snapshot(
    client: CoinMarketCapClient,
    date: pd.Timestamp,
    top_n: int,
    excluded_tags: list[str],
    excluded_symbols: list[str],
    date_str: str,
) -> tuple[pd.DataFrame, set[str], set[str]]:
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
        - DataFrame with v6 universe columns (see UNIVERSE_COLUMNS)
        - Set of symbols excluded by tags
        - Set of symbols excluded by symbol blacklist
    """
    # Fetch from CoinMarketCap historical listings API (async)
    coins = await client.get_historical_listings(date_str, top_n)
    if not coins:
        raise ValueError(f"CoinMarketCap returned no listings for {date_str}")

    # Convert excluded tags to lowercase once (for case-insensitive matching)
    excluded_tags_lower = [tag.lower() for tag in excluded_tags]

    # Convert excluded symbols to uppercase once (for case-insensitive matching)
    excluded_symbols_upper = [symbol.upper() for symbol in excluded_symbols]

    # Process each coin
    records = []
    excluded_by_tag: set[str] = set()
    excluded_by_symbol: set[str] = set()

    for coin in coins:
        symbol = str(coin.get("symbol") or "").upper()
        rank = coin.get("cmcRank")

        tags = coin.get("tags") or []
        tags_str = ",".join(tags)

        if has_excluded_tag(tags, excluded_tags_lower):
            logger.debug(f"  → Filtered {symbol} (excluded tag)")
            excluded_by_tag.add(symbol)
            continue

        if has_excluded_symbol(symbol, excluded_symbols_upper):
            logger.debug(f"  → Filtered {symbol} (blacklisted symbol)")
            excluded_by_symbol.add(symbol)
            continue

        quotes = coin.get("quotes") or []
        first_quote = quotes[0] if quotes else {}
        market_cap = first_quote.get("marketCap")
        fdmc = first_quote.get("fullyDilutedMarketCap")

        platform_field = coin.get("platform") or {}
        platform_name = platform_field.get("name") if isinstance(platform_field, dict) else None

        # `errors='coerce'` returns NaT for None or unparseable values, which
        # plays cleanly with the nullable datetime64[ns] column downstream.
        date_added = pd.to_datetime(coin.get("dateAdded"), errors="coerce")

        records.append(
            {
                "provider": "coinmarketcap",
                "provider_id": int(coin["id"]),
                "date": date,
                "symbol": symbol,
                "name": coin.get("name") or "",
                "slug": coin.get("slug"),
                "rank": rank,
                "market_cap": market_cap,
                "fully_diluted_market_cap": fdmc,
                "circulating_supply": coin.get("circulatingSupply"),
                "max_supply": coin.get("maxSupply"),
                "tags": tags_str,
                "platform": platform_name,
                "date_added": date_added,
            }
        )

    # Convert to DataFrame with stable columns, then deduplicate duplicate
    # symbols defensively before schema validation. CMC can expose duplicate
    # tickers for wrapped/secondary listings; keep the canonical lowest-rank
    # row and make the cleanup visible.
    df = pd.DataFrame(records, columns=UNIVERSE_COLUMNS)
    if not df.empty:
        before_dedup = len(df)
        df = df.sort_values(
            by=["provider_id", "rank", "market_cap"],
            ascending=[True, True, False],
            na_position="last",
        ).drop_duplicates(subset=["provider", "provider_id", "date"], keep="first")
        dropped = before_dedup - len(df)
        if dropped:
            logger.warning(
                "Dropped %s duplicate universe rows for %s on key ['provider', 'provider_id', 'date']",
                dropped,
                date_str,
            )

    try:
        df = UNIVERSE_SCHEMA.validate(df)
    except pa.errors.SchemaError as e:
        raise ValueError(f"Universe validation failed for {date_str}") from e

    return df, excluded_by_tag, excluded_by_symbol


async def update_coinmarketcap_universe(
    db_path: str,
    dates: list[str] | None = None,
    *,
    top_n: int = 100,
    exclude_tags: list[str] | None = None,
    exclude_symbols: list[str] | None = None,
    max_concurrent: int = 5,
    skip_existing: bool = True,
    daily_quota: int = 200,
) -> dict[str, set[str]]:
    """
    Fetch universe snapshots for multiple dates in parallel, write to DB sequentially.

    This function downloads all snapshots concurrently (max_concurrent at a time)
    and then writes them sequentially to avoid database write conflicts.

    Parameters
    ----------
    db_path : str
        Path to DuckDB database file
    dates : List[str], optional
        List of date strings in YYYY-MM-DD format
    top_n : int
        Number of top cryptocurrencies to fetch (default: 100)
    exclude_tags : List[str], optional
        List of CoinMarketCap tags to exclude (e.g., ['stablecoin', 'wrapped-tokens'])
        Default: stablecoins, wrapped assets, and tokenized assets
    exclude_symbols : List[str], optional
        List of symbols to exclude (e.g., ['LUNA', 'FTT', 'UST'])
    max_concurrent : int
        Maximum number of concurrent API requests (default: 5)
    skip_existing : bool
        Skip dates already present in crypto_universe (default: True).
        This is a date-only resume guard: it does not compare top_n,
        exclude_tags, or exclude_symbols. Set skip_existing=False when
        rebuilding existing snapshot dates with different universe settings.
    daily_quota : int
        Daily request quota for CMC sliding-window limiter (default: 200)

    Example
    -------
    >>> import asyncio
    >>> asyncio.run(update_coinmarketcap_universe(
    ...     db_path='crypto_data.db',
    ...     dates=['2024-01-01', '2024-02-01', '2024-03-01'],
    ...     top_n=100,
    ...     exclude_tags=['stablecoin'],
    ...     exclude_symbols=['LUNA'],
    ...     max_concurrent=5
    ... ))
    """
    exclude_tags = resolve_exclude_tags(exclude_tags)
    exclude_symbols = resolve_exclude_symbols(exclude_symbols)
    if dates is None:
        raise ValueError("`dates` must be provided as YYYY-MM-DD snapshot dates.")

    dates = list(dates or [])
    if skip_existing and dates:
        existing_dates = _get_existing_universe_dates(db_path)
        before = len(dates)
        dates = [date_str for date_str in dates if date_str not in existing_dates]
        skipped = before - len(dates)
        if skipped > 0:
            logger.info(f"Skipping {skipped} dates already present in crypto_universe")
        if not dates:
            logger.info("All requested universe snapshots already exist; nothing to fetch")
            return {"by_tag": set(), "by_symbol": set()}

    # Create async client with concurrency control
    async with CoinMarketCapClient(
        max_concurrent=max_concurrent,
        daily_quota=daily_quota,
    ) as client:
        # Build tasks for all dates
        tasks = []
        for date_str in dates:
            snapshot_date = pd.Timestamp(date_str)

            # Create task for fetching this snapshot date
            task = _fetch_snapshot(
                client=client,
                date=snapshot_date,
                top_n=top_n,
                excluded_tags=exclude_tags,
                excluded_symbols=exclude_symbols,
                date_str=date_str,
            )
            tasks.append((date_str, task))

        # Download all snapshots in parallel
        logger.info(f"Downloading {len(tasks)} snapshots in parallel...")
        results = await asyncio.gather(*[task for _, task in tasks], return_exceptions=True)

    # Write to database sequentially (avoid write conflicts)
    db = None
    success_count = 0
    fetch_fail_count = 0
    all_excluded_by_tag: set[str] = set()
    all_excluded_by_symbol: set[str] = set()

    try:
        db = CryptoDatabase(db_path)
        conn = db.conn
        for i, (date_str, _) in enumerate(tasks):
            result = results[i]

            # Handle exceptions
            if isinstance(result, Exception):
                logger.error(f"  ⚠ Warning: Failed {date_str}: {result}")
                fetch_fail_count += 1
                continue

            # result is a tuple: (DataFrame, excluded_by_tag, excluded_by_symbol)
            df_new, excluded_by_tag, excluded_by_symbol = result

            # Aggregate exclusions (UNION across snapshots)
            all_excluded_by_tag.update(excluded_by_tag)
            all_excluded_by_symbol.update(excluded_by_symbol)

            # Delete existing data for this date, then insert new data (ATOMIC).
            # This supports top_n/filter changes when the date is actually
            # refreshed. With skip_existing=True, existing dates are skipped
            # before this point as a date-only resume guard.
            committed = False
            try:
                conn.execute("BEGIN TRANSACTION")

                # Delete all existing records for this date
                conn.execute("DELETE FROM crypto_universe WHERE date = ?", [date_str])
                logger.debug(f"Deleted existing records for {date_str}")

                # Validate immediately before insertion as a DB boundary guard.
                if len(df_new) > 0:
                    df_new = UNIVERSE_SCHEMA.validate(df_new)

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
                        logger.debug(
                            f"Rollback not needed or failed (expected if BEGIN failed): {rollback_error}"
                        )
                    logger.debug(f"Transaction rolled back for {date_str}")
                logger.error(f"Failed to update universe for {date_str}: {e}")
                raise

    finally:
        if db:
            db.close()

    # Check if ALL fetches failed
    if fetch_fail_count == len(tasks) and len(tasks) > 0:
        raise RuntimeError(
            f"All {fetch_fail_count} universe fetches failed. Check network connectivity "
            f"and CoinMarketCap API status."
        )

    # Summary log
    logger.info(
        f"✓ Downloaded {success_count}/{len(tasks)} snapshots successfully"
        + (f" ({fetch_fail_count} failed)" if fetch_fail_count > 0 else "")
    )

    # Return exclusion summary
    return {"by_tag": all_excluded_by_tag, "by_symbol": all_excluded_by_symbol}


# =============================================================================
# UNIFIED DATABASE POPULATION FUNCTION
# =============================================================================


def create_binance_database(
    db_path: str,
    start_date: str,
    end_date: str,
    top_n: int,
    interval: Interval = Interval.MIN_5,
    data_types: list[DataType] | None = None,
    exclude_tags: list[str] | None = None,
    exclude_symbols: list[str] | None = None,
    universe_frequency: Frequency = "monthly",
    skip_existing_universe: bool = True,
    daily_quota: int = 200,
    repair_gaps_via_api: bool = False,
):
    """
    Complete workflow: universe + binance in one call.

    Orchestrates the full ingestion pipeline:
    1. Ingest universe snapshots at selected frequency
    2. Extract a UNION superset of symbols from universe for download coverage
    3. Download Binance OHLCV data (with auto-discovery of 1000-prefix futures)
    4. Display final summary (symbol count + database size)

    Note
    ----
    The symbol list extracted in step 2 is intentionally a full-period superset
    so the downloader does not miss assets that enter the universe later.
    Point-in-time membership is still expected to be enforced downstream by
    joining/filtering with ``crypto_universe`` snapshot by snapshot (or on each
    rebalance date) during research/backtesting.

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
    exclude_tags : List[str], optional
        List of CoinMarketCap tags to exclude (e.g., ['stablecoin', 'wrapped-tokens'])
        Default: stablecoins, wrapped assets, and tokenized assets
    exclude_symbols : List[str], optional
        List of symbols to exclude (e.g., ['LUNA', 'FTT', 'UST'])
        Default: []
    universe_frequency : {'daily', 'weekly', 'monthly'}
        Frequency of universe snapshots (default: 'monthly')
    skip_existing_universe : bool
        Skip dates already present in crypto_universe (default: True)
    daily_quota : int
        Daily request quota for CMC sliding-window limiter (default: 200)
    repair_gaps_via_api : bool
        When True, run Binance REST gap repair after Data Vision ingestion
        finishes (default: False).

    Example
    -------
    >>> from crypto_data import create_binance_database, DataType, Interval
    >>> create_binance_database(
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
    # Import here to avoid circular imports during package initialization.
    from crypto_data.binance_pipeline import update_binance_market_data

    data_types = list(data_types) if data_types is not None else [DataType.SPOT, DataType.FUTURES]
    exclude_tags = resolve_exclude_tags(exclude_tags)
    exclude_symbols = resolve_exclude_symbols(exclude_symbols)

    logger.info("=" * 60)
    logger.info("Starting Database Population")
    logger.info("=" * 60)
    logger.info(f"  Database: {db_path}")
    logger.info(f"  Period: {start_date} → {end_date}")
    logger.info(f"  Top N: {top_n}")
    logger.info(f"  Interval: {interval}")
    logger.info(f"  Data types: {', '.join(dt.value for dt in data_types)}")
    logger.info("")

    # Parse and validate dates for universe snapshots
    start, end = parse_date_range(start_date, end_date)

    # Generate snapshot dates based on selected frequency
    dates = generate_date_list(start, end, frequency=universe_frequency)

    logger.info(f"Step 1/2: Ingesting Universe ({len(dates)} {universe_frequency} snapshots)")
    logger.info("-" * 60)

    # Ingest universe snapshots in parallel (async)
    exclusions = run_async_from_sync(
        update_coinmarketcap_universe(
            db_path=db_path,
            dates=dates,
            top_n=top_n,
            exclude_tags=exclude_tags,
            exclude_symbols=exclude_symbols,
            max_concurrent=5,
            skip_existing=skip_existing_universe,
            daily_quota=daily_quota,
        ),
        "create_binance_database",
    )

    logger.info("")
    logger.info("Step 2/2: Ingesting Binance Data")
    logger.info("-" * 60)

    # Extract a full-period UNION superset for download coverage.
    # Point-in-time membership should still be enforced downstream with
    # crypto_universe on each rebalance month/date.
    symbols = get_binance_symbols_from_universe(
        db_path,
        start_date,
        end_date,
        top_n,
        exclude_tags=exclude_tags,
        exclude_symbols=exclude_symbols,
    )

    if not symbols:
        raise RuntimeError(
            "No symbols extracted from universe. Cannot proceed with Binance ingestion. "
            "Check that Step 1 (universe ingestion) completed successfully and that your "
            "exclude_tags/exclude_symbols aren't filtering everything out."
        )

    logger.info("")

    # Ingest Binance market data.
    update_binance_market_data(
        db_path=db_path,
        symbols=symbols,
        data_types=data_types,
        start_date=start_date,
        end_date=end_date,
        interval=interval,
        max_concurrent_klines=20,  # Optimal S3 performance for high-frequency data
        max_concurrent_metrics=100,  # Higher for daily open_interest (365 files/year)
        max_concurrent_funding=50,  # Higher for monthly funding_rates (12 files/year)
        failure_threshold=3,  # Stop after 3 consecutive missing months
        repair_gaps_via_api=repair_gaps_via_api,
        prune_klines_to_date_range=True,
    )

    logger.info("")
    logger.info("=" * 60)
    logger.info("✓ Database Population Finished!")

    # Display exclusion summary (yellow by default due to "Excluded" keyword)
    if exclusions and (exclusions["by_tag"] or exclusions["by_symbol"]):
        logger.info("")
        logger.info("Excluded assets:")

        if exclusions["by_tag"]:
            tag_list = sorted(exclusions["by_tag"])
            logger.info(f"  By tag ({len(tag_list)}): {', '.join(tag_list)}")

        if exclusions["by_symbol"]:
            symbol_list = sorted(exclusions["by_symbol"])
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
