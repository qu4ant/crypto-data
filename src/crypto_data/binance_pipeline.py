"""
Binance market data ingestion pipeline.

Provides update_binance_market_data(), the public entry point for downloading
Binance historical files and importing them into DuckDB.
"""

from __future__ import annotations

import logging
import tempfile
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from crypto_data.binance_downloader import BinanceDataVisionDownloader
from crypto_data.binance_importer import BinanceDuckDBImporter
from crypto_data.database import CryptoDatabase
from crypto_data.enums import DataType, Interval
from crypto_data.binance_datasets.base import DownloadResult, Period
from crypto_data.binance_datasets.registry import get_binance_dataset_strategy
from crypto_data.utils.dates import generate_day_list, parse_date_range
from crypto_data.utils.ingestion_helpers import initialize_ingestion_stats, log_ingestion_summary
from crypto_data.utils.runtime import run_async_from_sync

logger = logging.getLogger(__name__)


def _interval_to_seconds(interval: Optional[str]) -> Optional[int]:
    """
    Convert Binance interval string to seconds.

    Returns None for unsupported/ambiguous intervals.
    """
    if not interval:
        return None

    try:
        value = int(interval[:-1])
        unit = interval[-1]
    except (ValueError, TypeError):
        return None

    if value <= 0:
        return None

    if unit == 'm':
        return value * 60
    if unit == 'h':
        return value * 3600
    if unit == 'd':
        return value * 86400
    if unit == 'w':
        return value * 7 * 86400
    if unit == 'M':
        # Month interval length is variable.
        return None

    return None


def _period_exists_in_db(
    conn,
    table: str,
    symbol: str,
    interval: Optional[str],
    period: Period
) -> bool:
    """
    Check if data already exists in DB for a given period.

    Parameters
    ----------
    conn : duckdb.DuckDBPyConnection
        Database connection
    table : str
        Table name ('spot', 'futures', 'open_interest', 'funding_rates')
    symbol : str
        Symbol to check (e.g., 'BTCUSDT')
    interval : Optional[str]
        Kline interval (required for spot/futures)
    period : Period
        Period to check
    Returns
    -------
    bool
        True if data exists for this period
    """
    # Parse period to get date bounds
    if period.is_monthly:
        # Monthly: '2024-01' -> 2024-01-01 to 2024-02-01
        start = datetime.strptime(period.value, '%Y-%m')
        end = start + relativedelta(months=1)
    else:
        # Daily: '2024-01-15' -> 2024-01-15 to 2024-01-16
        start = datetime.strptime(period.value, '%Y-%m-%d')
        end = start + relativedelta(days=1)

    # Build query based on table type
    if table in ('spot', 'futures'):
        # Completeness check for OHLCV:
        # 1) At least one row exists in the period
        # 2) Rows are contiguous for the detected interval
        # 3) Coverage reaches both period edges (with one-interval tolerance)
        #
        # Edge coverage avoids false positives where only a contiguous sub-range
        # exists in the middle of the month/day.
        query = f"""
            SELECT
                MIN(timestamp) as min_ts,
                MAX(timestamp) as max_ts,
                COUNT(*) as row_count
            FROM {table}
            WHERE exchange = ?
              AND symbol = ?
              AND interval = ?
              AND timestamp > ?
              AND timestamp <= ?
        """
        result = conn.execute(query, ['binance', symbol, interval, start, end]).fetchone()

        if not result or result[2] == 0:
            return False

        min_ts, max_ts, row_count = result
        step_seconds = _interval_to_seconds(interval)

        if step_seconds is None:
            # Preserve previous behavior for unsupported intervals.
            return True

        # Check interior continuity (no missing rows between observed bounds).
        observed_seconds = (max_ts - min_ts).total_seconds()
        expected_rows = int(observed_seconds // step_seconds) + 1
        if row_count < expected_rows:
            return False

        # Check coverage near both period boundaries.
        edge_tolerance = timedelta(seconds=step_seconds)
        period_start_ceiling = start + edge_tolerance

        covers_period_start = min_ts <= period_start_ceiling
        covers_period_end = max_ts >= end

        return covers_period_start and covers_period_end
    else:
        # open_interest, funding_rates don't have interval column
        query = f"""
            SELECT 1 FROM {table}
            WHERE exchange = ?
              AND symbol = ?
              AND timestamp >= ?
              AND timestamp < ?
            LIMIT 1
        """
        result = conn.execute(query, ['binance', symbol, start, end]).fetchone()

    return result is not None


def _filter_existing_periods(
    conn,
    table: str,
    symbol: str,
    interval: Optional[str],
    periods: List[Period]
) -> Tuple[List[Period], int]:
    """
    Filter out periods that already have data in the database.

    Parameters
    ----------
    conn : duckdb.DuckDBPyConnection
        Database connection
    table : str
        Table name
    symbol : str
        Symbol to check
    interval : Optional[str]
        Kline interval
    periods : List[Period]
        All periods to potentially download
    Returns
    -------
    Tuple[List[Period], int]
        (periods_to_download, count_skipped)
    """
    missing = []
    skipped = 0

    for period in periods:
        if getattr(period, 'replace_existing', False):
            missing.append(period)
            continue

        if _period_exists_in_db(conn, table, symbol, interval, period):
            skipped += 1
        else:
            missing.append(period)

    return missing, skipped


def _process_results(
    results: List[DownloadResult],
    importer: BinanceDuckDBImporter,
    conn,
    stats: Dict[str, int],
    symbol: str
) -> None:
    """
    Process download results: import to DB, update stats, cleanup.

    Each file is imported in its own transaction for atomicity.

    Parameters
    ----------
    results : List[DownloadResult]
        Download results from BinanceDataVisionDownloader
    importer : BinanceDuckDBImporter
        Importer instance for database operations
    conn : duckdb.DuckDBPyConnection
        Database connection
    stats : Dict[str, int]
        Statistics dictionary to update
    symbol : str
        Original symbol (for logging)
    """
    for result in results:
        if result.success:
            try:
                # Each import in its own transaction for atomicity
                conn.execute("BEGIN TRANSACTION")
                importer.import_file(
                    conn,
                    result.file_path,
                    symbol,
                    period=result.period,
                    replace_existing=(
                        importer.dataset.table_name in ('spot', 'futures')
                        and len(result.period) == 10
                    ),
                )
                conn.execute("COMMIT")
                stats['downloaded'] += 1
                logger.debug(f"    Imported {result.period}")

                # Delete file after import
                if result.file_path and result.file_path.exists():
                    result.file_path.unlink()

            except Exception as e:
                # Rollback on any error
                try:
                    conn.execute("ROLLBACK")
                except Exception as rollback_error:
                    logger.warning(f"Failed to rollback transaction: {rollback_error}")
                logger.error(f"Import failed {result.period}: {e}")
                stats['failed'] += 1

                # Clean up temp file on failure
                if result.file_path and result.file_path.exists():
                    result.file_path.unlink()
        else:
            if result.is_not_found:
                stats['not_found'] += 1
            else:
                stats['failed'] += 1


def _daily_periods_for_monthly_period(
    period: Period,
    start: datetime,
    end: datetime,
) -> List[Period]:
    """Expand one monthly period into daily periods within the requested range."""
    month_start = datetime.strptime(period.value, '%Y-%m')
    month_end_exclusive = month_start + relativedelta(months=1)
    daily_start = max(start, month_start)
    daily_end = min(end, month_end_exclusive - timedelta(days=1))

    if daily_start > daily_end:
        return []

    return [
        Period(day, is_monthly=False)
        for day in generate_day_list(daily_start, daily_end)
    ]


async def _ingest_symbol_data_type(
    symbol: str,
    data_type: DataType,
    start: datetime,
    end: datetime,
    interval: Optional[Interval],
    temp_path: Path,
    conn,
    stats: Dict[str, int],
    max_concurrent: int,
    failure_threshold: int
) -> None:
    """
    Ingest a single symbol + data_type combination.

    Parameters
    ----------
    symbol : str
        Symbol to ingest (e.g., 'BTCUSDT')
    data_type : DataType
        Type of data to ingest
    start : datetime
        Start date
    end : datetime
        End date
    interval : Optional[Interval]
        Kline interval (required for SPOT/FUTURES)
    temp_path : Path
        Temporary directory for downloads
    conn : duckdb.DuckDBPyConnection
        Database connection
    stats : Dict[str, int]
        Statistics dictionary to update
    max_concurrent : int
        Maximum concurrent downloads
    failure_threshold : int
        Stop after N consecutive 404s
    """
    # Get Binance dataset handler.
    dataset = get_binance_dataset_strategy(data_type, interval)

    # Generate periods
    periods = dataset.generate_periods(start, end)

    if not periods:
        logger.debug(f"No periods to download for {symbol} {data_type.value}")
        return

    # Get interval string for klines
    interval_str = interval.value if interval else None

    # Filter out periods that already exist in DB
    periods_to_download, skipped = _filter_existing_periods(
        conn=conn,
        table=dataset.table_name,
        symbol=symbol,
        interval=interval_str,
        periods=periods
    )

    # ANSI codes for dimmed output
    DIM = "\033[90m"  # Dark gray
    RESET = "\033[0m"

    if skipped > 0:
        stats['skipped'] += skipped

    if not periods_to_download:
        logger.info(f"{DIM}  {symbol} {data_type.value}: already in DB ({skipped} months){RESET}")
        return
    elif skipped > 0:
        logger.info(f"{DIM}  {symbol} {data_type.value}: {len(periods_to_download)} months to download, {skipped} months already in DB{RESET}")

    # Download Binance Data Vision files for this symbol/data type.
    async with BinanceDataVisionDownloader(
        dataset=dataset,
        temp_path=temp_path,
        max_concurrent=max_concurrent
    ) as downloader:
        results = await downloader.download_symbol(
            symbol=symbol,
            periods=periods_to_download,
            interval=interval_str,
            failure_threshold=(
                0 if data_type in (DataType.SPOT, DataType.FUTURES)
                else failure_threshold
            )
        )

        if data_type in (DataType.SPOT, DataType.FUTURES):
            monthly_not_found = [
                result for result in results
                if result.is_not_found and len(result.period) == 7
            ]

            if monthly_not_found:
                fallback_periods: List[Period] = []
                for result in monthly_not_found:
                    fallback_periods.extend(
                        _daily_periods_for_monthly_period(
                            Period(result.period, is_monthly=True),
                            start,
                            end,
                        )
                    )

                if fallback_periods:
                    logger.info(
                        f"  {symbol} {data_type.value}: "
                        f"falling back to {len(fallback_periods)} daily files"
                    )
                    fallback_results = await downloader.download_symbol(
                        symbol=symbol,
                        periods=fallback_periods,
                        interval=interval_str,
                        failure_threshold=0,
                    )

                    results = [
                        result for result in results
                        if not (result.is_not_found and len(result.period) == 7)
                    ] + fallback_results

            if failure_threshold > 0:
                results = downloader._detect_gaps(
                    sorted(results, key=lambda result: result.period),
                    failure_threshold,
                )

    if not results:
        logger.debug(f"No results for {symbol} {data_type.value}")
        return

    # Create importer and process results (each file is its own transaction)
    importer = BinanceDuckDBImporter(dataset)
    _process_results(results, importer, conn, stats, symbol)


async def _run_all_ingestion(
    symbols: List[str],
    data_types: List[DataType],
    start: datetime,
    end: datetime,
    interval: Interval,
    temp_path: Path,
    conn,
    stats: Dict[str, int],
    max_concurrent_klines: int,
    max_concurrent_metrics: int,
    max_concurrent_funding: int,
    failure_threshold: int
) -> None:
    """
    Run all ingestion tasks in a single event loop.

    Parameters
    ----------
    symbols : List[str]
        List of symbols to download
    data_types : List[DataType]
        List of data types to download
    start : datetime
        Start date
    end : datetime
        End date
    interval : Interval
        Kline interval for SPOT/FUTURES data
    temp_path : Path
        Temporary directory for downloads
    conn : duckdb.DuckDBPyConnection
        Database connection
    stats : Dict[str, int]
        Statistics dictionary to update
    max_concurrent_klines : int
        Maximum concurrent downloads for klines
    max_concurrent_metrics : int
        Maximum concurrent downloads for open interest
    max_concurrent_funding : int
        Maximum concurrent downloads for funding rates
    failure_threshold : int
        Stop after N consecutive 404s
    """
    for symbol in symbols:
        logger.info(f"Processing {symbol}")

        for data_type in data_types:
            # Select max_concurrent based on data type
            if data_type in (DataType.SPOT, DataType.FUTURES):
                max_concurrent = max_concurrent_klines
            elif data_type == DataType.OPEN_INTEREST:
                max_concurrent = max_concurrent_metrics
            elif data_type == DataType.FUNDING_RATES:
                max_concurrent = max_concurrent_funding
            else:
                max_concurrent = max_concurrent_klines

            logger.debug(f"  {data_type.value} (max_concurrent={max_concurrent})")

            await _ingest_symbol_data_type(
                symbol=symbol,
                data_type=data_type,
                start=start,
                end=end,
                interval=interval if data_type in (DataType.SPOT, DataType.FUTURES) else None,
                temp_path=temp_path,
                conn=conn,
                stats=stats,
                max_concurrent=max_concurrent,
                failure_threshold=failure_threshold
            )


def update_binance_market_data(
    db_path: str,
    symbols: List[str],
    data_types: List[DataType],
    start_date: str,
    end_date: str,
    interval: Interval = Interval.MIN_5,
    max_concurrent_klines: int = 20,
    max_concurrent_metrics: int = 100,
    max_concurrent_funding: int = 50,
    failure_threshold: int = 3
) -> None:
    """
    Download Binance market data and import it into DuckDB.

    This function is synchronous but uses asyncio internally. It creates
    a single event loop for all downloads, making it efficient and avoiding
    nested event loop issues.

    Parameters
    ----------
    db_path : str
        Path to DuckDB database file
    symbols : List[str]
        List of symbols to download (e.g., ['BTCUSDT', 'ETHUSDT'])
    data_types : List[DataType]
        List of data types to download
    start_date : str
        Start date in YYYY-MM-DD format
    end_date : str
        End date in YYYY-MM-DD format
    interval : Interval, optional
        Kline interval for SPOT/FUTURES data (default: Interval.MIN_5)
    max_concurrent_klines : int, optional
        Maximum concurrent downloads for klines (default: 20)
    max_concurrent_metrics : int, optional
        Maximum concurrent downloads for open interest (default: 100)
    max_concurrent_funding : int, optional
        Maximum concurrent downloads for funding rates (default: 50)
    failure_threshold : int, optional
        Stop after N consecutive 404s (default: 3)
        Set to 0 to disable gap detection

    Examples
    --------
    >>> from crypto_data.enums import DataType, Interval
    >>> from crypto_data import update_binance_market_data
    >>> update_binance_market_data(
    ...     db_path='crypto_data.db',
    ...     symbols=['BTCUSDT', 'ETHUSDT'],
    ...     data_types=[DataType.SPOT, DataType.FUTURES],
    ...     start_date='2024-01-01',
    ...     end_date='2024-12-31',
    ...     interval=Interval.MIN_5
    ... )
    """
    # Validate and parse dates
    start, end = parse_date_range(start_date, end_date)

    # Initialize database
    db = CryptoDatabase(db_path)
    conn = db.conn

    # Initialize stats
    stats = initialize_ingestion_stats()

    try:
        # Create temp directory and run all ingestion in a single event loop
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Run all ingestion in one event loop (sync/async safe wrapper).
            run_async_from_sync(
                _run_all_ingestion(
                    symbols=symbols,
                    data_types=data_types,
                    start=start,
                    end=end,
                    interval=interval,
                    temp_path=temp_path,
                    conn=conn,
                    stats=stats,
                    max_concurrent_klines=max_concurrent_klines,
                    max_concurrent_metrics=max_concurrent_metrics,
                    max_concurrent_funding=max_concurrent_funding,
                    failure_threshold=failure_threshold,
                ),
                "update_binance_market_data",
            )
    finally:
        # Always close database, even on exceptions
        db.close()

    # Log summary
    log_ingestion_summary(
        stats=stats,
        db_path=db_path,
        symbols=symbols,
        start_date=start_date,
        end_date=end_date,
        interval=interval,
        show_availability=True
    )
