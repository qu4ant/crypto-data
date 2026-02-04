"""
Orchestrator for Binance data ingestion using the strategy pattern.

Provides ingest_binance_async() using the strategy pattern for cleaner,
more maintainable code.
"""

from __future__ import annotations

import asyncio
import logging
import tempfile
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from crypto_data.core.downloader import BatchDownloader
from crypto_data.core.importer import DataImporter
from crypto_data.database import CryptoDatabase
from crypto_data.enums import DataType, Interval
from crypto_data.exchanges.binance import BinanceExchange
from crypto_data.strategies.base import DownloadResult, Period
from crypto_data.strategies.registry import get_strategy
from crypto_data.utils.ingestion_helpers import initialize_ingestion_stats, log_ingestion_summary

logger = logging.getLogger(__name__)


def _period_exists_in_db(
    conn,
    table: str,
    symbol: str,
    interval: Optional[str],
    period: Period,
    exchange: str = 'binance'
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
    exchange : str
        Exchange name (default: 'binance')

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
        query = f"""
            SELECT 1 FROM {table}
            WHERE exchange = ?
              AND symbol = ?
              AND interval = ?
              AND timestamp >= ?
              AND timestamp < ?
            LIMIT 1
        """
        result = conn.execute(query, [exchange, symbol, interval, start, end]).fetchone()
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
        result = conn.execute(query, [exchange, symbol, start, end]).fetchone()

    return result is not None


def _filter_existing_periods(
    conn,
    table: str,
    symbol: str,
    interval: Optional[str],
    periods: List[Period],
    exchange: str = 'binance'
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
    exchange : str
        Exchange name

    Returns
    -------
    Tuple[List[Period], int]
        (periods_to_download, count_skipped)
    """
    missing = []
    skipped = 0

    for period in periods:
        if _period_exists_in_db(conn, table, symbol, interval, period, exchange):
            skipped += 1
        else:
            missing.append(period)

    return missing, skipped


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
        Parsed (start, end) datetime objects

    Raises
    ------
    ValueError
        If date format is invalid or start_date > end_date
    """
    try:
        start = datetime.strptime(start_date, '%Y-%m-%d')
    except ValueError as e:
        raise ValueError(f"Invalid start_date format '{start_date}': expected YYYY-MM-DD") from e

    try:
        end = datetime.strptime(end_date, '%Y-%m-%d')
    except ValueError as e:
        raise ValueError(f"Invalid end_date format '{end_date}': expected YYYY-MM-DD") from e

    if start > end:
        raise ValueError(f"start_date ({start_date}) cannot be after end_date ({end_date})")

    return start, end


def _process_results(
    results: List[DownloadResult],
    importer: DataImporter,
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
        Download results from BatchDownloader
    importer : DataImporter
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
                importer.import_file(conn, result.file_path, symbol, 'binance')
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
    # Get strategy
    strategy = get_strategy(data_type, interval)

    # Generate periods
    periods = strategy.generate_periods(start, end)

    if not periods:
        logger.debug(f"No periods to download for {symbol} {data_type.value}")
        return

    # Get interval string for klines
    interval_str = interval.value if interval else None

    # Filter out periods that already exist in DB
    periods_to_download, skipped = _filter_existing_periods(
        conn=conn,
        table=strategy.table_name,
        symbol=symbol,
        interval=interval_str,
        periods=periods,
        exchange='binance'
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

    # Download using BatchDownloader within BinanceExchange context
    async with BinanceExchange(max_concurrent=max_concurrent) as exchange:
        downloader = BatchDownloader(
            strategy=strategy,
            exchange=exchange,
            temp_path=temp_path,
            max_concurrent=max_concurrent
        )

        results = await downloader.download_symbol(
            symbol=symbol,
            periods=periods_to_download,
            interval=interval_str,
            failure_threshold=failure_threshold
        )

    if not results:
        logger.debug(f"No results for {symbol} {data_type.value}")
        return

    # Create importer and process results (each file is its own transaction)
    importer = DataImporter(strategy)
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


def ingest_binance_async(
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
    Download Binance data using strategy pattern.

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
    >>> from crypto_data import ingest_binance_async
    >>> ingest_binance_async(
    ...     db_path='crypto_data.db',
    ...     symbols=['BTCUSDT', 'ETHUSDT'],
    ...     data_types=[DataType.SPOT, DataType.FUTURES],
    ...     start_date='2024-01-01',
    ...     end_date='2024-12-31',
    ...     interval=Interval.MIN_5
    ... )
    """
    # Validate and parse dates
    start, end = _validate_and_parse_dates(start_date, end_date)

    # Initialize database
    db = CryptoDatabase(db_path)
    conn = db.conn

    # Initialize stats
    stats = initialize_ingestion_stats()

    try:
        # Create temp directory and run all ingestion in a single event loop
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Run all ingestion in a single asyncio.run() call
            asyncio.run(_run_all_ingestion(
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
                failure_threshold=failure_threshold
            ))
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
