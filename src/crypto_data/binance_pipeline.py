"""
Binance market data ingestion pipeline.

Provides update_binance_market_data(), the public entry point for downloading
Binance historical files and importing them into DuckDB.
"""

from __future__ import annotations

import logging
import tempfile
from datetime import datetime
from pathlib import Path

from crypto_data.binance_datasets.base import DownloadResult
from crypto_data.binance_datasets.registry import get_binance_dataset_strategy
from crypto_data.binance_downloader import BinanceDataVisionDownloader
from crypto_data.binance_importer import BinanceDuckDBImporter
from crypto_data.binance_repair import repair_binance_gaps
from crypto_data.completeness import (
    filter_existing_periods as _filter_existing_periods,
)
from crypto_data.completeness import (
    kline_close_window as _kline_close_window,
)
from crypto_data.completeness import (
    period_exists_in_db,
)
from crypto_data.database import CryptoDatabase
from crypto_data.enums import DataType, Interval
from crypto_data.tables import is_kline_table
from crypto_data.utils.dates import parse_date_range
from crypto_data.utils.ingestion_helpers import initialize_ingestion_stats, log_ingestion_summary
from crypto_data.utils.runtime import run_async_from_sync

logger = logging.getLogger(__name__)


def _period_exists_in_db(conn, table: str, symbol: str, interval: str | None, period) -> bool:
    """Backward-compatible private alias for tests/internal callers."""
    return period_exists_in_db(conn, table, symbol, interval, period)


def _prune_klines_outside_date_range(
    conn,
    *,
    symbols: list[str],
    data_types: list[DataType],
    interval: Interval,
    start: datetime,
    end: datetime,
) -> None:
    """Delete stale kline rows outside the requested close-time window."""
    tables = [
        data_type.value
        for data_type in data_types
        if data_type in (DataType.SPOT, DataType.FUTURES)
    ]
    if not symbols or not tables:
        return

    window_start, window_end = _kline_close_window(start, end)
    placeholders = ", ".join("?" for _ in symbols)

    for table in tables:
        delete_sql = f"""
            DELETE FROM {table}
            WHERE exchange = ?
              AND interval = ?
              AND symbol IN ({placeholders})
              AND (timestamp <= ? OR timestamp > ?)
        """
        conn.execute(
            delete_sql,
            ["binance", interval.value, *symbols, window_start, window_end],
        )


def _process_results(
    results: list[DownloadResult],
    importer: BinanceDuckDBImporter,
    conn,
    stats: dict[str, int],
    symbol: str,
    window_start: datetime | None = None,
    window_end: datetime | None = None,
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
                        is_kline_table(importer.dataset.table_name) and len(result.period) == 10
                    ),
                    window_start=window_start,
                    window_end=window_end,
                )
                conn.execute("COMMIT")
                stats["downloaded"] += 1
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
                stats["failed"] += 1

                # Clean up temp file on failure
                if result.file_path and result.file_path.exists():
                    result.file_path.unlink()
        else:
            if result.is_not_found:
                stats["not_found"] += 1
            else:
                stats["failed"] += 1


async def _ingest_symbol_data_type(
    symbol: str,
    data_type: DataType,
    start: datetime,
    end: datetime,
    interval: Interval | None,
    temp_path: Path,
    conn,
    stats: dict[str, int],
    max_concurrent: int,
    failure_threshold: int,
    window_start: datetime | None = None,
    window_end: datetime | None = None,
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
        conn=conn, table=dataset.table_name, symbol=symbol, interval=interval_str, periods=periods
    )

    # ANSI codes for dimmed output
    DIM = "\033[90m"  # Dark gray
    RESET = "\033[0m"

    if skipped > 0:
        stats["skipped"] += skipped

    if not periods_to_download:
        logger.info(f"{DIM}  {symbol} {data_type.value}: already in DB ({skipped} months){RESET}")
        return
    if skipped > 0:
        logger.info(
            f"{DIM}  {symbol} {data_type.value}: {len(periods_to_download)} months to download, {skipped} months already in DB{RESET}"
        )

    # Download Binance Data Vision files for this symbol/data type.
    async with BinanceDataVisionDownloader(
        dataset=dataset, temp_path=temp_path, max_concurrent=max_concurrent
    ) as downloader:
        results = await downloader.download_symbol(
            symbol=symbol,
            periods=periods_to_download,
            interval=interval_str,
            failure_threshold=(
                0 if data_type in (DataType.SPOT, DataType.FUTURES) else failure_threshold
            ),
        )

        if data_type in (DataType.SPOT, DataType.FUTURES) and failure_threshold > 0:
            results = downloader._detect_gaps(
                sorted(results, key=lambda result: result.period),
                failure_threshold,
            )

    if not results:
        logger.debug(f"No results for {symbol} {data_type.value}")
        return

    # Create importer and process results (each file is its own transaction)
    importer = BinanceDuckDBImporter(dataset)
    _process_results(
        results,
        importer,
        conn,
        stats,
        symbol,
        window_start=window_start,
        window_end=window_end,
    )


async def _run_all_ingestion(
    symbols: list[str],
    data_types: list[DataType],
    start: datetime,
    end: datetime,
    interval: Interval,
    temp_path: Path,
    conn,
    stats: dict[str, int],
    max_concurrent_klines: int,
    max_concurrent_metrics: int,
    max_concurrent_funding: int,
    failure_threshold: int,
    window_start: datetime | None = None,
    window_end: datetime | None = None,
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
            is_kline = data_type in (DataType.SPOT, DataType.FUTURES)
            # Select max_concurrent based on data type
            if is_kline:
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
                interval=interval if is_kline else None,
                temp_path=temp_path,
                conn=conn,
                stats=stats,
                max_concurrent=max_concurrent,
                failure_threshold=failure_threshold,
                window_start=window_start if is_kline else None,
                window_end=window_end if is_kline else None,
            )


def update_binance_market_data(
    db_path: str,
    symbols: list[str],
    data_types: list[DataType],
    start_date: str,
    end_date: str,
    interval: Interval = Interval.MIN_5,
    max_concurrent_klines: int = 20,
    max_concurrent_metrics: int = 100,
    max_concurrent_funding: int = 50,
    failure_threshold: int = 3,
    repair_gaps_via_api: bool = False,
    prune_klines_to_date_range: bool = False,
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
    repair_gaps_via_api : bool, optional
        When True, fill internal spot/futures/funding_rates gaps from Binance
        public REST after Data Vision import completes (default: False).
    prune_klines_to_date_range : bool, optional
        When True, remove existing spot/futures rows for the requested symbols
        and interval whose close timestamps fall outside start_date/end_date.
        This is intended for exact database rebuild workflows.

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
    window_start, window_end = _kline_close_window(start, end)

    if prune_klines_to_date_range:
        _prune_klines_outside_date_range(
            conn,
            symbols=symbols,
            data_types=data_types,
            interval=interval,
            start=start,
            end=end,
        )

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
                    window_start=window_start,
                    window_end=window_end,
                ),
                "update_binance_market_data",
            )
    finally:
        # Always close database, even on exceptions
        db.close()

    if repair_gaps_via_api:
        repair_tables = [
            data_type.value
            for data_type in data_types
            if data_type.value in ("spot", "futures", "funding_rates")
        ]
        logger.info("Running Binance REST gap repair")
        repair_report = repair_binance_gaps(
            db_path=db_path,
            tables=repair_tables,
            symbols=symbols,
            intervals=[interval.value],
        )
        logger.info(repair_report.summary())

    # Log summary
    log_ingestion_summary(
        stats=stats,
        db_path=db_path,
        symbols=symbols,
        start_date=start_date,
        end_date=end_date,
        interval=interval,
        show_availability=True,
        data_types=data_types,
    )
