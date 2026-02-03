"""
Orchestrator for Binance data ingestion using the strategy pattern.

Provides ingest_binance_async_v2(), a refactored version of ingest_binance_async()
that uses the strategy pattern for cleaner, more maintainable code.
"""

from __future__ import annotations

import asyncio
import logging
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from crypto_data.core.downloader import BatchDownloader
from crypto_data.core.importer import DataImporter
from crypto_data.database import CryptoDatabase
from crypto_data.enums import DataType, Interval
from crypto_data.exchanges.binance import BinanceExchange
from crypto_data.strategies.base import DownloadResult
from crypto_data.strategies.registry import get_strategy
from crypto_data.utils.ingestion_helpers import initialize_ingestion_stats, log_ingestion_summary

logger = logging.getLogger(__name__)


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
                importer.import_file(conn, result.file_path, symbol, 'binance')
                stats['downloaded'] += 1
                logger.debug(f"    Imported {result.period}")

                # Delete file after import
                if result.file_path and result.file_path.exists():
                    result.file_path.unlink()

            except Exception as e:
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
            periods=periods,
            interval=interval_str,
            failure_threshold=failure_threshold
        )

    if not results:
        logger.debug(f"No results for {symbol} {data_type.value}")
        return

    # Create importer
    importer = DataImporter(strategy)

    # Process results within transaction
    try:
        conn.execute("BEGIN TRANSACTION")
        _process_results(results, importer, conn, stats, symbol)
        conn.execute("COMMIT")
    except Exception as e:
        conn.execute("ROLLBACK")
        logger.error(f"Transaction failed for {symbol} {data_type.value}: {e}")
        raise


def ingest_binance_async_v2(
    db_path: str,
    symbols: List[str],
    data_types: List[DataType],
    start_date: str,
    end_date: str,
    interval: Interval = Interval.MIN_5,
    skip_existing: bool = True,  # NOTE: not implemented yet, for API compatibility
    max_concurrent_klines: int = 20,
    max_concurrent_metrics: int = 100,
    max_concurrent_funding: int = 50,
    failure_threshold: int = 3
) -> None:
    """
    Download Binance data using strategy pattern.

    This is the refactored version of ingest_binance_async() that uses
    the strategy pattern for cleaner, more maintainable code.

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
    skip_existing : bool, optional
        Skip existing data in database (default: True)
        NOTE: Not yet implemented in v2, included for API compatibility
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
    >>> from crypto_data.core import ingest_binance_async_v2
    >>> ingest_binance_async_v2(
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

    # Create temp directory
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        # Process each symbol
        for symbol in symbols:
            logger.info(f"Processing {symbol}")

            # Process each data type
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

                # Run async ingestion
                asyncio.run(_ingest_symbol_data_type(
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
                ))

    # Close database
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
