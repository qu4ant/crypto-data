"""
Binance Data Ingestion

Downloads historical data from Binance Data Vision and imports to DuckDB.
Implements the "Download → Import → Delete" pipeline.
"""

import logging
import tempfile
import time
import zipfile
from pathlib import Path
from datetime import datetime, timedelta
from typing import List
import requests
import duckdb

from crypto_data.database import CryptoDatabase
from crypto_data.validation import validate_interval_consistency

logger = logging.getLogger(__name__)

# Binance Data Vision base URL
BASE_URL = "https://data.binance.vision/"

# Data categories
DATA_CATEGORIES = {
    'spot': 'data/spot/monthly/klines',
    'futures': 'data/futures/um/monthly/klines'
}


def ingest_binance(
    db_path: str,
    symbols: List[str],
    data_types: List[str],
    start_date: str,
    end_date: str,
    interval: str = '5m',
    skip_existing: bool = True
):
    """
    Download Binance data and import to DuckDB.

    Process:
    1. Generate list of files to download (symbol, data_type, month)
    2. For each file:
       - Check if data already exists (if skip_existing=True)
       - Download ZIP to temp directory
       - Import to DuckDB using read_csv_auto()
       - Delete ZIP file
       - Log progress
    3. Commit changes

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

    Example
    -------
    >>> ingest_binance(
    ...     db_path='crypto_data.db',
    ...     symbols=['BTCUSDT', 'ETHUSDT'],
    ...     data_types=['spot', 'futures'],
    ...     start_date='2024-01-01',
    ...     end_date='2024-03-31'
    ... )
    """
    logger.info(f"Starting Binance data ingestion")
    logger.info(f"  Database: {db_path}")
    logger.info(f"  Symbols: {symbols}")
    logger.info(f"  Data types: {data_types}")
    logger.info(f"  Date range: {start_date} to {end_date}")
    logger.info(f"  Interval: {interval}")

    # Parse dates
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    # Generate month list
    months = _generate_month_list(start, end)
    logger.info(f"  Months to process: {len(months)}")

    # Connect to database and validate interval
    db = CryptoDatabase(db_path)
    conn = db.conn

    # Validate interval consistency (filename + database metadata)
    validate_interval_consistency(db_path, interval, db)

    # Statistics
    stats = {
        'downloaded': 0,
        'skipped': 0,
        'failed': 0,
        'not_found': 0
    }

    # Create temp directory for downloads
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        # Process each combination
        total = len(symbols) * len(data_types) * len(months)
        current = 0

        for symbol in symbols:
            for data_type in data_types:
                for month in months:
                    current += 1
                    progress = f"[{current}/{total}]"

                    logger.info(f"{progress} Processing {symbol} {data_type} {month}")

                    # Skip if data exists
                    if skip_existing and _data_exists(conn, symbol, month, data_type):
                        logger.info(f"{progress}   ↳ Skipped (already exists)")
                        stats['skipped'] += 1
                        continue

                    # Download to temp
                    temp_file = temp_path / f"{symbol}-{data_type}-{month}.zip"

                    try:
                        success = _download_file(
                            symbol=symbol,
                            data_type=data_type,
                            month=month,
                            interval=interval,
                            output_path=temp_file
                        )

                        if not success:
                            logger.warning(f"{progress}   ↳ Not found (data not available)")
                            stats['not_found'] += 1
                            continue

                        # Import to DuckDB
                        _import_to_duckdb(
                            conn=conn,
                            file_path=temp_file,
                            symbol=symbol,
                            data_type=data_type
                        )

                        stats['downloaded'] += 1
                        logger.info(f"{progress}   ↳ Success")

                    except Exception as e:
                        logger.error(f"{progress}   ↳ Failed: {e}")
                        stats['failed'] += 1

                    finally:
                        # Delete temp file
                        if temp_file.exists():
                            temp_file.unlink()

    db.close()

    # Log summary
    logger.info("=" * 60)
    logger.info("Ingestion Summary:")
    logger.info(f"  Downloaded: {stats['downloaded']}")
    logger.info(f"  Skipped: {stats['skipped']}")
    logger.info(f"  Failed: {stats['failed']}")
    logger.info(f"  Not found: {stats['not_found']}")

    if stats['not_found'] > 0:
        logger.info("")
        logger.info("Note: 'Not found' means data doesn't exist in Binance Data Vision.")
        logger.info("Common reasons:")
        logger.info("  - Symbol delisted (e.g., FTTUSDT after Nov 2022)")
        logger.info("  - Symbol not yet launched in that period")
        logger.info("  - Futures contract started later than spot market")
        logger.info("This is normal and not an error.")

    logger.info("=" * 60)


def _generate_month_list(start: datetime, end: datetime) -> List[str]:
    """Generate list of YYYY-MM strings between start and end dates."""
    months = []
    current = start.replace(day=1)
    end_month = end.replace(day=1)

    while current <= end_month:
        months.append(current.strftime("%Y-%m"))
        # Move to next month
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)

    return months


def _data_exists(conn, symbol: str, month: str, data_type: str) -> bool:
    """Check if data already exists in database for given symbol/month."""
    table = f"binance_{data_type}"

    # Parse month to get date range
    year, month_num = month.split('-')
    start_date = f"{year}-{month_num}-01"

    # Check next month for end date
    if month_num == '12':
        end_date = f"{int(year) + 1}-01-01"
    else:
        end_date = f"{year}-{int(month_num) + 1:02d}-01"

    result = conn.execute(f"""
        SELECT COUNT(*) FROM {table}
        WHERE symbol = ?
            AND timestamp >= ?
            AND timestamp < ?
    """, [symbol, start_date, end_date]).fetchone()

    return result[0] > 0 if result else False


def _download_file(
    symbol: str,
    data_type: str,
    month: str,
    interval: str,
    output_path: Path
) -> bool:
    """
    Download file from Binance Data Vision.

    Returns
    -------
    bool
        True if download successful, False if file not found
    """
    # Construct URL
    category = DATA_CATEGORIES[data_type]
    filename = f"{symbol}-{interval}-{month}.zip"
    url = f"{BASE_URL}{category}/{symbol}/{interval}/{filename}"

    logger.debug(f"Downloading: {url}")

    try:
        response = requests.get(url, timeout=30)

        if response.status_code == 404:
            logger.debug(f"  File not found (404)")
            return False

        response.raise_for_status()

        # Save to file
        output_path.write_bytes(response.content)
        logger.debug(f"  Downloaded: {len(response.content)} bytes")

        return True

    except requests.exceptions.RequestException as e:
        logger.error(f"  Download error: {e}")
        raise


def _import_to_duckdb(
    conn,
    file_path: Path,
    symbol: str,
    data_type: str
):
    """
    Import CSV from ZIP file into DuckDB.

    Extracts ZIP first, then reads CSV with DuckDB.
    """
    table = f"binance_{data_type}"

    logger.debug(f"Importing to {table}")

    # Extract ZIP file
    with zipfile.ZipFile(file_path, 'r') as zip_ref:
        # Get the CSV file name (should be only one file in the ZIP)
        csv_files = [f for f in zip_ref.namelist() if f.endswith('.csv')]

        if not csv_files:
            raise ValueError(f"No CSV file found in ZIP: {file_path}")

        csv_name = csv_files[0]

        # Extract to temp directory
        temp_dir = file_path.parent
        csv_path = temp_dir / csv_name
        zip_ref.extract(csv_name, temp_dir)

    # Detect if CSV has header
    has_header = False
    with open(csv_path, 'r') as f:
        first_line = f.readline().strip()
        # Check if first line contains header keywords
        if 'open_time' in first_line.lower() or 'close_time' in first_line.lower():
            has_header = True
            logger.debug(f"  Detected header in CSV")

    # Column names - futures uses "count" instead of "trades_count"
    count_col = 'count' if 'futures' in data_type else 'trades_count'
    taker_buy_base_col = 'taker_buy_volume' if has_header else 'taker_buy_base_volume'
    taker_buy_quote_col = 'taker_buy_quote_volume' if has_header else 'taker_buy_quote_volume'

    try:
        # If file has header, let DuckDB read it with auto columns
        if has_header:
            query = f"""
                INSERT OR REPLACE INTO {table}
                SELECT
                    '{symbol}' as symbol,
                    CASE
                        WHEN close_time > 1e15 THEN to_timestamp(close_time / 1000000.0)
                        ELSE to_timestamp(close_time / 1000.0)
                    END as timestamp,
                    CAST(open AS DOUBLE) as open,
                    CAST(high AS DOUBLE) as high,
                    CAST(low AS DOUBLE) as low,
                    CAST(close AS DOUBLE) as close,
                    CAST(volume AS DOUBLE) as volume,
                    CAST(quote_volume AS DOUBLE) as quote_volume,
                    CAST({count_col} AS INTEGER) as trades_count,
                    CAST({taker_buy_base_col} AS DOUBLE) as taker_buy_base_volume,
                    CAST({taker_buy_quote_col} AS DOUBLE) as taker_buy_quote_volume
                FROM read_csv('{csv_path}', header=true, delim=',')
            """
        else:
            query = f"""
                INSERT OR REPLACE INTO {table}
                SELECT
                    '{symbol}' as symbol,
                    CASE
                        WHEN close_time > 1e15 THEN to_timestamp(close_time / 1000000.0)
                        ELSE to_timestamp(close_time / 1000.0)
                    END as timestamp,
                    CAST(open AS DOUBLE) as open,
                    CAST(high AS DOUBLE) as high,
                    CAST(low AS DOUBLE) as low,
                    CAST(close AS DOUBLE) as close,
                    CAST(volume AS DOUBLE) as volume,
                    CAST(quote_volume AS DOUBLE) as quote_volume,
                    CAST(trades_count AS INTEGER) as trades_count,
                    CAST(taker_buy_base_volume AS DOUBLE) as taker_buy_base_volume,
                    CAST(taker_buy_quote_volume AS DOUBLE) as taker_buy_quote_volume
                FROM read_csv('{csv_path}',
                    header=false,
                    delim=',',
                    columns={{
                        'open_time': 'BIGINT',
                        'open': 'VARCHAR',
                        'high': 'VARCHAR',
                        'low': 'VARCHAR',
                        'close': 'VARCHAR',
                        'volume': 'VARCHAR',
                        'close_time': 'BIGINT',
                        'quote_volume': 'VARCHAR',
                        'trades_count': 'INTEGER',
                        'taker_buy_base_volume': 'VARCHAR',
                        'taker_buy_quote_volume': 'VARCHAR',
                        'ignore': 'VARCHAR'
                    }}
                )
            """

        conn.execute(query)

        logger.debug(f"  Import successful")

    except Exception as e:
        logger.error(f"  Import failed: {e}")
        raise

    finally:
        # Delete extracted CSV file
        if csv_path.exists():
            csv_path.unlink()
