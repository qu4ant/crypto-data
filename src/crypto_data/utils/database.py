"""
Database Operations Utilities

Provides database import and query utilities for the crypto-data package.
"""

import logging
import zipfile
from pathlib import Path
from typing import Optional
import pandas as pd
import pandera as pa

# Import Pandera validation functions
from crypto_data.schemas import (
    validate_ohlcv_dataframe,
    validate_open_interest_dataframe,
    validate_funding_rates_dataframe
)

logger = logging.getLogger(__name__)


def import_to_duckdb(
    conn,
    file_path: Path,
    symbol: str,
    data_type: str,
    interval: str,
    exchange: str = 'binance',
    original_symbol: Optional[str] = None
):
    """
    Import CSV from ZIP file into DuckDB.

    Uses Pandas to handle duplicates (e.g., daylight saving time issues).
    INSERT OR REPLACE handles re-imports automatically.

    IMPORTANT: Transaction Safety
    ------------------------------
    This function performs INSERT operations and MUST be called within an
    explicit transaction context (BEGIN TRANSACTION / COMMIT / ROLLBACK).
    The caller is responsible for transaction management to ensure atomicity.

    Example usage:
        conn.execute("BEGIN TRANSACTION")
        try:
            import_to_duckdb(conn, file_path, symbol, data_type, interval)
            conn.execute("COMMIT")
        except Exception as e:
            conn.execute("ROLLBACK")
            raise

    See ingestion.py lines 699-709 for production usage pattern.

    Parameters
    ----------
    conn : duckdb.DuckDBPyConnection
        Database connection
    file_path : Path
        Path to ZIP file containing CSV data
    symbol : str
        Symbol used for download (may be 1000-prefixed, e.g., '1000PEPEUSDT')
    data_type : str
        Data type ('spot' or 'futures')
    interval : str
        Kline interval (e.g., '5m', '1h')
    exchange : str, optional
        Exchange name (default: 'binance')
    original_symbol : str, optional
        Original symbol to store in database (e.g., 'PEPEUSDT').
        If None, uses symbol parameter. This ensures consistency when
        Binance uses different tickers for spot vs futures.
    """
    table = data_type  # 'spot' or 'futures'
    logger.debug(f"Importing to {table} (exchange={exchange})")

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

    try:
        # Detect if CSV has header
        has_header = False
        with open(csv_path, 'r') as f:
            first_line = f.readline().strip()
            if 'open_time' in first_line.lower() or 'close_time' in first_line.lower():
                has_header = True
                logger.debug(f"  Detected header in CSV")

        # Read CSV with pandas
        if has_header:
            df = pd.read_csv(csv_path)
        else:
            # Define column names for headerless CSV
            columns = ['open_time', 'open', 'high', 'low', 'close', 'volume',
                      'close_time', 'quote_volume', 'trades_count',
                      'taker_buy_base_volume', 'taker_buy_quote_volume', 'ignore']
            df = pd.read_csv(csv_path, header=None, names=columns)

        # Add exchange, symbol and interval columns
        # Use original_symbol if provided (handles 1000-prefix normalization)
        storage_symbol = original_symbol if original_symbol else symbol
        df['exchange'] = exchange
        df['symbol'] = storage_symbol
        df['interval'] = interval

        # Convert timestamp (handle both milliseconds and microseconds)
        # Auto-detect format based on magnitude:
        # - Milliseconds (13 digits): < 5e12 (~2128 AD, safe threshold)
        # - Microseconds (16 digits): >= 5e12
        # Note: Binance changed from ms to �s between 2024 and 2025
        df['timestamp'] = pd.to_datetime(
            df['close_time'].apply(
                lambda x: x / 1000000.0 if x >= 5e12 else x / 1000.0
            ),
            unit='s'
        ).dt.ceil('1s')  # Round up to full seconds (03:59:59.999 → 04:00:00)

        # Rename/harmonize column names for futures vs spot
        if 'count' in df.columns:
            df.rename(columns={'count': 'trades_count'}, inplace=True)
        if 'taker_buy_volume' in df.columns:
            df.rename(columns={'taker_buy_volume': 'taker_buy_base_volume'}, inplace=True)
        if has_header and 'taker_buy_quote_volume' not in df.columns:
            # Some headers might use different names
            pass

        # Select and reorder columns for insertion
        final_columns = ['exchange', 'symbol', 'interval', 'timestamp', 'open', 'high', 'low', 'close',
                        'volume', 'quote_volume', 'trades_count',
                        'taker_buy_base_volume', 'taker_buy_quote_volume']
        df = df[final_columns]

        # Drop duplicates (handles daylight saving time duplicates)
        original_len = len(df)
        df = df.drop_duplicates(subset=['exchange', 'symbol', 'interval', 'timestamp'], keep='first')
        if len(df) < original_len:
            logger.debug(f"  Removed {original_len - len(df)} duplicate timestamps")

        # VALIDATION: Validate data quality BEFORE import (Pandera schema check)
        # This prevents importing corrupted or invalid data into the database
        try:
            validate_ohlcv_dataframe(df, strict=True)  # Strict mode: reject invalid data
            logger.debug(f"  Data validation passed: {len(df)} rows")
        except pa.errors.SchemaError as e:
            # Data quality validation failed - reject this file
            logger.error(f"  ❌ Data validation FAILED for {storage_symbol} {table} {interval}")
            logger.error(f"  Validation errors: {e}")
            logger.error(f"  File rejected: {file_path.name}")
            logger.error(f"  Suggestion: Check data source or contact support if persistent")
            # Raise exception to trigger transaction rollback
            raise ValueError(f"Data validation failed: Invalid OHLCV data (OHLC relationships, negative prices, etc.)") from e

        # Insert into DuckDB
        try:
            conn.execute(f"INSERT INTO {table} SELECT * FROM df")
            logger.debug(f"  Import successful: {len(df)} rows")
        except Exception as insert_error:
            # Skip silencieusement si duplicate (données déjà là)
            if "Duplicate key" in str(insert_error):
                logger.debug(f"Skipped duplicate data for {storage_symbol} {table}")
                # Données déjà présentes, on continue sans erreur
            else:
                # Autre erreur, on la propage
                raise

    except Exception as e:
        logger.error(f"  Import failed: {e}")
        raise

    finally:
        # Delete extracted CSV file
        if csv_path.exists():
            csv_path.unlink()


def import_metrics_to_duckdb(
    conn,
    file_path: Path,
    symbol: str,
    exchange: str = 'binance'
):
    """
    Import open interest metrics from ZIP file into DuckDB.

    IMPORTANT: Transaction Safety
    ------------------------------
    This function performs INSERT operations and MUST be called within an
    explicit transaction context (BEGIN TRANSACTION / COMMIT / ROLLBACK).
    The caller is responsible for transaction management to ensure atomicity.

    Parameters
    ----------
    conn : duckdb.DuckDBPyConnection
        Database connection
    file_path : Path
        Path to ZIP file containing metrics CSV data
    symbol : str
        Trading pair symbol (e.g., 'BTCUSDT')
    exchange : str, optional
        Exchange name (default: 'binance')
    """
    table = 'open_interest'
    logger.debug(f"Importing to {table} (exchange={exchange})")

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

    try:
        # Read CSV with pandas (metrics always have headers)
        df = pd.read_csv(csv_path)

        # Validate required columns
        required_cols = ['create_time', 'symbol', 'sum_open_interest']
        if not all(col in df.columns for col in required_cols):
            raise ValueError(f"CSV missing required columns. Expected: {required_cols}, Found: {df.columns.tolist()}")

        # Add exchange column
        df['exchange'] = exchange

        # Convert timestamp string to datetime
        df['timestamp'] = pd.to_datetime(df['create_time'])

        # Rename columns to match database schema
        df.rename(columns={
            'sum_open_interest': 'open_interest'
        }, inplace=True)

        # Select only columns we want to store
        final_columns = ['exchange', 'symbol', 'timestamp', 'open_interest']
        df = df[final_columns]

        # Filter out rows where open_interest is zero (erroneous data)
        original_len_before_filter = len(df)
        df = df[df['open_interest'] != 0]
        if len(df) < original_len_before_filter:
            logger.debug(f"  Filtered out {original_len_before_filter - len(df)} rows with zero values")

        # Drop duplicates
        original_len = len(df)
        df = df.drop_duplicates(subset=['exchange', 'symbol', 'timestamp'], keep='first')
        if len(df) < original_len:
            logger.debug(f"  Removed {original_len - len(df)} duplicate timestamps")

        # VALIDATION: Validate data quality BEFORE import (Pandera schema check)
        # This prevents importing corrupted or invalid data into the database
        try:
            validate_open_interest_dataframe(df, strict=True)  # Strict mode: reject invalid data
            logger.debug(f"  Data validation passed: {len(df)} rows")
        except pa.errors.SchemaError as e:
            # Data quality validation failed - reject this file
            logger.error(f"  ❌ Data validation FAILED for {symbol} {table}")
            logger.error(f"  Validation errors: {e}")
            logger.error(f"  File rejected: {file_path.name}")
            logger.error(f"  Suggestion: Check data source or contact support if persistent")
            # Raise exception to trigger transaction rollback
            raise ValueError(f"Data validation failed: Invalid open interest data (negative values, outliers, etc.)") from e

        # Insert into DuckDB
        try:
            conn.execute(f"INSERT INTO {table} SELECT * FROM df")
            logger.debug(f"  Import successful: {len(df)} rows")
        except Exception as insert_error:
            # Skip silently if duplicate (data already there)
            if "Duplicate key" in str(insert_error):
                logger.debug(f"Skipped duplicate data for {symbol} {table}")
            else:
                # Other error, propagate
                raise

    except Exception as e:
        logger.error(f"  Import failed: {e}")
        raise

    finally:
        # Delete extracted CSV file
        if csv_path.exists():
            csv_path.unlink()


def import_funding_rates_to_duckdb(
    conn,
    file_path: Path,
    symbol: str,
    exchange: str = 'binance'
):
    """
    Import funding rate data from ZIP file into DuckDB.

    IMPORTANT: Transaction Safety
    ------------------------------
    This function performs INSERT operations and MUST be called within an
    explicit transaction context (BEGIN TRANSACTION / COMMIT / ROLLBACK).
    The caller is responsible for transaction management to ensure atomicity.

    Parameters
    ----------
    conn : duckdb.DuckDBPyConnection
        Database connection
    file_path : Path
        Path to ZIP file containing funding rate CSV data
    symbol : str
        Trading pair symbol (e.g., 'BTCUSDT')
    exchange : str, optional
        Exchange name (default: 'binance')
    """
    table = 'funding_rates'
    logger.debug(f"Importing to {table} (exchange={exchange})")

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

    try:
        # Read CSV with pandas (funding rates always have headers)
        df = pd.read_csv(csv_path)

        # Validate required columns
        required_cols = ['calc_time', 'last_funding_rate']
        if not all(col in df.columns for col in required_cols):
            raise ValueError(f"CSV missing required columns. Expected: {required_cols}, Found: {df.columns.tolist()}")

        # Add exchange column
        df['exchange'] = exchange
        df['symbol'] = symbol

        # Convert timestamp (milliseconds to datetime)
        df['timestamp'] = pd.to_datetime(df['calc_time'], unit='ms')

        # Rename columns to match database schema
        df.rename(columns={
            'last_funding_rate': 'funding_rate'
        }, inplace=True)

        # Select only columns we want to store (minimal: 2 fields)
        final_columns = ['exchange', 'symbol', 'timestamp', 'funding_rate']
        df = df[final_columns]

        # Drop duplicates
        original_len = len(df)
        df = df.drop_duplicates(subset=['exchange', 'symbol', 'timestamp'], keep='first')
        if len(df) < original_len:
            logger.debug(f"  Removed {original_len - len(df)} duplicate timestamps")

        # VALIDATION: Validate data quality BEFORE import (Pandera schema check)
        # This prevents importing corrupted or invalid data into the database
        try:
            validate_funding_rates_dataframe(df, strict=True)  # Strict mode: reject invalid data
            logger.debug(f"  Data validation passed: {len(df)} rows")
        except pa.errors.SchemaError as e:
            # Data quality validation failed - reject this file
            logger.error(f"  ❌ Data validation FAILED for {symbol} {table}")
            logger.error(f"  Validation errors: {e}")
            logger.error(f"  File rejected: {file_path.name}")
            logger.error(f"  Suggestion: Check data source or contact support if persistent")
            # Raise exception to trigger transaction rollback
            raise ValueError(f"Data validation failed: Invalid funding rate data (extreme values, etc.)") from e

        # Insert into DuckDB
        try:
            conn.execute(f"INSERT INTO {table} SELECT * FROM df")
            logger.debug(f"  Import successful: {len(df)} rows")
        except Exception as insert_error:
            # Skip silently if duplicate (data already there)
            if "Duplicate key" in str(insert_error):
                logger.debug(f"Skipped duplicate data for {symbol} {table}")
            else:
                # Other error, propagate
                raise

    except Exception as e:
        logger.error(f"  Import failed: {e}")
        raise

    finally:
        # Delete extracted CSV file
        if csv_path.exists():
            csv_path.unlink()


def data_exists(conn, symbol: str, month: str, data_type: str, interval: str = None, exchange: str = 'binance') -> bool:
    """
    Check if data already exists and is complete for given symbol/month/interval.

    A month is considered "complete" if MAX(timestamp) is at least at day 24.
    This prevents skipping months with only partial data from file overlaps.

    Parameters
    ----------
    conn : duckdb.DuckDBPyConnection
        Database connection
    symbol : str
        Symbol to check
    month : str
        Month in YYYY-MM format
    data_type : str
        Data type ('spot', 'futures', 'funding_rates')
    interval : str, optional
        Kline interval (e.g., '5m', '1h'). Required for spot/futures, not used for funding_rates.
    exchange : str, optional
        Exchange name (default: 'binance')

    Returns
    -------
    bool
        True if data exists and is complete, False otherwise
    """
    # Determine table name
    if data_type in ['spot', 'futures']:
        table = data_type
    elif data_type == 'funding_rates':
        table = 'funding_rates'
    else:
        return False  # Unknown data type

    # Parse month to get date range
    year, month_num = month.split('-')
    start_date = f"{year}-{month_num}-01"

    # Check next month for end date
    if month_num == '12':
        end_date = f"{int(year) + 1}-01-01"
    else:
        end_date = f"{year}-{int(month_num) + 1:02d}-01"

    # Build query based on data type
    if data_type in ['spot', 'futures']:
        # Klines have interval column
        result = conn.execute(f"""
            SELECT MAX(timestamp) FROM {table}
            WHERE exchange = ?
                AND symbol = ?
                AND interval = ?
                AND timestamp >= ?
                AND timestamp < ?
        """, [exchange, symbol, interval, start_date, end_date]).fetchone()
    else:
        # Funding rates don't have interval column
        result = conn.execute(f"""
            SELECT MAX(timestamp) FROM {table}
            WHERE exchange = ?
                AND symbol = ?
                AND timestamp >= ?
                AND timestamp < ?
        """, [exchange, symbol, start_date, end_date]).fetchone()

    if not result or not result[0]:
        # No data for this month
        return False

    max_timestamp = result[0]

    # Check if max timestamp is at least at day 24 of the month
    # This indicates the month is mostly complete (not just overlap from previous month)
    threshold_date = f"{year}-{month_num}-24"

    # Compare dates (convert to string for comparison)
    max_date_str = max_timestamp.strftime('%Y-%m-%d')

    return max_date_str >= threshold_date