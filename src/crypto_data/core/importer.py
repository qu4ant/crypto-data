"""
Generic Data Importer

Consolidates all import logic into a single class that uses the strategy pattern.
Replaces the duplicate import functions (import_to_duckdb, import_metrics_to_duckdb,
import_funding_rates_to_duckdb) with one unified implementation.
"""

import logging
import zipfile
from pathlib import Path

import pandera.pandas as pa

from crypto_data.strategies.base import DataTypeStrategy

logger = logging.getLogger(__name__)


class DataImporter:
    """
    Generic data importer for all data types.

    Uses strategy pattern to handle different data types (klines, open_interest,
    funding_rates). The strategy provides parsing logic and validation schemas.

    Parameters
    ----------
    strategy : DataTypeStrategy
        The strategy that defines how to parse and validate the data.

    Examples
    --------
    >>> from crypto_data.strategies.klines import KlinesStrategy
    >>> from crypto_data.enums import DataType, Interval
    >>> strategy = KlinesStrategy(DataType.SPOT, Interval.MIN_5)
    >>> importer = DataImporter(strategy)
    >>> # Within a transaction context:
    >>> rows = importer.import_file(conn, zip_path, 'BTCUSDT')
    """

    def __init__(self, strategy: DataTypeStrategy) -> None:
        """Initialize the importer with a strategy."""
        self.strategy = strategy

    def import_file(
        self,
        conn,
        file_path: Path,
        symbol: str,
        exchange: str = 'binance'
    ) -> int:
        """
        Import a downloaded ZIP file into DuckDB.

        Extracts the CSV from the ZIP, parses it using the strategy,
        validates with Pandera schema, and inserts into the database.

        IMPORTANT: Must be called within a transaction context.
        The caller is responsible for BEGIN/COMMIT/ROLLBACK.

        Parameters
        ----------
        conn : duckdb.DuckDBPyConnection
            Database connection (must be within a transaction)
        file_path : Path
            Path to ZIP file containing CSV data
        symbol : str
            Trading pair symbol (e.g., 'BTCUSDT')
        exchange : str, optional
            Exchange name (default: 'binance')

        Returns
        -------
        int
            Number of rows imported (0 if duplicate data)

        Raises
        ------
        ValueError
            If no CSV file found in ZIP, or if validation fails
        """
        table = self.strategy.table_name
        logger.debug(f"Importing to {table} (exchange={exchange}, symbol={symbol})")

        # Extract ZIP file
        with zipfile.ZipFile(file_path, 'r') as zip_ref:
            csv_files = [f for f in zip_ref.namelist() if f.endswith('.csv')]

            if not csv_files:
                raise ValueError(f"No CSV file found in ZIP: {file_path}")

            csv_name = csv_files[0]

            # Extract to temp directory (same directory as ZIP)
            temp_dir = file_path.parent
            csv_path = temp_dir / csv_name
            zip_ref.extract(csv_name, temp_dir)

        try:
            # Parse CSV using strategy
            df = self.strategy.parse_csv(csv_path, symbol, exchange)

            if df.empty:
                logger.debug(f"  No data after parsing (filtered out)")
                return 0

            # Validate using strategy's schema
            schema = self.strategy.get_schema()
            try:
                schema.validate(df)
                logger.debug(f"  Data validation passed: {len(df)} rows")
            except pa.errors.SchemaError as e:
                logger.error(f"  Data validation FAILED for {symbol} {table}")
                logger.error(f"  Validation errors: {e}")
                logger.error(f"  File rejected: {file_path.name}")
                raise ValueError(
                    f"Data validation failed for {symbol}: Invalid data format"
                ) from e

            # Insert into DuckDB
            try:
                conn.execute(f"INSERT INTO {table} SELECT * FROM df")
                logger.debug(f"  Import successful: {len(df)} rows")
                return len(df)
            except Exception as insert_error:
                # Skip silently if duplicate (data already exists)
                if "Duplicate key" in str(insert_error):
                    logger.debug(f"  Skipped duplicate data for {symbol} {table}")
                    return 0
                else:
                    raise

        except Exception as e:
            if not isinstance(e, ValueError):
                logger.error(f"  Import failed: {e}")
            raise

        finally:
            # Clean up extracted CSV file
            if csv_path.exists():
                csv_path.unlink()
