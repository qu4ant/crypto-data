"""
Binance DuckDB importer.

Imports parsed Binance Data Vision ZIP files into DuckDB.
"""

import logging
import zipfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import duckdb
import pandera.pandas as pa

from crypto_data.binance_datasets.base import BinanceDatasetStrategy

logger = logging.getLogger(__name__)


class BinanceDuckDBImporter:
    """
    DuckDB importer for Binance datasets.

    The dataset provides parsing logic and validation schemas.

    Parameters
    ----------
    dataset : BinanceDatasetStrategy
        The dataset that defines how to parse and validate the data.

    Examples
    --------
    >>> from crypto_data.binance_datasets.klines import BinanceKlinesDataset
    >>> from crypto_data.enums import DataType, Interval
    >>> dataset = BinanceKlinesDataset(DataType.SPOT, Interval.MIN_5)
    >>> importer = BinanceDuckDBImporter(dataset)
    >>> # Within a transaction context:
    >>> rows = importer.import_file(conn, zip_path, 'BTCUSDT')
    """

    def __init__(self, dataset: BinanceDatasetStrategy) -> None:
        """Initialize the importer with a Binance dataset."""
        self.dataset = dataset

    def import_file(
        self,
        conn,
        file_path: Path,
        symbol: str,
        period: Optional[str] = None,
        replace_existing: bool = False,
    ) -> int:
        """
        Import a downloaded ZIP file into DuckDB.

        Extracts the CSV from the ZIP, parses it using the dataset handler,
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
        period : str, optional
            Period string for replace operations (YYYY-MM-DD for daily klines)
        replace_existing : bool
            If True, replace existing rows for the parsed period after
            validation succeeds.

        Returns
        -------
        int
            Number of rows imported (0 if duplicate data)

        Raises
        ------
        ValueError
            If no CSV file found in ZIP, or if validation fails
        """
        table = self.dataset.table_name
        logger.debug(f"Importing to {table} (exchange=binance, symbol={symbol})")

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
            # Parse CSV using the dataset handler.
            df = self.dataset.parse_csv(csv_path, symbol)

            if df.empty:
                logger.debug(f"  No data after parsing (filtered out)")
                return 0

            # Validate using the dataset schema.
            schema = self.dataset.get_schema()
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

            if replace_existing:
                if table not in ('spot', 'futures') or not period:
                    raise ValueError("replace_existing requires a daily kline period")

                start = datetime.strptime(period, "%Y-%m-%d")
                end = start + timedelta(days=1)
                conn.execute(
                    f"""
                    DELETE FROM {table}
                    WHERE exchange = ?
                      AND symbol = ?
                      AND interval = ?
                      AND timestamp > ?
                      AND timestamp <= ?
                    """,
                    ['binance', symbol, self.dataset.interval.value, start, end],
                )
                conn.execute(f"INSERT INTO {table} SELECT * FROM df")
                logger.debug(f"  Replaced {len(df)} rows for {symbol} {table} {period}")
                return len(df)

            # Insert into DuckDB. Use an idempotent row-level insert so a
            # re-downloaded period can repair partial data without failing on
            # rows that are already present.
            key_columns = (
                ['exchange', 'symbol', 'interval', 'timestamp']
                if table in ('spot', 'futures')
                else ['exchange', 'symbol', 'timestamp']
            )
            join_conditions = " AND ".join(f"existing.{col} = incoming.{col}" for col in key_columns)
            inserted_count = conn.execute(f"""
                SELECT COUNT(*)
                FROM df AS incoming
                LEFT JOIN {table} AS existing
                  ON {join_conditions}
                WHERE existing.exchange IS NULL
            """).fetchone()[0]

            try:
                conn.execute(f"INSERT OR IGNORE INTO {table} SELECT * FROM df")
                logger.debug(f"  Import successful: {inserted_count} new rows")
                return inserted_count
            except duckdb.ConstraintException:
                logger.debug(f"  Skipped duplicate data for {symbol} {table}")
                return 0
            except Exception as insert_error:
                # Fallback for constraint messages from alternative drivers.
                if "duplicate key" in str(insert_error).lower():
                    logger.debug(f"  Skipped duplicate data for {symbol} {table}")
                    return 0
                raise

        except Exception as e:
            if not isinstance(e, ValueError):
                logger.error(f"  Import failed: {e}")
            raise

        finally:
            # Clean up extracted CSV file
            if csv_path.exists():
                csv_path.unlink()
