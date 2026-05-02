"""
Binance DuckDB importer.

Imports parsed Binance Data Vision ZIP files into DuckDB.
"""

import logging
import zipfile
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import pandera.pandas as pa

from crypto_data.binance_datasets.base import BinanceDatasetStrategy
from crypto_data.db_write import insert_idempotent
from crypto_data.import_anomalies import ImportAnomaly, contextualize_import_anomalies
from crypto_data.tables import is_kline_table

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
        self.last_import_anomalies: list[dict] = []

    def import_file(
        self,
        conn,
        file_path: Path,
        symbol: str,
        period: str | None = None,
        replace_existing: bool = False,
        window_start: datetime | None = None,
        window_end: datetime | None = None,
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
        window_start : datetime, optional
            Exclusive lower bound for kline close timestamps to import.
            Ignored for non-kline tables.
        window_end : datetime, optional
            Inclusive upper bound for kline close timestamps to import.
            Ignored for non-kline tables.

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
        self.last_import_anomalies = []

        # Extract ZIP file
        with zipfile.ZipFile(file_path, "r") as zip_ref:
            csv_files = [f for f in zip_ref.namelist() if f.endswith(".csv")]

            if not csv_files:
                raise ValueError(f"No CSV file found in ZIP: {file_path}")

            csv_name = csv_files[0]

            # Extract to temp directory (same directory as ZIP)
            temp_dir = file_path.parent
            csv_path = temp_dir / csv_name
            zip_ref.extract(csv_name, temp_dir)

        try:
            # Parse CSV using the dataset handler.
            df, anomalies = self.dataset.parse_csv_with_anomalies(csv_path, symbol)

            if is_kline_table(table) and (window_start is not None or window_end is not None):
                timestamps = df["timestamp"]
                keep = True
                if window_start is not None:
                    keep = timestamps > pd.Timestamp(window_start)
                if window_end is not None:
                    upper_keep = timestamps <= pd.Timestamp(window_end)
                    keep = keep & upper_keep if not isinstance(keep, bool) else upper_keep
                df = df.loc[keep].reset_index(drop=True)

            if df.empty:
                anomalies.append(
                    ImportAnomaly(
                        check_name="import_empty_after_parse",
                        count=1,
                        metadata={"reason": "no_rows_after_parse_or_window_filter"},
                    )
                )
                self.last_import_anomalies = contextualize_import_anomalies(
                    anomalies,
                    table=table,
                    symbol=symbol,
                    interval=self.dataset.interval.value if is_kline_table(table) else None,
                    period=period,
                    source_file=file_path.name,
                )
                logger.debug("  No data after parsing (filtered out)")
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
                raise ValueError(f"Data validation failed for {symbol}: Invalid data format") from e

            self.last_import_anomalies = contextualize_import_anomalies(
                anomalies,
                table=table,
                symbol=symbol,
                interval=self.dataset.interval.value if is_kline_table(table) else None,
                period=period,
                source_file=file_path.name,
            )

            if replace_existing:
                if not is_kline_table(table) or not period:
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
                    ["binance", symbol, self.dataset.interval.value, start, end],
                )
                conn.execute(f"INSERT INTO {table} SELECT * FROM df")
                logger.debug(f"  Replaced {len(df)} rows for {symbol} {table} {period}")
                return len(df)

            # Use an idempotent row-level insert so a re-downloaded period can
            # repair partial data without failing on rows that already exist.
            inserted_count = insert_idempotent(conn, table, df)
            logger.debug(f"  Import successful: {inserted_count} new rows")
            return inserted_count

        except Exception as e:
            if not isinstance(e, ValueError):
                logger.error(f"  Import failed: {e}")
            raise

        finally:
            # Clean up extracted CSV file
            if csv_path.exists():
                csv_path.unlink()
