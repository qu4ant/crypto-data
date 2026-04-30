"""
Binance open interest dataset.

Downloads and parses daily open interest metrics from Binance futures markets.
"""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
import pandera.pandas as pa

from crypto_data.binance_datasets.base import BinanceDatasetStrategy, Period
from crypto_data.enums import DataType
from crypto_data.schemas import OPEN_INTEREST_SCHEMA
from crypto_data.tables import OPEN_INTEREST_COLUMNS, get_table_spec
from crypto_data.utils.dates import generate_day_list

logger = logging.getLogger(__name__)


# Backward-compatible alias for tests/internal callers.
FINAL_COLUMNS = list(OPEN_INTEREST_COLUMNS)


class BinanceOpenInterestDataset(BinanceDatasetStrategy):
    """
    Dataset for downloading and parsing open interest metrics data.

    Open interest data is organized daily (not monthly) and comes from
    Binance futures markets only. Does not require an interval parameter.

    Examples
    --------
    >>> dataset = BinanceOpenInterestDataset()
    >>> dataset.table_name
    'open_interest'
    >>> dataset.is_monthly
    False
    """

    @property
    def data_type(self) -> DataType:
        """Return the DataType enum value for this dataset."""
        return DataType.OPEN_INTEREST

    @property
    def table_name(self) -> str:
        """Return the target database table name."""
        return "open_interest"

    @property
    def is_monthly(self) -> bool:
        """Return False - open interest data is organized by day."""
        return False

    @property
    def default_max_concurrent(self) -> int:
        """Return the default maximum concurrent downloads (100 for daily metrics)."""
        return get_table_spec(self.table_name).default_max_concurrent or 100

    def generate_periods(self, start: datetime, end: datetime) -> list[Period]:
        """
        Generate list of daily periods for the given date range.

        Parameters
        ----------
        start : datetime
            Start date (inclusive)
        end : datetime
            End date (inclusive)

        Returns
        -------
        List[Period]
            List of Period objects with daily granularity
        """
        days = generate_day_list(start, end)
        return [Period(day, is_monthly=False) for day in days]

    def get_schema(self) -> pa.DataFrameSchema:
        """
        Return the Pandera validation schema for open interest data.

        Returns
        -------
        pa.DataFrameSchema
            Open interest schema for validating data before import
        """
        return OPEN_INTEREST_SCHEMA

    def build_download_url(
        self, base_url: str, symbol: str, period: Period, interval: str | None = None
    ) -> str:
        """
        Build the download URL for a specific symbol and period.

        URL format:
            {base_url}data/futures/um/daily/metrics/{symbol}/{symbol}-metrics-{period}.zip

        Parameters
        ----------
        base_url : str
            Base URL for Binance Data Vision (e.g., 'https://data.binance.vision/')
        symbol : str
            Trading pair symbol (e.g., 'BTCUSDT')
        period : Period
            Time period to download (e.g., Period('2024-01-15', is_monthly=False))
        interval : Optional[str]
            Not used for open interest data (ignored)

        Returns
        -------
        str
            Full download URL
        """
        path = f"data/futures/um/daily/metrics/{symbol}"
        filename = f"{symbol}-metrics-{period.value}.zip"
        return f"{base_url}{path}/{filename}"

    def build_temp_filename(self, symbol: str, period: Period, interval: str | None = None) -> str:
        """
        Build the temporary filename for a download.

        Format: {symbol}-metrics-{period}.zip

        Parameters
        ----------
        symbol : str
            Trading pair symbol (e.g., 'BTCUSDT')
        period : Period
            Time period to download
        interval : Optional[str]
            Not used for open interest data (ignored)

        Returns
        -------
        str
            Temporary filename (without directory path)
        """
        return f"{symbol}-metrics-{period.value}.zip"

    def parse_csv(self, csv_path: Path, symbol: str) -> pd.DataFrame:
        """
        Parse a metrics CSV file into a DataFrame ready for database import.

        Metrics files always have headers. Reads 'create_time', 'symbol',
        and 'sum_open_interest' columns. Filters out rows with zero open interest.

        Parameters
        ----------
        csv_path : Path
            Path to the CSV file
        symbol : str
            Trading pair symbol (for overriding - handles 1000-prefix normalization)

        Returns
        -------
        pd.DataFrame
            Parsed DataFrame with columns: exchange, symbol, timestamp, open_interest
        """
        # Metrics files always have headers
        df = pd.read_csv(csv_path)

        # Validate required columns exist
        required_cols = ["create_time", "sum_open_interest"]
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            raise ValueError(
                f"CSV file {csv_path.name} missing required columns: {missing}. "
                f"Available columns: {list(df.columns)}"
            )

        # Add exchange column
        df["exchange"] = "binance"

        # Override symbol column (for 1000-prefix normalization)
        df["symbol"] = symbol

        # Convert timestamp with auto-detection for format
        create_time = df["create_time"]
        if pd.api.types.is_numeric_dtype(create_time):
            # Numeric timestamps: detect ms vs μs (same logic as klines)
            # timestamps >= 5e12 are microseconds
            if (create_time >= 5e12).any():
                # Microseconds
                df["timestamp"] = pd.to_datetime(create_time / 1_000_000, unit="s")
            else:
                # Milliseconds
                df["timestamp"] = pd.to_datetime(create_time / 1_000, unit="s")
        else:
            # String timestamps (e.g., '2024-01-01 00:00:00')
            df["timestamp"] = pd.to_datetime(create_time)

        # Rename sum_open_interest to open_interest
        df = df.rename(columns={"sum_open_interest": "open_interest"})

        # Select final columns
        df = df[FINAL_COLUMNS]

        # Filter out rows where open_interest == 0 (erroneous data)
        df = df[df["open_interest"] != 0]

        # Drop duplicates on primary key columns, but make the data issue visible.
        key_columns = ["exchange", "symbol", "timestamp"]
        before_dedup = len(df)
        df = df.drop_duplicates(subset=key_columns)
        dropped = before_dedup - len(df)
        if dropped:
            logger.warning(
                "Dropped %s duplicate open interest rows for %s on key %s",
                dropped,
                symbol,
                key_columns,
            )

        return df
