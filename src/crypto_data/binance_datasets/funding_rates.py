"""
Binance funding rates dataset.

Downloads and parses monthly funding rate data from Binance futures markets.
"""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
import pandera.pandas as pa

from crypto_data.binance_datasets.base import BinanceDatasetStrategy, Period
from crypto_data.enums import DataType
from crypto_data.import_anomalies import ImportAnomaly
from crypto_data.schemas import FUNDING_RATES_SCHEMA
from crypto_data.tables import FUNDING_RATES_COLUMNS, get_table_spec
from crypto_data.utils.dates import generate_month_list

logger = logging.getLogger(__name__)


# Backward-compatible alias for tests/internal callers.
FINAL_COLUMNS = list(FUNDING_RATES_COLUMNS)


class BinanceFundingRatesDataset(BinanceDatasetStrategy):
    """
    Dataset for downloading and parsing funding rate data.

    Funding rate data is organized monthly and comes from Binance futures
    markets only. Does not require an interval parameter.

    Examples
    --------
    >>> dataset = BinanceFundingRatesDataset()
    >>> dataset.table_name
    'funding_rates'
    >>> dataset.is_monthly
    True
    """

    @property
    def data_type(self) -> DataType:
        """Return the DataType enum value for this dataset."""
        return DataType.FUNDING_RATES

    @property
    def table_name(self) -> str:
        """Return the target database table name."""
        return "funding_rates"

    @property
    def is_monthly(self) -> bool:
        """Return True - funding rate data is organized by month."""
        return True

    @property
    def default_max_concurrent(self) -> int:
        """Return the default maximum concurrent downloads (50 for monthly metrics)."""
        return get_table_spec(self.table_name).default_max_concurrent or 50

    def generate_periods(self, start: datetime, end: datetime) -> list[Period]:
        """
        Generate list of monthly periods for the given date range.

        Parameters
        ----------
        start : datetime
            Start date (inclusive)
        end : datetime
            End date (inclusive)

        Returns
        -------
        List[Period]
            List of Period objects with monthly granularity
        """
        months = generate_month_list(start, end)
        return [Period(month, is_monthly=True) for month in months]

    def get_schema(self) -> pa.DataFrameSchema:
        """
        Return the Pandera validation schema for funding rate data.

        Returns
        -------
        pa.DataFrameSchema
            Funding rates schema for validating data before import
        """
        return FUNDING_RATES_SCHEMA

    def build_download_url(
        self, base_url: str, symbol: str, period: Period, interval: str | None = None
    ) -> str:
        """
        Build the download URL for a specific symbol and period.

        URL format:
            {base_url}data/futures/um/monthly/fundingRate/{symbol}/{symbol}-fundingRate-{period}.zip

        Parameters
        ----------
        base_url : str
            Base URL for Binance Data Vision (e.g., 'https://data.binance.vision/')
        symbol : str
            Trading pair symbol (e.g., 'BTCUSDT')
        period : Period
            Time period to download (e.g., Period('2024-01', is_monthly=True))
        interval : Optional[str]
            Not used for funding rate data (ignored)

        Returns
        -------
        str
            Full download URL
        """
        path = f"data/futures/um/monthly/fundingRate/{symbol}"
        filename = f"{symbol}-fundingRate-{period.value}.zip"
        return f"{base_url}{path}/{filename}"

    def build_temp_filename(self, symbol: str, period: Period, interval: str | None = None) -> str:
        """
        Build the temporary filename for a download.

        Format: {symbol}-fundingRate-{period}.zip

        Parameters
        ----------
        symbol : str
            Trading pair symbol (e.g., 'BTCUSDT')
        period : Period
            Time period to download
        interval : Optional[str]
            Not used for funding rate data (ignored)

        Returns
        -------
        str
            Temporary filename (without directory path)
        """
        return f"{symbol}-fundingRate-{period.value}.zip"

    def parse_csv(self, csv_path: Path, symbol: str) -> pd.DataFrame:
        """Parse a funding rate CSV file into a DataFrame ready for database import."""
        df, _ = self.parse_csv_with_anomalies(csv_path, symbol)
        return df

    def parse_csv_with_anomalies(
        self,
        csv_path: Path,
        symbol: str,
    ) -> tuple[pd.DataFrame, list[ImportAnomaly]]:
        """
        Parse a funding rate CSV file into a DataFrame ready for database import.

        Funding rate files always have headers. Reads 'calc_time' and
        'last_funding_rate' columns.

        Parameters
        ----------
        csv_path : Path
            Path to the CSV file
        symbol : str
            Trading pair symbol (for overriding - handles 1000-prefix normalization)

        Returns
        -------
        pd.DataFrame
            Parsed DataFrame with columns: exchange, symbol, timestamp, funding_rate
        """
        # Funding rate files always have headers
        df = pd.read_csv(csv_path)

        # Validate required columns exist
        required_cols = ["calc_time", "last_funding_rate"]
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

        # Convert timestamp: calc_time >= 5e12 means microseconds, otherwise milliseconds
        calc_time = pd.to_numeric(df["calc_time"], errors="raise")
        if (calc_time >= 5e12).any():
            # Microseconds (16+ digits)
            df["timestamp"] = pd.to_datetime(calc_time.astype("int64"), unit="us")
        else:
            # Milliseconds (13 digits)
            df["timestamp"] = pd.to_datetime(calc_time.astype("int64"), unit="ms")

        # Rename last_funding_rate to funding_rate
        df = df.rename(columns={"last_funding_rate": "funding_rate"})

        # Select final columns
        df = df[FINAL_COLUMNS]

        anomalies: list[ImportAnomaly] = []

        # Drop duplicates on primary key columns, but make the data issue visible.
        key_columns = ["exchange", "symbol", "timestamp"]
        before_dedup = len(df)
        df = df.drop_duplicates(subset=key_columns)
        dropped = before_dedup - len(df)
        if dropped:
            anomalies.append(
                ImportAnomaly(
                    check_name="import_dropped_duplicate_rows",
                    count=dropped,
                    metadata={"primary_key": key_columns},
                )
            )
            logger.warning(
                "Dropped %s duplicate funding rate rows for %s on key %s",
                dropped,
                symbol,
                key_columns,
            )

        return df, anomalies
