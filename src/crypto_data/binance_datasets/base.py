"""
Binance dataset abstractions.

Each dataset class knows how to build Binance Data Vision URLs, parse the
downloaded CSV files, and provide the validation schema for DuckDB import.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pandas as pd
import pandera.pandas as pa

from crypto_data.enums import DataType


@dataclass
class Period:
    """
    Represents a time period for data download.

    Attributes
    ----------
    value : str
        Period string: 'YYYY-MM' for monthly, 'YYYY-MM-DD' for daily
    is_monthly : bool
        True for monthly periods, False for daily periods (default: True)
    replace_existing : bool
        True when an existing period should be refreshed instead of skipped.

    Examples
    --------
    >>> monthly = Period('2024-01')
    >>> str(monthly)
    '2024-01'

    >>> daily = Period('2024-01-15', is_monthly=False)
    >>> str(daily)
    '2024-01-15'
    """

    value: str
    is_monthly: bool = True
    replace_existing: bool = False

    def __str__(self) -> str:
        """Return the period value as string."""
        return self.value


@dataclass
class DownloadResult:
    """
    Result of a download attempt.

    Attributes
    ----------
    success : bool
        True if download succeeded, False otherwise
    symbol : str
        Symbol that was downloaded (e.g., 'BTCUSDT')
    data_type : DataType
        Type of data downloaded (SPOT, FUTURES, etc.)
    period : str
        Period string (e.g., '2024-01' or '2024-01-15')
    file_path : Optional[Path]
        Path to downloaded file if successful
    error : Optional[str]
        Error description if download failed

    Properties
    ----------
    is_not_found : bool
        True if the download failed due to 404 (data not available)

    Examples
    --------
    >>> success = DownloadResult(
    ...     success=True,
    ...     symbol='BTCUSDT',
    ...     data_type=DataType.SPOT,
    ...     period='2024-01',
    ...     file_path=Path('/tmp/BTCUSDT-5m-2024-01.zip')
    ... )
    >>> success.is_not_found
    False

    >>> not_found = DownloadResult(
    ...     success=False,
    ...     symbol='BTCUSDT',
    ...     data_type=DataType.SPOT,
    ...     period='2024-01',
    ...     error='not_found'
    ... )
    >>> not_found.is_not_found
    True
    """

    success: bool
    symbol: str
    data_type: DataType
    period: str
    file_path: Path | None = None
    error: str | None = None

    @property
    def is_not_found(self) -> bool:
        """Return True if the download failed due to 404 (data not available)."""
        return not self.success and self.error == "not_found"


class BinanceDatasetStrategy(ABC):
    """
    Abstract base class for Binance datasets.

    Each Binance dataset (klines, open_interest, funding_rates) implements this
    interface to provide download and import logic.

    Subclasses must implement:
    - data_type: The DataType enum value
    - table_name: Target database table name
    - is_monthly: Whether data is organized monthly or daily
    - default_max_concurrent: Default concurrency for downloads
    - generate_periods(): Generate list of periods for date range
    - get_schema(): Return Pandera validation schema
    - build_download_url(): Build the download URL for a period
    - build_temp_filename(): Build temp filename for download
    - parse_csv(): Parse CSV data into DataFrame

    Examples
    --------
    Concrete strategies inherit from this class:

    >>> class BinanceKlinesDataset(BinanceDatasetStrategy):
    ...     @property
    ...     def data_type(self) -> DataType:
    ...         return DataType.SPOT
    ...     # ... implement other abstract methods
    """

    @property
    @abstractmethod
    def data_type(self) -> DataType:
        """Return the DataType enum value for this dataset."""
        ...

    @property
    @abstractmethod
    def table_name(self) -> str:
        """Return the target database table name."""
        ...

    @property
    @abstractmethod
    def is_monthly(self) -> bool:
        """Return True if data is organized by month, False for daily."""
        ...

    @property
    @abstractmethod
    def default_max_concurrent(self) -> int:
        """Return the default maximum concurrent downloads."""
        ...

    @abstractmethod
    def generate_periods(self, start: datetime, end: datetime) -> list[Period]:
        """
        Generate list of periods for the given date range.

        Parameters
        ----------
        start : datetime
            Start date (inclusive)
        end : datetime
            End date (inclusive)

        Returns
        -------
        List[Period]
            List of Period objects covering the date range
        """
        ...

    @abstractmethod
    def get_schema(self) -> pa.DataFrameSchema:
        """
        Return the Pandera validation schema for this data type.

        Returns
        -------
        pa.DataFrameSchema
            Pandera schema for validating data before import
        """
        ...

    @abstractmethod
    def build_download_url(
        self, base_url: str, symbol: str, period: Period, interval: str | None = None
    ) -> str:
        """
        Build the download URL for a specific symbol and period.

        Parameters
        ----------
        base_url : str
            Base URL for the data source
        symbol : str
            Trading pair symbol (e.g., 'BTCUSDT')
        period : Period
            Time period to download
        interval : Optional[str]
            Kline interval (e.g., '5m', '1h'). Only used for kline data.

        Returns
        -------
        str
            Full download URL
        """
        ...

    @abstractmethod
    def build_temp_filename(self, symbol: str, period: Period, interval: str | None = None) -> str:
        """
        Build the temporary filename for a download.

        Parameters
        ----------
        symbol : str
            Trading pair symbol (e.g., 'BTCUSDT')
        period : Period
            Time period to download
        interval : Optional[str]
            Kline interval (e.g., '5m', '1h'). Only used for kline data.

        Returns
        -------
        str
            Temporary filename (without directory path)
        """
        ...

    @abstractmethod
    def parse_csv(self, csv_path: Path, symbol: str) -> pd.DataFrame:
        """
        Parse a CSV file into a DataFrame ready for database import.

        Parameters
        ----------
        csv_path : Path
            Path to the CSV file
        symbol : str
            Trading pair symbol (for adding to DataFrame)

        Returns
        -------
        pd.DataFrame
            Parsed DataFrame with all required columns
        """
        ...
