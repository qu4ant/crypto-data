"""
Binance klines dataset for spot and futures OHLCV data.

Downloads and parses kline (candlestick) data from Binance spot and futures
markets.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import pandera.pandas as pa

from crypto_data.binance_datasets.base import BinanceDatasetStrategy, Period
from crypto_data.enums import DataType, Interval
from crypto_data.import_anomalies import ImportAnomaly
from crypto_data.schemas import OHLCV_SCHEMA
from crypto_data.tables import KLINE_TABLE_COLUMNS, get_table_spec
from crypto_data.utils.dates import generate_day_list, generate_month_list

logger = logging.getLogger(__name__)


# Column names for headerless Binance klines CSV files
KLINES_COLUMNS = [
    "open_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time",
    "quote_volume",
    "trades_count",
    "taker_buy_base_volume",
    "taker_buy_quote_volume",
    "ignore",
]

# Backward-compatible alias for tests/internal callers.
FINAL_COLUMNS = list(KLINE_TABLE_COLUMNS)

RECENT_DAILY_REFRESH_DAYS = 3


def _first_day_next_month(month_start: date) -> date:
    """Return the first day of the month after month_start."""
    if month_start.month == 12:
        return date(month_start.year + 1, 1, 1)
    return date(month_start.year, month_start.month + 1, 1)


class BinanceKlinesDataset(BinanceDatasetStrategy):
    """
    Dataset handler for downloading and parsing klines (OHLCV) data.

    Supports both spot and futures markets. URL structure differs
    between spot and futures endpoints.

    Parameters
    ----------
    data_type : DataType
        Either DataType.SPOT or DataType.FUTURES
    interval : Interval
        Kline interval (e.g., Interval.MIN_5, Interval.HOUR_1)

    Raises
    ------
    ValueError
        If data_type is not SPOT or FUTURES

    Examples
    --------
    >>> dataset = BinanceKlinesDataset(DataType.SPOT, Interval.MIN_5)
    >>> dataset.table_name
    'spot'
    """

    def __init__(
        self,
        data_type: DataType,
        interval: Interval,
        as_of: datetime | None = None,
    ) -> None:
        """Initialize the klines dataset."""
        if data_type not in (DataType.SPOT, DataType.FUTURES):
            raise ValueError(f"BinanceKlinesDataset only supports SPOT or FUTURES, got {data_type}")
        self._data_type = data_type
        self._interval = interval
        self._as_of = as_of

    @property
    def data_type(self) -> DataType:
        """Return the DataType enum value for this dataset."""
        return self._data_type

    @property
    def table_name(self) -> str:
        """Return the target database table name."""
        return self._data_type.value

    @property
    def is_monthly(self) -> bool:
        """Return True - klines data is organized by month."""
        return True

    @property
    def default_max_concurrent(self) -> int:
        """Return the default maximum concurrent downloads (20 for klines)."""
        return get_table_spec(self.table_name).default_max_concurrent or 20

    @property
    def interval(self) -> Interval:
        """Return the kline interval."""
        return self._interval

    def generate_periods(self, start: datetime, end: datetime) -> list[Period]:
        """
        Generate monthly or daily periods for the given date range.

        Complete months are downloaded as monthly files. Daily files are used
        only for the unfinished current month so historical missing monthly
        archives do not explode into thousands of daily requests.

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
        periods: list[Period] = []
        as_of_date = (self._as_of or datetime.utcnow()).date()
        latest_daily_date = as_of_date - timedelta(days=1)

        for month in generate_month_list(start, end):
            month_start = datetime.strptime(month, "%Y-%m").date()

            month_end = _first_day_next_month(month_start) - timedelta(days=1)

            if month_end < as_of_date:
                periods.append(Period(month, is_monthly=True))
                continue

            daily_start = max(start.date(), month_start)
            daily_end = min(end.date(), month_end, latest_daily_date)

            if daily_start <= daily_end:
                refresh_start = latest_daily_date - timedelta(days=RECENT_DAILY_REFRESH_DAYS - 1)
                periods.extend(
                    Period(
                        day,
                        is_monthly=False,
                        replace_existing=datetime.strptime(day, "%Y-%m-%d").date() >= refresh_start,
                    )
                    for day in generate_day_list(
                        datetime.combine(daily_start, datetime.min.time()),
                        datetime.combine(daily_end, datetime.min.time()),
                    )
                )

        return periods

    def get_schema(self) -> pa.DataFrameSchema:
        """
        Return the Pandera validation schema for OHLCV data.

        Returns
        -------
        pa.DataFrameSchema
            OHLCV schema for validating data before import
        """
        return OHLCV_SCHEMA

    def build_download_url(
        self, base_url: str, symbol: str, period: Period, interval: str | None = None
    ) -> str:
        """
        Build the download URL for a specific symbol and period.

        URL format for spot monthly:
            {base_url}data/spot/monthly/klines/{symbol}/{interval}/{symbol}-{interval}-{period}.zip

        URL format for spot daily:
            {base_url}data/spot/daily/klines/{symbol}/{interval}/{symbol}-{interval}-{period}.zip

        URL format for futures monthly:
            {base_url}data/futures/um/monthly/klines/{symbol}/{interval}/{symbol}-{interval}-{period}.zip

        URL format for futures daily:
            {base_url}data/futures/um/daily/klines/{symbol}/{interval}/{symbol}-{interval}-{period}.zip

        Parameters
        ----------
        base_url : str
            Base URL for Binance Data Vision (e.g., 'https://data.binance.vision/')
        symbol : str
            Trading pair symbol (e.g., 'BTCUSDT')
        period : Period
            Time period to download (e.g., Period('2024-01'))
        interval : Optional[str]
            Kline interval (e.g., '5m', '1h'). Uses self.interval if None.

        Returns
        -------
        str
            Full download URL
        """
        # Use instance interval if not provided
        interval_str = interval if interval is not None else self._interval.value

        archive_granularity = "monthly" if period.is_monthly else "daily"

        # Build path based on data type
        if self._data_type == DataType.SPOT:
            path = f"data/spot/{archive_granularity}/klines/{symbol}/{interval_str}"
        else:  # FUTURES
            path = f"data/futures/um/{archive_granularity}/klines/{symbol}/{interval_str}"

        filename = f"{symbol}-{interval_str}-{period.value}.zip"
        return f"{base_url}{path}/{filename}"

    def build_temp_filename(self, symbol: str, period: Period, interval: str | None = None) -> str:
        """
        Build the temporary filename for a download.

        Format: {symbol}-{data_type.value}-{interval}-{period}.zip

        Parameters
        ----------
        symbol : str
            Trading pair symbol (e.g., 'BTCUSDT')
        period : Period
            Time period to download
        interval : Optional[str]
            Kline interval. Uses self.interval if None.

        Returns
        -------
        str
            Temporary filename (without directory path)
        """
        interval_str = interval if interval is not None else self._interval.value
        return f"{symbol}-{self._data_type.value}-{interval_str}-{period.value}.zip"

    def parse_csv(self, csv_path: Path, symbol: str) -> pd.DataFrame:
        """Parse a klines CSV file into a DataFrame ready for database import."""
        df, _ = self.parse_csv_with_anomalies(csv_path, symbol)
        return df

    def parse_csv_with_anomalies(
        self,
        csv_path: Path,
        symbol: str,
    ) -> tuple[pd.DataFrame, list[ImportAnomaly]]:
        """
        Parse a klines CSV file into a DataFrame ready for database import.

        Handles both headerless and header-containing CSV formats from Binance.
        Automatically detects timestamp format (milliseconds vs microseconds).

        Parameters
        ----------
        csv_path : Path
            Path to the CSV file
        symbol : str
            Trading pair symbol (for adding to DataFrame)

        Returns
        -------
        pd.DataFrame
            Parsed DataFrame with all required columns, deduplicated
        """
        # Detect if CSV has header
        with Path(csv_path).open() as f:
            first_line = f.readline().lower()
            has_header = "open_time" in first_line or "close_time" in first_line

        # Read CSV with or without header
        if has_header:
            df = pd.read_csv(csv_path)
            # Normalize column names to lowercase
            df.columns = df.columns.str.lower()
        else:
            df = pd.read_csv(csv_path, header=None, names=KLINES_COLUMNS)

        # Rename 'count' to 'trades_count' if present (some files use 'count')
        if "count" in df.columns and "trades_count" not in df.columns:
            df = df.rename(columns={"count": "trades_count"})

        # Add missing optional columns with None (some intervals like 4h don't have taker_buy columns)
        for col in ["taker_buy_base_volume", "taker_buy_quote_volume"]:
            if col not in df.columns:
                df[col] = None

        # Add metadata columns
        df["exchange"] = "binance"
        df["symbol"] = symbol
        df["interval"] = self._interval.value

        # Convert timestamp: close_time >= 5e12 means microseconds, else milliseconds
        close_time = df["close_time"]
        divisor = 1_000_000 if (close_time >= 5e12).any() else 1_000

        df["timestamp"] = pd.to_datetime(close_time / divisor, unit="s").dt.ceil("1s")

        # Validate required columns exist before selection
        required_cols = ["close_time", "open", "high", "low", "close", "volume"]
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            raise ValueError(
                f"CSV file {csv_path.name} missing required columns: {missing}. "
                f"Available columns: {list(df.columns)}"
            )

        # Select and order final columns
        df = df[FINAL_COLUMNS]

        anomalies: list[ImportAnomaly] = []

        # Drop duplicates on primary key columns, but make the data issue visible.
        key_columns = ["exchange", "symbol", "interval", "timestamp"]
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
                "Dropped %s duplicate kline rows for %s %s on key %s",
                dropped,
                symbol,
                self._interval.value,
                key_columns,
            )

        return df, anomalies
