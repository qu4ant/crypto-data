"""
Enumerations for crypto-data package.

Defines type-safe enums for Binance data types and intervals.
"""

from enum import Enum


class DataType(str, Enum):
    """
    Supported data types for Binance ingestion.

    Attributes
    ----------
    SPOT : str
        Spot market OHLCV data (klines)
    FUTURES : str
        Futures market OHLCV data (klines)
    OPEN_INTEREST : str
        Futures open interest metrics (daily)
    FUNDING_RATES : str
        Futures funding rate data (monthly)

    Example
    -------
    >>> from crypto_data import DataType
    >>> data_types = [DataType.SPOT, DataType.FUTURES]
    >>> print(data_types[0].value)  # 'spot'
    """

    SPOT = "spot"
    FUTURES = "futures"
    OPEN_INTEREST = "open_interest"
    FUNDING_RATES = "funding_rates"


class Interval(str, Enum):
    """
    Binance kline intervals.

    Supported intervals for spot and futures OHLCV data.
    Note: Not all intervals are available for all symbols/periods.

    Attributes
    ----------
    MIN_1 : str
        1 minute interval
    MIN_5 : str
        5 minute interval
    MIN_15 : str
        15 minute interval
    MIN_30 : str
        30 minute interval
    HOUR_1 : str
        1 hour interval
    HOUR_2 : str
        2 hour interval
    HOUR_4 : str
        4 hour interval
    HOUR_6 : str
        6 hour interval
    HOUR_8 : str
        8 hour interval
    HOUR_12 : str
        12 hour interval
    DAY_1 : str
        1 day interval
    DAY_3 : str
        3 day interval
    WEEK_1 : str
        1 week interval
    MONTH_1 : str
        1 month interval

    Example
    -------
    >>> from crypto_data import Interval
    >>> interval = Interval.MIN_5
    >>> print(interval.value)  # '5m'
    """

    MIN_1 = "1m"
    MIN_5 = "5m"
    MIN_15 = "15m"
    MIN_30 = "30m"
    HOUR_1 = "1h"
    HOUR_2 = "2h"
    HOUR_4 = "4h"
    HOUR_6 = "6h"
    HOUR_8 = "8h"
    HOUR_12 = "12h"
    DAY_1 = "1d"
    DAY_3 = "3d"
    WEEK_1 = "1w"
    MONTH_1 = "1M"
