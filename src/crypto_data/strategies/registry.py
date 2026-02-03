"""
Strategy registry for data type strategies.

Provides factory methods to create strategies from DataType enums.
"""

from typing import Optional

from crypto_data.enums import DataType, Interval
from crypto_data.strategies.base import DataTypeStrategy
from crypto_data.strategies.funding_rates import FundingRatesStrategy
from crypto_data.strategies.klines import KlinesStrategy
from crypto_data.strategies.open_interest import OpenInterestStrategy


def get_strategy(
    data_type: DataType,
    interval: Optional[Interval] = None
) -> DataTypeStrategy:
    """
    Get the appropriate strategy for a data type.

    Parameters
    ----------
    data_type : DataType
        The data type to get a strategy for
    interval : Interval, optional
        Required for SPOT and FUTURES data types

    Returns
    -------
    DataTypeStrategy
        The appropriate strategy instance

    Raises
    ------
    ValueError
        If data type is unknown or interval is missing for klines

    Examples
    --------
    >>> from crypto_data import DataType, Interval
    >>> from crypto_data.strategies import get_strategy
    >>> strategy = get_strategy(DataType.SPOT, Interval.MIN_5)
    >>> strategy.table_name
    'spot'

    >>> strategy = get_strategy(DataType.OPEN_INTEREST)
    >>> strategy.table_name
    'open_interest'
    """
    if data_type == DataType.SPOT:
        if interval is None:
            raise ValueError("interval is required for SPOT data type")
        return KlinesStrategy(DataType.SPOT, interval)

    elif data_type == DataType.FUTURES:
        if interval is None:
            raise ValueError("interval is required for FUTURES data type")
        return KlinesStrategy(DataType.FUTURES, interval)

    elif data_type == DataType.OPEN_INTEREST:
        return OpenInterestStrategy()

    elif data_type == DataType.FUNDING_RATES:
        return FundingRatesStrategy()

    else:
        raise ValueError(f"Unknown data type: {data_type}")
