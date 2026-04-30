"""
Binance dataset registry.

Provides the factory that maps DataType enums to Binance dataset classes.
"""

from crypto_data.binance_datasets.base import BinanceDatasetStrategy
from crypto_data.binance_datasets.funding_rates import BinanceFundingRatesDataset
from crypto_data.binance_datasets.klines import BinanceKlinesDataset
from crypto_data.binance_datasets.open_interest import BinanceOpenInterestDataset
from crypto_data.enums import DataType, Interval


def get_binance_dataset_strategy(
    data_type: DataType, interval: Interval | None = None
) -> BinanceDatasetStrategy:
    """
    Get the Binance dataset handler for a data type.

    Parameters
    ----------
    data_type : DataType
        The data type to get a Binance dataset handler for
    interval : Interval, optional
        Required for SPOT and FUTURES data types

    Returns
    -------
    BinanceDatasetStrategy
        The appropriate dataset instance

    Raises
    ------
    ValueError
        If data type is unknown or interval is missing for klines

    Examples
    --------
    >>> from crypto_data import DataType, Interval
    >>> from crypto_data.binance_datasets import get_binance_dataset_strategy
    >>> dataset = get_binance_dataset_strategy(DataType.SPOT, Interval.MIN_5)
    >>> dataset.table_name
    'spot'

    >>> dataset = get_binance_dataset_strategy(DataType.OPEN_INTEREST)
    >>> dataset.table_name
    'open_interest'
    """
    if data_type == DataType.SPOT:
        if interval is None:
            raise ValueError("interval is required for SPOT data type")
        return BinanceKlinesDataset(DataType.SPOT, interval)

    if data_type == DataType.FUTURES:
        if interval is None:
            raise ValueError("interval is required for FUTURES data type")
        return BinanceKlinesDataset(DataType.FUTURES, interval)

    if data_type == DataType.OPEN_INTEREST:
        return BinanceOpenInterestDataset()

    if data_type == DataType.FUNDING_RATES:
        return BinanceFundingRatesDataset()

    raise ValueError(f"Unknown data type: {data_type}")
