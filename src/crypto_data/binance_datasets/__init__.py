"""
Binance dataset handlers.

Dataset handlers for Binance Data Vision files.
Each handler encapsulates:
- Period generation (months vs days)
- Download logic
- Import logic
- Validation schema
"""

from crypto_data.binance_datasets.base import BinanceDatasetStrategy, DownloadResult, Period
from crypto_data.binance_datasets.funding_rates import BinanceFundingRatesDataset
from crypto_data.binance_datasets.klines import BinanceKlinesDataset
from crypto_data.binance_datasets.open_interest import BinanceOpenInterestDataset
from crypto_data.binance_datasets.registry import get_binance_dataset_strategy

__all__ = [
    "BinanceDatasetStrategy",
    "BinanceFundingRatesDataset",
    "BinanceKlinesDataset",
    "BinanceOpenInterestDataset",
    "DownloadResult",
    "Period",
    "get_binance_dataset_strategy",
]
