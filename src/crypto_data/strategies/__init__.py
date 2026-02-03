"""
Data Type Strategies

Pluggable strategies for different data types (klines, open_interest, funding_rates).
Each strategy encapsulates:
- Period generation (months vs days)
- Download logic
- Import logic
- Validation schema
"""

from crypto_data.strategies.base import DataTypeStrategy, DownloadResult, Period
from crypto_data.strategies.funding_rates import FundingRatesStrategy
from crypto_data.strategies.klines import KlinesStrategy
from crypto_data.strategies.open_interest import OpenInterestStrategy
from crypto_data.strategies.registry import get_strategy

__all__ = [
    'DataTypeStrategy',
    'DownloadResult',
    'Period',
    'FundingRatesStrategy',
    'KlinesStrategy',
    'OpenInterestStrategy',
    'get_strategy',
]
