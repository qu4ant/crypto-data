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

__all__ = [
    'DataTypeStrategy',
    'DownloadResult',
    'Period',
]
