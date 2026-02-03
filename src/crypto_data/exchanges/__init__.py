"""
Exchange Clients

Pluggable exchange clients for downloading data from different sources.
Each exchange implements the ExchangeClient interface.
"""

from crypto_data.exchanges.base import ExchangeClient
from crypto_data.exchanges.binance import BinanceExchange

__all__ = [
    'ExchangeClient',
    'BinanceExchange',
]
