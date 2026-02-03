"""
Exchange Clients

Pluggable exchange clients for downloading data from different sources.
Each exchange implements the ExchangeClient interface.
"""

from crypto_data.exchanges.base import ExchangeClient

__all__ = [
    'ExchangeClient',
]
