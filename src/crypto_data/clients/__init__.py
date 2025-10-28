"""
Data Source Clients (Internal)

IMPORTANT: These are internal implementation details, not part of the public API.
Users should use the ingestion functions instead:
    - crypto_data.ingest_universe()
    - crypto_data.ingest_binance_async()

Client classes handle API communication, retries, and network operations.
They are used internally by the ingestion module.
"""

from .coinmarketcap import CoinMarketCapClient
from .binance_vision_async import BinanceDataVisionClientAsync

# Internal-only exports (used by ingestion.py, not part of public API)
__all__ = ['CoinMarketCapClient', 'BinanceDataVisionClientAsync']
