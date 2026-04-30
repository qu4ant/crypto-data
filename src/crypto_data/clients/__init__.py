"""
Data Source Clients (Internal)

IMPORTANT: These are internal implementation details, not part of the public API.
Users should use the ingestion functions instead:
    - crypto_data.update_coinmarketcap_universe()
    - crypto_data.update_binance_market_data()

Client classes handle API communication, retries, and network operations.
They are used internally by the database builder and pipeline modules.
"""

from .coinmarketcap import CoinMarketCapClient

# Internal-only exports (used by database_builder.py, not part of public API)
__all__ = ["CoinMarketCapClient"]
