"""
Crypto Data Infrastructure Package v3.0.0

Pure data ingestion pipeline for cryptocurrency data.
Downloads from Binance Data Vision and CoinMarketCap → Populates DuckDB.

This package ONLY handles data ingestion. For querying, use DuckDB directly:

    import duckdb
    conn = duckdb.connect('crypto_data.db')
    df = conn.execute("SELECT * FROM binance_spot WHERE ...").df()

Public API:
    - CryptoDatabase: Database schema management
    - ingest_universe(): Download and import CoinMarketCap rankings
    - ingest_binance_async(): Download and import Binance data (parallel, 5-10x faster)
    - sync(): Complete workflow (universe → binance, uses parallel downloads)
    - setup_colored_logging(), get_logger(): Logging utilities

Note: Client classes (CoinMarketCapClient, BinanceDataVisionClient) are internal
implementation details and not part of the public API.
"""

from .database import CryptoDatabase
from .ingestion import ingest_universe, sync, ingest_binance_async
from .logging_utils import setup_colored_logging, get_logger
from .utils.symbols import get_symbols_from_universe

__version__ = "3.0.0"
__author__ = "Quantura_labs"

__all__ = [
    "CryptoDatabase",
    "ingest_binance_async",
    "ingest_universe",
    "sync",
    "get_symbols_from_universe",
    "setup_colored_logging",
    "get_logger",
]