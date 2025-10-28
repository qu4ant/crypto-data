"""
Crypto Data Infrastructure Package v4.0.0

Pure data ingestion pipeline for cryptocurrency data.
Downloads from Binance Data Vision and CoinMarketCap → Populates DuckDB.

BREAKING CHANGE (v4.0.0): Multi-exchange schema
- Tables renamed: binance_spot → spot, binance_futures → futures
- New column: exchange (e.g., 'binance')
- Primary key now includes exchange: (exchange, symbol, interval, timestamp)

This package ONLY handles data ingestion. For querying, use DuckDB directly:

    import duckdb
    conn = duckdb.connect('crypto_data.db')
    df = conn.execute("SELECT * FROM spot WHERE exchange = 'binance' AND ...").df()

Public API:
    - CryptoDatabase: Database schema management
    - ingest_universe(): Async download and import CoinMarketCap rankings (parallel)
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

__version__ = "4.0.0"
__author__ = "Crypto Data Contributors"

__all__ = [
    "CryptoDatabase",
    "ingest_binance_async",
    "ingest_universe",
    "sync",
    "get_symbols_from_universe",
    "setup_colored_logging",
    "get_logger",
]