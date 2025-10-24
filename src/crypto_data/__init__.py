"""
Crypto Data Infrastructure Package v3.0.0

Pure data ingestion pipeline for cryptocurrency data.
Downloads from Binance Data Vision and CoinMarketCap → Populates DuckDB.

This package ONLY handles data ingestion. For querying, use DuckDB directly:

    import duckdb
    conn = duckdb.connect('crypto_data.db')
    df = conn.execute("SELECT * FROM binance_spot WHERE ...").df()
"""

from .database import CryptoDatabase
from .ingestion import ingest_binance, ingest_universe

__version__ = "3.0.0"
__author__ = "Quantura_labs"

__all__ = [
    "CryptoDatabase",
    "ingest_binance",
    "ingest_universe",
]