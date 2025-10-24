"""
Data Ingestion Module

Handles downloading and importing data from various sources into DuckDB.
"""

from .binance import ingest_binance
from .coinmarketcap import ingest_universe

__all__ = ['ingest_binance', 'ingest_universe']
