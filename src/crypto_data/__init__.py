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
    - populate_database(): Complete workflow (universe → binance, uses parallel downloads)
    - setup_colored_logging(), get_logger(): Logging utilities
    - Pandera schemas: OHLCV_SCHEMA, OPEN_INTEREST_SCHEMA, FUNDING_RATES_SCHEMA, UNIVERSE_SCHEMA
    - Validation functions: validate_ohlcv_dataframe, validate_open_interest_dataframe, etc.

Note: Client classes (CoinMarketCapClient, BinanceDataVisionClient) are internal
implementation details and not part of the public API.
"""

from .database import CryptoDatabase
from .ingestion import ingest_universe, populate_database, ingest_binance_async
from .logging_utils import setup_colored_logging, get_logger
from .utils.symbols import get_symbols_from_universe

# Import Pandera schemas and validation functions
from .schemas import (
    OHLCV_SCHEMA,
    OHLCV_STATISTICAL_SCHEMA,
    SPOT_SCHEMA,
    FUTURES_SCHEMA,
    OPEN_INTEREST_SCHEMA,
    OPEN_INTEREST_STATISTICAL_SCHEMA,
    FUNDING_RATES_SCHEMA,
    FUNDING_RATES_STATISTICAL_SCHEMA,
    UNIVERSE_SCHEMA,
    validate_ohlcv_dataframe,
    validate_ohlcv_statistical,
    validate_open_interest_dataframe,
    validate_open_interest_statistical,
    validate_funding_rates_dataframe,
    validate_funding_rates_statistical,
    validate_universe_dataframe
)

__version__ = "4.0.0"
__author__ = "Crypto Data Contributors"

__all__ = [
    # Database & Ingestion
    "CryptoDatabase",
    "ingest_binance_async",
    "ingest_universe",
    "populate_database",
    "get_symbols_from_universe",

    # Logging
    "setup_colored_logging",
    "get_logger",

    # Pandera Schemas
    "OHLCV_SCHEMA",
    "OHLCV_STATISTICAL_SCHEMA",
    "SPOT_SCHEMA",
    "FUTURES_SCHEMA",
    "OPEN_INTEREST_SCHEMA",
    "OPEN_INTEREST_STATISTICAL_SCHEMA",
    "FUNDING_RATES_SCHEMA",
    "FUNDING_RATES_STATISTICAL_SCHEMA",
    "UNIVERSE_SCHEMA",

    # Validation Functions
    "validate_ohlcv_dataframe",
    "validate_ohlcv_statistical",
    "validate_open_interest_dataframe",
    "validate_open_interest_statistical",
    "validate_funding_rates_dataframe",
    "validate_funding_rates_statistical",
    "validate_universe_dataframe",
]