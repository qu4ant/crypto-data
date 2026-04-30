"""
Crypto Data Infrastructure Package v6.0.0

Pure data ingestion pipeline for cryptocurrency data.
Downloads from Binance Data Vision and CoinMarketCap → Populates DuckDB.

BREAKING CHANGE (v6.0.0): Enriched CMC universe schema
- crypto_universe primary key: (provider, provider_id, date)
- 14 columns: provider, provider_id, date, symbol, name, slug, rank,
  market_cap, fully_diluted_market_cap, circulating_supply, max_supply,
  tags, platform, date_added
- 'categories' column is renamed to 'tags' (matches CMC API field)
- Pre-v6 databases must be deleted and re-ingested

BREAKING CHANGE (v5.0.0): Type-safe enums
- All data type and interval parameters now require enums instead of strings
- Use DataType.SPOT, DataType.FUTURES, Interval.MIN_5, etc.

BREAKING CHANGE (v4.0.0): Explicit exchange provenance column
- Tables renamed: binance_spot → spot, binance_futures → futures
- New column: exchange (always 'binance')
- Primary key now includes exchange: (exchange, symbol, interval, timestamp)

This package ONLY handles data ingestion. For querying, use DuckDB directly:

    import duckdb
    conn = duckdb.connect('crypto_data.db')
    df = conn.execute("SELECT * FROM spot WHERE exchange = 'binance' AND ...").df()

Public API:
    - CryptoDatabase: Database schema management
    - update_coinmarketcap_universe(): Async download and import CoinMarketCap rankings
    - update_binance_market_data(): Download and import Binance market data
    - repair_binance_gaps(): Fill Binance Data Vision gaps from public REST
    - create_binance_database(): Complete workflow (universe -> Binance)
    - setup_colored_logging(), get_logger(): Logging utilities
    - Pandera schemas: OHLCV_SCHEMA, OPEN_INTEREST_SCHEMA, FUNDING_RATES_SCHEMA, UNIVERSE_SCHEMA
    - Validation functions: validate_ohlcv_dataframe, validate_open_interest_dataframe, etc.

Note: downloader/importer/client/dataset classes are internal implementation
details and are not re-exported from the package root.
"""

from .binance_pipeline import update_binance_market_data
from .binance_repair import RepairReport, UnrecoverableGap, repair_binance_gaps
from .database import CryptoDatabase
from .database_builder import create_binance_database, update_coinmarketcap_universe
from .enums import DataType, Interval
from .logging_utils import get_logger, setup_colored_logging

# Import Pandera schemas and validation functions
from .schemas import (
    FUNDING_RATES_SCHEMA,
    FUNDING_RATES_STATISTICAL_SCHEMA,
    FUTURES_SCHEMA,
    OHLCV_SCHEMA,
    OHLCV_STATISTICAL_SCHEMA,
    OPEN_INTEREST_SCHEMA,
    OPEN_INTEREST_STATISTICAL_SCHEMA,
    SPOT_SCHEMA,
    UNIVERSE_SCHEMA,
    validate_funding_rates_dataframe,
    validate_funding_rates_statistical,
    validate_ohlcv_dataframe,
    validate_ohlcv_statistical,
    validate_open_interest_dataframe,
    validate_open_interest_statistical,
    validate_universe_dataframe,
)
from .universe_filters import DEFAULT_UNIVERSE_EXCLUDE_SYMBOLS, DEFAULT_UNIVERSE_EXCLUDE_TAGS
from .utils.symbols import get_binance_symbols_from_universe

__version__ = "6.0.0"
__author__ = "Crypto Data Contributors"

__all__ = [
    # Database & Ingestion
    "CryptoDatabase",
    "create_binance_database",
    "update_binance_market_data",
    "update_coinmarketcap_universe",
    "repair_binance_gaps",
    "get_binance_symbols_from_universe",
    "DEFAULT_UNIVERSE_EXCLUDE_TAGS",
    "DEFAULT_UNIVERSE_EXCLUDE_SYMBOLS",
    "RepairReport",
    "UnrecoverableGap",
    # Logging
    "setup_colored_logging",
    "get_logger",
    # Enums
    "DataType",
    "Interval",
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
