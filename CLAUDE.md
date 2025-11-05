# CLAUDE.md

Developer guidance for Claude Code when working with this repository.

## Project Overview

**Crypto Data v3.0.0** - DuckDB ingestion pipeline for cryptocurrency market data.

**Core Functions:**
- Downloads OHLCV data from Binance Data Vision
- Downloads universe rankings from CoinMarketCap
- Populates DuckDB database with cleaned data
- Provides CLI tool (`scripts/Download_data_universe.py`)

**Critical:** This is an **ingestion-only** package. Users query the database directly using DuckDB SQL.

### Main Components

**Public API** (`src/crypto_data/__init__.py`):
- `sync(db_path, start_date, end_date, top_n, interval, data_types, exclude_tags, exclude_symbols)` - End-to-end workflow
- `ingest_universe(db_path, months, top_n, exclude_tags, exclude_symbols)` - **Async** CoinMarketCap rankings to DuckDB (parallel downloads)
- `ingest_binance_async(db_path, symbols, ...)` - Binance OHLCV to DuckDB (async, 20 concurrent downloads)
- `CryptoDatabase` - Schema management, stats, context manager
- `setup_colored_logging()`, `get_logger()` - Colored logging utilities
- `get_symbols_from_universe(db_path, start_date, end_date, top_n)` - Extract symbols (UNION strategy)

**Internal Clients** (`src/crypto_data/clients/` - not exported):
- `CoinMarketCapClient` - **Async** client with retry logic (429→60s, 500/503→5s, max 3 retries)
- `BinanceDataVisionClientAsync` - Async HTTP downloads with aiohttp

**Scripts**:
- `scripts/Download_data_universe.py` - Example script using sync() function

**Data Types**:
- Binance: Spot/Futures Klines (OHLCV), default 5m interval
- CoinMarketCap: Universe rankings (top N by market cap), stablecoin flags
- Not included: Premium Index, Funding Rates

## Database Schema (v4.0.0 - Multi-Exchange)

**Single DuckDB file**: `crypto_data.db`

**Tables:**
1. `crypto_universe` - CoinMarketCap rankings
   - Primary key: `(date, symbol)`, Index: `(date, rank)`
   - Columns: date, symbol, rank, market_cap, categories
   - Symbols are base assets (BTC not BTCUSDT), interval-independent
   - Exchange-agnostic (rankings are global)

2. `spot`, `futures` - OHLCV data (MULTI-EXCHANGE)
   - **Primary key**: `(exchange, symbol, interval, timestamp)` - exchange is now part of PK
   - **Index**: `(exchange, symbol, interval, timestamp)`
   - **New column**: `exchange VARCHAR NOT NULL` - 'binance', 'bybit', 'kraken', etc.
   - Other columns: symbol, interval, timestamp, open, high, low, close, volume, quote_volume, trades_count, taker_buy_*
   - Interval stored as column (5m, 1h, 4h, 1d in same table)
   - **Current status**: Only Binance implemented (exchange='binance')

## Design Decisions

**Multi-Exchange Architecture (v4.0.0)**: Schema designed to support multiple exchanges
- Tables `spot` and `futures` include `exchange` column in primary key
- Allows future expansion to Bybit, Kraken, Coinbase, etc. without schema changes
- All queries filter by `WHERE exchange = 'binance'` currently
- Benefits: Cross-exchange analysis, arbitrage detection, data redundancy

**Rebrands as Separate Symbols**: MATIC→POL, RNDR→RENDER treated as different coins
- Rationale: Trading interruptions (gaps), separate files in data source, different liquidity
- Users: UNION queries to combine across rebrand periods

**UNION Strategy**: `get_symbols_from_universe()` returns ALL symbols that appeared in top N at ANY point
- Returns ~120-150 symbols for top 100 over 12 months (captures entries/exits)
- Avoids survivorship bias, captures failed/delisted coins
- Alternative INTERSECTION strategy not implemented (would miss market dynamics)

**Explicit Parameters Only**: Universe filtering follows "explicit is better than implicit"
- `sync()` and `ingest_universe()` require `exclude_tags` and `exclude_symbols` as direct parameters
- No config files, no hidden dependencies - everything is explicit and testable
- Benefits: Better testability, dependency injection, autodocumented API, zero hidden config
- Default: empty lists (no exclusions) if not provided
- Note: `ingest_universe()` is async - use `asyncio.run()` or await it within async context

## Common Commands

**Example Script**:
```bash
python scripts/Download_data_universe.py  # Runs complete sync workflow
```

**Python API**:
```python
from crypto_data import sync, ingest_universe, ingest_binance_async, setup_colored_logging

setup_colored_logging()  # Optional but recommended

# Complete workflow (one function call) - Explicit parameters
sync(
    db_path='crypto_data.db',
    start_date='2024-01-01',
    end_date='2024-12-31',
    top_n=100,
    interval='5m',
    data_types=['spot', 'futures'],
    exclude_tags=['stablecoin', 'wrapped-tokens', 'privacy'],
    exclude_symbols=['LUNA', 'FTT', 'UST']
)

# Without exclusions (default = empty lists)
sync(
    db_path='crypto_data.db',
    start_date='2024-01-01',
    end_date='2024-12-31',
    top_n=100,
    interval='5m',
    data_types=['spot', 'futures']
)

# Step-by-step: ingest_universe() → get_symbols_from_universe() → ingest_binance_async()
import asyncio

# 1. Ingest universe for multiple months (async, parallel)
asyncio.run(ingest_universe(
    db_path='crypto_data.db',
    months=['2024-01', '2024-02', '2024-03'],  # List of months
    top_n=100,
    exclude_tags=['stablecoin'],
    exclude_symbols=['LUNA', 'FTT']
))

# 2. Extract symbols (UNION strategy)
symbols = get_symbols_from_universe('crypto_data.db', '2024-01-01', '2024-12-31', 100)

# 3. Ingest Binance data (async)
ingest_binance_async('crypto_data.db', symbols, '2024-01-01', '2024-12-31', interval='5m')
```

**Testing**:
```bash
pytest tests/ -v  # All tests
pytest tests/test_database_basic.py -v  # Smoke tests only
pytest tests/ --cov=crypto_data --cov-report=html  # With coverage
pytest tests/schemas/ -v --tb=short  # Schema validation tests
```

**Installation**: `pip install -e .` or `pip install -e ".[dev]"`

## Data Validation (Pandera v2)

**Post-Import Quality Checks** using Pandera schemas:

```bash
# Run quality checks on database
python scripts/check_data_quality_pandera.py --db-path crypto_data.db

# Check specific table
python scripts/check_data_quality_pandera.py --db-path crypto_data.db --table spot --verbose

# Use sampling for large tables (faster)
python scripts/check_data_quality_pandera.py --db-path crypto_data.db --sample --sample-size 100000

# Run schema validation tests
pytest tests/schemas/ -v
```

**Available Schemas**:
- **OHLCV** (spot/futures): `crypto_data.schemas.OHLCV_SCHEMA`
  - OHLC relationship checks (high ≥ low, high ≥ open/close, low ≤ open/close)
  - Non-negative prices/volumes
  - Statistical checks (price continuity, volume outliers)
- **Open Interest**: `crypto_data.schemas.OPEN_INTEREST_SCHEMA`
  - Non-negative, non-null validation
  - Outlier detection (Z-score method)
- **Funding Rates**: `crypto_data.schemas.FUNDING_RATES_SCHEMA`
  - Extreme value warnings (>±1%)
  - Distribution checks (mean, std)
- **Universe**: `crypto_data.schemas.UNIVERSE_SCHEMA`
  - Rank consistency (no gaps, no duplicates)
  - Non-negative market_cap

**Validation Features**:
- ✅ Vectorized checks (8-12x faster than SQL)
- ✅ OHLC relationship validation
- ✅ Statistical hypothesis testing (outliers, continuity, distributions)
- ✅ Better error messages (Pandera shows exact failures + examples)
- ✅ Memory-efficient (optional sampling for large tables)
- ✅ Timestamp gap detection (kept from original SQL approach)

**Python Usage**:
```python
from crypto_data.schemas import OHLCV_SCHEMA, validate_ohlcv_dataframe

# Validate DataFrame
import pandas as pd
df = pd.read_sql("SELECT * FROM spot WHERE symbol = 'BTCUSDT' LIMIT 1000", conn)

# Option 1: Use schema directly
OHLCV_SCHEMA.validate(df)  # Raises SchemaError if invalid

# Option 2: Use validation function
validated_df = validate_ohlcv_dataframe(df, strict=True)
```

**What Gets Checked**:
- Type validation (correct data types)
- Range checks (non-negative, non-null)
- OHLC relationships (high ≥ low, etc.)
- Statistical outliers (price jumps, volume spikes)
- Timestamp gaps (using SQL queries)
- Rank consistency (universe data)
- Primary key uniqueness

## Code Architecture

```
src/crypto_data/
├── __init__.py              # Public API (7 exports)
├── database.py              # CryptoDatabase class
├── ingestion.py             # Unified ingestion (Universe + Binance async)
├── logging_utils.py         # Colored logging
├── clients/                 # INTERNAL (not exported)
│   ├── coinmarketcap.py     # CMC API client
│   └── binance_vision_async.py  # Binance async HTTP client
└── utils/                   # INTERNAL (except get_symbols_from_universe)
    ├── database.py          # import_to_duckdb, data_exists, etc.
    ├── dates.py             # generate_month_list
    ├── formatting.py        # format_availability_bar, format_file_size
    ├── ingestion_helpers.py # Shared ingestion helpers (stats, logging, result processing)
    └── symbols.py           # get_symbols_from_universe (PUBLIC)

scripts/: Download_data_universe.py
tests/: 6 test modules (database, universe, binance, 1000-prefix, timestamps, helpers)
```

## Workflow & Data Flow

**sync() function** (end-to-end):
1. **Universe**: Generate month list → Call `ingest_universe(months=[...])` (async, parallel downloads) → Atomic transaction (DELETE + INSERT per month)
2. **Symbol Extraction**: `get_symbols_from_universe()` - UNION strategy, adds USDT suffix
3. **Binance**: Async downloads (20 concurrent) → Auto-detect timestamp format (ms vs μs) → Auto-detect CSV headers → Import to DuckDB

**ingest_universe() function** (async batch):
- Takes a list of months (e.g., `['2024-01', '2024-02']`)
- Downloads snapshots in parallel (max 5 concurrent by default)
- Each month: Fetch from CoinMarketCap API → Filter by tags/symbols → DELETE + INSERT (atomic)
- Logs errors and continues (doesn't raise on API failures)

**Key Features**:
- **Transaction safety**: Each symbol+data_type import wrapped in BEGIN/COMMIT/ROLLBACK (atomic imports)
- **1000-prefix auto-discovery**: PEPEUSDT futures 404 → retry as 1000PEPEUSDT → store as PEPEUSDT (normalized)
- **Gap detection**: `failure_threshold=3` stops after N consecutive 404s (delisting detection)
- **Auto-retry**: 429→60s, 500/503→5s, max 3 retries
- **Session cache**: `_ticker_mappings` caches 1000-prefix mappings
- **Visual progress**: Progress bars show data availability with coverage %

## SQL Query Notes

**Case-Sensitivity**: DuckDB's `LIKE` is case-sensitive. Use `LOWER(categories) NOT LIKE '%stablecoin%'` or `ILIKE` for filtering.

**Universe JOINs**: Join crypto_universe with binance tables using `u.symbol || 'USDT' = s.symbol` and `u.date = DATE_TRUNC('day', s.timestamp)`

**Users write SQL directly** - this package does NOT provide query methods.

## Environment & Dependencies

**Conda**: Always use `/Users/user/miniconda3/envs/quant/`
- Activate: `source /Users/user/miniconda3/bin/activate quant`

**Dependencies**: duckdb, requests, pandas, aiohttp
**Dev**: pytest, pytest-cov

## Coding Standards

- Vectorize operations (pandas/numpy), use explicit type hints
- pytest for all tests
- Always use conda env `quant`
- **Philosophy**: Simplicity over features, SQL over abstraction, expose database directly

## Known Data Quality Issues (Auto-Handled)

**1. Timestamp Format Changes (2024→2025)**:
- Problem: 2024 uses milliseconds (13 digits), 2025 uses microseconds (16 digits)
- Solution: Auto-detection based on threshold `>= 5e12` → μs, `< 5e12` → ms (in `_import_to_duckdb`)

**2. Inconsistent CSV Headers**:
- Problem: Some files have headers, others don't
- Solution: Auto-detect by analyzing first line (keyword check + numeric/text detection)

**3. 1000-Prefix Tokens (PEPE, SHIB, BONK)**:
- Problem: Futures use `1000PEPEUSDT`, spot uses `PEPEUSDT`
- Solution: Auto-retry with 1000-prefix on 404, cache mappings in `_ticker_mappings`, **always store as original symbol** (PEPEUSDT)
- Location: `ingestion.py` (cache at line 46, auto-retry logic in ingest_binance_async)

**4. Gap Detection (Delisting)**:
- Problem: FTT delisted Nov 2022, don't download years of 404s
- Solution: `failure_threshold=3` (default) stops after N consecutive 404s within a session
- Note: No persistent blacklisting between runs - delisted symbols will retry on next ingestion
- Caveat: Set `failure_threshold=0` to disable gap detection completely
- Ignores leading 404s (before token launch)

**Testing**: ✅ 2024/2025 data, ✅ header variations, ✅ 1000-prefix auto-discovery, ✅ symbol normalization

## Development Notes

**v3.0.0 vs v2.0.0**: Complete rewrite. Removed Parquet storage, reader/loader classes. Added DuckDB-only storage. ~52% less code.

**Recent improvements (CoinMarketCap async migration)**:
- `CoinMarketCapClient` migrated to async (aiohttp instead of requests)
- `ingest_universe()` renamed from `ingest_universe_batch_async()` - now async with parallel downloads
- API simplified: One async function instead of two (sync + async versions)
- All 174 tests passing, no coroutine warnings

**When adding features**:
- Scope: Ingestion ONLY (not querying)
- Keep it simple, don't add loaders/readers/query helpers
- SQL-first: Expose database, don't hide it
- Add smoke tests for new functionality

**Philosophy**: This package does ONE thing - populates a DuckDB database. Users query directly using SQL.
