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
- `sync(db_path, start_date, end_date, top_n, interval, data_types)` - End-to-end workflow
- `ingest_universe(db_path, date, top_n)` - CoinMarketCap rankings to DuckDB
- `ingest_binance_async(db_path, symbols, ...)` - Binance OHLCV to DuckDB (async, 20 concurrent downloads)
- `CryptoDatabase` - Schema management, stats, context manager
- `setup_colored_logging()`, `get_logger()` - Colored logging utilities
- `get_symbols_from_universe(db_path, start_date, end_date, top_n)` - Extract symbols (UNION strategy)

**Internal Clients** (`src/crypto_data/clients/` - not exported):
- `CoinMarketCapClient` - Retry logic (429→60s, 500/503→5s, max 3 retries)
- `BinanceDataVisionClientAsync` - Async HTTP downloads with aiohttp

**Scripts**:
- `scripts/Download_data_universe.py` - Example script using sync() function

**Data Types**:
- Binance: Spot/Futures Klines (OHLCV), default 5m interval
- CoinMarketCap: Universe rankings (top N by market cap), stablecoin flags
- Not included: Premium Index, Funding Rates

## Database Schema

**Single DuckDB file**: `crypto_data.db`

**Tables:**
1. `crypto_universe` - CoinMarketCap rankings
   - Primary key: `(date, symbol)`, Index: `(date, rank)`
   - Columns: date, symbol, rank, market_cap, categories
   - Symbols are base assets (BTC not BTCUSDT), interval-independent

2. `binance_spot`, `binance_futures` - OHLCV data
   - Primary key: `(symbol, interval, timestamp)`, Index: `(symbol, interval, timestamp)`
   - Columns: symbol, interval, timestamp, open, high, low, close, volume, quote_volume, trades_count, taker_buy_*
   - Interval stored as column (5m, 1h, 4h, 1d in same table)

## Design Decisions

**Rebrands as Separate Symbols**: MATIC→POL, RNDR→RENDER treated as different coins
- Rationale: Trading interruptions (gaps), separate files in data source, different liquidity
- Users: UNION queries to combine across rebrand periods

**UNION Strategy**: `get_symbols_from_universe()` returns ALL symbols that appeared in top N at ANY point
- Returns ~120-150 symbols for top 100 over 12 months (captures entries/exits)
- Avoids survivorship bias, captures failed/delisted coins
- Alternative INTERSECTION strategy not implemented (would miss market dynamics)

## Common Commands

**Example Script**:
```bash
python scripts/Download_data_universe.py  # Runs complete sync workflow
```

**Python API**:
```python
from crypto_data import sync, ingest_universe, ingest_binance_async, setup_colored_logging

setup_colored_logging()  # Optional but recommended

# Complete workflow (one function call)
sync(db_path='crypto_data.db', start_date='2024-01-01', end_date='2024-12-31',
     top_n=100, interval='5m', data_types=['spot', 'futures'])

# Step-by-step: ingest_universe() → get_symbols_from_universe() → ingest_binance_async()
```

**Testing**:
```bash
pytest tests/ -v  # All tests
pytest tests/test_database_basic.py -v  # Smoke tests only
pytest tests/ --cov=crypto_data --cov-report=html  # With coverage
```

**Installation**: `pip install -e .` or `pip install -e ".[dev]"`

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
    ├── config.py            # Config loading
    ├── database.py          # import_to_duckdb, data_exists, etc.
    ├── dates.py             # generate_month_list
    ├── formatting.py        # format_availability_bar, format_file_size
    ├── ingestion_helpers.py # Shared ingestion helpers (stats, logging, result processing)
    └── symbols.py           # get_symbols_from_universe (PUBLIC)

scripts/: Download_data_universe.py
tests/: 7 test modules (database, universe, binance, config, 1000-prefix, timestamps)
config/: binance_config.yaml, universe_config.yaml
```

## Workflow & Data Flow

**sync() function** (end-to-end):
1. **Universe**: Generate month list → Call `ingest_universe()` for each month → Atomic transaction (DELETE + INSERT)
2. **Symbol Extraction**: `get_symbols_from_universe()` - UNION strategy, adds USDT suffix
3. **Binance**: Async downloads (20 concurrent) → Auto-detect timestamp format (ms vs μs) → Auto-detect CSV headers → Import to DuckDB

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

**Conda**: Always use `/Users/guillaumetonnerre/miniconda3/envs/quant/`
- Activate: `source /Users/guillaumetonnerre/miniconda3/bin/activate quant`

**Dependencies**: duckdb, requests, beautifulsoup4, PyYAML, pandas, aiohttp
**Dev**: pytest, pytest-cov

**Config files**:
- `config/binance_config.yaml` - Binance settings (database, symbols, data_types, interval, dates)
- `config/universe_config.yaml` - Universe filtering (excluded tags/symbols)

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

**When adding features**:
- Scope: Ingestion ONLY (not querying)
- Keep it simple, don't add loaders/readers/query helpers
- SQL-first: Expose database, don't hide it
- Add smoke tests for new functionality

**Philosophy**: This package does ONE thing - populates a DuckDB database. Users query directly using SQL.
