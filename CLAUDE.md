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
- `populate_database(db_path, start_date, end_date, top_n, interval, data_types, exclude_tags, exclude_symbols)` - End-to-end workflow
- `ingest_universe(db_path, months, top_n, exclude_tags, exclude_symbols)` - **Async** CoinMarketCap rankings to DuckDB (parallel downloads)
- `ingest_binance_async(db_path, symbols, ...)` - Binance OHLCV to DuckDB (async, 20 concurrent downloads)
- `CryptoDatabase` - Schema management, stats, context manager
- `setup_colored_logging()`, `get_logger()` - Colored logging utilities
- `get_symbols_from_universe(db_path, start_date, end_date, top_n)` - Extract symbols (UNION strategy)
- **Enums** (v5.0.0+): `DataType`, `Interval`, `Exchange` - Type-safe enumerations

**Internal Clients** (`src/crypto_data/clients/` - not exported):
- `CoinMarketCapClient` - **Async** client with retry logic (429→60s, 500/503→5s, max 3 retries)
- `BinanceDataVisionClientAsync` - Async HTTP downloads with aiohttp

**Scripts**:
- `scripts/Download_data_universe.py` - Example script using populate_database() function

**Data Types**:
- Binance: Spot/Futures Klines (OHLCV), Open Interest, Funding Rates
- CoinMarketCap: Universe rankings (top N by market cap), stablecoin flags

### Type-Safe Enums (v5.0.0+)

**Breaking Change**: All data type and interval parameters now require enums instead of strings.

**Available Enums**:
```python
from crypto_data import DataType, Interval, Exchange

# DataType enum
DataType.SPOT           # 'spot'
DataType.FUTURES        # 'futures'
DataType.OPEN_INTEREST  # 'open_interest'
DataType.FUNDING_RATES  # 'funding_rates'

# Interval enum
Interval.MIN_1    # '1m'
Interval.MIN_5    # '5m'
Interval.MIN_15   # '15m'
Interval.MIN_30   # '30m'
Interval.HOUR_1   # '1h'
Interval.HOUR_2   # '2h'
Interval.HOUR_4   # '4h'
Interval.HOUR_6   # '6h'
Interval.HOUR_8   # '8h'
Interval.HOUR_12  # '12h'
Interval.DAY_1    # '1d'
Interval.DAY_3    # '3d'
Interval.WEEK_1   # '1w'
Interval.MONTH_1  # '1M'

# Exchange enum (future expansion)
Exchange.BINANCE   # 'binance' (currently implemented)
Exchange.BYBIT     # 'bybit' (future)
Exchange.KRAKEN    # 'kraken' (future)
```

**Migration from v4.x to v5.x**:
```python
# Before (v4.x) - strings
ingest_binance_async(
    db_path='crypto_data.db',
    symbols=['BTCUSDT'],
    data_types=['spot', 'futures'],
    start_date='2024-01-01',
    end_date='2024-12-31',
    interval='5m'
)

# After (v5.x) - enums
from crypto_data import DataType, Interval

ingest_binance_async(
    db_path='crypto_data.db',
    symbols=['BTCUSDT'],
    data_types=[DataType.SPOT, DataType.FUTURES],
    start_date='2024-01-01',
    end_date='2024-12-31',
    interval=Interval.MIN_5
)
```

**Benefits**:
- ✅ IDE autocompletion
- ✅ Type checking (mypy, pyright)
- ✅ Protection against typos
- ✅ Self-documenting code

## Database Schema (v4.0.0 - Multi-Exchange)

**Single DuckDB file**: `crypto_data.db`

**Tables:**
1. `crypto_universe` - CoinMarketCap rankings
   - Primary key: `(date, symbol)`, Index: `(date, rank)`
   - Columns: date, symbol, rank, market_cap, categories
   - Symbols are base assets (BTC not BTCUSDT), interval-independent
   - Exchange-agnostic (rankings are global)
   - **Timestamp interpretation**: A date like `2024-01-01 00:00:00.000` means the coin was in top N for the ENTIRE month of January 2024 (monthly snapshot taken on the 1st)
   - **Update frequency**: Snapshots are taken on the 1st of each month ONLY (no daily/weekly updates). To backfill 12 months, you need 12 API calls

2. `spot`, `futures` - OHLCV data (MULTI-EXCHANGE)
   - **Primary key**: `(exchange, symbol, interval, timestamp)` - exchange is now part of PK
   - **Index**: `(exchange, symbol, interval, timestamp)`
   - **New column**: `exchange VARCHAR NOT NULL` - 'binance', 'bybit', 'kraken', etc.
   - Other columns: symbol, interval, timestamp, open, high, low, close, volume, quote_volume, trades_count, taker_buy_*
   - Interval stored as column (5m, 1h, 4h, 1d in same table)
   - **Multi-interval support**: You can store multiple intervals (5m, 1h, 4h, etc.) in the same database simultaneously. Each interval is a separate row with different primary key. Query by filtering `WHERE interval = '5m'`
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
- **Backtesting note**: Coins with data gaps at the end (delisted) should be kept in backtests even if they only appear in the training set - this reflects real market conditions and prevents survivorship bias
- **Important implication**: A symbol may be downloaded even if it wasn't in top N at the start_date. Example: requesting top 50 from 2024-01-01 to 2024-12-31 will download TON even though it only entered top 50 in June 2024. This is intentional - you get the complete universe evolution over the period

**Explicit Parameters Only**: Universe filtering follows "explicit is better than implicit"
- `populate_database()` and `ingest_universe()` require `exclude_tags` and `exclude_symbols` as direct parameters
- No config files, no hidden dependencies - everything is explicit and testable
- Benefits: Better testability, dependency injection, autodocumented API, zero hidden config
- Default: empty lists (no exclusions) if not provided
- Note: `ingest_universe()` is async - use `asyncio.run()` or await it within async context

## Known Limitations

### Technical Constraints

**Single-Writer Limitation** (DuckDB constraint)
- Only ONE process can write to database at a time
- Concurrent reads: unlimited ✅
- Concurrent writes: will raise "database is locked" error ❌
- **Solution**: Run one ingestion process at a time
- **Future**: Consider PostgreSQL for multi-writer scenarios

**No Checkpoint/Resume**
- If ingestion interrupted (Ctrl+C, crash), no automatic resume
- Must restart from beginning (but skip_existing=True prevents re-download)
- **Workaround**: Divide large ingestions into monthly batches
- **Future**: Could add checkpoint file tracking completed symbols

**No Disk Space Checks**
- Package doesn't verify available disk space before download
- Large downloads (5m interval, 100+ symbols) need 50-100GB
- **Risk**: Process may fail mid-download if disk fills up
- **Mitigation**: User should monitor disk space manually

**No Retry Logic for Failed Downloads**
- Partial downloads/corrupt ZIPs return False (not imported)
- Requires manual re-run of ingestion
- **Workaround**: Re-run populate_database() - skip_existing will only retry failed files
- **Future**: Could add configurable retry count (currently: detect + reject)

### Data Integrity Protections (v4.0.0+)

**Pre-Import Validation** ✅
- Pandera schema validation BEFORE database insertion
- Validates: OHLC relationships, non-negative prices/volumes, data types
- Rejects invalid data with clear error messages
- Transaction rollback on validation failure

**Download Validation** ✅
- Content-Length header check (partial download detection)
- ZIP integrity verification (zipfile.is_zipfile())
- Atomic write pattern (temp file → validation → rename)
- Auto-cleanup of corrupted temp files

**What's NOT Validated**
- Timestamp gaps within imported data (gaps between months only)
- Cross-exchange data consistency (future feature)
- Market cap accuracy (trusts CoinMarketCap API)

### API Rate Limits

**CoinMarketCap Free Tier**
- 333 API calls per day
- Universe ingestion: 1 call per month
- Recommendation: Don't ingest > 300 months in one day
- **Solution**: Use paid API key for large historical datasets

**Binance Data Vision**
- No official rate limits (S3-backed)
- Recommended: max_concurrent=20 for klines, 100 for metrics
- Too many concurrent requests may trigger throttling (retry 503 errors)

### Edge Cases Handled

✅ **Auto-handled**:
- 1000-prefix tokens (PEPE, SHIB, BONK): auto-retry with prefix on 404
- Timestamp format changes (ms vs μs): auto-detection
- CSV header inconsistencies: auto-detection
- Delisting detection: stop after N consecutive 404s (configurable)
- Duplicate timestamps: automatic deduplication
- Daylight saving duplicates: handled via pandas

❌ **Not handled** (user must handle):
- Rebrands (MATIC→POL): separate symbols, user must UNION
- Exchange maintenance windows: user must retry later
- Network failures during download: user must re-run
- Running out of disk space: user must free space

### Performance Considerations

**Memory Usage**
- Pandas DataFrames loaded in memory during import
- Large CSV files (5m interval, 1 month) can be 50-100MB
- Recommendation: 8GB+ RAM for smooth operation

**Download Concurrency**
- Default: 20 concurrent klines, 100 concurrent metrics
- Too high: may hit network limits or rate limiting
- Too low: slower downloads
- Tunable via `max_concurrent_klines`, `max_concurrent_metrics`

**Database Size**
- 5m interval, top 100, 1 year: ~30GB
- 1h interval, top 100, 1 year: ~5GB
- Recommendation: 50GB+ free space for safety margin

## Logging

**Automatic File Logging (v3.x+)**

All ingestion processes automatically log to timestamped files in `./logs/` directory:
- **Location**: `logs/crypto_data_YYYY-MM-DD_HH-MM-SS.log`
- **Format**: Includes ANSI color codes (viewable with `less -R` or similar)
- **Content**: Complete ingestion summary, progress bars, data availability reports
- **Retention**: Files are never deleted automatically - manage manually

**Setup**:
```python
from crypto_data import setup_colored_logging

# Automatic file + console logging
log_file = setup_colored_logging()  # Returns path to log file
print(f"Logs saved to: {log_file}")
```

**Features**:
- Dual output: Console (with TTY detection) + File (always colored)
- Timestamped filenames prevent overwrites
- Auto-creates `logs/` directory if missing
- Logs contain full ingestion summary from `log_ingestion_summary()`

**Viewing Colored Logs**:
```bash
# View with colors preserved
less -R logs/crypto_data_2025-11-06_18-10-45.log

# Strip colors for plain text
cat logs/crypto_data_2025-11-06_18-10-45.log | sed 's/\x1b\[[0-9;]*m//g' > plain.log

# Search logs
grep "ERROR" logs/*.log
```

**Log Content Example**:
```
2025-11-06 18:10:45 - INFO - Logging to file: logs/crypto_data_2025-11-06_18-10-45.log
2025-11-06 18:10:45 - INFO - ============================================================
2025-11-06 18:10:45 - INFO - Ingestion Summary:
2025-11-06 18:10:45 - INFO -   Downloaded: 98496
2025-11-06 18:10:45 - INFO -   Skipped (existing): 156
2025-11-06 18:10:45 - INFO -   Failed: 0
2025-11-06 18:10:45 - INFO -   Not found: 61426
2025-11-06 18:10:45 - INFO - Data Availability (2022-01-01 → 2025-10-01):
...
```

## Common Commands

**Example Script**:
```bash
python scripts/Download_data_universe.py  # Runs complete database population workflow
```

**Python API**:
```python
from crypto_data import (
    populate_database, ingest_universe, ingest_binance_async,
    setup_colored_logging, DataType, Interval
)

# Setup logging (auto-creates logs/crypto_data_YYYY-MM-DD_HH-MM-SS.log)
log_file = setup_colored_logging()

# Complete workflow (one function call) - Explicit parameters
populate_database(
    db_path='crypto_data.db',
    start_date='2024-01-01',
    end_date='2024-12-31',
    top_n=100,
    interval=Interval.MIN_5,  # Use Interval enum (MIN_5, MIN_15, HOUR_1, HOUR_4, DAY_1, etc.)
    data_types=[DataType.SPOT, DataType.FUTURES],
    exclude_tags=['stablecoin', 'wrapped-tokens', 'privacy'],
    exclude_symbols=['LUNA', 'FTT', 'UST']
)

# Without exclusions (default = empty lists)
populate_database(
    db_path='crypto_data.db',
    start_date='2024-01-01',
    end_date='2024-12-31',
    top_n=100,
    interval=Interval.MIN_5,
    data_types=[DataType.SPOT, DataType.FUTURES]
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
ingest_binance_async(
    db_path='crypto_data.db',
    symbols=symbols,
    data_types=[DataType.SPOT, DataType.FUTURES],
    start_date='2024-01-01',
    end_date='2024-12-31',
    interval=Interval.MIN_5
)
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

**populate_database() function** (end-to-end):
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
- **Important**: Progress bars show Binance data availability, NOT when the coin entered top N rankings. Example: TON entered top 50 in June 2024, but Binance data only available from August 2024. To check when a coin was in top N, query the `crypto_universe` table

## SQL Query Notes

**Case-Sensitivity**: DuckDB's `LIKE` is case-sensitive. Use `LOWER(categories) NOT LIKE '%stablecoin%'` or `ILIKE` for filtering.

**Universe JOINs**: Join crypto_universe with binance tables using `u.symbol || 'USDT' = s.symbol` and `u.date = DATE_TRUNC('day', s.timestamp)`

**Checking Top N Status**: To determine if a coin was in top N at a specific time, query the `crypto_universe` table directly:
```sql
SELECT date, symbol, rank
FROM crypto_universe
WHERE symbol = 'TON'
  AND date >= '2024-01-01'
  AND rank <= 50
ORDER BY date;
```
Do NOT rely on spot/futures tables for this - they only show data availability from Binance, not market cap rankings.

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
