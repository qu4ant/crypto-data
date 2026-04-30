# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [6.0.0] - 2026-04-30

### 🚨 Breaking Changes

**Enriched CoinMarketCap Universe Schema**
- `crypto_universe` now uses `(provider, provider_id, date)` as the primary key.
- Universe rows now store enriched CMC fields: `name`, `slug`, `fully_diluted_market_cap`, supply fields, `tags`, `platform`, and `date_added`.
- Pre-v6 DuckDB files must be deleted and re-ingested.

### ✨ New Features

**Additional Kline Interval**
- Added `Interval.MIN_3` for Binance `3m` klines.
- OHLCV schema interval validation now derives from shared interval metadata.

### 📦 Improvements

**Internal Architecture Simplification**
- Added shared table metadata in `crypto_data.tables`.
- Added shared interval metadata in `crypto_data.intervals`.
- Moved gap enumeration into neutral `crypto_data.gaps`, used by both quality audits and repair.
- Added shared idempotent DuckDB insert helper in `crypto_data.db_write`.
- Moved period completeness checks out of `binance_pipeline.py` into `crypto_data.completeness`.

### 📝 Documentation

- Updated README badges and examples for v6.0.0.
- Updated developer notes to reflect the v6 schema and interval set.

---

## [5.0.0] - 2025-11-21

### 🚨 Breaking Changes

**Type-Safe Enums Replace String Parameters**

All data type and interval parameters now require enums instead of strings. This is a breaking change from v4.x.

**Migration Required:**
```python
# ❌ Old way (v4.x) - strings
create_binance_database(
    data_types=['spot', 'futures'],
    interval='5m'
)

# ✅ New way (v5.x) - enums
from crypto_data import DataType, Interval

create_binance_database(
    data_types=[DataType.SPOT, DataType.FUTURES],
    interval=Interval.MIN_5
)
```

**Affected functions:**
- `create_binance_database()` - `data_types` and `interval` parameters
- `update_binance_market_data()` - `data_types` and `interval` parameters
- All internal functions that accept data types or intervals

### ✨ New Features

**Type-Safe Enumerations**
- **`DataType` enum**: `SPOT`, `FUTURES`, `OPEN_INTEREST`, `FUNDING_RATES`
- **`Interval` enum**: `MIN_1`, `MIN_5`, `MIN_15`, `MIN_30`, `HOUR_1`, `HOUR_2`, `HOUR_4`, `HOUR_6`, `HOUR_8`, `HOUR_12`, `DAY_1`, `DAY_3`, `WEEK_1`, `MONTH_1`

**Benefits:**
- ✅ IDE autocompletion for available options
- ✅ Type checking with mypy/pyright
- ✅ Protection against typos (`'spot'` vs `'spots'`)
- ✅ Self-documenting code

### 📦 Improvements

**Developer Experience**
- Better IDE support with autocompletion
- Catch errors at development time instead of runtime
- Clear, explicit parameter names (e.g., `Interval.HOUR_1` instead of `'1h'`)
- Reduced cognitive load - discover available options via autocomplete

**Code Quality**
- All enums properly documented with docstrings
- Exported in public API (`__init__.py`)
- Used consistently across all tests and examples
- Full backward compatibility removed (clean break)

### 📝 Documentation

**Updated Documentation:**
- README.md: Added "Type-Safe Enums (v5.0.0+)" section with migration guide
- README.fr.md: Added French translation of enum documentation
- CLAUDE.md: Already documented enums in v5.0.0+ section
- All code examples updated to use enums
- Migration examples in both English and French

**Version Updates:**
- `__init__.py`: Updated version to 5.0.0
- `pyproject.toml`: Already at 5.0.0
- README badges: Updated to v5.0.0

### 🔧 Technical Details

**New Module:**
- `src/crypto_data/enums.py` - Contains all enum definitions

**API Exports:**
- Added to `__all__`: `DataType`, `Interval`

**Test Coverage:**
- All tests updated to use enums
- No string-based tests remaining
- Example scripts updated (`scripts/Download_data_universe.py`)

---

## [4.0.0] - 2025-11-17

### 🚨 Breaking Changes

**Explicit Exchange Provenance Column**
- Tables renamed: `binance_spot` → `spot`, `binance_futures` → `futures`
- New column: `exchange VARCHAR NOT NULL` in all market data tables
- Primary key now includes exchange: `(exchange, symbol, interval, timestamp)`
- **Migration required**: If upgrading from v3.x, recreate database from scratch

**Reason**: Keep Binance provenance explicit while avoiding Binance-specific table names.

### ✨ New Features

**Data Integrity Protection**
- **Pre-import validation**: Pandera schema validation BEFORE database insertion
  - Validates OHLC relationships (high ≥ low, high ≥ open/close, etc.)
  - Rejects negative prices, negative volumes
  - Prevents importing corrupted data
  - Clear error messages with suggestions
- **Download integrity verification**:
  - Content-Length validation (partial download detection)
  - ZIP integrity check before import
  - Atomic write pattern (temp file → validation → rename)
  - Auto-cleanup of corrupted files
- **Transaction safety**: All imports wrapped in BEGIN/COMMIT/ROLLBACK

**New Data Types**
- `open_interest`: Futures open interest metrics (daily granularity)
- `funding_rates`: Perpetual futures funding rates (8h granularity)

**Async Universe Ingestion**
- `update_coinmarketcap_universe()` now fully async with parallel downloads (5 concurrent by default)
- 3-5x faster for multi-month snapshots
- Single function replaces old sync/async split

**Auto-Discovery Features**
- 1000-prefix tokens: Auto-retry futures with `1000{SYMBOL}` on 404 (PEPE, SHIB, BONK)
- Ticker mapping cache: Session-persistent to avoid repeated 404s
- Gap detection: Auto-stop after N consecutive missing months (delisting detection)

### 🐛 Bug Fixes

- Fixed timestamp format detection (ms vs μs auto-detection based on magnitude)
- Fixed CSV header detection (keyword matching + numeric analysis)
- Fixed duplicate timestamp handling (daylight saving edge case)
- Fixed transaction rollback on import errors
- Fixed temp file cleanup in error scenarios

### 📦 Improvements

**Performance**
- Async downloads: 5-10x faster than v3.x (20 concurrent klines, 100 concurrent metrics)
- Parallel universe downloads: 5 months fetched concurrently
- Reduced API calls via ticker mapping cache

**Code Quality**
- Added 23 new tests (validation + download robustness)
- All 187 tests passing
- Better error messages with actionable suggestions
- Comprehensive logging (DEBUG/INFO/ERROR levels)

**Documentation**
- Added "Known Limitations" section (single-writer, disk space, rate limits)
- Added "Troubleshooting" guide (common errors + solutions)
- Improved docstrings with type hints
- CLAUDE.md updated with v4.0.0 design decisions

### 📝 Documentation

- README: Added Known Limitations section
- README: Added Troubleshooting guide
- README: Updated schema documentation for the explicit `exchange='binance'` column
- CLAUDE.md: Documented design decisions and limitations
- Added this CHANGELOG.md

### 🔧 Technical Details

**Dependencies**
- Added: `pandera` for data validation
- Added: `aiohttp` for async HTTP requests
- Existing: `duckdb`, `pandas`, `requests`

**Database Changes**
- Schema version: 4.0.0 (explicit exchange provenance column)
- New tables: `open_interest`, `funding_rates`
- Modified tables: Added `exchange` column to `spot`, `futures`
- Indexes: Updated to include `exchange` in PK

**API Changes**
- `update_coinmarketcap_universe()`: Now async (use `asyncio.run()` or `await`)
- `update_binance_market_data()`: New parameters `max_concurrent_klines`, `max_concurrent_metrics`, `max_concurrent_funding`
- `create_binance_database()`: Updated to call async `update_coinmarketcap_universe()` internally
- All validation functions: New `strict` parameter (default: True)

---

## [3.0.0] - 2024-XX-XX

### 🚨 Breaking Changes

**Complete Rewrite**
- Removed Parquet storage (DuckDB-only now)
- Removed reader/loader classes (SQL-first philosophy)
- ~52% less code

### ✨ New Features

- DuckDB-based storage (single file)
- Transaction-safe imports
- Skip existing data automatically
- Colored logging

### 📦 Improvements

- Simpler API (no abstraction layers)
- Direct SQL queries
- Better testability

---

## [2.0.0] - 2024-XX-XX

_Historical release - see git history for details_

---

## [1.0.0] - 2024-XX-XX

_Initial release_

---

## Legend

- 🚨 Breaking Changes
- ✨ New Features
- 🐛 Bug Fixes
- 📦 Improvements
- 📝 Documentation
- 🔧 Technical Details
- ⚠️ Deprecations
- 🗑️ Removals
