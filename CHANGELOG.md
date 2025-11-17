# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [4.0.0] - 2025-11-17

### 🚨 Breaking Changes

**Multi-Exchange Schema**
- Tables renamed: `binance_spot` → `spot`, `binance_futures` → `futures`
- New column: `exchange VARCHAR NOT NULL` in all market data tables
- Primary key now includes exchange: `(exchange, symbol, interval, timestamp)`
- **Migration required**: If upgrading from v3.x, recreate database from scratch

**Reason**: Prepare for multi-exchange support (Bybit, Kraken, Coinbase in future releases)

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
- `ingest_universe()` now fully async with parallel downloads (5 concurrent by default)
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
- README: Updated schema documentation for multi-exchange
- CLAUDE.md: Documented design decisions and limitations
- Added this CHANGELOG.md

### 🔧 Technical Details

**Dependencies**
- Added: `pandera` for data validation
- Added: `aiohttp` for async HTTP requests
- Existing: `duckdb`, `pandas`, `requests`

**Database Changes**
- Schema version: 4.0.0 (multi-exchange)
- New tables: `open_interest`, `funding_rates`
- Modified tables: Added `exchange` column to `spot`, `futures`
- Indexes: Updated to include `exchange` in PK

**API Changes**
- `ingest_universe()`: Now async (use `asyncio.run()` or `await`)
- `ingest_binance_async()`: New parameters `max_concurrent_klines`, `max_concurrent_metrics`, `max_concurrent_funding`
- `populate_database()`: Updated to call async `ingest_universe()` internally
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
