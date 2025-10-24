# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**Crypto Data v3.0.0** - Pure data ingestion pipeline for cryptocurrency market data.

This package is a **DuckDB ingestion pipeline** that:
- ✅ Downloads historical OHLCV data from Binance Data Vision
- ✅ Downloads universe rankings from CoinMarketCap
- ✅ Populates a DuckDB database with cleaned data
- ✅ Provides CLI tools for database population

**Critical Principle:** This package is **NOT a query library**. It only handles data ingestion into DuckDB. Users query the database directly using DuckDB's SQL interface.

### Main Components

**Database** (`src/crypto_data/database.py`):
- `CryptoDatabase` - DuckDB schema management and connection handling
  - Creates tables: `binance_spot`, `binance_futures`, `crypto_universe`, `_metadata`
  - Manages indexes and primary keys
  - Provides stats and metadata methods (including interval storage)
  - Context manager support for automatic cleanup

**Validation** (`src/crypto_data/validation.py`):
- `validate_interval_consistency()` - Dual-layer interval validation
  - Filename validation: Parses `crypto_{interval}.db` pattern
  - Database metadata validation: Stores and validates interval in `_metadata` table
  - Prevents mixing different timeframes in same database
  - Clear error messages with suggested solutions

**Ingestion** (`src/crypto_data/ingestion/`):
1. **Binance Ingestion** (`binance.py`) - Download and import OHLCV data
   - `ingest_binance()` - Main entry point for Binance data
   - **Validates interval consistency** before downloading (filename + metadata)
   - Downloads from Binance Data Vision API to /tmp
   - Imports directly to DuckDB using `read_csv_auto()`
   - Deletes temporary files immediately after import
   - Supports: spot, futures (excludes premium_index, funding_rate for simplicity)

2. **CoinMarketCap Ingestion** (`coinmarketcap.py`) - Universe rankings with full metadata
   - `ingest_universe()` - Main entry point for CoinMarketCap data
   - **Full metadata API**: Uses `/coins/markets` + `/coins/{id}` endpoints
   - **API calls**: 1 + N per snapshot (e.g., 51 calls for top 100)
   - Fetches top N coins from CoinMarketCap API
   - Extracts: rank, market_cap, category, exchanges, stablecoin flag
   - Rate-limited with configurable delay (default: 0.5s)
   - **Safe with paid tier**: 500 calls/min supports 51 calls comfortably

**Scripts** (`scripts/`):
- `update_universe.py` - **NEW** dedicated script for CoinMarketCap universe rankings
  - Manages universe data independently from Binance OHLCV
  - Stores in separate `crypto_universe.db` (interval-independent)
  - Supports single snapshot or monthly range
  - Skips existing snapshots automatically
  - **1 API call per snapshot** (no rate limit issues!)

- `populate_binance.py` - **RENAMED** from populate_database.py, now Binance-only
  - Supports config files and command-line args
  - **Universe-based symbol selection:** When `--top-n` is specified, extracts symbols from universe database
    - Uses UNION strategy: Gets all symbols that appeared in top N at ANY point during the period
    - Example: `--top-n 50` from Jan-Dec → ~60-70 unique symbols (captures coins that entered/exited top 50)
  - **Manual symbol selection:** Use `--symbols BTCUSDT ETHUSDT ...`
  - **Mutually exclusive:** Cannot use both `--symbols` and `--top-n`
  - Requires `--universe-db` parameter when using `--top-n`

- `inspect_database.py` - View database contents and statistics

### Data Types Supported

**Binance:**
- Spot Klines (OHLCV) - 5m interval default
- USD-M Futures Klines (OHLCV) - 5m interval default

**CoinMarketCap:**
- Universe rankings (top N by market cap)
- Market capitalization in USD
- Stablecoin flags
- **Removed:** Categories, exchange listings (simplified for performance)

**Not Included** (for simplicity):
- ❌ Premium Index
- ❌ Funding Rates

## Database Architecture

**Two-Database System:**

1. **Universe Database** (`crypto_universe.db`)
   - Stores CoinMarketCap universe rankings
   - **Interval-independent** - shared across all intervals
   - Single database for all OHLCV timeframes
   - Prevents data duplication

2. **Binance Databases** (`crypto_{interval}.db`)
   - Stores OHLCV data for specific interval
   - **Interval-specific** - each interval needs its own database
   - Prevents mixing different timeframes

### Naming Convention
Use the pattern `crypto_{interval}.db` for Binance databases:
- `crypto_5m.db` - 5-minute data
- `crypto_1h.db` - 1-hour data
- `crypto_4h.db` - 4-hour data
- `crypto_1d.db` - 1-day data

Use `crypto_universe.db` for universe database (no interval suffix).

### Validation System (Dual-Layer)

**1. Filename Validation**
- Parses database filename using regex: `^crypto_([0-9]+[smhd])$`
- Extracts interval from pattern (e.g., `crypto_5m.db` → `5m`)
- Compares with requested `--interval` parameter
- Raises `ValueError` if mismatch detected

**2. Database Metadata Validation**
- Stores interval in `_metadata` table on first run
- Validates all subsequent runs match the stored interval
- Prevents reusing database files for different intervals
- Raises `ValueError` if database contains different interval data

### Error Messages
Users get clear, actionable error messages:

**Filename Mismatch:**
```
❌ Interval mismatch detected!

  Filename suggests: 5m (from crypto_5m.db)
  Requested interval: 1h

Solutions:
  • Use correct database: --db crypto_1h.db --interval 1h
  • Or use correct interval: --db crypto_5m.db --interval 5m
```

**Database Metadata Mismatch:**
```
❌ Database interval mismatch!

  This database contains: 5m data
  Requested interval: 1h

This database is dedicated to 5m data and cannot store 1h data.
```

### Implementation Details
- **Module:** `src/crypto_data/validation.py`
- **Function:** `validate_interval_consistency(db_path, requested_interval, db)`
- **Called by:** `ingest_binance()` before any downloads
- **Pattern:** `^crypto_([0-9]+[smhd])$` (matches 5m, 1h, 4h, 1d, etc.)
- **Storage:** `INSERT OR REPLACE INTO _metadata (key, value) VALUES ('interval', ?)`

## Common Commands

### Database Population (Two-Script Workflow)

**Step 1: Update Universe Rankings**

```bash
# Fetch current month (single snapshot)
python scripts/update_universe.py \
    --date 2025-11-01 \
    --top-n 100

# Fetch monthly snapshots for date range
python scripts/update_universe.py \
    --start 2024-01-01 \
    --end 2024-12-31 \
    --top-n 100

# Custom database path
python scripts/update_universe.py \
    --db my_universe.db \
    --date 2025-11-01 \
    --top-n 50
```

**Step 2: Download Binance Data**

```bash
# ⭐ RECOMMENDED: Auto-select symbols from universe
python scripts/populate_binance.py \
    --db crypto_5m.db \
    --interval 5m \
    --top-n 50 \
    --universe-db crypto_universe.db \
    --data-types spot futures \
    --start 2024-01-01 \
    --end 2024-12-31

# What this does:
# 1. Reads universe from crypto_universe.db
# 2. Extracts UNION of symbols in top 50 during period (~60-70 symbols)
# 3. Downloads Binance data for those symbols

# Manual symbol specification (alternative)
python scripts/populate_binance.py \
    --db crypto_5m.db \
    --interval 5m \
    --symbols BTCUSDT ETHUSDT SOLUSDT \
    --data-types spot futures \
    --start 2024-01-01 \
    --end 2024-03-31

# Using config file
python scripts/populate_binance.py --config config/config.yaml

# 1-hour data (separate database, same universe)
python scripts/populate_binance.py \
    --db crypto_1h.db \
    --interval 1h \
    --top-n 50 \
    --universe-db crypto_universe.db \
    --data-types spot futures \
    --start 2024-01-01 \
    --end 2024-03-31
```

### Database Inspection

```bash
# View database contents
python scripts/inspect_database.py crypto_data.db

# With sample data and symbol lists
python scripts/inspect_database.py crypto_data.db --samples --symbols

# Show more samples
python scripts/inspect_database.py crypto_data.db --samples --limit 10
```

### Testing

```bash
# Run all tests
pytest tests/ -v

# Run basic smoke tests only
pytest tests/test_database_basic.py -v

# With coverage
pytest tests/ --cov=crypto_data --cov-report=html
```

### Package Installation

```bash
# Install in development mode
pip install -e .

# Install with development dependencies
pip install -e ".[dev]"
```

## Code Architecture

### Project Structure (v3.0.0)

```
crypto-data/
├── src/crypto_data/                    # Main package
│   ├── __init__.py                     # Package exports (v3.0.0)
│   ├── database.py                     # CryptoDatabase class (~230 lines)
│   └── ingestion/                      # Data ingestion modules
│       ├── __init__.py                 # Ingestion exports
│       ├── binance.py                  # Binance ingestion (~330 lines)
│       └── coinmarketcap.py                # CoinMarketCap ingestion (~220 lines)
├── tests/                              # Test suite (pytest)
│   └── test_database_basic.py          # Basic smoke tests
├── scripts/
│   ├── update_universe.py              # Universe rankings CLI (NEW)
│   ├── populate_binance.py             # Binance data CLI (RENAMED)
│   └── inspect_database.py             # Database inspection tool
├── config/
│   └── config.yaml                     # Configuration template
├── setup.py                            # Package setup (crypto-data v3.0.0)
├── requirements.txt                    # Dependencies
├── README.md                           # User documentation
└── CLAUDE.md                           # This file (developer guidance)
```

**Total Package Size:** ~780 lines (vs v2.0.0's ~1,500 lines - 52% reduction)

### DuckDB Schema

**binance_spot** - Spot market OHLCV data
```sql
CREATE TABLE binance_spot (
    symbol VARCHAR NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    volume DOUBLE,
    quote_volume DOUBLE,
    trades_count INTEGER,
    taker_buy_base_volume DOUBLE,
    taker_buy_quote_volume DOUBLE,
    PRIMARY KEY (symbol, timestamp)
);
CREATE INDEX idx_spot_symbol_time ON binance_spot(symbol, timestamp);
```

**binance_futures** - Futures market OHLCV data (same schema as spot)

**crypto_universe** - Token rankings with full metadata (stored in `crypto_universe.db`)
```sql
CREATE TABLE crypto_universe (
    date DATE NOT NULL,
    symbol VARCHAR NOT NULL,
    rank INTEGER,
    is_stablecoin BOOLEAN,
    market_cap DOUBLE,
    category VARCHAR,
    exchanges VARCHAR,  -- Comma-separated exchange IDs
    PRIMARY KEY (date, symbol)
);
CREATE INDEX idx_universe_date ON crypto_universe(date);
```

**Full Metadata Schema:**
- ✅ `market_cap` - Market capitalization in USD
- ✅ `category` - Primary category (layer-1, smart-contract-platform, defi, etc.)
- ✅ `exchanges` - Comma-separated list of exchange IDs (for liquidity filtering)
- ❌ `exchange_count` - NOT stored (calculate in queries)

### Data Flow (v3.0.0)

**Two-Script Workflow:**

**Script 1: update_universe.py → crypto_universe.db**
1. **Fetch**: `/coins/markets` + individual coin details (1 + N API calls per snapshot)
2. **Transform**: Extract rank, market_cap, category, exchanges, stablecoin flag
3. **Import**: INSERT OR REPLACE into `crypto_universe.db`
4. **Result**: Universe snapshot with full metadata stored in dedicated database (interval-independent)
5. **API Calls**: 51 calls for top 100 (safe with paid tier: 500 calls/min)

**Script 2: populate_binance.py → crypto_{interval}.db**
1. **Symbol Selection** (if using `--top-n`):
   - Connect to `crypto_universe.db`
   - Extract UNION of symbols in top N during period
   - Add USDT suffix (BTC → BTCUSDT)
2. **Download**: Fetch ZIP files from Binance Data Vision API to `/tmp`
3. **Import**: Use DuckDB's `read_csv_auto()` to read directly from ZIP and INSERT
4. **Delete**: Remove temporary ZIP files immediately
5. **Result**: OHLCV data stored in interval-specific database

**Key Differences from v2.0.0:**
- ❌ No file-based storage (no Parquet files, no organized directories)
- ❌ No data readers or loaders (DuckDB handles reading)
- ❌ No quality checkers or cleaners (simplified for v3.0.0)
- ✅ Single database file (~1GB vs ~3.5GB with files)
- ✅ Direct SQL querying (no abstraction layers)

### Usage Examples (v3.0.0)

**Populate Database (Python API):**
```python
from crypto_data import CryptoDatabase, ingest_binance, ingest_universe

# ===== STEP 1: Populate Universe Database =====
# Create/connect to universe database
universe_db = CryptoDatabase('crypto_universe.db')

# Ingest universe snapshot
ingest_universe(
    db_path='crypto_universe.db',
    date='2024-01-01',
    top_n=100
)

universe_db.close()

# ===== STEP 2: Populate Binance Database =====
# Create/connect to Binance database (interval-specific)
binance_db = CryptoDatabase('crypto_5m.db')

# Ingest Binance data
ingest_binance(
    db_path='crypto_5m.db',
    symbols=['BTCUSDT', 'ETHUSDT'],
    data_types=['spot', 'futures'],
    start_date='2024-01-01',
    end_date='2024-03-31',
    interval='5m',
    skip_existing=True
)

# View stats
stats = binance_db.get_table_stats()
print(stats)

binance_db.close()
```

**Query Database (Users do this - NOT part of this package):**
```python
import duckdb

# Connect to Binance database
conn = duckdb.connect('crypto_5m.db')

# Simple query
df = conn.execute("""
    SELECT * FROM binance_spot
    WHERE symbol = 'BTCUSDT'
        AND timestamp BETWEEN '2024-01-01' AND '2024-03-31'
    ORDER BY timestamp
""").df()

# Complex query with joins
df = conn.execute("""
    SELECT
        s.timestamp,
        s.symbol,
        s.close as spot_close,
        f.close as futures_close,
        (f.close - s.close) / s.close * 100 as basis_bps
    FROM binance_spot s
    JOIN binance_futures f
        ON s.symbol = f.symbol
        AND s.timestamp = f.timestamp
    WHERE s.symbol IN ('BTCUSDT', 'ETHUSDT')
        AND s.timestamp > '2024-01-01'
    ORDER BY s.timestamp
""").df()

conn.close()
```

**Multi-Database Queries (Universe + Binance):**
```python
import duckdb

# Connect to Binance database and attach universe
conn = duckdb.connect('crypto_5m.db')
conn.execute("ATTACH 'crypto_universe.db' AS universe")

# Get top 50 symbols for specific date
df = conn.execute("""
    WITH top_symbols AS (
        SELECT DISTINCT symbol || 'USDT' as binance_symbol
        FROM universe.crypto_universe
        WHERE date = '2024-01-01'
          AND rank <= 50
          AND is_stablecoin = false
    )
    SELECT s.*
    FROM binance_spot s
    INNER JOIN top_symbols t ON s.symbol = t.binance_symbol
    WHERE s.timestamp >= '2024-01-01'
    ORDER BY s.timestamp
""").df()

conn.close()
```

**IMPORTANT:** This package does NOT provide query methods. Users write SQL directly using DuckDB.

### Script Implementation Details

**update_universe.py** - Helper functions for universe snapshots:

**1. `_generate_month_list(start_date: str, end_date: str) -> list`**
- Generates list of YYYY-MM strings between start and end dates
- Normalizes dates to 1st of each month
- Used for monthly universe snapshot generation
- Example: `'2024-01-15'` to `'2024-03-20'` → `['2024-01', '2024-02', '2024-03']`

**2. `_universe_snapshot_exists(db_path: str, date: str) -> bool`**
- Checks if universe snapshot exists for given date
- Returns `False` on database errors (graceful handling)
- Used for skip logic to avoid duplicate API calls
- Query: `SELECT COUNT(*) FROM crypto_universe WHERE date = ?`

**populate_binance.py** - Helper function for symbol extraction:

**`_get_symbols_from_universe(universe_db_path: str, start_date: str, end_date: str, top_n: int) -> list`**
- Extracts unique symbols from universe snapshots (UNION strategy)
- Returns all symbols that appeared in top N at ANY point during period
- Adds USDT suffix automatically (converts `'BTC'` → `'BTCUSDT'`)
- Query: `SELECT DISTINCT symbol FROM crypto_universe WHERE date >= ? AND date <= ? AND rank <= ? ORDER BY symbol`
- Example: top_n=50 from Jan-Dec → ~60-70 unique symbols (captures coins that entered/exited top 50)

**Workflow:**
1. User runs `update_universe.py` to fetch monthly snapshots → `crypto_universe.db`
2. User runs `populate_binance.py` with `--top-n 50 --universe-db crypto_universe.db`
3. Script validates `--symbols` not provided (mutually exclusive)
4. Script extracts UNION of symbols from universe database
5. Script passes extracted symbols to `ingest_binance()`
6. Result: All symbols that were in top 50 at any point during period are downloaded

### Key Classes

**CryptoDatabase** (`src/crypto_data/database.py`)
```python
class CryptoDatabase:
    """Manages DuckDB schema and connections."""

    def __init__(self, db_path: str):
        """Connect to database and create schema if needed."""

    def execute(self, query: str, params: tuple = None):
        """Execute SQL query."""

    def get_table_stats(self) -> Dict[str, Dict]:
        """Get statistics for all tables."""

    def close(self):
        """Close database connection."""

    def __enter__(self) / __exit__(self):
        """Context manager support."""
```

**ingest_binance()** (`src/crypto_data/ingestion/binance.py`)
```python
def ingest_binance(
    db_path: str,
    symbols: List[str],
    data_types: List[str],  # ['spot', 'futures']
    start_date: str,        # 'YYYY-MM-DD'
    end_date: str,          # 'YYYY-MM-DD'
    interval: str = '5m',
    skip_existing: bool = True
) -> None:
    """Download and import Binance data to DuckDB.

    Downloads to /tmp, imports to database, deletes temp files.
    Uses DuckDB's read_csv_auto() for direct ZIP reading.
    """
```

**ingest_universe()** (`src/crypto_data/ingestion/coinmarketcap.py`)
```python
def ingest_universe(
    db_path: str,
    date: str,              # 'YYYY-MM-DD'
    top_n: int = 100,
    rate_limit_delay: float = 1.5
) -> None:
    """Fetch and import CoinMarketCap universe to DuckDB.

    Uses single /coins/markets endpoint (1 API call per snapshot).
    Fetches top N coins, extracts rank/market_cap/stablecoin flag.
    No longer fetches category or exchanges (simplified).
    """
```

### Configuration

**config/config.yaml** - Database population configuration:
```yaml
# Database path
database: ./crypto_data.db

# Symbols to download
symbols:
  - BTCUSDT
  - ETHUSDT
  - SOLUSDT

# Data types
data_types:
  - spot
  - futures

# Kline interval
interval: 5m

# Date range (also used for universe snapshots)
start_date: 2024-01-01
end_date: 2024-03-31

# Universe settings (optional)
# If specified, monthly snapshots fetched from start_date to end_date
# top_n: 100
```

Both CLI args and config file supported, with CLI taking precedence.

### Package Dependencies

```python
# Core dependencies
duckdb >= 0.9.0      # Embedded database
requests >= 2.31.0   # HTTP requests
beautifulsoup4       # HTML parsing (Binance Data Vision)
PyYAML               # Config file parsing
pandas >= 2.0.0      # DataFrame support (optional for users)

# Development dependencies
pytest >= 7.4.0      # Testing
pytest-cov           # Coverage reports
```

### Environment

- Uses conda environment at `/Users/guillaumetonnerre/miniconda3/envs/quant/`
- Always activate with: `source /Users/guillaumetonnerre/miniconda3/bin/activate quant`
- Package structure follows Python best practices with src layout

## Coding Standards and Preferences

### General
- **Vectorize operations** instead of loops with pandas and numpy when possible
- **Use Manifest Typing** (explicit type hints) for clarity and maintainability
- **Testing**: Use pytest for all testing purposes

### Environment
- **Always run code** with conda environment 'quant'
- Command: `source /Users/guillaumetonnerre/miniconda3/bin/activate quant`

### Notebooks
- **Limit print statements** - prefer returning values or display methods
- **Always use dark theme** for plotting

### Architecture
- **Use Model Builder pattern** when >10 arguments or complex objects
- **Simplicity over features** - this package is intentionally minimal
- **SQL over abstraction** - expose database, don't hide it

## Known Data Quality Issues & Solutions

### Binance Data Format Inconsistencies

Through extensive testing, we have identified critical data quality issues in Binance historical data that require automatic handling during ingestion:

### 1. Timestamp Format Changes (2024 → 2025)

**Problem**: Binance changed timestamp format between years:
- **2024 and earlier**: Millisecond timestamps (13 digits, e.g., 1704067200000)
- **2025 and later**: Microsecond timestamps (16 digits, e.g., 1735689600000000)
- **Impact**: Could cause NaT (Not a Time) values when importing

**Solution Implemented**:
- Auto-detection based on digit count in `binance.py`
- Centralized conversion logic handles both formats seamlessly
- Applied across all data types (spot, futures)

### 2. Inconsistent CSV Headers

**Problem**: Binance CSV files have inconsistent header presence:
- **Some files**: Include column headers in first row (especially newer data)
- **Other files**: No headers, data starts immediately (especially older data)
- **Impact**: Could cause parsing errors during import

**Solution Implemented**:
- Header detection analyzes first line before import
- Looks for header keywords ('open_time', 'close_time', etc.)
- Checks if first column is numeric (timestamp) or text (header)
- DuckDB's `read_csv_auto()` configured accordingly

### 3. Data Type Specific Issues

**Spot/Futures Klines**:
- 12 columns of OHLCV data
- Variable header presence across different periods
- Timestamp format changes affect both open_time and close_time columns

**Implementation Notes**:
- All timestamp and header detection is automatic
- Detection works for both ZIP and raw CSV files
- No user intervention required during ingestion

### Testing Results

✅ **2024 Data (milliseconds)**: All data imports without errors
✅ **2025 Data (microseconds)**: All data imports without errors
✅ **Mixed periods**: Seamless handling across timestamp formats
✅ **Header variations**: Automatic detection and proper parsing

## Migration from v2.0.0

**v3.0.0 is a complete rewrite.** v2.x data cannot be automatically migrated.

### Major Changes:
- ❌ Removed all file-based storage (no Parquet, no organized directories)
- ❌ Removed all reader classes (BinanceDataReader, etc.)
- ❌ Removed all loader classes (BinanceLoader, UniverseLoader)
- ❌ Removed all quality/cleaner/merger classes
- ❌ Removed premium_index and funding_rate support (simplified)
- ❌ Removed exchange_count from universe (can be calculated)
- ✅ Added DuckDB-based storage
- ✅ Added direct SQL querying
- ✅ Added CLI scripts for population
- ✅ Simpler codebase (~52% less code)

### To migrate:
1. Install v3.0.0: `pip install -e .`
2. Re-download universe: `python scripts/update_universe.py --start 2024-01-01 --end 2024-12-31 --top-n 100`
3. Re-download Binance: `python scripts/populate_binance.py --config config.yaml`
4. Update query code to use DuckDB SQL instead of loaders

**Note:** This is intentional - the new architecture is fundamentally different and simpler.

## Development Workflow

### Adding New Features

When adding features to this package, remember:
1. **Scope**: This package is ONLY for data ingestion, NOT querying
2. **Simplicity**: Prefer simple solutions over complex abstractions
3. **SQL-first**: Expose database directly, don't hide it behind APIs
4. **Testing**: Add smoke tests for new ingestion functionality

### Testing New Code

```bash
# Run basic smoke tests (fast)
pytest tests/test_database_basic.py -v

# Run all tests
pytest tests/ -v

# With coverage
pytest tests/ --cov=crypto_data --cov-report=html
```

### File Organization

```
src/crypto_data/
├── database.py      # Schema and connection management ONLY
└── ingestion/       # Data ingestion modules ONLY
    ├── binance.py   # Binance download/import
    └── coinmarketcap.py # CoinMarketCap fetch/import
```

Keep it simple. Don't add loaders, readers, or query helpers.

---

**Philosophy:** This package does ONE thing well - populates a DuckDB database with crypto data. For querying and analysis, users should use DuckDB's powerful SQL interface directly.
