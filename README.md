# 📊 Crypto Data

> **[Français](README.fr.md)** | **English**

**Cryptocurrency Data Infrastructure** - Automated multi-exchange OHLCV data and market rankings downloader.

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.10+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![Version](https://img.shields.io/badge/version-5.0.0-green.svg)](https://github.com/qu4ant/crypto-data/releases/tag/v5.0.0)
[![codecov](https://codecov.io/gh/qu4ant/crypto-data/branch/main/graph/badge.svg)](https://codecov.io/gh/qu4ant/crypto-data)
[![Tests](https://github.com/qu4ant/crypto-data/workflows/Tests/badge.svg)](https://github.com/qu4ant/crypto-data/actions)

---

## 🎯 Overview

**Crypto Data** is an ingestion pipeline that automatically downloads cryptocurrency market data and stores it in a local DuckDB database.

✨ **Philosophy**: This package does **ONE thing** - populate a database. You then query the database directly with SQL.

### Key Features

- 📈 **OHLCV Data**: Downloads from Binance Data Vision (spot + futures)
- 🌍 **Universe Rankings**: Top N cryptocurrencies by market cap via CoinMarketCap
- 🚀 **Async Downloads**: 20 parallel downloads for maximum speed
- 🔄 **Auto-Management**: Format detection, intelligent retry, 1000-prefix token handling
- 💾 **DuckDB**: Embedded database, fast SQL queries
- 🏗️ **Multi-exchange ready**: Schema v4.0.0 prepared for Bybit, Kraken, etc.

---

## 🔄 Input/Output Schema

```
┌─────────────────────────────────────────────────────────────┐
│                         INPUTS                               │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  📊 CoinMarketCap API                                       │
│  └─> Top N rankings by market cap                          │
│      (solves survivorship bias)                            │
│                                                              │
│  📈 Binance Data Vision                                     │
│  └─> Historical OHLCV data                                  │
│      (spot + futures, 5m/1h/4h/1d intervals)               │
│                                                              │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│                   CRYPTO-DATA PIPELINE                       │
│                                                              │
│  ⚙️  Async downloads (20 threads)                           │
│  ⚙️  Auto-detect timestamp formats                          │
│  ⚙️  Handle 1000-prefix (PEPE, SHIB, BONK)                 │
│  ⚙️  Atomic transaction per symbol                          │
│                                                              │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│                         OUTPUT                               │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  💾 crypto_data.db (DuckDB)                                 │
│                                                              │
│  Tables:                                                     │
│  • crypto_universe  → Historical rankings                   │
│  • spot             → OHLCV spot prices                     │
│  • futures          → OHLCV futures prices                  │
│                                                              │
│  📝 Query with SQL:                                         │
│                                                              │
│  SELECT symbol, close, volume                               │
│  FROM spot                                                   │
│  WHERE exchange = 'binance'                                 │
│    AND symbol = 'BTCUSDT'                                   │
│    AND interval = '1h'                                      │
│    AND timestamp >= '2024-01-01'                            │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

---

## ⚠️ Why This Project? Survivorship Bias

### The Problem

Imagine analyzing cryptos by **only taking today's top 100**. Your analysis completely ignores cryptocurrencies that were in the top 100 before but disappeared:

- **FTX Token (FTT)**: #25 in 2022, collapsed November 2022
- **Terra LUNA**: #10 in 2022, catastrophic crash May 2022
- **Bitconnect (BCC)**: Top crypto, scam revealed in 2018

If you don't include these cryptos in your backtest, your results will be **artificially optimistic** - this is **survivorship bias**.

### The Solution: CoinMarketCap + UNION Strategy

✅ **crypto-data** solves this problem by:

1. **Downloading historical rankings** via CoinMarketCap each month
2. **Using a UNION strategy**: retrieves ALL symbols that were in the top N **at any point** during the period

**Concrete Example**:
```python
# Top 100 over 12 months
get_symbols_from_universe(
    db_path='crypto_data.db',
    start_date='2024-01-01',
    end_date='2024-12-31',
    top_n=100
)
# Result: ~120-150 symbols
# (100 current + entries/exits from top 100)
```

You thus capture **the full market dynamics**: entries, exits, failures, delistings.

**Important for Backtesting**: Coins with data gaps at the end (delisted) should be kept in backtests even if they only appear in the training set. This reflects real market conditions and prevents survivorship bias - failed coins are part of the reality you must learn from.

---

## 🚀 Installation

### Install from GitHub (Recommended)

```bash
# Install latest version from main branch
pip install git+https://github.com/qu4ant/crypto-data.git

# Or install specific release version
pip install git+https://github.com/qu4ant/crypto-data.git@v4.0.0
```

### Install from Source

```bash
# Clone and install in development mode
git clone https://github.com/qu4ant/crypto-data.git
cd crypto-data
pip install -e .

# For development with testing tools
pip install -e ".[dev]"
```

### PyPI Installation

> **Coming Soon**: `pip install crypto-data` will be available once published to PyPI

**Requirements**: Python 3.10+, duckdb, aiohttp, pandas, pyarrow

---

## 💻 Quick Start

> **Tip**: Download 1-minute data once, then aggregate to your desired timeframe (5m, 1h, 4h...)
> with SQL. This is faster and more flexible than downloading multiple intervals separately.

### Option 1: Complete Workflow with `populate_database()`

The `populate_database()` function does everything in one call: downloads universe + OHLCV data.

```python
from crypto_data import populate_database, setup_colored_logging, DataType, Interval

# Colored logs (optional but recommended)
setup_colored_logging()

# Complete download
populate_database(
    db_path='crypto_data.db',
    start_date='2024-01-01',
    end_date='2024-12-31',
    top_n=100,                    # Top 100 by market cap
    interval=Interval.HOUR_1,     # Use Interval enum (MIN_5, HOUR_1, HOUR_4, DAY_1, etc.)
    data_types=[DataType.SPOT, DataType.FUTURES],  # Use DataType enum
    exclude_tags=['stablecoin', 'wrapped-tokens'],  # Optional filters
    exclude_symbols=['LUNA', 'FTT', 'UST']
)
```

**Console output:**

![Console output](console_output.png)

### Option 2: Step-by-Step

```python
import asyncio
from crypto_data import (
    ingest_universe,
    get_symbols_from_universe,
    ingest_binance_async,
    setup_colored_logging,
    DataType,
    Interval
)

setup_colored_logging()

# 1. Download CoinMarketCap rankings (async, parallel downloads)
asyncio.run(ingest_universe(
    db_path='crypto_data.db',
    months=['2024-01', '2024-02', '2024-03'],  # List of months to download
    top_n=100,
    exclude_tags=['stablecoin'],
    exclude_symbols=[]
))

# 2. Extract symbols with UNION strategy
symbols = get_symbols_from_universe(
    db_path='crypto_data.db',
    start_date='2024-01-01',
    end_date='2024-12-31',
    top_n=100
)

# 3. Download Binance data (async)
ingest_binance_async(
    db_path='crypto_data.db',
    symbols=symbols,
    start_date='2024-01-01',
    end_date='2024-12-31',
    data_types=[DataType.SPOT, DataType.FUTURES],  # Use DataType enum
    interval=Interval.HOUR_1  # Use Interval enum (MIN_5, HOUR_1, HOUR_4, DAY_1, etc.)
)
```

---

## 🔐 Type-Safe Enums

All data type and interval parameters use enums for type safety and IDE autocompletion.

```python
from crypto_data import DataType, Interval, Exchange

# DataType enum
DataType.SPOT           # Spot market OHLCV
DataType.FUTURES        # Futures market OHLCV
DataType.OPEN_INTEREST  # Futures open interest (daily)
DataType.FUNDING_RATES  # Funding rates (8h)

# Interval enum
Interval.MIN_1, MIN_5, MIN_15, MIN_30
Interval.HOUR_1, HOUR_2, HOUR_4, HOUR_6, HOUR_8, HOUR_12
Interval.DAY_1, DAY_3, WEEK_1, MONTH_1
```

> **Tip**: Prefer downloading `MIN_1` and aggregating with SQL to other timeframes.
> This avoids re-downloading data for each interval you need.

```python
# Exchange enum
Exchange.BINANCE  # Currently implemented
Exchange.BYBIT    # Planned
Exchange.KRAKEN   # Planned
```

> **📦 Data Source**: All data types download from **Binance Data Vision** (official historical archives).
> The pipeline uses **monthly ZIP files** for complete past months (faster, fewer HTTP requests) and
> **daily ZIP files** for current month days when monthly files aren't yet available.
> This is important for Open Interest and Funding Rates, as the Binance REST API only provides
> recent data (~6 months), while Data Vision has years of history.

---

## 📊 SQL Query Examples

Once data is downloaded, query directly with SQL (DuckDB, pandas, Jupyter...).

### 1. List Available Symbols

```sql
SELECT DISTINCT symbol, COUNT(*) as nb_rows
FROM spot
WHERE exchange = 'binance'
  AND interval = '1h'
GROUP BY symbol
ORDER BY nb_rows DESC;
```

### 2. Bitcoin Price History

```sql
SELECT
    timestamp,
    open,
    high,
    low,
    close,
    volume
FROM spot
WHERE exchange = 'binance'
  AND symbol = 'BTCUSDT'
  AND interval = '1h'
  AND timestamp >= '2024-01-01'
ORDER BY timestamp;
```

### 3. Join Universe + Prices (Market Cap)

```sql
SELECT
    u.date,
    u.symbol,
    u.rank,
    u.market_cap,
    s.close,
    s.volume
FROM crypto_universe u
JOIN spot s
  ON u.symbol || 'USDT' = s.symbol
  AND u.date = DATE_TRUNC('day', s.timestamp)
WHERE s.exchange = 'binance'
  AND s.interval = '1h'
  AND u.date >= '2024-01-01'
ORDER BY u.date, u.rank;
```

### 4. Volume Analysis by Exchange (Future)

```sql
-- Today: Binance only
-- Future: compare Binance vs Bybit vs Kraken
SELECT
    exchange,
    symbol,
    interval,
    SUM(volume) as total_volume
FROM spot
WHERE timestamp >= '2024-01-01'
GROUP BY exchange, symbol, interval
ORDER BY total_volume DESC;
```

### 5. Top 10 Cryptos by Volume (24h)

```sql
SELECT
    symbol,
    SUM(volume) as volume_24h,
    AVG(close) as avg_price
FROM spot
WHERE exchange = 'binance'
  AND interval = '1h'
  AND timestamp >= NOW() - INTERVAL '24 hours'
GROUP BY symbol
ORDER BY volume_24h DESC
LIMIT 10;
```

### 6. Check When a Coin Was in Top N Rankings

```sql
-- Find when TON entered/exited top 50
SELECT date, symbol, rank, market_cap
FROM crypto_universe
WHERE symbol = 'TON'
  AND date >= '2024-01-01'
  AND rank <= 50
ORDER BY date;
```

> **Important**: Use the `crypto_universe` table to check rankings, NOT the spot/futures tables. Progress bars show Binance data availability (e.g., TON data from August 2024), not when the coin entered top N (June 2024).

### 7. Query Multiple Intervals from Same Database

```sql
-- Compare 1h vs 4h data for the same symbol
SELECT
    interval,
    COUNT(*) as nb_candles,
    MIN(timestamp) as first_date,
    MAX(timestamp) as last_date
FROM spot
WHERE exchange = 'binance'
  AND symbol = 'BTCUSDT'
  AND interval IN ('1h', '4h')
GROUP BY interval;
```

> **Note**: You can store multiple intervals (5m, 1h, 4h, etc.) in the same database. Each interval is stored as a separate row with a different primary key.

---

## 🚀 Direct Database Exports (Faster than API)

Once data is ingested, you can query the `spot` and `futures` tables directly for your own exports - **much faster than calling Binance API repeatedly**.

### Use Cases

- 📊 Export to CSV/Parquet for machine learning pipelines
- 📈 Create custom aggregations (daily VWAP, rolling volatility, etc.)
- 🖥️ Build real-time dashboards with read-only connections
- 🔍 Cross-exchange analysis (future: compare Binance vs Bybit)

### Example: Export to Pandas DataFrame

```python
import duckdb

# Connect to database (read-only mode)
conn = duckdb.connect('crypto_data.db', read_only=True)

# Export BTC 1h data for 2024
df = conn.execute("""
    SELECT timestamp, open, high, low, close, volume
    FROM spot
    WHERE exchange = 'binance'
      AND symbol = 'BTCUSDT'
      AND interval = '1h'
      AND timestamp >= '2024-01-01'
    ORDER BY timestamp
""").df()

# Save to Parquet (fast, compressed)
df.to_parquet('btc_2024_1h.parquet')

conn.close()
```

### Performance

**DuckDB queries on local database are 10-100x faster than API calls**, with **no rate limits**.

- ✅ **No rate limiting**: unlimited queries
- ✅ **Instant access**: no network latency
- ✅ **Complex aggregations**: SQL-based analytics
- ✅ **Parquet export**: optimized for ML pipelines

---

## 🗄️ Database Schema (v4.0.0)

Single file: `crypto_data.db`

### Table `crypto_universe` - Historical Rankings

Stores CoinMarketCap rankings (top N by market cap).

| Column      | Type      | Description                                      |
|------------|-----------|--------------------------------------------------|
| `date`       | DATE      | Ranking date (monthly)                          |
| `symbol`     | VARCHAR   | Base symbol (BTC, ETH, not BTCUSDT)             |
| `rank`       | INTEGER   | Market cap ranking                              |
| `market_cap` | DOUBLE    | Market cap in USD                               |
| `categories` | VARCHAR   | CoinMarketCap tags (stablecoin, DeFi, etc.)     |

**Primary key**: `(date, symbol)`
**Index**: `(date, rank)`

> **Timestamp Interpretation**: A date like `2024-01-01 00:00:00.000` means the coin was in top N for the **ENTIRE month** of January 2024 (monthly snapshot taken on the 1st).
>
> **Update Frequency**: Snapshots are taken on the **1st of each month ONLY** (no daily/weekly updates). To backfill 12 months, you need 12 API calls.

### Tables `spot` and `futures` - OHLCV Data

Historical price data multi-exchange (currently Binance only).

| Column           | Type      | Description                                   |
|------------------|-----------|-----------------------------------------------|
| `exchange`        | VARCHAR   | Exchange ('binance', future: 'bybit', etc.)  |
| `symbol`          | VARCHAR   | Trading pair (BTCUSDT, ETHUSDT, etc.)        |
| `interval`        | VARCHAR   | Interval (5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w, 1M) |
| `timestamp`       | TIMESTAMP | Candle timestamp                             |
| `open`            | DOUBLE    | Open price                                   |
| `high`            | DOUBLE    | High price                                   |
| `low`             | DOUBLE    | Low price                                    |
| `close`           | DOUBLE    | Close price                                  |
| `volume`          | DOUBLE    | Volume in base asset                         |
| `quote_volume`    | DOUBLE    | Volume in quote asset (USDT)                 |
| `trades_count`    | INTEGER   | Number of trades                             |
| `taker_buy_*`     | DOUBLE    | Taker buy volumes                            |

**Primary key**: `(exchange, symbol, interval, timestamp)`
**Index**: `(exchange, symbol, interval, timestamp)`

> **Multi-Interval Support**: You can store multiple intervals (5m, 1h, 4h, etc.) in the same database simultaneously. Each interval is stored as a separate row with a different primary key. Query by filtering `WHERE interval = '5m'`.

---

## 🔧 Advanced Features

### 🤖 Intelligent Auto-Management

The pipeline automatically handles several data issues:

#### 1. **Variable Timestamp Formats**
- 2024: milliseconds (13 digits)
- 2025: microseconds (16 digits)
- ✅ Automatic detection and conversion

#### 2. **Inconsistent CSV Headers**
- Some files have headers, others don't
- ✅ Automatic detection by analyzing first line

#### 3. **1000-Prefix Tokens (PEPE, SHIB, BONK)**
- Futures: `1000PEPEUSDT`, Spot: `PEPEUSDT`
- ✅ Automatic retry with prefix, normalized in database

#### 4. **Delisting Detection**
- FTT delisted November 2022
- ✅ Stops after 3 consecutive failures (configurable threshold)

### 🎯 Design Decisions

**Multi-Exchange v4.0.0**: Schema ready for Bybit, Kraken, Coinbase
- `exchange` column in primary key
- Cross-exchange analysis, arbitrage detection, redundancy

**Rebrands = Separate Symbols**: MATIC→POL, RNDR→RENDER treated differently
- Reason: trading interruptions, separate files, different liquidity
- Solution: UNION queries to combine periods

**UNION Strategy**: captures ALL symbols in top N over the period
- ~120-150 symbols for top 100 over 12 months
- Avoids survivorship bias

**Explicit Parameters**: No hidden config files
- `exclude_tags` and `exclude_symbols` explicit in each call
- Better testability, zero hidden dependencies

---

## 🔄 Data Transformations

The pipeline applies the following transformations to raw Binance data. **No other modifications are made** - prices, volumes, and counts are stored exactly as received.

| Transformation | Raw Data | Stored Data | Reason |
|----------------|----------|-------------|--------|
| **Timestamp rounding** | `1704067499999` ms → `2024-01-01 00:04:59.999` | `2024-01-01 00:05:00` | Ceil to 1 second avoids `.999` milliseconds |
| **Timestamp unit** | Milliseconds (13 digits) or microseconds (16 digits) | datetime | Auto-detected via threshold `>= 5e12` |
| **1000-prefix symbols** | `1000PEPEUSDT` (futures file) | `PEPEUSDT` | Stored with original symbol for consistency |
| **Column names** | Mixed case (`Open`, `OPEN`) | lowercase (`open`) | Normalized for consistent queries |
| **Column rename** | `count` | `trades_count` | Some files use `count` instead |
| **Missing columns** | (absent) | `NULL` | `taker_buy_*` columns added as NULL if missing |
| **Duplicates** | Multiple rows same timestamp | Single row | Deduplicated on primary key before insert |

**What is NOT modified:**
- OHLC prices (open, high, low, close)
- Volume and quote_volume
- Trade counts
- Any numerical values
- Gaps in data remain as gaps - users must handle this in their analysis/backtesting

---

## ⚠️ Known Limitations

### Technical Constraints

**Single-Writer Only**: DuckDB supports only one write process at a time
- ✅ Unlimited concurrent reads OK
- ❌ Running multiple `populate_database()` in parallel → lock error
- **Solution**: Run one instance at a time

**Minimum Disk Space**: ~50GB recommended for top 100 over 1 year
- 5m interval: ~30GB (105k candles/month × 100 symbols)
- 1h interval: ~5GB
- Temp files during download: +10-20GB additional
- **Solution**: Use larger interval (1h/4h instead of 5m) or reduce `top_n`

**API Rate Limits**: CoinMarketCap free tier = 333 calls/day
- Universe ingestion: 1 call per month
- 12 months = 12 calls → OK
- **Limitation**: Don't run > 300 months in 1 day
- **Solution**: Use paid API key for massive historical datasets

### Re-run Behavior (Idempotency)

✅ **Safe**: Re-running multiple times is safe and idempotent

- **Universe**: DELETE + INSERT atomic per month (clean update)
- **Binance**: Automatic skip if data exists (`skip_existing=True` by default)
- **Transactions**: Atomic per symbol (all-or-nothing, automatic rollback on error)

⚠️ **Interruption**: If process killed mid-run (Ctrl+C, crash)

- Already committed data: preserved (safe)
- Data in progress: automatic rollback (safe)
- **Limitation**: No resume/checkpoint → restart from scratch
- **Workaround**: Divide into smaller monthly batches

### Data Validation

✅ **Corruption Protection**: Automatic validation since v4.0.0

- **Pre-import validation**: Pandera schema check BEFORE insertion (OHLC relationships, negative prices, etc.)
- **Download validation**: Content-Length check + ZIP integrity verification
- **Rejected files**: Logged with clear message (no silent import of invalid data)

❌ **No Automatic Retry**: Failed downloads require manual re-run

- Partial downloads/corrupt ZIPs → Returns False (not imported)
- **Solution**: Re-run `populate_database()` or `ingest_binance_async()` → skip existing + retry failed

### Data Source: Binance Data Vision vs REST API

This package downloads from **Binance Data Vision** (official ZIP archives), not the REST API.

| Data Type | Data Vision (this package) | REST API |
|-----------|---------------------------|----------|
| OHLCV (spot/futures) | Full history (years) | Full history |
| Open Interest | Full history (years) | Recent only (~30 days) |
| Funding Rates | Full history (years) | ~6 months max |

**Advantage**: You get complete historical data that's not available via REST API.

**Reference**: [Binance API Docs](https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Open-Interest-Statistics)

---

## 🔧 Troubleshooting

### Error: "Database is locked"

**Cause:** Multiple processes attempting to write simultaneously
**Solution:**
```bash
# Check only one instance is running
ps aux | grep python | grep crypto

# Kill conflicting processes if necessary
kill <PID>
```

### Error: "No space left on device"

**Cause:** Insufficient disk space (temp files + database)
**Solution:**
```python
# Option 1: Free up space
df -h  # Check available space

# Option 2: Use larger interval
populate_database(interval=Interval.HOUR_1)  # Instead of Interval.MIN_5

# Option 3: Reduce top_n
populate_database(top_n=50)  # Instead of 100
```

### Error: "429 Too Many Requests" (CoinMarketCap)

**Cause:** API rate limit exceeded (333 calls/day free tier)
**Solution:**
```python
# Wait 24h OR reduce number of months
ingest_universe(
    months=['2024-01', '2024-02'],  # Instead of 12+ months
    top_n=100
)
```

### Missing Data for Some Symbols

**Cause:** Delisting detected (`failure_threshold=3` by default)
**Normal behavior:** Stops after 3 consecutive missing months (e.g., FTT after Nov 2022)

```python
# To force complete download (ignore gaps)
ingest_binance_async(
    db_path='crypto_data.db',
    symbols=['FTTUSDT'],
    data_types=[DataType.SPOT],  # Use DataType enum
    interval=Interval.HOUR_1,
    failure_threshold=0  # Disable gap detection
)
```

### Error: "Data validation FAILED"

**Cause:** Corrupt source file (OHLC violation, negative prices, invalid ZIP)
**Solution:**
```bash
# 1. Check logs for details
# Example: "high < low" or "negative price"

# 2. Re-download (may be temporary)
python scripts/Download_data_universe.py

# 3. If persistent, report on GitHub Issues
# https://github.com/qu4ant/crypto-data/issues
```

### Slow Performance (Downloads)

**Cause:** Slow network or too much concurrency
**Solution:**
```python
# Reduce concurrency (default: 20 klines, 100 metrics)
ingest_binance_async(
    max_concurrent_klines=10,  # Instead of 20
    max_concurrent_metrics=50   # Instead of 100
)
```

---

## 📚 Additional Documentation

- 📖 [CLAUDE.md](CLAUDE.md) - Complete technical documentation for developers
- 📓 [Jupyter Notebook](exemples/explore_crypto_data.ipynb) - Query examples and visualizations
- 🐛 [GitHub Issues](https://github.com/qu4ant/crypto-data/issues) - Report a bug
- 🤝 [CONTRIBUTING.md](CONTRIBUTING.md) - Contribution guidelines

---

## 🧪 Tests

```bash
# All tests
pytest tests/ -v

# Basic tests only
pytest tests/database/test_database_basic.py -v

# With coverage
pytest tests/ --cov=crypto_data --cov-report=html
```

**Test Results**: 347 tests passing, 88.44% coverage

---

## 📝 License

**MIT License** - Copyright (c) 2025 Crypto Data Contributors

See [LICENSE](LICENSE) for details.

---

## 🤝 Contributing

Contributions are welcome! To contribute:

1. Fork the project
2. Create a feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

See [CONTRIBUTING.md](CONTRIBUTING.md) for detailed guidelines.

**Philosophy**: Simplicity > Features. This package does **one thing**: data ingestion. No loaders/readers/query helpers.

---

## ⚡ Why crypto-data?

✅ **Simple**: One function to download everything
✅ **Fast**: 20 parallel downloads
✅ **Reliable**: Automatic retry, intelligent error handling
✅ **Unbiased**: UNION strategy captures all historical symbols
✅ **SQL-first**: Direct queries, no unnecessary abstraction
✅ **Multi-exchange ready**: Schema v4.0.0 prepared for the future

---

**Developed with ❤️ for the quant/crypto community**
