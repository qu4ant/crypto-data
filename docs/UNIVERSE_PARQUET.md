# Universe Data in Parquet Format

This guide explains how to use the **standalone CoinMarketCap universe fetcher** that saves data to Parquet files instead of DuckDB.

## Overview

The `fetch_universe.py` script is completely **decoupled** from Binance and DuckDB:

- ✅ **Standalone**: No dependencies on Binance APIs or validation
- ✅ **Portable**: Parquet format is universal (pandas, DuckDB, Spark, etc.)
- ✅ **Simple**: Pure CoinMarketCap → Parquet pipeline
- ✅ **Efficient**: Automatic deduplication and compression
- ✅ **No bias**: Captures ALL cryptocurrencies (no survivorship bias)

## Quick Start

### Fetch Single Snapshot

```bash
python scripts/fetch_universe.py \
    --date 2024-01-01 \
    --top-n 100
```

### Fetch Monthly Range

```bash
python scripts/fetch_universe.py \
    --start 2022-01-01 \
    --end 2024-12-31 \
    --top-n 100
```

### Custom Output Path

```bash
python scripts/fetch_universe.py \
    --date 2024-01-01 \
    --top-n 50 \
    --output data/universe.parquet
```

## Configuration

Edit [config/universe_parquet_config.yaml](../config/universe_parquet_config.yaml) to exclude specific tag categories:

```yaml
exclude_categories:
  - Stablecoins
  - USD Stablecoin
  - Wrapped-Tokens
  - Privacy
  # Add more tags as needed
```

The script will automatically filter out cryptocurrencies matching ANY of these tags.

## Data Schema

The Parquet file contains the following columns:

| Column | Type | Description |
|--------|------|-------------|
| `date` | datetime | Snapshot date (monthly, 1st of month) |
| `symbol` | string | Cryptocurrency ticker (BTC, ETH, etc.) |
| `rank` | int | CoinMarketCap rank by market cap |
| `market_cap` | float | Market capitalization in USD |
| `categories` | string | Comma-separated tags (layer-1, defi, etc.) |

## Usage Examples

### Python (Pandas)

```python
import pandas as pd

# Load universe
df = pd.read_parquet('crypto_universe.parquet')

# Get top 50 coins for specific date
top_50 = df[
    (df['date'] == '2024-01-01') &
    (df['rank'] <= 50)
]

# Get all symbols that were in top 100 during 2024
symbols_2024 = df[
    (df['date'] >= '2024-01-01') &
    (df['date'] <= '2024-12-31') &
    (df['rank'] <= 100)
]['symbol'].unique()

print(f"Found {len(symbols_2024)} unique symbols in top 100 during 2024")
```

### DuckDB (Direct Query)

```python
import duckdb

conn = duckdb.connect()

# Query Parquet directly (no import needed!)
result = conn.execute("""
    SELECT
        symbol,
        AVG(rank) as avg_rank,
        AVG(market_cap) as avg_market_cap
    FROM read_parquet('crypto_universe.parquet')
    WHERE date >= '2024-01-01'
      AND date <= '2024-12-31'
      AND rank <= 50
    GROUP BY symbol
    ORDER BY avg_rank
""").df()

print(result.head(10))
```

### Integration with Binance Data

Use the universe to select symbols for Binance download:

```python
import pandas as pd

# Load universe
df_universe = pd.read_parquet('crypto_universe.parquet')

# Get UNION of top 50 symbols during period (captures coins that entered/exited top 50)
symbols = df_universe[
    (df_universe['date'] >= '2024-01-01') &
    (df_universe['date'] <= '2024-12-31') &
    (df_universe['rank'] <= 50)
]['symbol'].unique()

# Add USDT suffix for Binance
binance_symbols = [f"{s}USDT" for s in symbols]

print(f"Will download {len(binance_symbols)} symbols:")
print(binance_symbols[:10])  # First 10
```

## Features

### Automatic Deduplication

If you re-run the script for an existing date, it automatically replaces the old snapshot:

```bash
# First run - creates snapshot
python scripts/fetch_universe.py --date 2024-01-01 --top-n 100

# Second run - replaces snapshot (not duplicates!)
python scripts/fetch_universe.py --date 2024-01-01 --top-n 100
```

### Skip Existing Snapshots

By default, the script skips snapshots that already exist (saves API calls):

```bash
# This will skip 2024-01-01 if it already exists
python scripts/fetch_universe.py \
    --start 2024-01-01 \
    --end 2024-03-01 \
    --top-n 100
```

### Tag Filtering

The config file [universe_parquet_config.yaml](../config/universe_parquet_config.yaml) contains sensible defaults:

- ✅ **Excluded by default**: Stablecoins, wrapped tokens, privacy coins, RWAs
- ❌ **Not excluded**: Meme coins, gaming tokens, NFTs (commented out)

You can customize this based on your analysis needs.

## File Size Estimates

For **36 months × 100 coins** (typical backfill):

- **Records**: ~3,600 (100 coins × 36 snapshots)
- **File size**: ~100-200 KB (with Snappy compression)
- **Load time**: < 50ms (pandas.read_parquet)

The Parquet format is extremely efficient for this type of data.

## Comparison with DuckDB Version

| Aspect | Old (DuckDB) | New (Parquet) |
|--------|--------------|---------------|
| **Storage** | `crypto_universe.db` | `crypto_universe.parquet` |
| **Binance validation** | ✅ Yes (survivorship bias!) | ❌ No (complete data) |
| **Dependencies** | DuckDB required | Pandas + PyArrow |
| **Portability** | Binary DB format | Standard Parquet |
| **File size** | ~200-500 KB | ~100-200 KB |
| **Query support** | SQL only | SQL + pandas + any tool |
| **Versionable** | ❌ Hard to git | ✅ Can use Git LFS |

## Advanced Usage

### Custom Config File

```bash
python scripts/fetch_universe.py \
    --date 2024-01-01 \
    --top-n 100 \
    --config my_custom_config.yaml
```

### Backfill Historical Data (FREE!)

CoinMarketCap's API is free for historical data:

```bash
# Fetch 5 years of monthly snapshots
python scripts/fetch_universe.py \
    --start 2020-01-01 \
    --end 2024-12-31 \
    --top-n 250
```

**Estimated time**: ~30 minutes for 60 snapshots (0.5 min/snapshot)

### File Statistics

The script automatically displays stats at the end:

```
Parquet File Statistics:
  Total records: 3,600
  Snapshots: 36
  Date range: 2022-01-01 to 2024-12-31
  Unique symbols: 150
  File size: 0.15 MB
```

## Troubleshooting

### Rate Limits (429 Error)

The script automatically retries with 60-second delays. If you hit rate limits frequently:

1. Reduce batch size (fetch smaller date ranges)
2. Add delays between runs
3. The free API should handle ~1 request/second

### Server Errors (500/503)

The script retries with 5-second delays. These are usually temporary.

### Missing Snapshots

If some snapshots fail, just re-run the script:

```bash
# Re-run same command - only failed snapshots will be fetched
python scripts/fetch_universe.py \
    --start 2022-01-01 \
    --end 2024-12-31 \
    --top-n 100
```

## Next Steps

After fetching the universe:

1. **Analyze universe data** (see pandas/DuckDB examples above)
2. **Download Binance data** using `populate_binance.py` (work in progress)
3. **Join universe + OHLCV** for complete analysis

## Questions?

- Check [CLAUDE.md](../CLAUDE.md) for overall project documentation
- Review [config/universe_parquet_config.yaml](../config/universe_parquet_config.yaml) for filtering options
- Inspect the code at [src/crypto_data/universe.py](../src/crypto_data/universe.py)
