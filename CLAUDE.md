# CLAUDE.md

## Project Overview

**Crypto Data v5.0.0** - DuckDB ingestion pipeline for cryptocurrency market data.

- Downloads OHLCV from Binance Data Vision + universe rankings from CoinMarketCap
- **Ingestion-only** package - users query directly with DuckDB SQL
- CLI: `uv run python scripts/Download_data_universe.py`

## Quick Start

```python
from crypto_data import create_binance_database, DataType, Interval, setup_colored_logging

setup_colored_logging()  # Logs to ./logs/crypto_data_YYYY-MM-DD_HH-MM-SS.log

create_binance_database(
    db_path='crypto_data.db',
    start_date='2024-01-01',
    end_date='2024-12-31',
    top_n=100,
    interval=Interval.MIN_5,  # MIN_1, MIN_5, MIN_15, HOUR_1, HOUR_4, DAY_1, etc.
    data_types=[DataType.SPOT, DataType.FUTURES],  # OPEN_INTEREST, FUNDING_RATES
    exclude_tags=['stablecoin', 'wrapped-tokens'],
    exclude_symbols=['LUNA', 'FTT']
)
```

**Testing**: `pytest tests/ -v` | **Install**: `uv sync`

## Public API

| Function | Description |
|----------|-------------|
| `create_binance_database(...)` | End-to-end workflow |
| `update_coinmarketcap_universe(db_path, dates=..., ...)` | Async CMC rankings |
| `update_binance_market_data(db_path, symbols, ...)` | Async Binance OHLCV (20 concurrent) |
| `get_binance_symbols_from_universe(db_path, start, end, top_n)` | UNION dataset symbol extraction |
| `CryptoDatabase` | Schema management, context manager |
| `setup_colored_logging()` | File + console logging |

**Enums** (v5.0.0+): `DataType`, `Interval` - required instead of strings.

## Database Schema

**Tables** in single DuckDB file:

| Table | Primary Key | Notes |
|-------|-------------|-------|
| `crypto_universe` | `(provider, provider_id, date)` | CoinMarketCap-enriched snapshots; columns: `provider`, `provider_id` (cmc_id), `date`, `symbol` (mutable, not identity), `name`, `slug`, `rank`, `market_cap`, `fully_diluted_market_cap`, `circulating_supply`, `max_supply`, `tags`, `platform`, `date_added` |
| `spot`, `futures` | `(exchange, symbol, interval, timestamp)` | Binance-only; `exchange` is constrained to `binance`; `timestamp` is candle close time |

**OHLCV columns**: exchange, symbol, interval, timestamp, open, high, low, close, volume, quote_volume, trades_count, taker_buy_*
- `timestamp` is the normalized candle close-time key

## Design Decisions

**UNION Approach**: `get_binance_symbols_from_universe()` returns ALL symbols that appeared in top N at ANY point
- ~120-150 symbols for top 100 over 12 months (captures entries/exits)
- Avoids survivorship bias - intentional behavior
- Download coverage only; point-in-time membership must still be filtered from `crypto_universe` snapshot by snapshot / rebalance by rebalance

**Rebrands as Separate Symbols**: MATIC→POL treated as different coins (user must UNION)

**Explicit Parameters Only**: No config files - `exclude_tags` and `exclude_symbols` as direct params

**Binance-only pipeline**: schema keeps an `exchange` column set to `binance` for explicit provenance.

## Auto-Handled Edge Cases

| Issue | Solution |
|-------|----------|
| 1000-prefix tokens (PEPE, SHIB) | Auto-retry with prefix on 404, store as original symbol |
| Timestamp format (ms vs μs) | Auto-detection threshold `>= 5e12` |
| CSV header inconsistencies | Auto-detect by analyzing first line |
| Delisting detection | Stop after 3 consecutive 404s (`failure_threshold`) |
| Duplicate timestamps | Automatic deduplication |

**Not auto-handled**: Rebrands, network failures, disk space - user must handle.

## Limitations

- **Single-writer**: DuckDB allows one writer at a time
- **No checkpoint/resume**: Re-run uses `skip_existing=True`
- **No disk space checks**: 5m/100 symbols/1 year ≈ 30GB
- **CMC rate limit**: configurable sliding-window limiter (default 200 calls/24h)
- **Memory**: 8GB+ recommended for large imports

## Validation

Pre-import Pandera validation: OHLC relationships, non-negative values, data types.
Post-import: `python scripts/check_data_quality_pandera.py --db-path crypto_data.db`

Schemas: `OHLCV_SCHEMA`, `OPEN_INTEREST_SCHEMA`, `FUNDING_RATES_SCHEMA`, `UNIVERSE_SCHEMA`

## SQL Notes

```sql
-- Case-sensitive LIKE: use ILIKE or LOWER()
-- Universe JOIN: u.symbol || 'USDT' = s.symbol  (symbol is mutable, not identity)
-- Track an asset across symbol renames via provider_id.
-- Check top N status (don't use spot/futures for this):
SELECT date, symbol, rank FROM crypto_universe WHERE provider_id = 11419 AND rank <= 50;
```

## Environment

```bash
uv sync                    # Install
uv run python script.py    # Run
pytest tests/ -v           # Test
```

**Deps**: duckdb, pandas, aiohttp, pandera | **Dev**: pytest, pytest-cov, pytest-asyncio

## Coding Standards

- Vectorize operations (pandas/numpy), explicit type hints
- pytest for all tests
- **Philosophy**: Simplicity over features, SQL over abstraction
