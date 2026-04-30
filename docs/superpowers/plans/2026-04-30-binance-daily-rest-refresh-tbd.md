# Binance Daily REST Refresh — TBD Plan

**Date:** 2026-04-30
**Status:** TBD
**Scope:** Lightweight daily update path for keeping a DuckDB Binance database current without WebSockets.

---

## Problem

Binance Data Vision remains a good source for bulk historical data, but daily archives are not always available or complete early the next day. The current REST gap repair flow fixes internal gaps that are bounded by existing rows, but it does not detect or fill the trailing edge when the latest candles are simply missing from the database.

For daily freshness, the database needs a small append-style REST update that fetches the most recent closed candles and funding-rate rows, then writes them idempotently.

## Recommendation

Use a hybrid ingestion model:

1. **Data Vision** for bulk history, backfills, monthly archives, and occasional reconciliation.
2. **Binance REST** for daily incremental updates over a short trailing window.
3. **Existing REST gap repair** after the trailing update, to repair any internal holes introduced by late or partial archive availability.

No WebSocket is needed for this use case.

## Proposed Daily Workflow

Run once per day after the UTC daily boundary, for example between `00:15` and `01:00 UTC`.

For each tracked symbol:

1. Fetch the last `48h` to `72h` of closed spot klines for the selected interval, for example `1h`.
2. Fetch the last `48h` to `72h` of closed futures klines for the same interval.
3. Fetch the last `72h` of futures funding rates.
4. Insert rows with the existing primary-key semantics using `INSERT OR IGNORE`.
5. Run `repair_binance_gaps()` to fix bounded internal holes.
6. Run `scripts/validate_data_quality.py` and store the report.

This should stay light for a top-60 universe: roughly one REST call per symbol/table per day, capped with conservative concurrency such as `--max-concurrent 5`.

## Proposed CLI

```bash
uv run python scripts/update_binance_recent_rest.py crypto_data.db \
  --lookback-days 3 \
  --interval 1h \
  --tables spot futures funding_rates \
  --max-concurrent 5 \
  --json daily_update_report.json
```

Optional follow-up quality check:

```bash
uv run python scripts/validate_data_quality.py crypto_data.db \
  --tables spot futures funding_rates \
  --interval 1h \
  --json daily_quality_report.json
```

## Proposed Python API

```python
from crypto_data import update_binance_recent_rest

report = update_binance_recent_rest(
    "crypto_data.db",
    lookback_days=3,
    interval="1h",
    tables=["spot", "futures", "funding_rates"],
    max_concurrent=5,
)
```

## Design Notes

- Reuse `BinanceRestClient` from `src/crypto_data/clients/binance_rest.py`.
- Reuse the kline and funding-rate payload parsing rules from `src/crypto_data/binance_repair.py`.
- Keep writes idempotent with the same database primary keys:
  - `spot` / `futures`: `(exchange, symbol, interval, timestamp)`
  - `funding_rates`: `(exchange, symbol, timestamp)`
- Fetch a trailing window instead of only "yesterday" so delayed API/archive availability or interrupted jobs self-heal on the next run.
- Trim parsed REST payloads to closed candles only before insert.
- Keep REST fetches concurrent and DB writes sequential, matching the repair module's safety model.
- Do not include `open_interest` in v1; Binance historical open interest REST coverage is limited.

## Open Decisions

- Default interval: likely `1h`, but should remain configurable.
- Default lookback: `3` days is a practical starting point.
- Symbol source:
  - default to symbols already present in the database;
  - optionally allow repeated `--symbol BTCUSDT` filters.
- Scheduling:
  - document a cron example;
  - keep the package itself scheduler-agnostic.
- Whether this should be wired into `create_binance_database()` / `update_binance_market_data()` or remain a dedicated daily script.

## Acceptance Criteria

- Running the daily REST updater twice inserts rows on the first run and zero duplicates on the second run.
- A database missing the latest closed candles becomes current after the updater runs.
- The updater works for `spot`, `futures`, and `funding_rates`.
- The updater reports per-table inserted row counts, fetch counts, and unrecoverable errors.
- Existing `repair_binance_gaps()` remains focused on internal bounded gaps.
- Existing Data Vision ingestion remains the preferred bulk-history path.

## Suggested Tests

- Unit test trailing kline window calculation for `1h` and `4h`.
- Unit test funding-rate window calculation.
- Unit test idempotent insert behavior on a small DuckDB fixture.
- Unit test symbol discovery from existing DB tables.
- Validation test against a temporary DB and live Binance REST, guarded by the existing `--run-validation` marker.
