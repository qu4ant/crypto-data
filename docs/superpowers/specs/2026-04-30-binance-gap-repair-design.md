# Binance Gap Repair Module — Design Spec

**Date**: 2026-04-30
**Status**: Approved (post-brainstorm)
**Scope**: New ingestion stage that fills time-series gaps left by Binance Data Vision, by fetching missing candles from the Binance REST API and inserting them idempotently.

---

## 1. Problem

Binance Data Vision archives are the primary source for `spot`, `futures`, and `funding_rates` tables. Some monthly/daily archives are incomplete or absent, producing real time gaps in the DuckDB tables. The existing `quality.audit_database()` already detects these as `time_gaps` findings (30 such findings on the current top-60 4h dataset).

Empirical check on `SOLUSDT` futures 4h confirmed the **Binance public REST API has every missing candle** that Data Vision lacked. Forward-fill is therefore unnecessary — we can repair from the same upstream source via a different transport.

## 2. Goals

- Provide a function and CLI that walks a DuckDB database, identifies kline and funding-rate gaps, fetches the missing rows from Binance REST, and inserts them idempotently.
- Stay strictly aligned with the existing schema, primary keys, and timestamp normalization rules — repaired rows must be indistinguishable from rows ingested via Data Vision.
- Be safe to re-run (idempotent) and report clearly which gaps could not be filled.

## 3. Non-goals (v1)

- **No `open_interest` repair.** The REST endpoint `/futures/data/openInterestHist` is capped at the last ~30 days (verified live: `D=30` OK, `D=31` returns `code -1130`). Out of scope. Existing OI gaps in the database remain visible via `quality.audit_database()`.
- **No replacement of `volume=0` rows.** A zero-volume candle is a legitimate "no trades" signal. The repair module never overwrites existing rows; it only fills holes.
- **No pagination across REST `limit`.** A single REST call covers up to ~250 days at 4h intervals. If a gap exceeds the per-call limit, the module raises a clear error directing the user to re-download the relevant Data Vision archive.
- **No dry-run mode.** `INSERT OR IGNORE` is idempotent; the `RepairReport` produced after a real run already shows exactly what happened.
- **No automatic re-audit after repair.** The user runs `validate_data_quality.py` separately when desired.
- **No coverage of "edge gaps"** (missing rows before the first or after the last DB timestamp for a symbol). These are not detectable by `LAG()` — out of scope; user re-runs the Data Vision pipeline with a wider date range.

## 4. Public API

```python
from crypto_data import repair_binance_gaps, RepairReport

report: RepairReport = repair_binance_gaps(
    db_path: str,
    *,
    tables: list[str] | None = None,        # default: ['spot', 'futures', 'funding_rates']
    symbols: list[str] | None = None,        # default: all symbols in DB
    intervals: list[str] | None = None,      # default: all intervals in DB
    max_concurrent: int = 5,
)
```

Pipeline opt-in:
```python
update_binance_market_data(..., repair_gaps_via_api: bool = False)  # default False
create_binance_database(..., repair_gaps_via_api: bool = False)     # default False
```

CLI:
```
uv run python scripts/repair_binance_gaps.py <db_path> \
    [--tables spot futures funding_rates] \
    [--symbol SOLUSDT --symbol BTCUSDT] \
    [--interval 4h] \
    [--max-concurrent 5] \
    [--json report.json]
```

## 5. Architecture

### 5.1 New files

| File | Responsibility |
|---|---|
| `src/crypto_data/clients/binance_rest.py` | Async HTTP client for Binance public REST. URL building, sémaphore concurrency, retry on 429/418/5xx with `Retry-After`, retry on aiohttp errors. Knows nothing about gaps or DataFrames. |
| `src/crypto_data/binance_repair.py` | Single module containing: gap enumerator (shared with `quality.py`), two repair strategies (klines, funding rates), `repair_binance_gaps()` orchestrator, `RepairReport` dataclass. |
| `scripts/repair_binance_gaps.py` | Thin CLI wrapper around `repair_binance_gaps()`. |

### 5.2 Modified files

| File | Change |
|---|---|
| `src/crypto_data/quality.py` | `_check_kline_gaps` and `_check_metric_gaps` now call the shared enumerator; public behavior of `audit_database()` is unchanged (same `QualityFinding` aggregates). |
| `src/crypto_data/binance_pipeline.py` | New parameter `repair_gaps_via_api: bool = False`. When true, calls `repair_binance_gaps()` after Data Vision import completes. |
| `src/crypto_data/database_builder.py` | Propagates `repair_gaps_via_api` into `create_binance_database()`. |
| `src/crypto_data/__init__.py` | Exports `repair_binance_gaps`, `RepairReport`, `UnrecoverableGap`. |

### 5.3 Why a single `binance_repair.py` file?

For a feature this scoped (~400 LoC total), splitting into a sub-package would be premature decomposition. The transport layer (`BinanceRestClient`) is the only piece that lives separately, because it is a generic HTTP utility that future code may reuse for non-repair needs.

## 6. Data flow

### 6.1 Klines gap

```
1. enumerate_kline_gaps(conn, table='futures', symbols=['SOLUSDT'], intervals=['4h'])
   → SQL window over (exchange, symbol, interval), partitioned LAG on timestamp
   → list[GapBoundary(table, symbol, interval, prev_close, next_close, expected_seconds)]

2. For each gap, dispatch to KlinesRepairStrategy(data_type, interval).

3. Strategy computes:
     start_ms = to_utc_ms(prev_close) + interval_ms
     end_ms   = to_utc_ms(next_close) - interval_ms - 1
   Guard: assert (start_ms - prev_close_ms) % interval_ms == 0, else raise.
   Guard: assert (end_ms - start_ms) <= REST_LIMIT * interval_ms, else raise
          ("gap too large, re-download Data Vision archive").

4. BinanceRestClient.fetch_spot_klines / fetch_futures_klines
   → 12-tuple list per Binance kline schema.

5. Strategy parses to DataFrame using the same FINAL_COLUMNS and timestamp
   convention as binance_datasets.klines.parse_csv:
     timestamp = pd.to_datetime((closeTime + 1) / 1000, unit='s').dt.ceil('1s')
     exchange = 'binance', symbol, interval

6. Trim to (prev_close, next_close) exclusive.

7. OHLCV_SCHEMA.validate(df) → SchemaError on unexpected payload.

8. Pipeline computes residuals:
     n_expected = (next_close - prev_close) // interval - 1     # candles strictly between
     expected_keys = {prev_close + i*interval for i in range(1, n_expected + 1)}
     missing_after = expected_keys - set(df['timestamp'])
   If missing_after non-empty:
     report.unrecoverable_gaps.append(
       UnrecoverableGap(..., reason='partial_fill', missing_count=len(missing_after))
     )

9. Idempotent insert in its own transaction:
     conn.execute("BEGIN")
     n = conn.execute(f"""
         SELECT COUNT(*) FROM df AS i
         LEFT JOIN {table} AS e ON e.exchange=i.exchange AND e.symbol=i.symbol
                                AND e.interval=i.interval AND e.timestamp=i.timestamp
         WHERE e.exchange IS NULL
     """).fetchone()[0]
     conn.execute(f"INSERT OR IGNORE INTO {table} SELECT * FROM df")
     conn.execute("COMMIT")
     report.inserted_rows[table] += n
```

### 6.2 Funding rates gap

Identical shape. Differences:
- Endpoint `/fapi/v1/fundingRate`.
- Response is a list of dicts `{symbol, fundingTime, fundingRate, markPrice}`.
- `timestamp = pd.to_datetime(fundingTime, unit='ms')` (no `+1`, no ceil — funding_time is the event timestamp itself).
- `markPrice == ""` is normalized to `NaN` before validation.
- Validation against `FUNDING_RATES_SCHEMA`.
- Idempotent insert join uses primary key `(exchange, symbol, timestamp)` — no `interval` column, since `funding_rates` table has no per-interval partition.
- `expected_seconds = 8 * 3600` is the assumed funding cadence (matches `quality._check_metric_gaps`).

### 6.3 Concurrency

`asyncio.gather` on N gaps in parallel, capped by `BinanceRestClient.max_concurrent` (default 5). Each gap is one REST call. The semaphore lives in the client.

### 6.4 Idempotence

Three layers:
1. The fetch window `(prev_close, next_close)` exclusive — the API never returns a candle already in DB.
2. DataFrame trim on the same exclusive interval as a guard.
3. `INSERT OR IGNORE` final.

## 7. Error handling

### 7.1 HTTP errors (in `BinanceRestClient`)

| Condition | Policy |
|---|---|
| `200 OK` body `[]` | Return `[]`. Pipeline classifies as `partial_fill`. |
| `429` | Read `Retry-After`, sleep, retry up to 5 times. Then raise `BinanceRateLimitError`. |
| `418` (IP banned) | Read `Retry-After`, sleep, retry **once**. Then raise — pipeline aborts (returns partial report with this in `errors`). |
| `5xx` | Backoff 1s, 2s, 4s — 3 retries. Then raise. |
| `aiohttp.ClientError` (timeout, conn reset, DNS) | Backoff 1s, 2s, 4s — 3 retries. Then raise. |
| `4xx` other than 429/418 | No retry. Raise immediately. |

There is **no preemptive throttling** based on `X-MBX-USED-WEIGHT-1m`. Realistic load (≤ a few hundred calls per repair run) is well below Binance caps; reactive 429 handling is the only safety net needed.

### 7.2 Pipeline-level error classification

| Outcome | `RepairReport` field |
|---|---|
| All expected candles inserted | `gaps_fully_repaired` += 1, `inserted_rows[table]` += n |
| API returned fewer candles than expected (200 OK, including `[]`) | `unrecoverable_gaps.append(reason='partial_fill', missing_count=...)` |
| Any fetch-time failure: HTTP final error after retries, Pandera `SchemaError`, parse exception, off-grid guard violation | `unrecoverable_gaps.append(reason='network_error')`, raw message in `errors` |

Only two reasons. `network_error` is a catch-all for "the gap could not be processed end-to-end"; the message in `errors` carries the precise cause. The mechanical distinction between "delisted" and "trading halt" within `partial_fill` is not modeled because it doesn't change user action.

### 7.3 1000-prefix symbols (PEPE, SHIB, BONK, …)

The DB stores the original symbol (e.g. `PEPEUSDT`). Binance REST may require the prefixed form (`1000PEPEUSDT`). Probe pattern, local to each repair run:
1. First call uses the DB symbol as-is.
2. On HTTP 400 "Invalid symbol", retry with `1000` prefix.
3. Cache the resolved name for the rest of the run (local `dict`, not the global `_ticker_mappings` of `binance_downloader`).

### 7.4 Schema-aligned guards

- `(start_ms - prev_close_ms) % interval_ms != 0` → raise (off-grid prev_close, would corrupt timeline). Per §7.2, this is classified as `network_error` with the precise message in `errors`.
- `(end_ms - start_ms) > REST_LIMIT * interval_ms` → raise with explicit message ("gap too large, re-download via Data Vision"). Same classification.
- API row with `volume < 0` or `high < low` → Pandera `SchemaError`, no insert, classified as `network_error` with the Pandera message in `errors`.

## 8. Logging

- `INFO`: repair start/end, summary, per-table counts.
- `WARNING`: 429 received and retried, gap classified unrecoverable.
- `ERROR`: 5xx final, schema validation failure, DB error.
- `DEBUG`: each request URL, response size, per-gap detail.

Reuses existing `setup_colored_logging` / `get_logger(__name__)`.

## 9. Testing

### 9.1 Layout

```
tests/repair/
├── __init__.py
├── conftest.py                     # mock_rest_client, db_with_kline_gap, db_with_funding_gap
├── test_enumerator.py
├── test_strategies.py              # klines + funding combined
├── test_pipeline.py
├── test_binance_rest_client.py
└── test_repair_network.py          # @pytest.mark.network
```

### 9.2 Unit coverage

`test_enumerator.py`:
- 0 rows → `[]`.
- 1 row → `[]` (no boundary).
- 2 contiguous rows → `[]`.
- 2 rows separated by exactly the expected interval → `[]`.
- 2 rows separated by 2× the interval → 1 `GapBoundary`.
- Multi-symbol / multi-interval correctness.
- Filters by `symbols=...` and `intervals=...`.
- **Regression**: `quality._check_kline_gaps` produces identical findings before/after enumerator refactor.

`test_strategies.py`:
- Authentic 12-tuple Binance payload → DataFrame with `FINAL_COLUMNS`, dtypes correct.
- `timestamp = (closeTime + 1) / 1000 ceil('1s')` matches `parse_csv` convention.
- Trim is exclusive — boundary candles never present in the output.
- API returns `[]` → empty DataFrame, no exception.
- API returns candle with `volume=0` → kept (alignment with v1 decision).
- Pandera `SchemaError` propagates on payload corruption.
- Off-grid `prev_close` → guard raises.
- Gap exceeding REST limit → guard raises with explicit message.
- Funding payload with `markPrice=""` → normalized to NaN, passes validation.

`test_binance_rest_client.py`:
- URL building (spot, futures, funding) correct.
- Sémaphore enforces `max_concurrent` (mock counter).
- 429 + `Retry-After: 2` → 1 retry after sleep, then success.
- 429 × 5 → `BinanceRateLimitError`.
- 418 + `Retry-After` → 1 retry max, then raise.
- 5xx → 3 retries with backoff, then raise.
- Timeout → 3 retries.
- 4xx other than 429/418 → no retry, immediate raise.

`test_pipeline.py`:
- Happy path: 1 gap, full fill, `gaps_fully_repaired=1`, `unrecoverable_gaps=[]`, correct insert count.
- Partial fill: `partial_fill` with correct `missing_count`.
- Empty response: 1 `partial_fill` with `missing_count == expected`.
- Final 429: 1 `network_error`, other gaps continue.
- Multi-symbol/multi-table: report aggregates correctly.
- `tables=['futures']` filter respected.
- 1000-prefix probe: first call 400 → second call with prefix → success.
- Idempotence: run twice → second run inserts 0.

### 9.3 Network test (opt-in)

```python
@pytest.mark.network
def test_repair_real_solusdt_futures_gap(tmp_path):
    """Live API call against a known SOLUSDT futures gap."""
```

Skipped by default. Configured via:
```toml
[tool.pytest.ini_options]
markers = ["network: live HTTP calls to Binance REST (-m network to enable)"]
addopts = "-m 'not network'"
```

### 9.4 Coverage targets

- `binance_repair.py`: ≥85%.
- `clients/binance_rest.py`: ≥85% (full retry/error matrix).

## 10. Sequence: end-to-end use

```
1. user installs repair on existing DB:
     uv run python scripts/repair_binance_gaps.py crypto_top60_h4_2022_01_2026_04.db --json repair_report.json

2. CLI parses args, calls repair_binance_gaps(...).

3. repair_binance_gaps():
     a. Connect DuckDB read-write.
     b. Enumerate gaps for each requested table.
     c. asyncio.gather over gaps with max_concurrent.
     d. Per gap: strategy.fetch_repair_rows → validate → trim → insert in own transaction.
     e. Build RepairReport.
     f. Close DB.

4. CLI prints report.summary() and writes report.to_jsonable() to --json path.

5. user re-runs validate_data_quality.py to confirm time_gaps reduction.
```

## 11. Backwards compatibility

- No schema change.
- No public API removal or rename.
- New optional kwarg `repair_gaps_via_api` defaults to `False` — no behavior change for existing callers.
- `quality.py` refactor is internal; public `audit_database()`, `QualityFinding`, `format_findings`, `findings_to_jsonable` unchanged. Regression test asserts identical findings on the top-60 4h dataset.

## 12. Open items deferred to future work

- Open interest repair would require a multi-source strategy (REST for last 30d, alternative provider or re-download for older). Out of scope until a concrete need emerges.
- Pagination beyond `REST_LIMIT * interval_ms` per gap. Currently raises with explicit guidance; revisit only if a real-world gap exceeds this bound.
- Edge gaps (before first / after last DB timestamp). Currently invisible to `LAG()`; revisit if a use case appears.
