# Binance Gap Repair Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a repair stage that fills time-series gaps in DuckDB tables (`spot`, `futures`, `funding_rates`) by fetching missing candles/rows from the Binance public REST API and inserting them idempotently.

**Architecture:** Single module `binance_repair.py` (gap enumerator + 2 repair strategies + orchestrator + report dataclass) plus a separate transport client `clients/binance_rest.py`. Existing `quality.py` is refactored to share the gap enumerator with the repair module. New CLI script `scripts/repair_binance_gaps.py`. New optional flag `repair_gaps_via_api` plumbed through `update_binance_market_data` and `create_binance_database`.

**Tech Stack:** Python 3.11+, async (`aiohttp` + `asyncio`), DuckDB, Pandas, Pandera, Pytest. Reuses existing `OHLCV_SCHEMA`, `FUNDING_RATES_SCHEMA`, `FINAL_COLUMNS` from `binance_datasets.klines`.

**Reference spec:** `docs/superpowers/specs/2026-04-30-binance-gap-repair-design.md`

**Note on test markers:** The plan uses the existing project `validation` marker (with `--run-validation` flag, defined in `tests/conftest.py`) for the live-API test, rather than introducing a new `network` marker. This keeps a single project convention.

---

## File Structure

**New files:**
- `src/crypto_data/clients/binance_rest.py` — async HTTP client (URL building, semaphore, retry matrix)
- `src/crypto_data/binance_repair.py` — `GapBoundary`, `enumerate_kline_gaps`, `enumerate_metric_gaps`, `KlinesRepairStrategy`, `FundingRatesRepairStrategy`, `repair_binance_gaps()`, `RepairReport`, `UnrecoverableGap`
- `scripts/repair_binance_gaps.py` — CLI wrapper
- `tests/repair/__init__.py`
- `tests/repair/conftest.py` — `mock_rest_client`, `db_with_kline_gap`, `db_with_funding_gap` fixtures
- `tests/repair/test_enumerator.py`
- `tests/repair/test_strategies.py`
- `tests/repair/test_binance_rest_client.py`
- `tests/repair/test_pipeline.py`
- `tests/repair/test_repair_validation.py` — `@pytest.mark.validation` live-API test

**Modified files:**
- `src/crypto_data/quality.py` — `_check_kline_gaps` and `_check_metric_gaps` consume the new shared enumerator; public `audit_database()` behavior unchanged
- `src/crypto_data/binance_pipeline.py` — new `repair_gaps_via_api: bool = False` parameter
- `src/crypto_data/database_builder.py` — propagates `repair_gaps_via_api` into `create_binance_database()`
- `src/crypto_data/__init__.py` — re-exports `repair_binance_gaps`, `RepairReport`, `UnrecoverableGap`
- `tests/quality/test_quality.py` — regression test that audit findings are unchanged after the enumerator refactor

---

## Task 1: Test scaffolding

**Files:**
- Create: `tests/repair/__init__.py`
- Create: `tests/repair/conftest.py`

- [ ] **Step 1: Create empty package marker**

```python
# tests/repair/__init__.py
```

- [ ] **Step 2: Create conftest with shared DuckDB fixtures**

```python
# tests/repair/conftest.py
"""Shared fixtures for the binance_repair tests."""

from __future__ import annotations

import duckdb
import pandas as pd
import pytest

from crypto_data.database import CryptoDatabase


def _insert_klines(conn, table: str, symbol: str, interval: str, timestamps: list[pd.Timestamp]) -> None:
    """Insert simple kline rows. All numeric columns set to 1.0."""
    rows = [
        (
            "binance", symbol, interval, ts,
            1.0, 1.0, 1.0, 1.0,    # open, high, low, close
            1.0, 1.0,              # volume, quote_volume
            1,                     # trades_count
            1.0, 1.0,              # taker_buy_base_volume, taker_buy_quote_volume
        )
        for ts in timestamps
    ]
    conn.executemany(
        f"INSERT INTO {table} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        rows,
    )


def _insert_funding(conn, symbol: str, timestamps: list[pd.Timestamp]) -> None:
    rows = [("binance", symbol, ts, 0.0001) for ts in timestamps]
    conn.executemany(
        "INSERT INTO funding_rates VALUES (?, ?, ?, ?)",
        rows,
    )


@pytest.fixture
def empty_db(tmp_path):
    """Empty CryptoDatabase, all tables created."""
    db_path = tmp_path / "test.db"
    db = CryptoDatabase(str(db_path))
    yield db
    db.close()


@pytest.fixture
def db_with_kline_gap(empty_db):
    """
    futures table with 5 rows pre-gap, 5 rows post-gap, 4h interval.
    Gap between 2024-01-01T20:00 and 2024-01-02T16:00 (4 missing candles).
    """
    pre = pd.date_range("2024-01-01T00:00", periods=5, freq="4h")
    post = pd.date_range("2024-01-02T16:00", periods=5, freq="4h")
    _insert_klines(empty_db.conn, "futures", "SOLUSDT", "4h", list(pre) + list(post))
    return empty_db


@pytest.fixture
def db_with_funding_gap(empty_db):
    """
    funding_rates table with 4 rows pre-gap, 4 rows post-gap, 8h cadence.
    Gap between 2024-01-02T08:00 and 2024-01-03T08:00 (2 missing entries).
    """
    pre = pd.date_range("2024-01-01T00:00", periods=4, freq="8h")
    post = pd.date_range("2024-01-03T08:00", periods=4, freq="8h")
    _insert_funding(empty_db.conn, "BTCUSDT", list(pre) + list(post))
    return empty_db
```

- [ ] **Step 3: Verify the fixtures load**

Run: `pytest tests/repair/ -v --collect-only`
Expected: pytest collects 0 tests (no test files yet) but no import error.

- [ ] **Step 4: Commit**

```bash
git add tests/repair/__init__.py tests/repair/conftest.py
git commit -m "test: scaffold tests/repair/ with shared fixtures"
```

---

## Task 2: GapBoundary + enumerate_kline_gaps (TDD)

**Files:**
- Create: `src/crypto_data/binance_repair.py`
- Create: `tests/repair/test_enumerator.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/repair/test_enumerator.py
"""Tests for the gap enumerator used by both quality.py and binance_repair."""

from __future__ import annotations

import pandas as pd
import pytest

from crypto_data.binance_repair import GapBoundary, enumerate_kline_gaps


def test_no_rows_returns_empty(empty_db):
    assert enumerate_kline_gaps(empty_db.conn, table="futures") == []


def test_contiguous_rows_no_gap(empty_db):
    from tests.repair.conftest import _insert_klines
    ts = pd.date_range("2024-01-01T00:00", periods=5, freq="4h")
    _insert_klines(empty_db.conn, "futures", "SOLUSDT", "4h", list(ts))
    assert enumerate_kline_gaps(empty_db.conn, table="futures") == []


def test_single_gap_returns_one_boundary(db_with_kline_gap):
    gaps = enumerate_kline_gaps(db_with_kline_gap.conn, table="futures")
    assert len(gaps) == 1
    gap = gaps[0]
    assert isinstance(gap, GapBoundary)
    assert gap.table == "futures"
    assert gap.symbol == "SOLUSDT"
    assert gap.interval == "4h"
    assert gap.prev_close == pd.Timestamp("2024-01-01T16:00").to_pydatetime()
    assert gap.next_close == pd.Timestamp("2024-01-02T16:00").to_pydatetime()
    assert gap.expected_seconds == 14400
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `pytest tests/repair/test_enumerator.py -v`
Expected: ImportError — `binance_repair` module does not exist yet.

- [ ] **Step 3: Create the module skeleton with GapBoundary and enumerate_kline_gaps**

```python
# src/crypto_data/binance_repair.py
"""
Binance gap repair module.

Detects time-series gaps in spot/futures klines and funding_rates tables,
fetches missing rows from the Binance public REST API, and inserts them
idempotently.

See docs/superpowers/specs/2026-04-30-binance-gap-repair-design.md for design.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Sequence

import duckdb

logger = logging.getLogger(__name__)

# Kline interval -> expected seconds. Mirror of quality.KLINE_INTERVAL_SECONDS.
KLINE_INTERVAL_SECONDS = {
    "1m": 60, "3m": 180, "5m": 300, "15m": 900, "30m": 1800,
    "1h": 3600, "2h": 7200, "4h": 14400, "6h": 21600, "8h": 28800,
    "12h": 43200, "1d": 86400, "3d": 259200, "1w": 604800,
}

# Funding rate cadence on Binance (8h).
FUNDING_EXPECTED_SECONDS = 8 * 3600


@dataclass(frozen=True)
class GapBoundary:
    """One missing range in a time-series table.

    The candles strictly between prev_close and next_close are absent
    from the database. Both bounds are present in the database.
    """
    table: str
    symbol: str
    interval: str | None       # None for funding_rates
    prev_close: datetime
    next_close: datetime
    expected_seconds: int


def enumerate_kline_gaps(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    *,
    symbols: Sequence[str] | None = None,
    intervals: Sequence[str] | None = None,
) -> list[GapBoundary]:
    """List every individual kline gap in the table.

    Each returned GapBoundary corresponds to one consecutive run of missing
    candles for a single (symbol, interval) pair.
    """
    if table not in ("spot", "futures"):
        raise ValueError(f"enumerate_kline_gaps only supports spot/futures, got {table!r}")

    expected_values = ", ".join(
        f"('{interval}', {seconds})" for interval, seconds in KLINE_INTERVAL_SECONDS.items()
    )
    filter_clauses: list[str] = []
    params: list = []
    if symbols:
        placeholders = ", ".join(["?"] * len(symbols))
        filter_clauses.append(f"t.symbol IN ({placeholders})")
        params.extend(symbols)
    if intervals:
        placeholders = ", ".join(["?"] * len(intervals))
        filter_clauses.append(f"t.interval IN ({placeholders})")
        params.extend(intervals)
    filter_sql = ("WHERE " + " AND ".join(filter_clauses)) if filter_clauses else ""

    sql = f"""
        WITH expected(interval, expected_seconds) AS (VALUES {expected_values}),
        ordered AS (
            SELECT
                t.symbol,
                t.interval,
                t.timestamp,
                LAG(t.timestamp) OVER (
                    PARTITION BY t.exchange, t.symbol, t.interval
                    ORDER BY t.timestamp
                ) AS prev_timestamp,
                expected.expected_seconds
            FROM {table} AS t
            JOIN expected ON expected.interval = t.interval
            {filter_sql}
        )
        SELECT symbol, interval, prev_timestamp, timestamp, expected_seconds
        FROM ordered
        WHERE prev_timestamp IS NOT NULL
          AND date_diff('second', prev_timestamp, timestamp) > expected_seconds
        ORDER BY symbol, interval, timestamp
    """
    rows = conn.execute(sql, params).fetchall()
    return [
        GapBoundary(
            table=table,
            symbol=symbol,
            interval=interval,
            prev_close=prev,
            next_close=curr,
            expected_seconds=int(expected_seconds),
        )
        for symbol, interval, prev, curr, expected_seconds in rows
    ]
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `pytest tests/repair/test_enumerator.py -v`
Expected: 3 PASS.

- [ ] **Step 5: Commit**

```bash
git add src/crypto_data/binance_repair.py tests/repair/test_enumerator.py
git commit -m "feat(repair): add GapBoundary and enumerate_kline_gaps"
```

---

## Task 3: enumerate_kline_gaps filters + multi-symbol/interval coverage (TDD)

**Files:**
- Modify: `tests/repair/test_enumerator.py` (add tests)

- [ ] **Step 1: Add the new failing tests at the bottom of `test_enumerator.py`**

```python
def test_multi_symbol_multi_interval(empty_db):
    from tests.repair.conftest import _insert_klines
    # SOLUSDT 4h gap, BTCUSDT 1h gap
    sol_pre = pd.date_range("2024-01-01T00:00", periods=3, freq="4h")
    sol_post = pd.date_range("2024-01-02T00:00", periods=3, freq="4h")
    btc_pre = pd.date_range("2024-01-01T00:00", periods=3, freq="1h")
    btc_post = pd.date_range("2024-01-01T10:00", periods=3, freq="1h")
    _insert_klines(empty_db.conn, "futures", "SOLUSDT", "4h", list(sol_pre) + list(sol_post))
    _insert_klines(empty_db.conn, "futures", "BTCUSDT", "1h", list(btc_pre) + list(btc_post))

    gaps = enumerate_kline_gaps(empty_db.conn, table="futures")
    assert len(gaps) == 2
    assert {g.symbol for g in gaps} == {"SOLUSDT", "BTCUSDT"}


def test_filter_by_symbol(empty_db):
    from tests.repair.conftest import _insert_klines
    sol_pre = pd.date_range("2024-01-01T00:00", periods=3, freq="4h")
    sol_post = pd.date_range("2024-01-02T00:00", periods=3, freq="4h")
    btc_pre = pd.date_range("2024-01-01T00:00", periods=3, freq="4h")
    btc_post = pd.date_range("2024-01-02T00:00", periods=3, freq="4h")
    _insert_klines(empty_db.conn, "futures", "SOLUSDT", "4h", list(sol_pre) + list(sol_post))
    _insert_klines(empty_db.conn, "futures", "BTCUSDT", "4h", list(btc_pre) + list(btc_post))

    gaps = enumerate_kline_gaps(empty_db.conn, table="futures", symbols=["SOLUSDT"])
    assert len(gaps) == 1
    assert gaps[0].symbol == "SOLUSDT"


def test_filter_by_interval(empty_db):
    from tests.repair.conftest import _insert_klines
    pre_4h = pd.date_range("2024-01-01T00:00", periods=3, freq="4h")
    post_4h = pd.date_range("2024-01-02T00:00", periods=3, freq="4h")
    pre_1h = pd.date_range("2024-01-01T00:00", periods=3, freq="1h")
    post_1h = pd.date_range("2024-01-01T10:00", periods=3, freq="1h")
    _insert_klines(empty_db.conn, "futures", "SOLUSDT", "4h", list(pre_4h) + list(post_4h))
    _insert_klines(empty_db.conn, "futures", "SOLUSDT", "1h", list(pre_1h) + list(post_1h))

    gaps = enumerate_kline_gaps(empty_db.conn, table="futures", intervals=["4h"])
    assert len(gaps) == 1
    assert gaps[0].interval == "4h"


def test_unsupported_table_raises(empty_db):
    with pytest.raises(ValueError, match="spot/futures"):
        enumerate_kline_gaps(empty_db.conn, table="open_interest")
```

- [ ] **Step 2: Run tests to verify they pass**

Run: `pytest tests/repair/test_enumerator.py -v`
Expected: 7 PASS (3 from Task 2 + 4 new).

- [ ] **Step 3: Commit**

```bash
git add tests/repair/test_enumerator.py
git commit -m "test(repair): cover multi-symbol/interval and filter paths in enumerator"
```

---

## Task 4: enumerate_metric_gaps (funding rates) (TDD)

**Files:**
- Modify: `src/crypto_data/binance_repair.py`
- Modify: `tests/repair/test_enumerator.py`

- [ ] **Step 1: Write the failing tests**

Add to `tests/repair/test_enumerator.py`:

```python
from crypto_data.binance_repair import enumerate_metric_gaps, FUNDING_EXPECTED_SECONDS


def test_funding_no_gap_returns_empty(empty_db):
    from tests.repair.conftest import _insert_funding
    ts = pd.date_range("2024-01-01T00:00", periods=4, freq="8h")
    _insert_funding(empty_db.conn, "BTCUSDT", list(ts))
    assert enumerate_metric_gaps(
        empty_db.conn, table="funding_rates", expected_seconds=FUNDING_EXPECTED_SECONDS
    ) == []


def test_funding_single_gap(db_with_funding_gap):
    gaps = enumerate_metric_gaps(
        db_with_funding_gap.conn,
        table="funding_rates",
        expected_seconds=FUNDING_EXPECTED_SECONDS,
    )
    assert len(gaps) == 1
    gap = gaps[0]
    assert gap.table == "funding_rates"
    assert gap.symbol == "BTCUSDT"
    assert gap.interval is None
    assert gap.expected_seconds == FUNDING_EXPECTED_SECONDS
    assert gap.prev_close == pd.Timestamp("2024-01-02T00:00").to_pydatetime()
    assert gap.next_close == pd.Timestamp("2024-01-03T08:00").to_pydatetime()


def test_funding_filter_by_symbol(empty_db):
    from tests.repair.conftest import _insert_funding
    btc_pre = pd.date_range("2024-01-01T00:00", periods=3, freq="8h")
    btc_post = pd.date_range("2024-01-02T08:00", periods=3, freq="8h")
    eth_pre = pd.date_range("2024-01-01T00:00", periods=3, freq="8h")
    eth_post = pd.date_range("2024-01-02T08:00", periods=3, freq="8h")
    _insert_funding(empty_db.conn, "BTCUSDT", list(btc_pre) + list(btc_post))
    _insert_funding(empty_db.conn, "ETHUSDT", list(eth_pre) + list(eth_post))

    gaps = enumerate_metric_gaps(
        empty_db.conn,
        table="funding_rates",
        expected_seconds=FUNDING_EXPECTED_SECONDS,
        symbols=["BTCUSDT"],
    )
    assert len(gaps) == 1
    assert gaps[0].symbol == "BTCUSDT"
```

- [ ] **Step 2: Run to verify failure**

Run: `pytest tests/repair/test_enumerator.py -v`
Expected: ImportError on `enumerate_metric_gaps`.

- [ ] **Step 3: Implement enumerate_metric_gaps**

Add to `src/crypto_data/binance_repair.py`:

```python
def enumerate_metric_gaps(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    expected_seconds: int,
    *,
    symbols: Sequence[str] | None = None,
) -> list[GapBoundary]:
    """List every gap in a single-cadence metric table (no interval column)."""
    if table not in ("open_interest", "funding_rates"):
        raise ValueError(f"enumerate_metric_gaps only supports open_interest/funding_rates, got {table!r}")

    filter_sql = ""
    params: list = []
    if symbols:
        placeholders = ", ".join(["?"] * len(symbols))
        filter_sql = f"WHERE t.symbol IN ({placeholders})"
        params.extend(symbols)

    sql = f"""
        WITH ordered AS (
            SELECT
                t.symbol,
                t.timestamp,
                LAG(t.timestamp) OVER (
                    PARTITION BY t.exchange, t.symbol
                    ORDER BY t.timestamp
                ) AS prev_timestamp
            FROM {table} AS t
            {filter_sql}
        )
        SELECT symbol, prev_timestamp, timestamp
        FROM ordered
        WHERE prev_timestamp IS NOT NULL
          AND date_diff('second', prev_timestamp, timestamp) > ?
        ORDER BY symbol, timestamp
    """
    rows = conn.execute(sql, params + [expected_seconds]).fetchall()
    return [
        GapBoundary(
            table=table,
            symbol=symbol,
            interval=None,
            prev_close=prev,
            next_close=curr,
            expected_seconds=expected_seconds,
        )
        for symbol, prev, curr in rows
    ]
```

- [ ] **Step 4: Run tests**

Run: `pytest tests/repair/test_enumerator.py -v`
Expected: 10 PASS.

- [ ] **Step 5: Commit**

```bash
git add src/crypto_data/binance_repair.py tests/repair/test_enumerator.py
git commit -m "feat(repair): add enumerate_metric_gaps for funding_rates and open_interest"
```

---

## Task 5: Refactor quality.py to consume the shared enumerator (regression-safe)

**Files:**
- Modify: `src/crypto_data/quality.py:349-428` (`_check_kline_gaps`)
- Modify: `src/crypto_data/quality.py:748-815` (`_check_metric_gaps`)
- Create: `tests/quality/test_quality_enumerator_regression.py`

- [ ] **Step 1: Write the regression test FIRST (red phase)**

```python
# tests/quality/test_quality_enumerator_regression.py
"""Regression: audit_database findings unchanged after enumerator factorization."""

from __future__ import annotations

import pandas as pd
import pytest

from crypto_data.quality import audit_database


def _populate_kline_gap(db, symbol="SOLUSDT", interval="4h"):
    from tests.repair.conftest import _insert_klines
    pre = pd.date_range("2024-01-01T00:00", periods=5, freq="4h")
    post = pd.date_range("2024-01-02T16:00", periods=5, freq="4h")
    _insert_klines(db.conn, "futures", symbol, interval, list(pre) + list(post))


def test_kline_gap_finding_shape(empty_db):
    _populate_kline_gap(empty_db)
    findings = audit_database(empty_db.db_path, tables=["futures"])
    gap_findings = [f for f in findings if f.check_name == "time_gaps"]
    assert len(gap_findings) == 1
    f = gap_findings[0]
    assert f.severity == "ERROR"
    assert f.symbol == "SOLUSDT"
    assert f.interval == "4h"
    assert f.count == 1
    assert f.metadata["expected_seconds"] == 14400
    assert f.metadata["max_delta_seconds"] >= 14400


def test_funding_gap_finding_shape(db_with_funding_gap):
    findings = audit_database(db_with_funding_gap.db_path, tables=["funding_rates"])
    gap_findings = [f for f in findings if f.check_name == "time_gaps"]
    assert len(gap_findings) == 1
    f = gap_findings[0]
    assert f.symbol == "BTCUSDT"
    assert f.count == 1
```

- [ ] **Step 2: Run to confirm tests already pass with current implementation**

Run: `pytest tests/quality/test_quality_enumerator_regression.py -v`
Expected: 2 PASS (current implementation already produces these findings).

This test is the **safety net** for the refactor: it must still pass after rewriting `_check_kline_gaps` and `_check_metric_gaps`.

- [ ] **Step 3: Refactor `_check_kline_gaps` to consume `enumerate_kline_gaps`**

Replace the body of `_check_kline_gaps` in `src/crypto_data/quality.py` (line ~349):

```python
def _check_kline_gaps(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    symbols: Sequence[str] | None,
    intervals: Sequence[str] | None,
) -> list[QualityFinding]:
    from crypto_data.binance_repair import enumerate_kline_gaps

    boundaries = enumerate_kline_gaps(conn, table=table, symbols=symbols, intervals=intervals)
    if not boundaries:
        return []

    # Aggregate per (symbol, interval).
    grouped: dict[tuple[str, str], list] = {}
    for b in boundaries:
        grouped.setdefault((b.symbol, b.interval), []).append(b)

    findings: list[QualityFinding] = []
    for (symbol, interval), gaps in grouped.items():
        deltas = [
            int((g.next_close - g.prev_close).total_seconds()) for g in gaps
        ]
        max_delta = max(deltas)
        expected = gaps[0].expected_seconds
        findings.append(
            QualityFinding(
                severity="ERROR",
                table=table,
                check_name="time_gaps",
                message=(
                    f"{len(gaps)} gaps detected; max gap "
                    f"{_format_seconds(max_delta)} vs expected {_format_seconds(expected)}"
                ),
                count=len(gaps),
                symbol=symbol,
                interval=interval,
                first_timestamp=min(g.next_close for g in gaps),
                last_timestamp=max(g.next_close for g in gaps),
                metadata={
                    "max_delta_seconds": max_delta,
                    "expected_seconds": expected,
                },
            )
        )
    return findings
```

- [ ] **Step 4: Refactor `_check_metric_gaps` analogously**

Replace the body of `_check_metric_gaps` in `src/crypto_data/quality.py` (line ~748):

```python
def _check_metric_gaps(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    symbols: Sequence[str] | None,
    expected_seconds: int,
) -> list[QualityFinding]:
    from crypto_data.binance_repair import enumerate_metric_gaps

    boundaries = enumerate_metric_gaps(
        conn, table=table, expected_seconds=expected_seconds, symbols=symbols
    )
    if not boundaries:
        return []

    grouped: dict[str, list] = {}
    for b in boundaries:
        grouped.setdefault(b.symbol, []).append(b)

    findings: list[QualityFinding] = []
    for symbol, gaps in grouped.items():
        deltas = [int((g.next_close - g.prev_close).total_seconds()) for g in gaps]
        max_delta = max(deltas)
        findings.append(
            QualityFinding(
                severity="ERROR",
                table=table,
                check_name="time_gaps",
                message=(
                    f"{len(gaps)} gaps detected; max gap "
                    f"{_format_seconds(max_delta)} vs expected {_format_seconds(expected_seconds)}"
                ),
                count=len(gaps),
                symbol=symbol,
                first_timestamp=min(g.next_close for g in gaps),
                last_timestamp=max(g.next_close for g in gaps),
                metadata={
                    "max_delta_seconds": max_delta,
                    "expected_seconds": expected_seconds,
                },
            )
        )
    return findings
```

- [ ] **Step 5: Run regression tests + the existing quality test**

Run: `pytest tests/quality/ tests/repair/test_enumerator.py -v`
Expected: all PASS.

- [ ] **Step 6: Commit**

```bash
git add src/crypto_data/quality.py tests/quality/test_quality_enumerator_regression.py
git commit -m "refactor(quality): share gap enumerator with binance_repair"
```

---

## Task 6: BinanceRestClient skeleton + URL building (TDD)

**Files:**
- Create: `src/crypto_data/clients/binance_rest.py`
- Create: `tests/repair/test_binance_rest_client.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/repair/test_binance_rest_client.py
"""Tests for the Binance public REST client."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from crypto_data.clients.binance_rest import BinanceRestClient, BinanceRateLimitError


def test_spot_klines_url():
    url = BinanceRestClient.build_spot_klines_url(
        symbol="BTCUSDT", interval="4h", start_ms=1000, end_ms=2000, limit=500
    )
    assert url.startswith("https://api.binance.com/api/v3/klines?")
    assert "symbol=BTCUSDT" in url
    assert "interval=4h" in url
    assert "startTime=1000" in url
    assert "endTime=2000" in url
    assert "limit=500" in url


def test_futures_klines_url():
    url = BinanceRestClient.build_futures_klines_url(
        symbol="SOLUSDT", interval="4h", start_ms=1000, end_ms=2000, limit=1500
    )
    assert url.startswith("https://fapi.binance.com/fapi/v1/klines?")
    assert "limit=1500" in url


def test_funding_rate_url():
    url = BinanceRestClient.build_funding_rate_url(
        symbol="BTCUSDT", start_ms=1000, end_ms=2000, limit=1000
    )
    assert url.startswith("https://fapi.binance.com/fapi/v1/fundingRate?")
    assert "symbol=BTCUSDT" in url
    assert "limit=1000" in url
```

- [ ] **Step 2: Run to verify failure**

Run: `pytest tests/repair/test_binance_rest_client.py -v`
Expected: ImportError — `binance_rest` module not found.

- [ ] **Step 3: Implement skeleton**

```python
# src/crypto_data/clients/binance_rest.py
"""Async HTTP client for the Binance public REST API.

Provides URL building, semaphore-bounded concurrency, and a retry matrix
covering 429/418/5xx and aiohttp client errors. Knows nothing about gaps,
DataFrames, or the database — it is a transport utility.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any
from urllib.parse import urlencode

import aiohttp

logger = logging.getLogger(__name__)

SPOT_BASE_URL = "https://api.binance.com"
FUTURES_BASE_URL = "https://fapi.binance.com"


class BinanceRateLimitError(RuntimeError):
    """Raised after exhausting all retries on 429 or 418 responses."""


class BinanceRestClient:
    """Async client for Binance public REST endpoints used by the gap repair."""

    def __init__(self, max_concurrent: int = 5) -> None:
        self._max_concurrent = max_concurrent
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._session: aiohttp.ClientSession | None = None

    async def __aenter__(self) -> "BinanceRestClient":
        self._session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._session is not None:
            await self._session.close()
            self._session = None

    @staticmethod
    def build_spot_klines_url(symbol: str, interval: str, start_ms: int, end_ms: int, limit: int) -> str:
        params = urlencode({
            "symbol": symbol, "interval": interval,
            "startTime": start_ms, "endTime": end_ms, "limit": limit,
        })
        return f"{SPOT_BASE_URL}/api/v3/klines?{params}"

    @staticmethod
    def build_futures_klines_url(symbol: str, interval: str, start_ms: int, end_ms: int, limit: int) -> str:
        params = urlencode({
            "symbol": symbol, "interval": interval,
            "startTime": start_ms, "endTime": end_ms, "limit": limit,
        })
        return f"{FUTURES_BASE_URL}/fapi/v1/klines?{params}"

    @staticmethod
    def build_funding_rate_url(symbol: str, start_ms: int, end_ms: int, limit: int) -> str:
        params = urlencode({
            "symbol": symbol,
            "startTime": start_ms, "endTime": end_ms, "limit": limit,
        })
        return f"{FUTURES_BASE_URL}/fapi/v1/fundingRate?{params}"
```

- [ ] **Step 4: Run tests**

Run: `pytest tests/repair/test_binance_rest_client.py -v`
Expected: 3 PASS.

- [ ] **Step 5: Commit**

```bash
git add src/crypto_data/clients/binance_rest.py tests/repair/test_binance_rest_client.py
git commit -m "feat(clients): add BinanceRestClient with URL builders"
```

---

## Task 7: BinanceRestClient.get_json with semaphore concurrency (TDD)

**Files:**
- Modify: `src/crypto_data/clients/binance_rest.py`
- Modify: `tests/repair/test_binance_rest_client.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/repair/test_binance_rest_client.py`:

```python
class _FakeResponse:
    def __init__(self, status: int = 200, json_body: Any = None, headers: dict | None = None):
        self.status = status
        self._json_body = json_body if json_body is not None else []
        self.headers = headers or {}

    async def __aenter__(self): return self
    async def __aexit__(self, *a): return None

    async def json(self):
        return self._json_body

    async def text(self):
        return ""


class _FakeSession:
    """Minimal aiohttp session stub. Records calls + returns scripted responses."""
    def __init__(self, responses):
        self._responses = list(responses)
        self.calls: list[str] = []
        self.in_flight = 0
        self.max_in_flight = 0

    def get(self, url, **_):
        self.calls.append(url)
        self.in_flight += 1
        self.max_in_flight = max(self.max_in_flight, self.in_flight)
        resp = self._responses.pop(0)
        return _FakeContext(resp, self)

    async def close(self): pass


class _FakeContext:
    def __init__(self, resp, session):
        self._resp = resp
        self._session = session

    async def __aenter__(self): return self._resp

    async def __aexit__(self, *a):
        self._session.in_flight -= 1
        return None


@pytest.mark.asyncio
async def test_get_json_success_returns_body():
    client = BinanceRestClient(max_concurrent=2)
    fake = _FakeSession([_FakeResponse(200, json_body=[1, 2, 3])])
    client._session = fake  # bypass __aenter__ for the test
    body = await client.get_json("https://example/x")
    assert body == [1, 2, 3]
    assert fake.calls == ["https://example/x"]


@pytest.mark.asyncio
async def test_semaphore_caps_concurrency():
    client = BinanceRestClient(max_concurrent=2)
    fake = _FakeSession([_FakeResponse(200, json_body=[]) for _ in range(5)])
    client._session = fake
    await asyncio.gather(*(client.get_json(f"https://example/{i}") for i in range(5)))
    assert fake.max_in_flight <= 2
```

Note: this requires `pytest-asyncio` already in dev-deps. Verify with `grep pytest-asyncio pyproject.toml` (if missing, add it in step 3).

- [ ] **Step 2: Run to verify failure**

Run: `pytest tests/repair/test_binance_rest_client.py -v`
Expected: AttributeError on `get_json`.

- [ ] **Step 3: Add `get_json` and ensure `pytest-asyncio` configured**

```python
# Append to src/crypto_data/clients/binance_rest.py

async def get_json(self, url: str) -> Any:
    """Issue a GET. Returns parsed JSON. Caller handles classification."""
    if self._session is None:
        raise RuntimeError("BinanceRestClient must be used as an async context manager")
    async with self._semaphore:
        async with self._session.get(url) as resp:
            if resp.status == 200:
                return await resp.json()
            # Fallthrough handlers go in subsequent tasks (429/418/5xx).
            text = await resp.text()
            raise aiohttp.ClientResponseError(
                request_info=resp.request_info if hasattr(resp, "request_info") else None,
                history=(),
                status=resp.status,
                message=text,
            )
```

If `pyproject.toml` does not declare `asyncio_mode = "auto"` or similar, add this to the project pytest config:

```toml
[tool.pytest.ini_options]
asyncio_mode = "auto"
```

- [ ] **Step 4: Run tests**

Run: `pytest tests/repair/test_binance_rest_client.py -v`
Expected: 5 PASS.

- [ ] **Step 5: Commit**

```bash
git add src/crypto_data/clients/binance_rest.py tests/repair/test_binance_rest_client.py pyproject.toml
git commit -m "feat(clients): add get_json with semaphore-bounded concurrency"
```

---

## Task 8: BinanceRestClient retry on 429 with Retry-After (TDD)

**Files:**
- Modify: `src/crypto_data/clients/binance_rest.py`
- Modify: `tests/repair/test_binance_rest_client.py`

- [ ] **Step 1: Write the failing tests**

```python
@pytest.mark.asyncio
async def test_429_retries_after_retry_after():
    sleeps: list[float] = []

    async def fake_sleep(s):
        sleeps.append(s)

    fake = _FakeSession([
        _FakeResponse(429, headers={"Retry-After": "2"}),
        _FakeResponse(200, json_body=[42]),
    ])
    client = BinanceRestClient(max_concurrent=1)
    client._session = fake
    with patch("asyncio.sleep", new=fake_sleep):
        body = await client.get_json("https://example/x")
    assert body == [42]
    assert sleeps == [2.0]


@pytest.mark.asyncio
async def test_429_exhausted_raises_rate_limit_error():
    async def fake_sleep(_): pass
    fake = _FakeSession([_FakeResponse(429, headers={"Retry-After": "1"}) for _ in range(6)])
    client = BinanceRestClient(max_concurrent=1)
    client._session = fake
    with patch("asyncio.sleep", new=fake_sleep):
        with pytest.raises(BinanceRateLimitError):
            await client.get_json("https://example/x")
```

- [ ] **Step 2: Run to verify failure**

Run: `pytest tests/repair/test_binance_rest_client.py::test_429_retries_after_retry_after -v`
Expected: FAIL — current code raises `ClientResponseError` immediately on 429.

- [ ] **Step 3: Add 429 retry loop in get_json**

Replace `get_json` in `src/crypto_data/clients/binance_rest.py`:

```python
MAX_429_RETRIES = 5

async def get_json(self, url: str) -> Any:
    if self._session is None:
        raise RuntimeError("BinanceRestClient must be used as an async context manager")
    attempt_429 = 0
    while True:
        async with self._semaphore:
            async with self._session.get(url) as resp:
                if resp.status == 200:
                    return await resp.json()
                if resp.status == 429:
                    attempt_429 += 1
                    if attempt_429 > self.MAX_429_RETRIES:
                        raise BinanceRateLimitError(
                            f"429 received {attempt_429} times for {url}"
                        )
                    retry_after = float(resp.headers.get("Retry-After", "1"))
                    logger.warning("429 from Binance; sleeping %.1fs (attempt %d)", retry_after, attempt_429)
                    await asyncio.sleep(retry_after)
                    continue
                # Other status codes: handled in later tasks; raise for now.
                text = await resp.text()
                raise aiohttp.ClientResponseError(
                    request_info=resp.request_info,
                    history=(),
                    status=resp.status,
                    message=text,
                )
```

(Promote `MAX_429_RETRIES` to a class attribute — adjust `class BinanceRestClient:` accordingly.)

- [ ] **Step 4: Run tests**

Run: `pytest tests/repair/test_binance_rest_client.py -v`
Expected: 7 PASS.

- [ ] **Step 5: Commit**

```bash
git add src/crypto_data/clients/binance_rest.py tests/repair/test_binance_rest_client.py
git commit -m "feat(clients): retry 429 with Retry-After up to 5 attempts"
```

---

## Task 9: BinanceRestClient retry on 5xx and aiohttp errors with backoff (TDD)

**Files:**
- Modify: `src/crypto_data/clients/binance_rest.py`
- Modify: `tests/repair/test_binance_rest_client.py`

- [ ] **Step 1: Write the failing tests**

```python
@pytest.mark.asyncio
async def test_5xx_retries_with_backoff_and_succeeds():
    sleeps: list[float] = []
    async def fake_sleep(s): sleeps.append(s)

    fake = _FakeSession([
        _FakeResponse(503),
        _FakeResponse(503),
        _FakeResponse(200, json_body=[7]),
    ])
    client = BinanceRestClient(max_concurrent=1)
    client._session = fake
    with patch("asyncio.sleep", new=fake_sleep):
        body = await client.get_json("https://example/x")
    assert body == [7]
    assert sleeps == [1.0, 2.0]


@pytest.mark.asyncio
async def test_5xx_exhausts_retries_and_raises():
    async def fake_sleep(_): pass
    fake = _FakeSession([_FakeResponse(500) for _ in range(4)])
    client = BinanceRestClient(max_concurrent=1)
    client._session = fake
    with patch("asyncio.sleep", new=fake_sleep):
        with pytest.raises(aiohttp.ClientResponseError):
            await client.get_json("https://example/x")
```

- [ ] **Step 2: Run to verify failure**

Run: `pytest tests/repair/test_binance_rest_client.py -v`
Expected: 5xx test fails — currently no retry on 5xx.

- [ ] **Step 3: Extend get_json with 5xx backoff**

Replace `get_json` in `src/crypto_data/clients/binance_rest.py` to add a 5xx counter:

```python
MAX_5XX_RETRIES = 3
BACKOFF_BASE = 1.0  # seconds: 1, 2, 4

async def get_json(self, url: str) -> Any:
    if self._session is None:
        raise RuntimeError("BinanceRestClient must be used as an async context manager")
    attempt_429 = 0
    attempt_5xx = 0
    while True:
        async with self._semaphore:
            async with self._session.get(url) as resp:
                status = resp.status
                if status == 200:
                    return await resp.json()
                if status == 429:
                    attempt_429 += 1
                    if attempt_429 > self.MAX_429_RETRIES:
                        raise BinanceRateLimitError(f"429 received {attempt_429} times for {url}")
                    retry_after = float(resp.headers.get("Retry-After", "1"))
                    logger.warning("429 from Binance; sleeping %.1fs (attempt %d)", retry_after, attempt_429)
                    await asyncio.sleep(retry_after)
                    continue
                if 500 <= status < 600:
                    attempt_5xx += 1
                    if attempt_5xx > self.MAX_5XX_RETRIES:
                        text = await resp.text()
                        raise aiohttp.ClientResponseError(
                            request_info=resp.request_info, history=(),
                            status=status, message=text,
                        )
                    sleep = self.BACKOFF_BASE * (2 ** (attempt_5xx - 1))
                    logger.warning("%d from Binance; backoff %.1fs (attempt %d)", status, sleep, attempt_5xx)
                    await asyncio.sleep(sleep)
                    continue
                # Other 4xx and non-handled — fall through to raise (later task adds 418).
                text = await resp.text()
                raise aiohttp.ClientResponseError(
                    request_info=resp.request_info, history=(),
                    status=status, message=text,
                )
```

- [ ] **Step 4: Add aiohttp.ClientError retry**

Wrap the inner `async with self._session.get(url)` in a `try/except aiohttp.ClientError` block sharing the same `attempt_5xx` counter. Final form:

```python
async def get_json(self, url: str) -> Any:
    if self._session is None:
        raise RuntimeError("BinanceRestClient must be used as an async context manager")
    attempt_429 = 0
    attempt_5xx = 0
    while True:
        try:
            async with self._semaphore:
                async with self._session.get(url) as resp:
                    status = resp.status
                    if status == 200:
                        return await resp.json()
                    if status == 429:
                        attempt_429 += 1
                        if attempt_429 > self.MAX_429_RETRIES:
                            raise BinanceRateLimitError(f"429 received {attempt_429} times for {url}")
                        retry_after = float(resp.headers.get("Retry-After", "1"))
                        logger.warning("429 from Binance; sleeping %.1fs (attempt %d)", retry_after, attempt_429)
                        await asyncio.sleep(retry_after)
                        continue
                    if 500 <= status < 600:
                        raise _Retryable5xx(status)
                    text = await resp.text()
                    raise aiohttp.ClientResponseError(
                        request_info=resp.request_info, history=(),
                        status=status, message=text,
                    )
        except (_Retryable5xx, aiohttp.ClientError) as exc:
            attempt_5xx += 1
            if attempt_5xx > self.MAX_5XX_RETRIES:
                if isinstance(exc, _Retryable5xx):
                    raise aiohttp.ClientResponseError(
                        request_info=None, history=(), status=exc.status, message=f"5xx exhausted on {url}",
                    )
                raise
            sleep = self.BACKOFF_BASE * (2 ** (attempt_5xx - 1))
            logger.warning("transient error %r on %s; backoff %.1fs (attempt %d)", exc, url, sleep, attempt_5xx)
            await asyncio.sleep(sleep)


class _Retryable5xx(Exception):
    def __init__(self, status: int): self.status = status
```

- [ ] **Step 5: Run all client tests**

Run: `pytest tests/repair/test_binance_rest_client.py -v`
Expected: 9 PASS.

- [ ] **Step 6: Commit**

```bash
git add src/crypto_data/clients/binance_rest.py tests/repair/test_binance_rest_client.py
git commit -m "feat(clients): retry 5xx and aiohttp errors with exponential backoff"
```

---

## Task 10: BinanceRestClient 418 single retry + 4xx fast-fail (TDD)

**Files:**
- Modify: `src/crypto_data/clients/binance_rest.py`
- Modify: `tests/repair/test_binance_rest_client.py`

- [ ] **Step 1: Write the failing tests**

```python
@pytest.mark.asyncio
async def test_418_retries_once_then_raises():
    async def fake_sleep(_): pass
    fake = _FakeSession([
        _FakeResponse(418, headers={"Retry-After": "60"}),
        _FakeResponse(418, headers={"Retry-After": "60"}),
    ])
    client = BinanceRestClient(max_concurrent=1)
    client._session = fake
    with patch("asyncio.sleep", new=fake_sleep):
        with pytest.raises(BinanceRateLimitError):
            await client.get_json("https://example/x")
    assert len(fake.calls) == 2  # one initial + one retry


@pytest.mark.asyncio
async def test_418_then_success():
    async def fake_sleep(_): pass
    fake = _FakeSession([
        _FakeResponse(418, headers={"Retry-After": "5"}),
        _FakeResponse(200, json_body=[9]),
    ])
    client = BinanceRestClient(max_concurrent=1)
    client._session = fake
    with patch("asyncio.sleep", new=fake_sleep):
        body = await client.get_json("https://example/x")
    assert body == [9]


@pytest.mark.asyncio
async def test_400_invalid_symbol_no_retry():
    fake = _FakeSession([_FakeResponse(400, headers={})])
    client = BinanceRestClient(max_concurrent=1)
    client._session = fake
    with pytest.raises(aiohttp.ClientResponseError) as ei:
        await client.get_json("https://example/x")
    assert ei.value.status == 400
    assert len(fake.calls) == 1
```

- [ ] **Step 2: Run to verify failure**

Run: `pytest tests/repair/test_binance_rest_client.py -v`
Expected: 418 tests fail — currently 418 raises immediately (in the fallthrough).

- [ ] **Step 3: Add 418 branch in get_json**

Insert this block before the existing `if 500 <= status < 600:` line:

```python
if status == 418:
    attempt_418 = locals().setdefault("attempt_418", 0)  # noqa
    # actually use a proper counter — see snippet below
```

Cleaner: declare `attempt_418 = 0` at the top of `get_json` next to `attempt_429`, and add this branch:

```python
if status == 418:
    attempt_418 += 1
    if attempt_418 > 1:
        raise BinanceRateLimitError(f"418 received {attempt_418} times for {url}")
    retry_after = float(resp.headers.get("Retry-After", "60"))
    logger.warning("418 from Binance; sleeping %.1fs (attempt %d)", retry_after, attempt_418)
    await asyncio.sleep(retry_after)
    continue
```

- [ ] **Step 4: Run all client tests**

Run: `pytest tests/repair/test_binance_rest_client.py -v`
Expected: 12 PASS.

- [ ] **Step 5: Commit**

```bash
git add src/crypto_data/clients/binance_rest.py tests/repair/test_binance_rest_client.py
git commit -m "feat(clients): handle 418 (1 retry) and 4xx (no retry) explicitly"
```

---

## Task 11: KlinesRepairStrategy — parse 12-tuple → DataFrame (TDD)

**Files:**
- Modify: `src/crypto_data/binance_repair.py`
- Create: `tests/repair/test_strategies.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/repair/test_strategies.py
"""Tests for repair strategies."""

from __future__ import annotations

from datetime import datetime

import pandas as pd
import pytest

from crypto_data.binance_repair import (
    GapBoundary,
    KlinesRepairStrategy,
)
from crypto_data.enums import DataType, Interval


# Authentic Binance kline payload shape: [openTime, o, h, l, c, vol, closeTime,
#   qVol, n_trades, takerBase, takerQuote, ignore]
def _kline(open_ts_ms: int) -> list:
    close_ts_ms = open_ts_ms + 4 * 3600 * 1000 - 1  # 4h candle
    return [
        open_ts_ms, "1.0", "2.0", "0.5", "1.5", "100.0",
        close_ts_ms, "150.0", 42, "50.0", "75.0", "0",
    ]


def test_parse_klines_payload_to_dataframe():
    strategy = KlinesRepairStrategy(data_type=DataType.FUTURES, interval=Interval.HOUR_4)
    payload = [_kline(int(pd.Timestamp("2024-01-01T20:00", tz="UTC").timestamp() * 1000))]
    df = strategy.parse_payload(payload, symbol="SOLUSDT")

    assert list(df.columns) == [
        "exchange", "symbol", "interval", "timestamp",
        "open", "high", "low", "close", "volume", "quote_volume",
        "trades_count", "taker_buy_base_volume", "taker_buy_quote_volume",
    ]
    assert len(df) == 1
    row = df.iloc[0]
    assert row["exchange"] == "binance"
    assert row["symbol"] == "SOLUSDT"
    assert row["interval"] == "4h"
    # closeTime + 1 ms → 2024-01-02T00:00:00 (next 4h boundary)
    assert row["timestamp"] == pd.Timestamp("2024-01-02T00:00")
    assert row["close"] == 1.5
```

- [ ] **Step 2: Run to verify failure**

Run: `pytest tests/repair/test_strategies.py -v`
Expected: ImportError on `KlinesRepairStrategy`.

- [ ] **Step 3: Add KlinesRepairStrategy with parse_payload**

Append to `src/crypto_data/binance_repair.py`:

```python
import pandas as pd

from crypto_data.binance_datasets.klines import FINAL_COLUMNS as KLINE_FINAL_COLUMNS
from crypto_data.enums import DataType, Interval
from crypto_data.schemas import OHLCV_SCHEMA


class KlinesRepairStrategy:
    """Fetch missing candles from /klines and produce schema-conformant DataFrames."""

    REST_LIMIT_SPOT = 1000
    REST_LIMIT_FUTURES = 1500

    def __init__(self, data_type: DataType, interval: Interval) -> None:
        if data_type not in (DataType.SPOT, DataType.FUTURES):
            raise ValueError(f"KlinesRepairStrategy supports SPOT/FUTURES only, got {data_type}")
        self.data_type = data_type
        self.interval = interval
        self.table = data_type.value
        self.rest_limit = self.REST_LIMIT_SPOT if data_type == DataType.SPOT else self.REST_LIMIT_FUTURES

    def parse_payload(self, payload: list[list], symbol: str) -> pd.DataFrame:
        """Convert Binance 12-tuples to a DataFrame with KLINE_FINAL_COLUMNS."""
        if not payload:
            return pd.DataFrame(columns=KLINE_FINAL_COLUMNS)

        df = pd.DataFrame(payload, columns=[
            "open_time", "open", "high", "low", "close", "volume",
            "close_time", "quote_volume", "trades_count",
            "taker_buy_base_volume", "taker_buy_quote_volume", "ignore",
        ])
        for col in ["open", "high", "low", "close", "volume", "quote_volume",
                    "taker_buy_base_volume", "taker_buy_quote_volume"]:
            df[col] = df[col].astype(float)
        df["trades_count"] = df["trades_count"].astype(int)
        df["close_time"] = df["close_time"].astype("int64")

        df["exchange"] = "binance"
        df["symbol"] = symbol
        df["interval"] = self.interval.value
        df["timestamp"] = pd.to_datetime((df["close_time"] + 1) / 1000, unit="s").dt.ceil("1s")

        return df[KLINE_FINAL_COLUMNS]
```

- [ ] **Step 4: Run test**

Run: `pytest tests/repair/test_strategies.py -v`
Expected: 1 PASS.

- [ ] **Step 5: Commit**

```bash
git add src/crypto_data/binance_repair.py tests/repair/test_strategies.py
git commit -m "feat(repair): KlinesRepairStrategy.parse_payload"
```

---

## Task 12: KlinesRepairStrategy — fetch_repair_rows with trim + guards (TDD)

**Files:**
- Modify: `src/crypto_data/binance_repair.py`
- Modify: `tests/repair/test_strategies.py`

- [ ] **Step 1: Write the failing tests**

```python
class _FakeRestClient:
    def __init__(self, payload):
        self._payload = payload
        self.calls: list[str] = []

    async def get_json(self, url):
        self.calls.append(url)
        return self._payload


@pytest.mark.asyncio
async def test_fetch_repair_rows_trims_to_exclusive_window():
    strategy = KlinesRepairStrategy(data_type=DataType.FUTURES, interval=Interval.HOUR_4)
    # Gap between 2024-01-01T16:00 and 2024-01-02T00:00 → 1 missing candle at 20:00.
    gap = GapBoundary(
        table="futures", symbol="SOLUSDT", interval="4h",
        prev_close=datetime(2024, 1, 1, 16),
        next_close=datetime(2024, 1, 2, 0),
        expected_seconds=14400,
    )
    # API payload includes the candles strictly within the window.
    open_20h = int(pd.Timestamp("2024-01-01T16:00", tz="UTC").timestamp() * 1000)
    payload = [_kline(open_20h)]   # closeTime → 2024-01-01T20:00 timestamp key
    client = _FakeRestClient(payload)
    df = await strategy.fetch_repair_rows(client, gap)
    assert len(df) == 1
    assert df.iloc[0]["timestamp"] == pd.Timestamp("2024-01-01T20:00")


@pytest.mark.asyncio
async def test_fetch_repair_rows_off_grid_raises():
    strategy = KlinesRepairStrategy(data_type=DataType.FUTURES, interval=Interval.HOUR_4)
    gap = GapBoundary(
        table="futures", symbol="SOLUSDT", interval="4h",
        prev_close=datetime(2024, 1, 1, 16, 7),  # off-grid by 7 minutes
        next_close=datetime(2024, 1, 2, 0),
        expected_seconds=14400,
    )
    client = _FakeRestClient([])
    with pytest.raises(ValueError, match="off-grid"):
        await strategy.fetch_repair_rows(client, gap)


@pytest.mark.asyncio
async def test_fetch_repair_rows_too_large_raises():
    strategy = KlinesRepairStrategy(data_type=DataType.FUTURES, interval=Interval.HOUR_4)
    # 1500 candles × 4h = 250 days. Make a gap of 300 days.
    gap = GapBoundary(
        table="futures", symbol="SOLUSDT", interval="4h",
        prev_close=datetime(2024, 1, 1),
        next_close=datetime(2024, 11, 1),  # ~305 days
        expected_seconds=14400,
    )
    client = _FakeRestClient([])
    with pytest.raises(ValueError, match="too large"):
        await strategy.fetch_repair_rows(client, gap)
```

- [ ] **Step 2: Run to verify failure**

Run: `pytest tests/repair/test_strategies.py -v`
Expected: AttributeError on `fetch_repair_rows`.

- [ ] **Step 3: Implement fetch_repair_rows**

Append to `KlinesRepairStrategy` in `src/crypto_data/binance_repair.py`:

```python
from datetime import timezone

from crypto_data.clients.binance_rest import BinanceRestClient


def _to_utc_ms(dt: datetime) -> int:
    return int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000)


class KlinesRepairStrategy:
    # ... (existing) ...

    async def fetch_repair_rows(self, client, gap: GapBoundary) -> pd.DataFrame:
        interval_ms = gap.expected_seconds * 1000
        prev_ms = _to_utc_ms(gap.prev_close)
        next_ms = _to_utc_ms(gap.next_close)
        start_ms = prev_ms + interval_ms
        end_ms = next_ms - interval_ms - 1
        if (start_ms - prev_ms) % interval_ms != 0:
            raise ValueError(f"off-grid prev_close for gap {gap}")
        if (end_ms - start_ms) > self.rest_limit * interval_ms:
            raise ValueError(
                f"gap too large for single REST call ({self.rest_limit} max); "
                f"re-download via Data Vision"
            )

        if self.data_type == DataType.SPOT:
            url = BinanceRestClient.build_spot_klines_url(
                gap.symbol, self.interval.value, start_ms, end_ms, self.rest_limit
            )
        else:
            url = BinanceRestClient.build_futures_klines_url(
                gap.symbol, self.interval.value, start_ms, end_ms, self.rest_limit
            )
        payload = await client.get_json(url)
        df = self.parse_payload(payload, symbol=gap.symbol)
        # Trim exclusive
        keep = (df["timestamp"] > gap.prev_close) & (df["timestamp"] < gap.next_close)
        df = df[keep].reset_index(drop=True)
        if not df.empty:
            OHLCV_SCHEMA.validate(df)
        return df
```

- [ ] **Step 4: Run tests**

Run: `pytest tests/repair/test_strategies.py -v`
Expected: 4 PASS.

- [ ] **Step 5: Commit**

```bash
git add src/crypto_data/binance_repair.py tests/repair/test_strategies.py
git commit -m "feat(repair): KlinesRepairStrategy.fetch_repair_rows with trim and guards"
```

---

## Task 13: FundingRatesRepairStrategy — parse + fetch (TDD)

**Files:**
- Modify: `src/crypto_data/binance_repair.py`
- Modify: `tests/repair/test_strategies.py`

- [ ] **Step 1: Write the failing tests**

```python
from crypto_data.binance_repair import FundingRatesRepairStrategy


def test_parse_funding_payload_to_dataframe():
    strategy = FundingRatesRepairStrategy()
    payload = [
        {"symbol": "BTCUSDT", "fundingTime": 1704067200000, "fundingRate": "0.00012", "markPrice": ""},
        {"symbol": "BTCUSDT", "fundingTime": 1704096000000, "fundingRate": "-0.00005", "markPrice": "42000"},
    ]
    df = strategy.parse_payload(payload, symbol="BTCUSDT")
    assert list(df.columns) == ["exchange", "symbol", "timestamp", "funding_rate"]
    assert len(df) == 2
    assert df.iloc[0]["timestamp"] == pd.Timestamp("2024-01-01T00:00")
    assert df.iloc[0]["funding_rate"] == pytest.approx(0.00012)
    assert df.iloc[1]["funding_rate"] == pytest.approx(-0.00005)


@pytest.mark.asyncio
async def test_fetch_funding_repair_rows_trim_exclusive():
    strategy = FundingRatesRepairStrategy()
    gap = GapBoundary(
        table="funding_rates", symbol="BTCUSDT", interval=None,
        prev_close=datetime(2024, 1, 2, 0, 0),
        next_close=datetime(2024, 1, 3, 8, 0),
        expected_seconds=8 * 3600,
    )
    # API returns 2 entries strictly between bounds + 1 boundary entry that must be trimmed.
    payload = [
        {"symbol": "BTCUSDT", "fundingTime": 1704153600000, "fundingRate": "0.0001", "markPrice": ""},  # 2024-01-02T08:00
        {"symbol": "BTCUSDT", "fundingTime": 1704182400000, "fundingRate": "0.0002", "markPrice": ""},  # 2024-01-02T16:00
        {"symbol": "BTCUSDT", "fundingTime": 1704211200000, "fundingRate": "0.0003", "markPrice": ""},  # 2024-01-03T00:00
        {"symbol": "BTCUSDT", "fundingTime": 1704240000000, "fundingRate": "0.0004", "markPrice": ""},  # 2024-01-03T08:00 (boundary, exclude)
    ]
    client = _FakeRestClient(payload)
    df = await strategy.fetch_repair_rows(client, gap)
    assert len(df) == 3
    assert all(df["timestamp"] > pd.Timestamp("2024-01-02T00:00"))
    assert all(df["timestamp"] < pd.Timestamp("2024-01-03T08:00"))
```

- [ ] **Step 2: Run to verify failure**

Run: `pytest tests/repair/test_strategies.py -v`
Expected: ImportError on `FundingRatesRepairStrategy`.

- [ ] **Step 3: Implement FundingRatesRepairStrategy**

Append to `src/crypto_data/binance_repair.py`:

```python
from crypto_data.schemas import FUNDING_RATES_SCHEMA


class FundingRatesRepairStrategy:
    REST_LIMIT = 1000
    table = "funding_rates"

    def parse_payload(self, payload: list[dict], symbol: str) -> pd.DataFrame:
        if not payload:
            return pd.DataFrame(columns=["exchange", "symbol", "timestamp", "funding_rate"])
        df = pd.DataFrame(payload)
        df["funding_rate"] = df["fundingRate"].astype(float)
        df["timestamp"] = pd.to_datetime(df["fundingTime"].astype("int64"), unit="ms")
        df["exchange"] = "binance"
        df["symbol"] = symbol
        return df[["exchange", "symbol", "timestamp", "funding_rate"]]

    async def fetch_repair_rows(self, client, gap: GapBoundary) -> pd.DataFrame:
        interval_ms = gap.expected_seconds * 1000
        start_ms = _to_utc_ms(gap.prev_close) + interval_ms
        end_ms = _to_utc_ms(gap.next_close) - 1
        if (end_ms - start_ms) > self.REST_LIMIT * interval_ms:
            raise ValueError(
                f"funding gap too large for single REST call ({self.REST_LIMIT} max); "
                f"re-download via Data Vision"
            )
        url = BinanceRestClient.build_funding_rate_url(gap.symbol, start_ms, end_ms, self.REST_LIMIT)
        payload = await client.get_json(url)
        df = self.parse_payload(payload, symbol=gap.symbol)
        keep = (df["timestamp"] > gap.prev_close) & (df["timestamp"] < gap.next_close)
        df = df[keep].reset_index(drop=True)
        if not df.empty:
            FUNDING_RATES_SCHEMA.validate(df)
        return df
```

- [ ] **Step 4: Run tests**

Run: `pytest tests/repair/test_strategies.py -v`
Expected: 6 PASS.

- [ ] **Step 5: Commit**

```bash
git add src/crypto_data/binance_repair.py tests/repair/test_strategies.py
git commit -m "feat(repair): FundingRatesRepairStrategy with parse + fetch"
```

---

## Task 14: UnrecoverableGap + RepairReport dataclasses (TDD)

**Files:**
- Modify: `src/crypto_data/binance_repair.py`
- Create: `tests/repair/test_pipeline.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/repair/test_pipeline.py
"""Tests for the repair_binance_gaps orchestrator and report dataclasses."""

from __future__ import annotations

from datetime import datetime

import pytest

from crypto_data.binance_repair import RepairReport, UnrecoverableGap


def test_repair_report_to_jsonable_round_trip():
    gap = UnrecoverableGap(
        table="futures", symbol="SOLUSDT", interval="4h",
        prev_close=datetime(2024, 1, 1, 16),
        next_close=datetime(2024, 1, 2, 0),
        missing_count=1,
        reason="partial_fill",
    )
    report = RepairReport(
        inserted_rows={"futures": 5},
        gaps_processed=2,
        gaps_fully_repaired=1,
        unrecoverable_gaps=[gap],
        errors=["network blip"],
        duration_seconds=1.5,
    )
    j = report.to_jsonable()
    assert j["inserted_rows"] == {"futures": 5}
    assert j["gaps_processed"] == 2
    assert j["unrecoverable_gaps"][0]["reason"] == "partial_fill"
    assert j["unrecoverable_gaps"][0]["prev_close"] == "2024-01-01T16:00:00"

    summary = report.summary()
    assert "5" in summary
    assert "partial_fill" in summary
```

- [ ] **Step 2: Run to verify failure**

Run: `pytest tests/repair/test_pipeline.py -v`
Expected: ImportError on RepairReport / UnrecoverableGap.

- [ ] **Step 3: Implement the dataclasses**

Append to `src/crypto_data/binance_repair.py`:

```python
from dataclasses import asdict, field
from typing import Any, Literal

GapReason = Literal["partial_fill", "network_error"]


@dataclass(frozen=True)
class UnrecoverableGap:
    table: str
    symbol: str
    interval: str | None
    prev_close: datetime
    next_close: datetime
    missing_count: int
    reason: GapReason


@dataclass(frozen=True)
class RepairReport:
    inserted_rows: dict[str, int]
    gaps_processed: int
    gaps_fully_repaired: int
    unrecoverable_gaps: list[UnrecoverableGap]
    errors: list[str]
    duration_seconds: float

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "inserted_rows": dict(self.inserted_rows),
            "gaps_processed": self.gaps_processed,
            "gaps_fully_repaired": self.gaps_fully_repaired,
            "unrecoverable_gaps": [
                {
                    **asdict(g),
                    "prev_close": g.prev_close.isoformat(),
                    "next_close": g.next_close.isoformat(),
                }
                for g in self.unrecoverable_gaps
            ],
            "errors": list(self.errors),
            "duration_seconds": self.duration_seconds,
        }

    def summary(self) -> str:
        total_inserted = sum(self.inserted_rows.values())
        lines = [
            f"Repair complete in {self.duration_seconds:.1f}s",
            f"  Gaps processed:       {self.gaps_processed}",
            f"  Fully repaired:       {self.gaps_fully_repaired}",
            f"  Rows inserted:        {total_inserted} ({self.inserted_rows})",
            f"  Unrecoverable gaps:   {len(self.unrecoverable_gaps)}",
        ]
        for g in self.unrecoverable_gaps:
            lines.append(
                f"    - {g.table}/{g.symbol}/{g.interval or '-'}: "
                f"{g.missing_count} missing, reason={g.reason}"
            )
        if self.errors:
            lines.append(f"  Errors:               {len(self.errors)}")
            for e in self.errors:
                lines.append(f"    - {e}")
        return "\n".join(lines)
```

- [ ] **Step 4: Run test**

Run: `pytest tests/repair/test_pipeline.py -v`
Expected: 1 PASS.

- [ ] **Step 5: Commit**

```bash
git add src/crypto_data/binance_repair.py tests/repair/test_pipeline.py
git commit -m "feat(repair): UnrecoverableGap and RepairReport dataclasses"
```

---

## Task 15: repair_binance_gaps — happy path single kline gap (TDD)

**Files:**
- Modify: `src/crypto_data/binance_repair.py`
- Modify: `tests/repair/conftest.py` (add `mock_rest_client_factory`)
- Modify: `tests/repair/test_pipeline.py`

- [ ] **Step 1: Add `mock_rest_client_factory` fixture**

Append to `tests/repair/conftest.py`:

```python
class MockRestClient:
    """Drop-in async client that returns scripted payloads keyed by URL substring."""
    def __init__(self):
        self._responses: dict[str, list] = {}      # url-substring -> list[payload]
        self.calls: list[str] = []

    def set_response(self, url_substring: str, payloads: list):
        self._responses[url_substring] = list(payloads)

    async def __aenter__(self): return self
    async def __aexit__(self, *a): return None

    async def get_json(self, url: str):
        self.calls.append(url)
        for sub, payloads in self._responses.items():
            if sub in url and payloads:
                return payloads.pop(0)
        return []


@pytest.fixture
def mock_rest_client():
    return MockRestClient()
```

- [ ] **Step 2: Write the failing pipeline test**

Append to `tests/repair/test_pipeline.py`:

```python
import pandas as pd

from crypto_data.binance_repair import repair_binance_gaps
from tests.repair.conftest import MockRestClient


def _kline_tuple(open_ts: pd.Timestamp) -> list:
    open_ms = int(open_ts.tz_localize("UTC").timestamp() * 1000)
    close_ms = open_ms + 4 * 3600 * 1000 - 1
    return [
        open_ms, "1.0", "2.0", "0.5", "1.5", "100.0",
        close_ms, "150.0", 42, "50.0", "75.0", "0",
    ]


def test_repair_happy_path_single_kline_gap(db_with_kline_gap, mock_rest_client):
    # 4 candles missing between 2024-01-01T20:00 and 2024-01-02T16:00 (exclusive both ends)
    missing_open = pd.date_range("2024-01-01T16:00", periods=4, freq="4h")
    payload = [_kline_tuple(ts) for ts in missing_open]
    mock_rest_client.set_response("/fapi/v1/klines", [payload])

    report = repair_binance_gaps(
        db_path=str(db_with_kline_gap.db_path),
        tables=["futures"],
        rest_client=mock_rest_client,
    )
    assert report.gaps_processed == 1
    assert report.gaps_fully_repaired == 1
    assert report.inserted_rows == {"futures": 4}
    assert report.unrecoverable_gaps == []

    # Verify DB
    rows = db_with_kline_gap.conn.execute(
        "SELECT timestamp FROM futures WHERE symbol='SOLUSDT' ORDER BY timestamp"
    ).fetchall()
    timestamps = [r[0] for r in rows]
    assert len(timestamps) == 14  # 5 + 4 + 5
```

- [ ] **Step 3: Run to verify failure**

Run: `pytest tests/repair/test_pipeline.py -v`
Expected: ImportError on `repair_binance_gaps`.

- [ ] **Step 4: Implement `repair_binance_gaps` core orchestration**

Append to `src/crypto_data/binance_repair.py`:

```python
import asyncio
import time

import duckdb

from crypto_data.utils.runtime import run_async_from_sync


DEFAULT_REPAIR_TABLES = ("spot", "futures", "funding_rates")


def repair_binance_gaps(
    db_path: str,
    *,
    tables: list[str] | None = None,
    symbols: list[str] | None = None,
    intervals: list[str] | None = None,
    max_concurrent: int = 5,
    rest_client=None,                # for tests; real callers don't pass this
) -> RepairReport:
    """Fill time-series gaps in DuckDB tables by fetching from Binance REST API.

    See docs/superpowers/specs/2026-04-30-binance-gap-repair-design.md for design.
    """
    tables = list(tables) if tables is not None else list(DEFAULT_REPAIR_TABLES)

    started = time.monotonic()
    inserted_rows: dict[str, int] = {t: 0 for t in tables}
    unrecoverable: list[UnrecoverableGap] = []
    errors: list[str] = []
    gaps_processed = 0
    gaps_fully_repaired = 0

    conn = duckdb.connect(db_path)
    try:
        # Enumerate gaps per table
        all_gaps: list[GapBoundary] = []
        for table in tables:
            if table in ("spot", "futures"):
                all_gaps.extend(enumerate_kline_gaps(conn, table=table, symbols=symbols, intervals=intervals))
            elif table == "funding_rates":
                all_gaps.extend(enumerate_metric_gaps(
                    conn, table="funding_rates",
                    expected_seconds=FUNDING_EXPECTED_SECONDS,
                    symbols=symbols,
                ))
            else:
                raise ValueError(f"Unsupported table for repair: {table}")

        gaps_processed = len(all_gaps)
        if not all_gaps:
            return RepairReport(
                inserted_rows=inserted_rows,
                gaps_processed=0, gaps_fully_repaired=0,
                unrecoverable_gaps=[], errors=[],
                duration_seconds=time.monotonic() - started,
            )

        # Run async fetch loop
        async def run_all():
            owns_client = rest_client is None
            from crypto_data.clients.binance_rest import BinanceRestClient
            client = rest_client or BinanceRestClient(max_concurrent=max_concurrent)
            ctx = client if owns_client else _NullContext(client)
            async with ctx as c:
                sem = asyncio.Semaphore(max_concurrent)
                async def process(gap: GapBoundary):
                    async with sem:
                        return await _process_one_gap(conn, c, gap)
                return await asyncio.gather(*(process(g) for g in all_gaps))

        results = run_async_from_sync(run_all(), "repair_binance_gaps")
        for outcome in results:
            inserted_rows.setdefault(outcome.table, 0)
            inserted_rows[outcome.table] += outcome.inserted
            if outcome.unrecoverable is not None:
                unrecoverable.append(outcome.unrecoverable)
            if outcome.error is not None:
                errors.append(outcome.error)
            if outcome.fully_repaired:
                gaps_fully_repaired += 1

    finally:
        conn.close()

    return RepairReport(
        inserted_rows=inserted_rows,
        gaps_processed=gaps_processed,
        gaps_fully_repaired=gaps_fully_repaired,
        unrecoverable_gaps=unrecoverable,
        errors=errors,
        duration_seconds=time.monotonic() - started,
    )


@dataclass
class _GapOutcome:
    table: str
    inserted: int = 0
    fully_repaired: bool = False
    unrecoverable: UnrecoverableGap | None = None
    error: str | None = None


class _NullContext:
    def __init__(self, c): self._c = c
    async def __aenter__(self): return self._c
    async def __aexit__(self, *a): return None


def _expected_count(gap: GapBoundary) -> int:
    interval = gap.expected_seconds
    span = int((gap.next_close - gap.prev_close).total_seconds())
    return span // interval - 1


async def _process_one_gap(conn, client, gap: GapBoundary) -> _GapOutcome:
    expected = _expected_count(gap)
    try:
        if gap.table in ("spot", "futures"):
            from crypto_data.enums import DataType, Interval
            data_type = DataType.SPOT if gap.table == "spot" else DataType.FUTURES
            interval_enum = Interval(gap.interval)
            strategy = KlinesRepairStrategy(data_type=data_type, interval=interval_enum)
        elif gap.table == "funding_rates":
            strategy = FundingRatesRepairStrategy()
        else:
            raise ValueError(f"unsupported table {gap.table!r}")

        df = await strategy.fetch_repair_rows(client, gap)
    except Exception as exc:  # noqa: BLE001
        logger.error("Gap fetch failed for %s: %r", gap, exc)
        return _GapOutcome(
            table=gap.table,
            unrecoverable=UnrecoverableGap(
                table=gap.table, symbol=gap.symbol, interval=gap.interval,
                prev_close=gap.prev_close, next_close=gap.next_close,
                missing_count=expected, reason="network_error",
            ),
            error=f"{gap.symbol}/{gap.interval}: {exc!r}",
        )

    inserted = 0
    if not df.empty:
        inserted = _insert_idempotent(conn, gap.table, df)

    if len(df) >= expected:
        return _GapOutcome(table=gap.table, inserted=inserted, fully_repaired=True)
    return _GapOutcome(
        table=gap.table,
        inserted=inserted,
        unrecoverable=UnrecoverableGap(
            table=gap.table, symbol=gap.symbol, interval=gap.interval,
            prev_close=gap.prev_close, next_close=gap.next_close,
            missing_count=expected - len(df), reason="partial_fill",
        ),
    )


def _insert_idempotent(conn, table: str, df: pd.DataFrame) -> int:
    if table in ("spot", "futures"):
        join = ("e.exchange=i.exchange AND e.symbol=i.symbol "
                "AND e.interval=i.interval AND e.timestamp=i.timestamp")
    else:
        join = "e.exchange=i.exchange AND e.symbol=i.symbol AND e.timestamp=i.timestamp"
    conn.execute("BEGIN")
    try:
        n = conn.execute(
            f"SELECT COUNT(*) FROM df AS i LEFT JOIN {table} AS e ON {join} WHERE e.exchange IS NULL"
        ).fetchone()[0]
        conn.execute(f"INSERT OR IGNORE INTO {table} SELECT * FROM df")
        conn.execute("COMMIT")
        return int(n)
    except Exception:
        conn.execute("ROLLBACK")
        raise
```

- [ ] **Step 5: Run tests**

Run: `pytest tests/repair/test_pipeline.py -v`
Expected: 2 PASS.

- [ ] **Step 6: Commit**

```bash
git add src/crypto_data/binance_repair.py tests/repair/conftest.py tests/repair/test_pipeline.py
git commit -m "feat(repair): repair_binance_gaps orchestration core (happy path)"
```

---

## Task 16: repair_binance_gaps — partial_fill and network_error classification (TDD)

**Files:**
- Modify: `tests/repair/test_pipeline.py`

- [ ] **Step 1: Write the failing tests**

```python
def test_repair_partial_fill_classified(db_with_kline_gap, mock_rest_client):
    # API returns only 2 of the 4 missing candles
    missing_open = pd.date_range("2024-01-01T16:00", periods=2, freq="4h")
    payload = [_kline_tuple(ts) for ts in missing_open]
    mock_rest_client.set_response("/fapi/v1/klines", [payload])

    report = repair_binance_gaps(
        db_path=str(db_with_kline_gap.db_path),
        tables=["futures"],
        rest_client=mock_rest_client,
    )
    assert report.gaps_processed == 1
    assert report.gaps_fully_repaired == 0
    assert report.inserted_rows == {"futures": 2}
    assert len(report.unrecoverable_gaps) == 1
    u = report.unrecoverable_gaps[0]
    assert u.reason == "partial_fill"
    assert u.missing_count == 2


def test_repair_empty_response_full_partial_fill(db_with_kline_gap, mock_rest_client):
    mock_rest_client.set_response("/fapi/v1/klines", [[]])
    report = repair_binance_gaps(
        db_path=str(db_with_kline_gap.db_path),
        tables=["futures"],
        rest_client=mock_rest_client,
    )
    assert report.inserted_rows == {"futures": 0}
    assert len(report.unrecoverable_gaps) == 1
    assert report.unrecoverable_gaps[0].reason == "partial_fill"
    assert report.unrecoverable_gaps[0].missing_count == 4


def test_repair_network_error_classified(db_with_kline_gap):
    class RaisingClient:
        async def __aenter__(self): return self
        async def __aexit__(self, *a): return None
        async def get_json(self, url):
            from crypto_data.clients.binance_rest import BinanceRateLimitError
            raise BinanceRateLimitError("simulated 429 exhausted")

    report = repair_binance_gaps(
        db_path=str(db_with_kline_gap.db_path),
        tables=["futures"],
        rest_client=RaisingClient(),
    )
    assert report.inserted_rows == {"futures": 0}
    assert len(report.unrecoverable_gaps) == 1
    assert report.unrecoverable_gaps[0].reason == "network_error"
    assert any("simulated 429" in e for e in report.errors)
```

- [ ] **Step 2: Run tests**

Run: `pytest tests/repair/test_pipeline.py -v`
Expected: 5 PASS (3 new + 2 existing). The orchestrator from Task 15 already classifies these correctly, since `_process_one_gap` distinguishes exception path vs incomplete-DataFrame path.

- [ ] **Step 3: Commit**

```bash
git add tests/repair/test_pipeline.py
git commit -m "test(repair): cover partial_fill and network_error classifications"
```

---

## Task 17: repair_binance_gaps — 1000-prefix probe + multi-table aggregation + idempotence (TDD)

**Files:**
- Modify: `src/crypto_data/binance_repair.py`
- Modify: `tests/repair/test_pipeline.py`

- [ ] **Step 1: Write the failing tests**

```python
def test_repair_1000_prefix_probe(empty_db, mock_rest_client):
    # DB has PEPEUSDT (original symbol). Binance REST requires 1000PEPEUSDT.
    from tests.repair.conftest import _insert_klines
    pre = pd.date_range("2024-01-01T00:00", periods=3, freq="4h")
    post = pd.date_range("2024-01-02T00:00", periods=3, freq="4h")
    _insert_klines(empty_db.conn, "futures", "PEPEUSDT", "4h", list(pre) + list(post))

    # First call (PEPEUSDT) → 400; retry call (1000PEPEUSDT) → success.
    class ProbingClient:
        def __init__(self):
            self.calls = []
            self.candles = pd.date_range("2024-01-01T08:00", periods=2, freq="4h")
        async def __aenter__(self): return self
        async def __aexit__(self, *a): return None
        async def get_json(self, url):
            self.calls.append(url)
            if "symbol=1000PEPEUSDT" in url:
                return [_kline_tuple(ts) for ts in self.candles]
            import aiohttp
            raise aiohttp.ClientResponseError(
                request_info=None, history=(), status=400, message="invalid symbol"
            )
    client = ProbingClient()

    report = repair_binance_gaps(
        db_path=str(empty_db.db_path),
        tables=["futures"],
        rest_client=client,
    )
    # The probe should have been triggered, second call succeeded
    assert any("1000PEPEUSDT" in c for c in client.calls)
    assert report.inserted_rows == {"futures": 2}
    assert report.gaps_fully_repaired == 1


def test_repair_idempotent_second_run(db_with_kline_gap, mock_rest_client):
    missing_open = pd.date_range("2024-01-01T16:00", periods=4, freq="4h")
    mock_rest_client.set_response("/fapi/v1/klines", [
        [_kline_tuple(ts) for ts in missing_open],
        [_kline_tuple(ts) for ts in missing_open],   # second run gets same payload
    ])
    r1 = repair_binance_gaps(
        db_path=str(db_with_kline_gap.db_path),
        tables=["futures"],
        rest_client=mock_rest_client,
    )
    assert r1.inserted_rows["futures"] == 4

    r2 = repair_binance_gaps(
        db_path=str(db_with_kline_gap.db_path),
        tables=["futures"],
        rest_client=mock_rest_client,
    )
    # Second run: gaps already filled, enumerator returns [], so no API call needed.
    assert r2.gaps_processed == 0
    assert r2.inserted_rows == {"futures": 0}


def test_repair_filters_by_table(db_with_kline_gap, db_with_funding_gap, mock_rest_client):
    # Combined DB: write funding into the kline-gap DB so both tables have gaps.
    from tests.repair.conftest import _insert_funding
    pre = pd.date_range("2024-01-01T00:00", periods=4, freq="8h")
    post = pd.date_range("2024-01-03T08:00", periods=4, freq="8h")
    _insert_funding(db_with_kline_gap.conn, "BTCUSDT", list(pre) + list(post))

    missing_open = pd.date_range("2024-01-01T16:00", periods=4, freq="4h")
    mock_rest_client.set_response("/fapi/v1/klines", [[_kline_tuple(ts) for ts in missing_open]])

    report = repair_binance_gaps(
        db_path=str(db_with_kline_gap.db_path),
        tables=["futures"],
        rest_client=mock_rest_client,
    )
    # Only futures gap was processed; funding ignored.
    assert report.gaps_processed == 1
    assert "funding_rates" not in report.inserted_rows or report.inserted_rows["funding_rates"] == 0
```

- [ ] **Step 2: Run to verify failure**

Run: `pytest tests/repair/test_pipeline.py -v`
Expected: the 1000-prefix test fails — orchestrator currently has no probe logic.

- [ ] **Step 3: Add 1000-prefix probe in `_process_one_gap`**

In `src/crypto_data/binance_repair.py`, replace `_process_one_gap` to wrap `strategy.fetch_repair_rows(...)` with a probe-on-400 helper that mutates the URL by prefixing `1000` to the symbol once:

```python
import aiohttp


_TICKER_PROBE_CACHE: dict[str, str] = {}  # local-to-process cache for the run


async def _fetch_with_prefix_probe(strategy, client, gap: GapBoundary) -> pd.DataFrame:
    cached = _TICKER_PROBE_CACHE.get(gap.symbol)
    effective_gap = gap if cached is None else _replace_symbol(gap, cached)
    try:
        return await strategy.fetch_repair_rows(client, effective_gap)
    except aiohttp.ClientResponseError as exc:
        if exc.status != 400 or cached is not None or gap.table == "spot":
            raise
        # Probe with 1000 prefix
        if gap.symbol.startswith("1000"):
            raise
        prefixed = "1000" + gap.symbol
        logger.info("Probing %s with prefix → %s", gap.symbol, prefixed)
        new_gap = _replace_symbol(gap, prefixed)
        df = await strategy.fetch_repair_rows(client, new_gap)
        _TICKER_PROBE_CACHE[gap.symbol] = prefixed
        # Restore original symbol on the DataFrame for DB insert.
        df = df.copy()
        if "symbol" in df.columns:
            df["symbol"] = gap.symbol
        return df


def _replace_symbol(gap: GapBoundary, new_symbol: str) -> GapBoundary:
    return GapBoundary(
        table=gap.table, symbol=new_symbol, interval=gap.interval,
        prev_close=gap.prev_close, next_close=gap.next_close,
        expected_seconds=gap.expected_seconds,
    )
```

Replace the `df = await strategy.fetch_repair_rows(client, gap)` line in `_process_one_gap` with:

```python
df = await _fetch_with_prefix_probe(strategy, client, gap)
```

Also clear `_TICKER_PROBE_CACHE` at the top of `repair_binance_gaps`:

```python
_TICKER_PROBE_CACHE.clear()
```

- [ ] **Step 4: Run all repair tests**

Run: `pytest tests/repair/ -v`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add src/crypto_data/binance_repair.py tests/repair/test_pipeline.py
git commit -m "feat(repair): add 1000-prefix probe and cover idempotence + table filter"
```

---

## Task 18: Pipeline integration — `repair_gaps_via_api` flag in `update_binance_market_data` (TDD)

**Files:**
- Modify: `src/crypto_data/binance_pipeline.py:495-598` (function signature + body)
- Modify: `tests/binance/test_pipeline.py` (add unit test)

- [ ] **Step 1: Write the failing test**

Append to `tests/binance/test_pipeline.py` (or a new file `tests/binance/test_pipeline_repair_flag.py`):

```python
from unittest.mock import patch

import pytest

from crypto_data import update_binance_market_data
from crypto_data.enums import DataType, Interval


def test_update_binance_market_data_calls_repair_when_flag_set(tmp_path):
    db_path = tmp_path / "test.db"
    with patch("crypto_data.binance_pipeline.run_async_from_sync") as fake_async, \
         patch("crypto_data.binance_pipeline.repair_binance_gaps") as fake_repair:
        fake_async.return_value = None
        update_binance_market_data(
            db_path=str(db_path),
            symbols=["BTCUSDT"],
            data_types=[DataType.SPOT],
            start_date="2024-01-01",
            end_date="2024-01-02",
            interval=Interval.HOUR_4,
            repair_gaps_via_api=True,
        )
    fake_repair.assert_called_once()


def test_update_binance_market_data_skips_repair_by_default(tmp_path):
    db_path = tmp_path / "test.db"
    with patch("crypto_data.binance_pipeline.run_async_from_sync") as fake_async, \
         patch("crypto_data.binance_pipeline.repair_binance_gaps") as fake_repair:
        fake_async.return_value = None
        update_binance_market_data(
            db_path=str(db_path),
            symbols=["BTCUSDT"],
            data_types=[DataType.SPOT],
            start_date="2024-01-01",
            end_date="2024-01-02",
            interval=Interval.HOUR_4,
        )
    fake_repair.assert_not_called()
```

- [ ] **Step 2: Run to verify failure**

Run: `pytest tests/binance/ -v -k repair`
Expected: FAIL — `repair_gaps_via_api` not a recognized kwarg yet.

- [ ] **Step 3: Add the flag and the call**

In `src/crypto_data/binance_pipeline.py`:

1. Add at top:
```python
from crypto_data.binance_repair import repair_binance_gaps
```

2. Update signature of `update_binance_market_data` to add `repair_gaps_via_api: bool = False`.

3. After `run_async_from_sync(...)` and before `log_ingestion_summary(...)`, add:
```python
if repair_gaps_via_api:
    logger.info("Running gap repair via Binance REST API…")
    repair_report = repair_binance_gaps(
        db_path=db_path,
        symbols=symbols,
        intervals=[interval.value],
        tables=[dt.value for dt in data_types if dt.value in ("spot", "futures", "funding_rates")],
    )
    logger.info(repair_report.summary())
```

- [ ] **Step 4: Run tests**

Run: `pytest tests/binance/ -v -k repair`
Expected: 2 PASS.

- [ ] **Step 5: Commit**

```bash
git add src/crypto_data/binance_pipeline.py tests/binance/test_pipeline.py
git commit -m "feat(pipeline): repair_gaps_via_api flag in update_binance_market_data"
```

---

## Task 19: Pipeline integration — propagate flag in `create_binance_database`

**Files:**
- Modify: `src/crypto_data/database_builder.py:367-498`
- Modify: `tests/binance/test_pipeline.py` or `tests/pipeline/`

- [ ] **Step 1: Write the failing test**

```python
def test_create_binance_database_propagates_repair_flag(tmp_path):
    from unittest.mock import patch
    from crypto_data import create_binance_database

    db_path = str(tmp_path / "test.db")
    with patch("crypto_data.database_builder.run_async_from_sync"), \
         patch("crypto_data.database_builder.update_binance_market_data") as fake_update, \
         patch("crypto_data.database_builder.update_coinmarketcap_universe"):
        create_binance_database(
            db_path=db_path,
            start_date="2024-01-01",
            end_date="2024-01-02",
            top_n=10,
            repair_gaps_via_api=True,
        )
    args, kwargs = fake_update.call_args
    assert kwargs.get("repair_gaps_via_api") is True
```

- [ ] **Step 2: Run to verify failure**

Expected: `create_binance_database` does not yet accept `repair_gaps_via_api`.

- [ ] **Step 3: Add the parameter and pass-through**

In `src/crypto_data/database_builder.py`:
1. Add `repair_gaps_via_api: bool = False` to `create_binance_database` signature.
2. In the call to `update_binance_market_data(...)` near the end, add `repair_gaps_via_api=repair_gaps_via_api`.
3. Update the docstring.

- [ ] **Step 4: Run test**

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/crypto_data/database_builder.py tests/binance/test_pipeline.py
git commit -m "feat(pipeline): propagate repair_gaps_via_api in create_binance_database"
```

---

## Task 20: CLI script + Public API exports

**Files:**
- Create: `scripts/repair_binance_gaps.py`
- Modify: `src/crypto_data/__init__.py`

- [ ] **Step 1: Write the CLI**

```python
#!/usr/bin/env python3
"""Repair gaps in a crypto-data DuckDB by fetching missing rows from Binance REST."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from crypto_data import repair_binance_gaps
from crypto_data.logging_utils import setup_colored_logging


def parse_args(argv=None):
    p = argparse.ArgumentParser(description="Fill time-series gaps via Binance REST API.")
    p.add_argument("db_path")
    p.add_argument("--tables", nargs="+",
                   choices=["spot", "futures", "funding_rates"],
                   default=None,
                   help="Subset of tables to repair. Default: all three.")
    p.add_argument("--symbol", action="append", dest="symbols",
                   help="Symbol filter (repeatable).")
    p.add_argument("--interval", action="append", dest="intervals",
                   help="Kline interval filter (repeatable).")
    p.add_argument("--max-concurrent", type=int, default=5)
    p.add_argument("--json", dest="json_path", default=None,
                   help="Write the report as JSON to this path.")
    return p.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)
    setup_colored_logging()
    report = repair_binance_gaps(
        db_path=args.db_path,
        tables=args.tables,
        symbols=args.symbols,
        intervals=args.intervals,
        max_concurrent=args.max_concurrent,
    )
    print(report.summary())
    if args.json_path:
        Path(args.json_path).write_text(
            json.dumps(report.to_jsonable(), indent=2, sort_keys=True),
            encoding="utf-8",
        )
    return 1 if report.unrecoverable_gaps else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
```

- [ ] **Step 2: Update package __init__.py**

Add to `src/crypto_data/__init__.py`:

```python
from .binance_repair import repair_binance_gaps, RepairReport, UnrecoverableGap
```

And add the names to `__all__`:

```python
"repair_binance_gaps",
"RepairReport",
"UnrecoverableGap",
```

- [ ] **Step 3: Smoke-test the CLI on an empty DB**

Run:
```bash
uv run python -c "from crypto_data import CryptoDatabase; CryptoDatabase('/tmp/empty.db').close()"
uv run python scripts/repair_binance_gaps.py /tmp/empty.db
```
Expected: prints "Repair complete in 0.0s … Gaps processed: 0".

- [ ] **Step 4: Commit**

```bash
git add scripts/repair_binance_gaps.py src/crypto_data/__init__.py
git commit -m "feat(repair): add CLI script and public API exports"
```

---

## Task 21: Live API validation test

**Files:**
- Create: `tests/repair/test_repair_validation.py`

- [ ] **Step 1: Write the validation test**

```python
# tests/repair/test_repair_validation.py
"""Live Binance REST round-trip — opt-in via --run-validation."""

from __future__ import annotations

import pandas as pd
import pytest

from crypto_data.binance_repair import (
    GapBoundary,
    KlinesRepairStrategy,
)
from crypto_data.clients.binance_rest import BinanceRestClient
from crypto_data.enums import DataType, Interval


@pytest.mark.validation
@pytest.mark.asyncio
async def test_live_solusdt_futures_4h_gap_returns_candles():
    """Sanity-check the SOLUSDT futures 4h gap reported by validate_data_quality.py.

    Gap reference: 2022-03-01T04:00 → 2022-04-03T04:00 (33 days, 198 candles).
    Single REST call via fetch_repair_rows must return all 197 missing candles.
    """
    gap = GapBoundary(
        table="futures", symbol="SOLUSDT", interval="4h",
        prev_close=pd.Timestamp("2022-03-01T04:00").to_pydatetime(),
        next_close=pd.Timestamp("2022-04-03T04:00").to_pydatetime(),
        expected_seconds=14400,
    )
    strategy = KlinesRepairStrategy(data_type=DataType.FUTURES, interval=Interval.HOUR_4)
    async with BinanceRestClient(max_concurrent=1) as client:
        df = await strategy.fetch_repair_rows(client, gap)
    expected_count = 33 * 24 // 4 - 1  # 197 strictly between bounds
    assert len(df) == expected_count
    assert df["timestamp"].is_monotonic_increasing
    assert df["timestamp"].min() > gap.prev_close
    assert df["timestamp"].max() < gap.next_close
```

- [ ] **Step 2: Verify the test is skipped by default**

Run: `pytest tests/repair/test_repair_validation.py -v`
Expected: SKIPPED (because `validation` marker is excluded by default).

- [ ] **Step 3: Verify the test runs when opt-in**

Run: `pytest tests/repair/test_repair_validation.py -v --run-validation`
Expected: PASS (live HTTP call to Binance, ~1-2 seconds). If Binance is unreachable the test fails — that is the intended signal.

- [ ] **Step 4: Commit**

```bash
git add tests/repair/test_repair_validation.py
git commit -m "test(repair): live validation test against SOLUSDT futures gap"
```

---

## Task 22: Final regression sweep

**Files:** none (validation only)

- [ ] **Step 1: Run the full unit test suite**

Run: `pytest tests/ -v`
Expected: all green, no regressions in `tests/quality/`, `tests/binance/`, `tests/binance_datasets/`, etc.

- [ ] **Step 2: Run quality audit on a real DB to confirm no regression**

Run: `uv run python scripts/validate_data_quality.py crypto_top60_h4_2022_01_2026_04.db --tables futures --json /tmp/post_refactor_quality.json`
Diff against the pre-existing `quality_report_top60_h4_2022_01_2026_04.json`:

```bash
diff <(jq '.[] | select(.check_name=="time_gaps") | {symbol,interval,count,metadata}' \
        quality_report_top60_h4_2022_01_2026_04.json | sort) \
     <(jq '.[] | select(.check_name=="time_gaps") | {symbol,interval,count,metadata}' \
        /tmp/post_refactor_quality.json | sort)
```
Expected: empty diff. Same `time_gaps` findings as before the refactor.

- [ ] **Step 3: Smoke-test repair on the real DB (optional dry run)**

Run:
```bash
uv run python scripts/repair_binance_gaps.py crypto_top60_h4_2022_01_2026_04.db \
    --tables futures \
    --json /tmp/repair_smoke.json
```
Expected: report shows `gaps_processed=30`, most or all `gaps_fully_repaired`. Verify by re-running `validate_data_quality.py` and confirming the `time_gaps` count drops.

- [ ] **Step 4: Commit nothing if all green; otherwise create follow-up tasks**

If anything regresses, do NOT commit "fix" patches inside this final task — surface the regression and add a new task to address it. Otherwise the implementation is complete.

---

## Self-Review

**Spec coverage check** (against `docs/superpowers/specs/2026-04-30-binance-gap-repair-design.md`):

| Spec section | Covered by tasks |
|---|---|
| §3 Non-goals (no OI, no v=0 replacement, no pagination, no dry_run, no re-audit, no edge gaps) | Reflected throughout — never implemented |
| §4 Public API (`repair_binance_gaps`, flag in pipeline + create_binance_database, CLI) | Tasks 15-20 |
| §5.1 New files | Tasks 6-13 (client + repair module), Task 20 (CLI) |
| §5.2 Modified files | Task 5 (quality.py), Task 18 (binance_pipeline.py), Task 19 (database_builder.py), Task 20 (__init__.py) |
| §6.1 Klines flow + idempotent insert | Task 11-12 (parse + fetch), Task 15 (insert) |
| §6.2 Funding flow | Task 13 |
| §6.3 Concurrency (asyncio.gather + sem) | Task 7 + Task 15 |
| §6.4 3-layer idempotence | Tasks 12, 13, 15 + idempotence test in Task 17 |
| §7.1 HTTP retry matrix | Tasks 8, 9, 10 |
| §7.2 Pipeline classification (2 reasons) | Task 16 |
| §7.3 1000-prefix probe | Task 17 |
| §7.4 Schema-aligned guards | Task 12 (off-grid + too-large), Pandera in Tasks 12-13 |
| §8 Logging | Implicit throughout (`logger.info/warning/error/debug` in implementations) |
| §9.1-9.3 Test layout | Tasks 1-21 each create a tests/repair/ file matching the spec table |
| §11 Backwards compatibility (regression test) | Task 5 step 1 |

No spec section is uncovered. No types or method names diverge between tasks (`fetch_repair_rows`, `parse_payload`, `enumerate_kline_gaps`, `enumerate_metric_gaps`, `GapBoundary`, `RepairReport`, `UnrecoverableGap`, `repair_binance_gaps`, `BinanceRestClient.get_json` are consistent end-to-end).

No placeholders or "similar to Task N" stubs.
