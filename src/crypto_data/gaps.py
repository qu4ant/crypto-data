"""Neutral gap enumeration utilities for DuckDB time-series tables."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import duckdb

from crypto_data.intervals import KLINE_INTERVAL_SECONDS
from crypto_data.tables import KLINE_TABLES, METRIC_TABLES


@dataclass(frozen=True)
class GapBoundary:
    """One internal missing range bounded by two observed rows."""

    table: str
    symbol: str
    interval: str | None
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
    """List every individual gap in a fixed-cadence kline table."""
    if table not in KLINE_TABLES:
        raise ValueError(f"enumerate_kline_gaps only supports spot/futures, got {table!r}")

    expected_values = ", ".join(
        f"('{interval}', {seconds})" for interval, seconds in KLINE_INTERVAL_SECONDS.items()
    )
    filter_sql, params = _filters(symbols=symbols, intervals=intervals, alias="t")
    rows = conn.execute(
        f"""
        WITH expected(interval, expected_seconds) AS (
            VALUES {expected_values}
        ),
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
        """,
        params,
    ).fetchall()

    return [
        GapBoundary(
            table=table,
            symbol=symbol,
            interval=interval,
            prev_close=prev_close,
            next_close=next_close,
            expected_seconds=int(expected_seconds),
        )
        for symbol, interval, prev_close, next_close, expected_seconds in rows
    ]


def enumerate_metric_gaps(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    expected_seconds: int,
    *,
    symbols: Sequence[str] | None = None,
) -> list[GapBoundary]:
    """List every individual gap in a single-cadence metric table."""
    if table not in METRIC_TABLES:
        raise ValueError(
            f"enumerate_metric_gaps only supports open_interest/funding_rates, got {table!r}"
        )

    filter_sql, params = _filters(symbols=symbols, alias="t")
    rows = conn.execute(
        f"""
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
        """,
        [*params, expected_seconds],
    ).fetchall()

    return [
        GapBoundary(
            table=table,
            symbol=symbol,
            interval=None,
            prev_close=prev_close,
            next_close=next_close,
            expected_seconds=expected_seconds,
        )
        for symbol, prev_close, next_close in rows
    ]


def _filters(
    *,
    symbols: Sequence[str] | None = None,
    intervals: Sequence[str] | None = None,
    alias: str = "t",
) -> tuple[str, list[Any]]:
    conditions = [f"{alias}.exchange = 'binance'"]
    params: list[Any] = []
    if symbols:
        conditions.append(f"{alias}.symbol IN ({_placeholders(symbols)})")
        params.extend(symbols)
    if intervals:
        conditions.append(f"{alias}.interval IN ({_placeholders(intervals)})")
        params.extend(intervals)
    return "WHERE " + " AND ".join(conditions), params


def _placeholders(values: Sequence[str]) -> str:
    return ", ".join("?" for _ in values)
