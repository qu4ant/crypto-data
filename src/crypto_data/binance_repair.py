"""Repair Binance time-series gaps through the public REST API."""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

import aiohttp
import duckdb
import pandas as pd

from crypto_data.binance_datasets.klines import FINAL_COLUMNS as KLINE_FINAL_COLUMNS
from crypto_data.clients.binance_rest import BinanceRestClient
from crypto_data.enums import DataType, Interval
from crypto_data.schemas import FUNDING_RATES_SCHEMA, OHLCV_SCHEMA
from crypto_data.utils.runtime import run_async_from_sync

logger = logging.getLogger(__name__)

KLINE_INTERVAL_SECONDS = {
    "1m": 60,
    "3m": 3 * 60,
    "5m": 5 * 60,
    "15m": 15 * 60,
    "30m": 30 * 60,
    "1h": 60 * 60,
    "2h": 2 * 60 * 60,
    "4h": 4 * 60 * 60,
    "6h": 6 * 60 * 60,
    "8h": 8 * 60 * 60,
    "12h": 12 * 60 * 60,
    "1d": 24 * 60 * 60,
    "3d": 3 * 24 * 60 * 60,
    "1w": 7 * 24 * 60 * 60,
}
FUNDING_EXPECTED_SECONDS = 8 * 60 * 60
DEFAULT_REPAIR_TABLES = ("spot", "futures", "funding_rates")

GapReason = Literal["partial_fill", "network_error"]


@dataclass(frozen=True)
class GapBoundary:
    """One internal missing range bounded by two observed rows."""

    table: str
    symbol: str
    interval: str | None
    prev_close: datetime
    next_close: datetime
    expected_seconds: int


@dataclass(frozen=True)
class UnrecoverableGap:
    """Gap that the REST repair run could not completely fill."""

    table: str
    symbol: str
    interval: str | None
    prev_close: datetime
    next_close: datetime
    missing_count: int
    reason: GapReason


@dataclass(frozen=True)
class RepairReport:
    """Summary returned by :func:`repair_binance_gaps`."""

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
                    **asdict(gap),
                    "prev_close": gap.prev_close.isoformat(),
                    "next_close": gap.next_close.isoformat(),
                }
                for gap in self.unrecoverable_gaps
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
        lines.extend(
            f"    - {gap.table}/{gap.symbol}/{gap.interval or '-'}: "
            f"{gap.missing_count} missing, reason={gap.reason}"
            for gap in self.unrecoverable_gaps
        )
        if self.errors:
            lines.append(f"  Errors:               {len(self.errors)}")
            lines.extend(f"    - {error}" for error in self.errors)
        return "\n".join(lines)


@dataclass(frozen=True)
class _FetchedGap:
    gap: GapBoundary
    df: pd.DataFrame
    missing_count: int
    error: str | None = None


def enumerate_kline_gaps(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    *,
    symbols: Sequence[str] | None = None,
    intervals: Sequence[str] | None = None,
) -> list[GapBoundary]:
    """List every individual gap in a kline table."""
    if table not in ("spot", "futures"):
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
    if table not in ("open_interest", "funding_rates"):
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


class KlinesRepairStrategy:
    """Fetch missing kline candles and parse them to DB-ready DataFrames."""

    REST_LIMIT_SPOT = 1000
    REST_LIMIT_FUTURES = 1500

    def __init__(self, data_type: DataType, interval: Interval) -> None:
        if data_type not in (DataType.SPOT, DataType.FUTURES):
            raise ValueError(f"KlinesRepairStrategy supports SPOT/FUTURES only, got {data_type}")
        self.data_type = data_type
        self.interval = interval
        self.rest_limit = (
            self.REST_LIMIT_SPOT if data_type == DataType.SPOT else self.REST_LIMIT_FUTURES
        )

    def parse_payload(self, payload: list[list], symbol: str) -> pd.DataFrame:
        if not payload:
            return pd.DataFrame(columns=KLINE_FINAL_COLUMNS)

        df = pd.DataFrame(
            payload,
            columns=[
                "open_time",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "close_time",
                "quote_volume",
                "trades_count",
                "taker_buy_base_volume",
                "taker_buy_quote_volume",
                "ignore",
            ],
        )
        numeric_columns = [
            "open",
            "high",
            "low",
            "close",
            "volume",
            "quote_volume",
            "taker_buy_base_volume",
            "taker_buy_quote_volume",
        ]
        for column in numeric_columns:
            df[column] = df[column].astype(float)
        df["trades_count"] = df["trades_count"].astype("Int64")
        df["close_time"] = df["close_time"].astype("int64")

        df["exchange"] = "binance"
        df["symbol"] = symbol
        df["interval"] = self.interval.value
        df["timestamp"] = pd.to_datetime(df["close_time"] + 1, unit="ms").dt.ceil("1s")
        df = df[KLINE_FINAL_COLUMNS].drop_duplicates(
            subset=["exchange", "symbol", "interval", "timestamp"]
        )
        return df.reset_index(drop=True)

    async def fetch_repair_rows(self, client, gap: GapBoundary) -> pd.DataFrame:
        if gap.interval != self.interval.value:
            raise ValueError(
                f"gap interval {gap.interval!r} does not match {self.interval.value!r}"
            )

        interval_ms = gap.expected_seconds * 1000
        prev_ms = _to_utc_ms(gap.prev_close)
        next_ms = _to_utc_ms(gap.next_close)
        # Binance kline startTime filters on open time, while our DB key is
        # the normalized close boundary. The first missing close key is
        # prev_close + interval, whose open time is prev_close.
        start_ms = prev_ms
        end_ms = next_ms - interval_ms - 1

        if prev_ms % interval_ms != 0 or next_ms % interval_ms != 0:
            raise ValueError(f"off-grid gap bounds for {gap}")
        if start_ms > end_ms:
            return pd.DataFrame(columns=KLINE_FINAL_COLUMNS)
        if _expected_missing_count(gap) > self.rest_limit:
            raise ValueError(
                f"gap too large for single REST call ({self.rest_limit} max); "
                "re-download via Data Vision"
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
        df = _trim_exclusive(df, gap)
        if not df.empty:
            df = OHLCV_SCHEMA.validate(df)
        return df


class FundingRatesRepairStrategy:
    """Fetch missing funding-rate rows and parse them to DB-ready DataFrames."""

    REST_LIMIT = 1000

    def parse_payload(self, payload: list[dict], symbol: str) -> pd.DataFrame:
        columns = ["exchange", "symbol", "timestamp", "funding_rate"]
        if not payload:
            return pd.DataFrame(columns=columns)

        df = pd.DataFrame(payload)
        df["exchange"] = "binance"
        df["symbol"] = symbol
        df["timestamp"] = pd.to_datetime(df["fundingTime"].astype("int64"), unit="ms")
        df["funding_rate"] = df["fundingRate"].astype(float)
        df = df[columns].drop_duplicates(subset=["exchange", "symbol", "timestamp"])
        return df.reset_index(drop=True)

    async def fetch_repair_rows(self, client, gap: GapBoundary) -> pd.DataFrame:
        interval_ms = gap.expected_seconds * 1000
        start_ms = _to_utc_ms(gap.prev_close) + interval_ms
        end_ms = _to_utc_ms(gap.next_close) - 1

        if start_ms > end_ms:
            return pd.DataFrame(columns=["exchange", "symbol", "timestamp", "funding_rate"])
        if (end_ms - start_ms) >= self.REST_LIMIT * interval_ms:
            raise ValueError(
                f"funding gap too large for single REST call ({self.REST_LIMIT} max); "
                "re-download via Data Vision"
            )

        url = BinanceRestClient.build_funding_rate_url(
            gap.symbol, start_ms, end_ms, self.REST_LIMIT
        )
        payload = await client.get_json(url)
        df = self.parse_payload(payload, symbol=gap.symbol)
        df = _trim_exclusive(df, gap)
        if not df.empty:
            df = FUNDING_RATES_SCHEMA.validate(df)
        return df


def repair_binance_gaps(
    db_path: str | Path,
    *,
    tables: Sequence[str] | None = None,
    symbols: Sequence[str] | None = None,
    intervals: Sequence[str] | None = None,
    max_concurrent: int = 5,
    rest_client=None,
) -> RepairReport:
    """Fill internal Binance gaps in DuckDB using Binance public REST data."""
    selected_tables = list(tables) if tables is not None else list(DEFAULT_REPAIR_TABLES)
    _validate_repair_tables(selected_tables)

    started = time.monotonic()
    inserted_rows = dict.fromkeys(selected_tables, 0)
    unrecoverable_gaps: list[UnrecoverableGap] = []
    errors: list[str] = []

    conn = duckdb.connect(str(db_path))
    try:
        gaps = _enumerate_repair_gaps(
            conn,
            tables=selected_tables,
            symbols=symbols,
            intervals=intervals,
        )
        if gaps:
            fetched = run_async_from_sync(
                _fetch_all_gaps(
                    gaps,
                    max_concurrent=max_concurrent,
                    rest_client=rest_client,
                ),
                "repair_binance_gaps",
            )
        else:
            fetched = []

        gaps_fully_repaired = 0
        for result in fetched:
            if result.error is not None:
                unrecoverable_gaps.append(
                    UnrecoverableGap(
                        table=result.gap.table,
                        symbol=result.gap.symbol,
                        interval=result.gap.interval,
                        prev_close=result.gap.prev_close,
                        next_close=result.gap.next_close,
                        missing_count=_expected_missing_count(result.gap),
                        reason="network_error",
                    )
                )
                errors.append(result.error)
                continue

            inserted_rows[result.gap.table] += _insert_idempotent(conn, result.gap.table, result.df)
            if result.missing_count == 0:
                gaps_fully_repaired += 1
            else:
                unrecoverable_gaps.append(
                    UnrecoverableGap(
                        table=result.gap.table,
                        symbol=result.gap.symbol,
                        interval=result.gap.interval,
                        prev_close=result.gap.prev_close,
                        next_close=result.gap.next_close,
                        missing_count=result.missing_count,
                        reason="partial_fill",
                    )
                )
    finally:
        conn.close()

    return RepairReport(
        inserted_rows=inserted_rows,
        gaps_processed=len(gaps) if "gaps" in locals() else 0,
        gaps_fully_repaired=gaps_fully_repaired if "gaps_fully_repaired" in locals() else 0,
        unrecoverable_gaps=unrecoverable_gaps,
        errors=errors,
        duration_seconds=time.monotonic() - started,
    )


async def _fetch_all_gaps(
    gaps: Sequence[GapBoundary],
    *,
    max_concurrent: int,
    rest_client,
) -> list[_FetchedGap]:
    prefix_cache: dict[str, str] = {}
    semaphore = asyncio.Semaphore(max_concurrent)
    owns_client = rest_client is None
    client = rest_client or BinanceRestClient(max_concurrent=max_concurrent)
    context = client if owns_client else _NullAsyncContext(client)

    async with context as active_client:

        async def fetch_one(gap: GapBoundary) -> _FetchedGap:
            async with semaphore:
                try:
                    strategy = _strategy_for_gap(gap)
                    df = await _fetch_with_prefix_probe(strategy, active_client, gap, prefix_cache)
                    missing_count = _missing_count_after_fetch(gap, df)
                    return _FetchedGap(gap=gap, df=df, missing_count=missing_count)
                except Exception as exc:
                    logger.error("Gap fetch failed for %s: %r", gap, exc)
                    return _FetchedGap(
                        gap=gap,
                        df=_empty_df_for_table(gap.table),
                        missing_count=_expected_missing_count(gap),
                        error=f"{gap.table}/{gap.symbol}/{gap.interval or '-'}: {exc!r}",
                    )

        return await asyncio.gather(*(fetch_one(gap) for gap in gaps))


async def _fetch_with_prefix_probe(
    strategy: KlinesRepairStrategy | FundingRatesRepairStrategy,
    client,
    gap: GapBoundary,
    prefix_cache: dict[str, str],
) -> pd.DataFrame:
    cached_symbol = prefix_cache.get(gap.symbol)
    effective_gap = _replace_symbol(gap, cached_symbol) if cached_symbol else gap

    try:
        df = await strategy.fetch_repair_rows(client, effective_gap)
    except aiohttp.ClientResponseError as exc:
        if (
            exc.status != 400
            or cached_symbol is not None
            or gap.table == "spot"
            or gap.symbol.startswith("1000")
        ):
            raise

        prefixed_symbol = f"1000{gap.symbol}"
        logger.info("Probing Binance futures symbol %s as %s", gap.symbol, prefixed_symbol)
        df = await strategy.fetch_repair_rows(client, _replace_symbol(gap, prefixed_symbol))
        prefix_cache[gap.symbol] = prefixed_symbol

    if (not df.empty and cached_symbol) or (not df.empty and df["symbol"].iloc[0] != gap.symbol):
        df = df.copy()
        df["symbol"] = gap.symbol
    return df


def _enumerate_repair_gaps(
    conn: duckdb.DuckDBPyConnection,
    *,
    tables: Sequence[str],
    symbols: Sequence[str] | None,
    intervals: Sequence[str] | None,
) -> list[GapBoundary]:
    gaps: list[GapBoundary] = []
    for table in tables:
        if table in ("spot", "futures"):
            gaps.extend(
                enumerate_kline_gaps(conn, table=table, symbols=symbols, intervals=intervals)
            )
        elif table == "funding_rates":
            gaps.extend(
                enumerate_metric_gaps(
                    conn,
                    table=table,
                    expected_seconds=FUNDING_EXPECTED_SECONDS,
                    symbols=symbols,
                )
            )
    return gaps


def _strategy_for_gap(gap: GapBoundary) -> KlinesRepairStrategy | FundingRatesRepairStrategy:
    if gap.table in ("spot", "futures"):
        if gap.interval is None:
            raise ValueError(f"kline gap has no interval: {gap}")
        data_type = DataType.SPOT if gap.table == "spot" else DataType.FUTURES
        return KlinesRepairStrategy(data_type=data_type, interval=Interval(gap.interval))
    if gap.table == "funding_rates":
        return FundingRatesRepairStrategy()
    raise ValueError(f"Unsupported table for repair: {gap.table}")


def _insert_idempotent(conn: duckdb.DuckDBPyConnection, table: str, df: pd.DataFrame) -> int:
    if df.empty:
        return 0

    if table in ("spot", "futures"):
        join_condition = (
            "existing.exchange = incoming.exchange "
            "AND existing.symbol = incoming.symbol "
            "AND existing.interval = incoming.interval "
            "AND existing.timestamp = incoming.timestamp"
        )
    else:
        join_condition = (
            "existing.exchange = incoming.exchange "
            "AND existing.symbol = incoming.symbol "
            "AND existing.timestamp = incoming.timestamp"
        )

    conn.execute("BEGIN TRANSACTION")
    try:
        inserted_count = conn.execute(
            f"""
            SELECT COUNT(*)
            FROM df AS incoming
            LEFT JOIN {table} AS existing
              ON {join_condition}
            WHERE existing.exchange IS NULL
            """
        ).fetchone()[0]
        conn.execute(f"INSERT OR IGNORE INTO {table} SELECT * FROM df")
        conn.execute("COMMIT")
        return int(inserted_count)
    except Exception:
        conn.execute("ROLLBACK")
        raise


def _missing_count_after_fetch(gap: GapBoundary, df: pd.DataFrame) -> int:
    expected = _expected_timestamps(gap)
    if not expected:
        return 0
    observed = (
        {timestamp.to_pydatetime() for timestamp in pd.to_datetime(df["timestamp"])}
        if not df.empty
        else set()
    )
    return len(expected - observed)


def _expected_missing_count(gap: GapBoundary) -> int:
    return len(_expected_timestamps(gap))


def _expected_timestamps(gap: GapBoundary) -> set[datetime]:
    start = pd.Timestamp(gap.prev_close) + pd.Timedelta(seconds=gap.expected_seconds)
    end = pd.Timestamp(gap.next_close) - pd.Timedelta(seconds=gap.expected_seconds)
    if start > end:
        return set()
    return set(pd.date_range(start, end, freq=f"{gap.expected_seconds}s").to_pydatetime())


def _trim_exclusive(df: pd.DataFrame, gap: GapBoundary) -> pd.DataFrame:
    if df.empty:
        return df
    timestamps = pd.to_datetime(df["timestamp"])
    keep = (timestamps > pd.Timestamp(gap.prev_close)) & (timestamps < pd.Timestamp(gap.next_close))
    return df.loc[keep].reset_index(drop=True)


def _to_utc_ms(value: datetime) -> int:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    else:
        value = value.astimezone(timezone.utc)
    return int(value.timestamp() * 1000)


def _replace_symbol(gap: GapBoundary, symbol: str) -> GapBoundary:
    return GapBoundary(
        table=gap.table,
        symbol=symbol,
        interval=gap.interval,
        prev_close=gap.prev_close,
        next_close=gap.next_close,
        expected_seconds=gap.expected_seconds,
    )


def _filters(
    *,
    symbols: Sequence[str] | None = None,
    intervals: Sequence[str] | None = None,
    alias: str = "t",
) -> tuple[str, list[Any]]:
    clauses: list[str] = []
    params: list[Any] = []
    if symbols:
        placeholders = ", ".join(["?"] * len(symbols))
        clauses.append(f"{alias}.symbol IN ({placeholders})")
        params.extend(symbols)
    if intervals:
        placeholders = ", ".join(["?"] * len(intervals))
        clauses.append(f"{alias}.interval IN ({placeholders})")
        params.extend(intervals)
    filter_sql = "WHERE " + " AND ".join(clauses) if clauses else ""
    return filter_sql, params


def _validate_repair_tables(tables: Sequence[str]) -> None:
    unsupported = sorted(set(tables) - set(DEFAULT_REPAIR_TABLES))
    if unsupported:
        raise ValueError(f"Unsupported table(s) for Binance REST repair: {unsupported}")


def _empty_df_for_table(table: str) -> pd.DataFrame:
    if table in ("spot", "futures"):
        return pd.DataFrame(columns=KLINE_FINAL_COLUMNS)
    return pd.DataFrame(columns=["exchange", "symbol", "timestamp", "funding_rate"])


class _NullAsyncContext:
    def __init__(self, value) -> None:
        self._value = value

    async def __aenter__(self):
        return self._value

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None
