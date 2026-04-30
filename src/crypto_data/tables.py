"""Shared table metadata for DuckDB-backed crypto data tables."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import pandera.pandas as pa

from crypto_data.intervals import KLINE_INTERVAL_SECONDS
from crypto_data.schemas import (
    FUNDING_RATES_SCHEMA,
    OHLCV_SCHEMA,
    OPEN_INTEREST_SCHEMA,
    UNIVERSE_SCHEMA,
)

TableKind = Literal["kline", "metric", "universe"]

KLINE_TABLE_COLUMNS = (
    "exchange",
    "symbol",
    "interval",
    "timestamp",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "quote_volume",
    "trades_count",
    "taker_buy_base_volume",
    "taker_buy_quote_volume",
)
OPEN_INTEREST_COLUMNS = ("exchange", "symbol", "timestamp", "open_interest")
FUNDING_RATES_COLUMNS = ("exchange", "symbol", "timestamp", "funding_rate")
UNIVERSE_COLUMNS = (
    "provider",
    "provider_id",
    "date",
    "symbol",
    "name",
    "slug",
    "rank",
    "market_cap",
    "fully_diluted_market_cap",
    "circulating_supply",
    "max_supply",
    "tags",
    "platform",
    "date_added",
)

KLINE_PRIMARY_KEY = ("exchange", "symbol", "interval", "timestamp")
METRIC_PRIMARY_KEY = ("exchange", "symbol", "timestamp")
UNIVERSE_PRIMARY_KEY = ("provider", "provider_id", "date")

KLINE_TABLES = ("spot", "futures")
METRIC_TABLES = ("open_interest", "funding_rates")
UNIVERSE_TABLE = "crypto_universe"
SUPPORTED_TABLES = (*KLINE_TABLES, *METRIC_TABLES, UNIVERSE_TABLE)

OPEN_INTEREST_EXPECTED_SECONDS = 60 * 60
FUNDING_RATES_EXPECTED_SECONDS = 8 * 60 * 60


@dataclass(frozen=True)
class TableSpec:
    """Stable metadata shared by import, repair, quality, and orchestration code."""

    name: str
    kind: TableKind
    primary_key: tuple[str, ...]
    columns: tuple[str, ...]
    schema: pa.DataFrameSchema
    requires_interval: bool = False
    expected_seconds: int | None = None
    repair_supported: bool = False
    default_max_concurrent: int | None = None


TABLE_SPECS: dict[str, TableSpec] = {
    "spot": TableSpec(
        name="spot",
        kind="kline",
        primary_key=KLINE_PRIMARY_KEY,
        columns=KLINE_TABLE_COLUMNS,
        schema=OHLCV_SCHEMA,
        requires_interval=True,
        repair_supported=True,
        default_max_concurrent=20,
    ),
    "futures": TableSpec(
        name="futures",
        kind="kline",
        primary_key=KLINE_PRIMARY_KEY,
        columns=KLINE_TABLE_COLUMNS,
        schema=OHLCV_SCHEMA,
        requires_interval=True,
        repair_supported=True,
        default_max_concurrent=20,
    ),
    "open_interest": TableSpec(
        name="open_interest",
        kind="metric",
        primary_key=METRIC_PRIMARY_KEY,
        columns=OPEN_INTEREST_COLUMNS,
        schema=OPEN_INTEREST_SCHEMA,
        expected_seconds=OPEN_INTEREST_EXPECTED_SECONDS,
        repair_supported=False,
        default_max_concurrent=100,
    ),
    "funding_rates": TableSpec(
        name="funding_rates",
        kind="metric",
        primary_key=METRIC_PRIMARY_KEY,
        columns=FUNDING_RATES_COLUMNS,
        schema=FUNDING_RATES_SCHEMA,
        expected_seconds=FUNDING_RATES_EXPECTED_SECONDS,
        repair_supported=True,
        default_max_concurrent=50,
    ),
    "crypto_universe": TableSpec(
        name="crypto_universe",
        kind="universe",
        primary_key=UNIVERSE_PRIMARY_KEY,
        columns=UNIVERSE_COLUMNS,
        schema=UNIVERSE_SCHEMA,
    ),
}


def get_table_spec(table: str) -> TableSpec:
    """Return metadata for a supported table."""
    try:
        return TABLE_SPECS[table]
    except KeyError as exc:
        valid = ", ".join(SUPPORTED_TABLES)
        raise ValueError(f"Unsupported table: {table!r}. Expected one of: {valid}") from exc


def is_kline_table(table: str) -> bool:
    """Return True for spot/futures kline tables."""
    return get_table_spec(table).kind == "kline"


def is_metric_table(table: str) -> bool:
    """Return True for single-cadence metric tables."""
    return get_table_spec(table).kind == "metric"


def primary_key_for(table: str) -> tuple[str, ...]:
    """Return the DuckDB primary key columns for a table."""
    return get_table_spec(table).primary_key


def expected_seconds_for_interval(interval: str) -> int | None:
    """Return fixed cadence seconds for a kline interval."""
    return KLINE_INTERVAL_SECONDS.get(interval)
