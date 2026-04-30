"""
Post-import data quality audit utilities.

The checks in this module are read-only DuckDB audits. They complement the
Pandera schemas used during import by checking time-series continuity and
statistical anomalies after data has landed in the database.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import asdict, dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd
from pandera.pandas import Check, Column, DataFrameSchema

from crypto_data.gaps import enumerate_kline_gaps, enumerate_metric_gaps
from crypto_data.tables import (
    FUNDING_RATES_EXPECTED_SECONDS,
    KLINE_TABLES,
    OPEN_INTEREST_EXPECTED_SECONDS,
    SUPPORTED_TABLES,
    UNIVERSE_TABLE,
)
from crypto_data.utils.dates import Frequency, generate_date_list

FINDING_COLUMNS = [
    "severity",
    "table",
    "check_name",
    "message",
    "count",
    "symbol",
    "interval",
    "first_timestamp",
    "last_timestamp",
    "metadata",
]

FINDINGS_SCHEMA = DataFrameSchema(
    {
        "severity": Column(str, Check.isin(["ERROR", "WARN"]), nullable=False),
        "table": Column(str, Check.isin(list(SUPPORTED_TABLES)), nullable=False),
        "check_name": Column(str, nullable=False),
        "message": Column(str, nullable=False),
        "count": Column(int, Check.greater_than_or_equal_to(0), nullable=False),
        "symbol": Column(str, nullable=True),
        "interval": Column(str, nullable=True),
        "first_timestamp": Column(object, nullable=True),
        "last_timestamp": Column(object, nullable=True),
        "metadata": Column(object, nullable=True),
    },
    strict=True,
    coerce=True,
    ordered=True,
)


@dataclass(frozen=True)
class QualityConfig:
    """Thresholds for statistical quality checks."""

    price_return_sigma: float = 10.0
    volume_iqr_multiplier: float = 10.0
    open_interest_change_iqr_multiplier: float = 8.0
    funding_rate_abs_warn: float = 0.01
    funding_rate_mean_abs_warn: float = 0.001
    funding_rate_std_warn: float = 0.01
    min_outlier_observations: int = 20
    universe_frequency: Frequency | None = None
    universe_start_date: str | date | datetime | None = None
    universe_end_date: str | date | datetime | None = None
    universe_top_n: int | None = None


@dataclass(frozen=True)
class QualityFinding:
    """Single data quality finding."""

    severity: str
    table: str
    check_name: str
    message: str
    count: int
    symbol: str | None = None
    interval: str | None = None
    first_timestamp: Any = None
    last_timestamp: Any = None
    metadata: dict[str, Any] = field(default_factory=dict)


def audit_database(
    db_path: str | Path,
    *,
    tables: Sequence[str] | None = None,
    symbols: Sequence[str] | None = None,
    intervals: Sequence[str] | None = None,
    config: QualityConfig | None = None,
) -> list[QualityFinding]:
    """Run the quality audit against a DuckDB database file."""
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        return audit_connection(
            conn, tables=tables, symbols=symbols, intervals=intervals, config=config
        )
    finally:
        conn.close()


def audit_connection(
    conn: duckdb.DuckDBPyConnection,
    *,
    tables: Sequence[str] | None = None,
    symbols: Sequence[str] | None = None,
    intervals: Sequence[str] | None = None,
    config: QualityConfig | None = None,
) -> list[QualityFinding]:
    """Run the quality audit against an existing DuckDB connection."""
    config = config or QualityConfig()
    selected_tables = _normalize_tables(tables)
    symbols = _normalize_values(symbols)
    intervals = _normalize_values(intervals)

    findings: list[QualityFinding] = []
    for table in selected_tables:
        if not _table_exists(conn, table):
            findings.append(
                QualityFinding(
                    severity="ERROR",
                    table=table,
                    check_name="missing_table",
                    message=f"Table '{table}' does not exist",
                    count=1,
                )
            )
            continue

        if table in KLINE_TABLES:
            findings.extend(_audit_kline_table(conn, table, symbols, intervals, config))
        elif table == "open_interest":
            findings.extend(_audit_open_interest(conn, symbols, config))
        elif table == "funding_rates":
            findings.extend(_audit_funding_rates(conn, symbols, config))
        elif table == UNIVERSE_TABLE:
            findings.extend(_audit_universe(conn, symbols, config))

    findings_to_dataframe(findings)
    return findings


def findings_to_dataframe(findings: Sequence[QualityFinding]) -> pd.DataFrame:
    """Convert findings to a validated DataFrame report."""
    records = [asdict(finding) for finding in findings]
    df = pd.DataFrame(records, columns=FINDING_COLUMNS)
    if df.empty:
        df = pd.DataFrame({column: pd.Series(dtype=object) for column in FINDING_COLUMNS})
    return FINDINGS_SCHEMA.validate(df, lazy=True)


def findings_to_jsonable(findings: Sequence[QualityFinding]) -> list[dict[str, Any]]:
    """Return JSON-serializable finding dictionaries."""
    return [_jsonable(asdict(finding)) for finding in findings]


def format_findings(
    findings: Sequence[QualityFinding], *, db_path: str | Path | None = None
) -> str:
    """Format findings as a concise console report."""
    error_count = sum(1 for finding in findings if finding.severity == "ERROR")
    warning_count = sum(1 for finding in findings if finding.severity == "WARN")
    lines = []
    title = "Data quality report"
    if db_path is not None:
        title += f": {db_path}"
    lines.extend([title, "", f"ERRORS: {error_count}", f"WARNINGS: {warning_count}"])

    if not findings:
        lines.extend(["", "No findings."])
        return "\n".join(lines)

    lines.append("")
    for finding in sorted(findings, key=_finding_sort_key):
        scope_parts = [finding.table]
        if finding.symbol:
            scope_parts.append(finding.symbol)
        if finding.interval:
            scope_parts.append(finding.interval)
        scope = " ".join(scope_parts)
        lines.append(
            f"[{finding.severity}] {scope} {finding.check_name}: "
            f"{finding.message} (count={finding.count})"
        )

    return "\n".join(lines)


def has_errors(findings: Sequence[QualityFinding]) -> bool:
    """Return True when the report contains at least one ERROR finding."""
    return any(finding.severity == "ERROR" for finding in findings)


def has_warnings(findings: Sequence[QualityFinding]) -> bool:
    """Return True when the report contains at least one WARN finding."""
    return any(finding.severity == "WARN" for finding in findings)


def _audit_kline_table(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    symbols: Sequence[str] | None,
    intervals: Sequence[str] | None,
    config: QualityConfig,
) -> list[QualityFinding]:
    findings: list[QualityFinding] = []
    findings.extend(_check_kline_duplicates(conn, table, symbols, intervals))
    findings.extend(_check_kline_gaps(conn, table, symbols, intervals))
    findings.extend(_check_kline_invalid_values(conn, table, symbols, intervals))
    findings.extend(_check_kline_ohlc(conn, table, symbols, intervals))
    findings.extend(_check_kline_price_outliers(conn, table, symbols, intervals, config))
    findings.extend(_check_kline_volume_outliers(conn, table, symbols, intervals, config))
    return findings


def _audit_open_interest(
    conn: duckdb.DuckDBPyConnection,
    symbols: Sequence[str] | None,
    config: QualityConfig,
) -> list[QualityFinding]:
    findings: list[QualityFinding] = []
    findings.extend(_check_metric_duplicates(conn, "open_interest", symbols))
    findings.extend(
        _check_metric_gaps(
            conn,
            "open_interest",
            symbols,
            expected_seconds=OPEN_INTEREST_EXPECTED_SECONDS,
        )
    )
    findings.extend(
        _check_metric_invalid_values(
            conn,
            "open_interest",
            "open_interest",
            symbols,
            positive=True,
        )
    )
    findings.extend(_check_open_interest_outliers(conn, symbols, config))
    return findings


def _audit_funding_rates(
    conn: duckdb.DuckDBPyConnection,
    symbols: Sequence[str] | None,
    config: QualityConfig,
) -> list[QualityFinding]:
    findings: list[QualityFinding] = []
    findings.extend(_check_metric_duplicates(conn, "funding_rates", symbols))
    findings.extend(
        _check_metric_gaps(
            conn,
            "funding_rates",
            symbols,
            expected_seconds=FUNDING_RATES_EXPECTED_SECONDS,
        )
    )
    findings.extend(
        _check_metric_invalid_values(
            conn,
            "funding_rates",
            "funding_rate",
            symbols,
            positive=False,
        )
    )
    findings.extend(_check_funding_rate_extremes(conn, symbols, config))
    findings.extend(_check_funding_rate_distribution(conn, symbols, config))
    return findings


def _audit_universe(
    conn: duckdb.DuckDBPyConnection,
    symbols: Sequence[str] | None,
    config: QualityConfig,
) -> list[QualityFinding]:
    findings: list[QualityFinding] = []
    findings.extend(_check_universe_duplicates(conn, symbols))
    findings.extend(_check_universe_duplicate_ranks(conn, symbols))
    findings.extend(_check_universe_invalid_values(conn, symbols))
    if not symbols:
        findings.extend(_check_universe_date_coverage(conn, config))
        findings.extend(_check_universe_top_n_coverage(conn, config))
        findings.extend(_check_universe_rank_ordering(conn))
    return findings


def _check_kline_duplicates(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    symbols: Sequence[str] | None,
    intervals: Sequence[str] | None,
) -> list[QualityFinding]:
    filter_sql, params = _filters(symbols=symbols, intervals=intervals, alias="t")
    rows = conn.execute(
        f"""
        WITH duplicates AS (
            SELECT
                t.symbol,
                t.interval,
                t.timestamp,
                COUNT(*) AS duplicate_count
            FROM {table} AS t
            {filter_sql}
            GROUP BY t.exchange, t.symbol, t.interval, t.timestamp
            HAVING COUNT(*) > 1
        )
        SELECT
            symbol,
            interval,
            CAST(SUM(duplicate_count - 1) AS BIGINT) AS bad_count,
            MIN(timestamp) AS first_timestamp,
            MAX(timestamp) AS last_timestamp,
            MAX(duplicate_count) AS max_occurrences
        FROM duplicates
        GROUP BY symbol, interval
        """,
        params,
    ).fetchall()

    return [
        QualityFinding(
            severity="ERROR",
            table=table,
            check_name="duplicate_primary_key",
            message=f"{bad_count} duplicate rows on primary key",
            count=int(bad_count),
            symbol=symbol,
            interval=interval,
            first_timestamp=first_timestamp,
            last_timestamp=last_timestamp,
            metadata={"max_occurrences": int(max_occurrences)},
        )
        for symbol, interval, bad_count, first_timestamp, last_timestamp, max_occurrences in rows
    ]


def _check_kline_gaps(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    symbols: Sequence[str] | None,
    intervals: Sequence[str] | None,
) -> list[QualityFinding]:
    gaps = enumerate_kline_gaps(conn, table=table, symbols=symbols, intervals=intervals)
    grouped: dict[tuple[str, str], list] = {}
    for gap in gaps:
        grouped.setdefault((gap.symbol, gap.interval), []).append(gap)

    findings: list[QualityFinding] = []
    for (symbol, interval), group in grouped.items():
        deltas = [int((gap.next_close - gap.prev_close).total_seconds()) for gap in group]
        max_delta_seconds = max(deltas)
        expected_seconds = group[0].expected_seconds
        findings.append(
            QualityFinding(
                severity="ERROR",
                table=table,
                check_name="time_gaps",
                message=(
                    f"{len(group)} gaps detected; max gap "
                    f"{_format_seconds(max_delta_seconds)} vs expected "
                    f"{_format_seconds(expected_seconds)}"
                ),
                count=len(group),
                symbol=symbol,
                interval=interval,
                first_timestamp=min(gap.next_close for gap in group),
                last_timestamp=max(gap.next_close for gap in group),
                metadata={
                    "max_delta_seconds": max_delta_seconds,
                    "expected_seconds": expected_seconds,
                },
            )
        )
    return findings


def _check_kline_invalid_values(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    symbols: Sequence[str] | None,
    intervals: Sequence[str] | None,
) -> list[QualityFinding]:
    filter_sql, params = _filters(symbols=symbols, intervals=intervals, alias="t")
    condition = """
        t.timestamp IS NULL
        OR t.open IS NULL OR NOT isfinite(t.open) OR t.open <= 0
        OR t.high IS NULL OR NOT isfinite(t.high) OR t.high <= 0
        OR t.low IS NULL OR NOT isfinite(t.low) OR t.low <= 0
        OR t.close IS NULL OR NOT isfinite(t.close) OR t.close <= 0
        OR t.volume IS NULL OR NOT isfinite(t.volume) OR t.volume < 0
        OR t.quote_volume IS NULL OR NOT isfinite(t.quote_volume) OR t.quote_volume < 0
        OR (t.trades_count IS NOT NULL AND t.trades_count < 0)
        OR (t.taker_buy_base_volume IS NOT NULL AND (
            NOT isfinite(t.taker_buy_base_volume) OR t.taker_buy_base_volume < 0
        ))
        OR (t.taker_buy_quote_volume IS NOT NULL AND (
            NOT isfinite(t.taker_buy_quote_volume) OR t.taker_buy_quote_volume < 0
        ))
    """
    where_sql = _append_condition(filter_sql, condition)
    rows = conn.execute(
        f"""
        SELECT
            t.symbol,
            t.interval,
            COUNT(*) AS bad_count,
            MIN(t.timestamp) AS first_timestamp,
            MAX(t.timestamp) AS last_timestamp
        FROM {table} AS t
        {where_sql}
        GROUP BY t.symbol, t.interval
        """,
        params,
    ).fetchall()

    return [
        QualityFinding(
            severity="ERROR",
            table=table,
            check_name="invalid_values",
            message=f"{bad_count} rows contain null, infinite, or out-of-range values",
            count=int(bad_count),
            symbol=symbol,
            interval=interval,
            first_timestamp=first_timestamp,
            last_timestamp=last_timestamp,
        )
        for symbol, interval, bad_count, first_timestamp, last_timestamp in rows
    ]


def _check_kline_ohlc(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    symbols: Sequence[str] | None,
    intervals: Sequence[str] | None,
) -> list[QualityFinding]:
    filter_sql, params = _filters(symbols=symbols, intervals=intervals, alias="t")
    condition = """
        t.high < t.low
        OR t.high < t.open
        OR t.high < t.close
        OR t.low > t.open
        OR t.low > t.close
    """
    where_sql = _append_condition(filter_sql, condition)
    rows = conn.execute(
        f"""
        SELECT
            t.symbol,
            t.interval,
            COUNT(*) AS bad_count,
            MIN(t.timestamp) AS first_timestamp,
            MAX(t.timestamp) AS last_timestamp
        FROM {table} AS t
        {where_sql}
        GROUP BY t.symbol, t.interval
        """,
        params,
    ).fetchall()

    return [
        QualityFinding(
            severity="ERROR",
            table=table,
            check_name="invalid_ohlc",
            message=f"{bad_count} rows violate OHLC relationships",
            count=int(bad_count),
            symbol=symbol,
            interval=interval,
            first_timestamp=first_timestamp,
            last_timestamp=last_timestamp,
        )
        for symbol, interval, bad_count, first_timestamp, last_timestamp in rows
    ]


def _check_kline_price_outliers(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    symbols: Sequence[str] | None,
    intervals: Sequence[str] | None,
    config: QualityConfig,
) -> list[QualityFinding]:
    filter_sql, params = _filters(symbols=symbols, intervals=intervals, alias="t")
    where_sql = _append_condition(
        filter_sql,
        "t.close IS NOT NULL AND isfinite(t.close) AND t.close > 0",
    )
    rows = conn.execute(
        f"""
        WITH ordered AS (
            SELECT
                t.symbol,
                t.interval,
                t.timestamp,
                t.close,
                LAG(t.close) OVER (
                    PARTITION BY t.exchange, t.symbol, t.interval
                    ORDER BY t.timestamp
                ) AS prev_close
            FROM {table} AS t
            {where_sql}
        ),
        returns AS (
            SELECT
                symbol,
                interval,
                timestamp,
                ln(close / prev_close) AS log_return
            FROM ordered
            WHERE prev_close IS NOT NULL AND prev_close > 0
        ),
        stats AS (
            SELECT
                symbol,
                interval,
                COUNT(*) AS n,
                AVG(log_return) AS mean_return,
                STDDEV_SAMP(log_return) AS std_return
            FROM returns
            GROUP BY symbol, interval
        ),
        outliers AS (
            SELECT r.*
            FROM returns AS r
            JOIN stats AS s USING (symbol, interval)
            WHERE s.n >= ?
              AND s.std_return IS NOT NULL
              AND s.std_return > 0
              AND ABS(r.log_return - s.mean_return) > ? * s.std_return
        )
        SELECT
            symbol,
            interval,
            COUNT(*) AS outlier_count,
            MIN(timestamp) AS first_timestamp,
            MAX(timestamp) AS last_timestamp,
            MAX(ABS(log_return)) AS max_abs_log_return
        FROM outliers
        GROUP BY symbol, interval
        """,
        [*params, config.min_outlier_observations, config.price_return_sigma],
    ).fetchall()

    return [
        QualityFinding(
            severity="WARN",
            table=table,
            check_name="price_return_outliers",
            message=f"{outlier_count} extreme log-return outliers detected",
            count=int(outlier_count),
            symbol=symbol,
            interval=interval,
            first_timestamp=first_timestamp,
            last_timestamp=last_timestamp,
            metadata={"max_abs_log_return": float(max_abs_log_return)},
        )
        for symbol, interval, outlier_count, first_timestamp, last_timestamp, max_abs_log_return in rows
    ]


def _check_kline_volume_outliers(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    symbols: Sequence[str] | None,
    intervals: Sequence[str] | None,
    config: QualityConfig,
) -> list[QualityFinding]:
    filter_sql, params = _filters(symbols=symbols, intervals=intervals, alias="t")
    where_sql = _append_condition(
        filter_sql,
        "t.volume IS NOT NULL AND isfinite(t.volume) AND t.volume > 0",
    )
    rows = conn.execute(
        f"""
        WITH base AS (
            SELECT t.symbol, t.interval, t.timestamp, t.volume
            FROM {table} AS t
            {where_sql}
        ),
        stats AS (
            SELECT
                symbol,
                interval,
                COUNT(*) AS n,
                quantile_cont(volume, 0.25) AS q1,
                quantile_cont(volume, 0.75) AS q3
            FROM base
            GROUP BY symbol, interval
        ),
        bounds AS (
            SELECT
                symbol,
                interval,
                n,
                q1,
                q3,
                q3 - q1 AS iqr
            FROM stats
        ),
        outliers AS (
            SELECT b.*, bounds.q1, bounds.q3, bounds.iqr
            FROM base AS b
            JOIN bounds USING (symbol, interval)
            WHERE bounds.n >= ?
              AND bounds.iqr > 0
              AND (
                  b.volume < bounds.q1 - ? * bounds.iqr
                  OR b.volume > bounds.q3 + ? * bounds.iqr
              )
        )
        SELECT
            symbol,
            interval,
            COUNT(*) AS outlier_count,
            MIN(timestamp) AS first_timestamp,
            MAX(timestamp) AS last_timestamp,
            MAX(volume) AS max_volume
        FROM outliers
        GROUP BY symbol, interval
        """,
        [
            *params,
            config.min_outlier_observations,
            config.volume_iqr_multiplier,
            config.volume_iqr_multiplier,
        ],
    ).fetchall()

    return [
        QualityFinding(
            severity="WARN",
            table=table,
            check_name="volume_outliers",
            message=f"{outlier_count} volume outliers detected by IQR",
            count=int(outlier_count),
            symbol=symbol,
            interval=interval,
            first_timestamp=first_timestamp,
            last_timestamp=last_timestamp,
            metadata={"max_volume": float(max_volume)},
        )
        for symbol, interval, outlier_count, first_timestamp, last_timestamp, max_volume in rows
    ]


def _check_metric_duplicates(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    symbols: Sequence[str] | None,
) -> list[QualityFinding]:
    filter_sql, params = _filters(symbols=symbols, alias="t")
    rows = conn.execute(
        f"""
        WITH duplicates AS (
            SELECT
                t.symbol,
                t.timestamp,
                COUNT(*) AS duplicate_count
            FROM {table} AS t
            {filter_sql}
            GROUP BY t.exchange, t.symbol, t.timestamp
            HAVING COUNT(*) > 1
        )
        SELECT
            symbol,
            CAST(SUM(duplicate_count - 1) AS BIGINT) AS bad_count,
            MIN(timestamp) AS first_timestamp,
            MAX(timestamp) AS last_timestamp,
            MAX(duplicate_count) AS max_occurrences
        FROM duplicates
        GROUP BY symbol
        """,
        params,
    ).fetchall()

    return [
        QualityFinding(
            severity="ERROR",
            table=table,
            check_name="duplicate_primary_key",
            message=f"{bad_count} duplicate rows on primary key",
            count=int(bad_count),
            symbol=symbol,
            first_timestamp=first_timestamp,
            last_timestamp=last_timestamp,
            metadata={"max_occurrences": int(max_occurrences)},
        )
        for symbol, bad_count, first_timestamp, last_timestamp, max_occurrences in rows
    ]


def _check_metric_gaps(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    symbols: Sequence[str] | None,
    *,
    expected_seconds: int,
) -> list[QualityFinding]:
    gaps = enumerate_metric_gaps(
        conn,
        table=table,
        expected_seconds=expected_seconds,
        symbols=symbols,
    )
    grouped: dict[str, list] = {}
    for gap in gaps:
        grouped.setdefault(gap.symbol, []).append(gap)

    findings: list[QualityFinding] = []
    for symbol, group in grouped.items():
        deltas = [int((gap.next_close - gap.prev_close).total_seconds()) for gap in group]
        max_delta_seconds = max(deltas)
        findings.append(
            QualityFinding(
                severity="ERROR",
                table=table,
                check_name="time_gaps",
                message=(
                    f"{len(group)} gaps detected; max gap "
                    f"{_format_seconds(max_delta_seconds)} vs expected "
                    f"{_format_seconds(expected_seconds)}"
                ),
                count=len(group),
                symbol=symbol,
                first_timestamp=min(gap.next_close for gap in group),
                last_timestamp=max(gap.next_close for gap in group),
                metadata={
                    "max_delta_seconds": max_delta_seconds,
                    "expected_seconds": expected_seconds,
                },
            )
        )
    return findings


def _check_metric_invalid_values(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    value_column: str,
    symbols: Sequence[str] | None,
    *,
    positive: bool,
) -> list[QualityFinding]:
    filter_sql, params = _filters(symbols=symbols, alias="t")
    range_check = f"OR t.{value_column} <= 0" if positive else ""
    condition = f"""
        t.timestamp IS NULL
        OR t.{value_column} IS NULL
        OR NOT isfinite(t.{value_column})
        {range_check}
    """
    where_sql = _append_condition(filter_sql, condition)
    rows = conn.execute(
        f"""
        SELECT
            t.symbol,
            COUNT(*) AS bad_count,
            MIN(t.timestamp) AS first_timestamp,
            MAX(t.timestamp) AS last_timestamp
        FROM {table} AS t
        {where_sql}
        GROUP BY t.symbol
        """,
        params,
    ).fetchall()

    return [
        QualityFinding(
            severity="ERROR",
            table=table,
            check_name="invalid_values",
            message=f"{bad_count} rows contain null, infinite, or out-of-range values",
            count=int(bad_count),
            symbol=symbol,
            first_timestamp=first_timestamp,
            last_timestamp=last_timestamp,
        )
        for symbol, bad_count, first_timestamp, last_timestamp in rows
    ]


def _check_open_interest_outliers(
    conn: duckdb.DuckDBPyConnection,
    symbols: Sequence[str] | None,
    config: QualityConfig,
) -> list[QualityFinding]:
    filter_sql, params = _filters(symbols=symbols, alias="t")
    where_sql = _append_condition(
        filter_sql,
        "t.open_interest IS NOT NULL AND isfinite(t.open_interest) AND t.open_interest > 0",
    )
    rows = conn.execute(
        f"""
        WITH ordered AS (
            SELECT
                t.symbol,
                t.timestamp,
                t.open_interest,
                LAG(t.open_interest) OVER (
                    PARTITION BY t.exchange, t.symbol
                    ORDER BY t.timestamp
                ) AS prev_open_interest
            FROM open_interest AS t
            {where_sql}
        ),
        changes AS (
            SELECT
                symbol,
                timestamp,
                ABS(open_interest / prev_open_interest - 1) AS relative_change
            FROM ordered
            WHERE prev_open_interest IS NOT NULL AND prev_open_interest > 0
        ),
        stats AS (
            SELECT
                symbol,
                COUNT(*) AS n,
                quantile_cont(relative_change, 0.25) AS q1,
                quantile_cont(relative_change, 0.75) AS q3
            FROM changes
            GROUP BY symbol
        ),
        bounds AS (
            SELECT symbol, n, q1, q3, q3 - q1 AS iqr
            FROM stats
        ),
        outliers AS (
            SELECT changes.*
            FROM changes
            JOIN bounds USING (symbol)
            WHERE bounds.n >= ?
              AND bounds.iqr > 0
              AND changes.relative_change > bounds.q3 + ? * bounds.iqr
        )
        SELECT
            symbol,
            COUNT(*) AS outlier_count,
            MIN(timestamp) AS first_timestamp,
            MAX(timestamp) AS last_timestamp,
            MAX(relative_change) AS max_relative_change
        FROM outliers
        GROUP BY symbol
        """,
        [*params, config.min_outlier_observations, config.open_interest_change_iqr_multiplier],
    ).fetchall()

    return [
        QualityFinding(
            severity="WARN",
            table="open_interest",
            check_name="open_interest_change_outliers",
            message=f"{outlier_count} relative-change outliers detected by IQR",
            count=int(outlier_count),
            symbol=symbol,
            first_timestamp=first_timestamp,
            last_timestamp=last_timestamp,
            metadata={"max_relative_change": float(max_relative_change)},
        )
        for symbol, outlier_count, first_timestamp, last_timestamp, max_relative_change in rows
    ]


def _check_funding_rate_extremes(
    conn: duckdb.DuckDBPyConnection,
    symbols: Sequence[str] | None,
    config: QualityConfig,
) -> list[QualityFinding]:
    filter_sql, params = _filters(symbols=symbols, alias="t")
    where_sql = _append_condition(filter_sql, "ABS(t.funding_rate) > ?")
    rows = conn.execute(
        f"""
        SELECT
            t.symbol,
            COUNT(*) AS outlier_count,
            MIN(t.timestamp) AS first_timestamp,
            MAX(t.timestamp) AS last_timestamp,
            MAX(ABS(t.funding_rate)) AS max_abs_funding_rate
        FROM funding_rates AS t
        {where_sql}
        GROUP BY t.symbol
        """,
        [*params, config.funding_rate_abs_warn],
    ).fetchall()

    return [
        QualityFinding(
            severity="WARN",
            table="funding_rates",
            check_name="funding_rate_extremes",
            message=f"{outlier_count} funding rates exceed absolute threshold",
            count=int(outlier_count),
            symbol=symbol,
            first_timestamp=first_timestamp,
            last_timestamp=last_timestamp,
            metadata={
                "threshold": config.funding_rate_abs_warn,
                "max_abs_funding_rate": float(max_abs_funding_rate),
            },
        )
        for symbol, outlier_count, first_timestamp, last_timestamp, max_abs_funding_rate in rows
    ]


def _check_funding_rate_distribution(
    conn: duckdb.DuckDBPyConnection,
    symbols: Sequence[str] | None,
    config: QualityConfig,
) -> list[QualityFinding]:
    filter_sql, params = _filters(symbols=symbols, alias="t")
    where_sql = _append_condition(
        filter_sql,
        "t.funding_rate IS NOT NULL AND isfinite(t.funding_rate)",
    )
    rows = conn.execute(
        f"""
        SELECT
            t.symbol,
            COUNT(*) AS n,
            MIN(t.timestamp) AS first_timestamp,
            MAX(t.timestamp) AS last_timestamp,
            AVG(t.funding_rate) AS mean_funding_rate,
            STDDEV_SAMP(t.funding_rate) AS std_funding_rate
        FROM funding_rates AS t
        {where_sql}
        GROUP BY t.symbol
        HAVING COUNT(*) >= ?
           AND (
               ABS(AVG(t.funding_rate)) > ?
               OR STDDEV_SAMP(t.funding_rate) > ?
           )
        """,
        [
            *params,
            config.min_outlier_observations,
            config.funding_rate_mean_abs_warn,
            config.funding_rate_std_warn,
        ],
    ).fetchall()

    return [
        QualityFinding(
            severity="WARN",
            table="funding_rates",
            check_name="funding_rate_distribution",
            message="funding rate distribution mean or standard deviation exceeds threshold",
            count=int(n),
            symbol=symbol,
            first_timestamp=first_timestamp,
            last_timestamp=last_timestamp,
            metadata={
                "mean_funding_rate": float(mean_funding_rate),
                "std_funding_rate": float(std_funding_rate),
            },
        )
        for symbol, n, first_timestamp, last_timestamp, mean_funding_rate, std_funding_rate in rows
    ]


def _check_universe_date_coverage(
    conn: duckdb.DuckDBPyConnection,
    config: QualityConfig,
) -> list[QualityFinding]:
    if config.universe_frequency is None:
        return []

    bounds = _universe_expected_bounds(conn, config)
    if bounds is None:
        return []
    start_dt, end_dt = bounds
    if start_dt > end_dt:
        return []

    expected_dates = set(generate_date_list(start_dt, end_dt, frequency=config.universe_frequency))
    if not expected_dates:
        return []

    rows = conn.execute(
        """
        SELECT DISTINCT strftime(date, '%Y-%m-%d')
        FROM crypto_universe
        WHERE date >= ? AND date <= ?
        """,
        [start_dt.date(), end_dt.date()],
    ).fetchall()
    observed_dates = {row[0] for row in rows if row and row[0]}
    missing_dates = sorted(expected_dates - observed_dates)

    if not missing_dates:
        return []

    return [
        QualityFinding(
            severity="ERROR",
            table="crypto_universe",
            check_name="missing_snapshots",
            message=(
                f"{len(missing_dates)} expected {config.universe_frequency} "
                "CoinMarketCap snapshots are missing"
            ),
            count=len(missing_dates),
            first_timestamp=missing_dates[0],
            last_timestamp=missing_dates[-1],
            metadata={
                "frequency": config.universe_frequency,
                "expected_start_date": start_dt.date().isoformat(),
                "expected_end_date": end_dt.date().isoformat(),
                "sample_missing_dates": missing_dates[:20],
            },
        )
    ]


def _check_universe_top_n_coverage(
    conn: duckdb.DuckDBPyConnection,
    config: QualityConfig,
) -> list[QualityFinding]:
    if config.universe_top_n is None:
        return []

    top_n = int(config.universe_top_n)
    findings: list[QualityFinding] = []
    date_filter_sql, date_filter_params = _universe_date_filter(conn, config)

    row = conn.execute(
        f"""
        SELECT
            COUNT(*) AS bad_count,
            MIN(date) AS first_timestamp,
            MAX(date) AS last_timestamp,
            MAX(rank) AS max_rank
        FROM crypto_universe
        {_append_condition(date_filter_sql, "rank > ?")}
        """,
        [*date_filter_params, top_n],
    ).fetchone()
    bad_count, first_timestamp, last_timestamp, max_rank = row
    if bad_count:
        findings.append(
            QualityFinding(
                severity="ERROR",
                table="crypto_universe",
                check_name="rank_above_top_n",
                message=f"{bad_count} rows have rank above expected top_n={top_n}",
                count=int(bad_count),
                first_timestamp=first_timestamp,
                last_timestamp=last_timestamp,
                metadata={"top_n": top_n, "max_rank": int(max_rank)},
            )
        )

    rows = conn.execute(
        f"""
        WITH per_date AS (
            SELECT
                date,
                COUNT(*) AS row_count,
                MAX(rank) AS max_rank
            FROM crypto_universe
            {date_filter_sql}
            GROUP BY date
            HAVING MAX(rank) < ?
        )
        SELECT
            COUNT(*) AS bad_count,
            MIN(date) AS first_timestamp,
            MAX(date) AS last_timestamp,
            MIN(max_rank) AS min_observed_max_rank,
            MAX(max_rank) AS max_observed_max_rank,
            MIN(row_count) AS min_row_count
        FROM per_date
        """,
        [*date_filter_params, top_n],
    ).fetchone()
    (
        low_coverage_count,
        first_timestamp,
        last_timestamp,
        min_observed_max_rank,
        max_observed_max_rank,
        min_row_count,
    ) = rows
    if low_coverage_count:
        findings.append(
            QualityFinding(
                severity="WARN",
                table="crypto_universe",
                check_name="rank_coverage_below_top_n",
                message=(
                    f"{low_coverage_count} snapshots have max retained rank below "
                    f"expected top_n={top_n}"
                ),
                count=int(low_coverage_count),
                first_timestamp=first_timestamp,
                last_timestamp=last_timestamp,
                metadata={
                    "top_n": top_n,
                    "min_observed_max_rank": int(min_observed_max_rank),
                    "max_observed_max_rank": int(max_observed_max_rank),
                    "min_row_count": int(min_row_count),
                    "note": (
                        "This can be expected when excluded assets occupy the highest "
                        "requested ranks; investigate if the gap is large or unexpected."
                    ),
                },
            )
        )

    return findings


def _check_universe_duplicates(
    conn: duckdb.DuckDBPyConnection,
    symbols: Sequence[str] | None,
) -> list[QualityFinding]:
    filter_sql, params = _filters(symbols=symbols, alias="t")
    key_columns = _universe_identity_columns(conn)
    group_sql = ", ".join(f"t.{column}" for column in key_columns)
    symbol_expr = "t.symbol" if "symbol" in key_columns else "MIN(t.symbol)"
    rows = conn.execute(
        f"""
        WITH duplicates AS (
            SELECT
                {symbol_expr} AS symbol,
                t.date,
                COUNT(*) AS duplicate_count
            FROM crypto_universe AS t
            {filter_sql}
            GROUP BY {group_sql}
            HAVING COUNT(*) > 1
        )
        SELECT
            symbol,
            CAST(SUM(duplicate_count - 1) AS BIGINT) AS bad_count,
            MIN(date) AS first_timestamp,
            MAX(date) AS last_timestamp,
            MAX(duplicate_count) AS max_occurrences
        FROM duplicates
        GROUP BY symbol
        """,
        params,
    ).fetchall()

    return [
        QualityFinding(
            severity="ERROR",
            table="crypto_universe",
            check_name="duplicate_primary_key",
            message=f"{bad_count} duplicate rows on primary key",
            count=int(bad_count),
            symbol=symbol,
            first_timestamp=first_timestamp,
            last_timestamp=last_timestamp,
            metadata={"max_occurrences": int(max_occurrences)},
        )
        for symbol, bad_count, first_timestamp, last_timestamp, max_occurrences in rows
    ]


def _check_universe_duplicate_ranks(
    conn: duckdb.DuckDBPyConnection,
    symbols: Sequence[str] | None,
) -> list[QualityFinding]:
    filter_sql, params = _filters(symbols=symbols, alias="t")
    rows = conn.execute(
        f"""
        WITH duplicate_ranks AS (
            SELECT
                t.date,
                t.rank,
                COUNT(*) AS duplicate_count
            FROM crypto_universe AS t
            {filter_sql}
            GROUP BY t.date, t.rank
            HAVING COUNT(*) > 1
        )
        SELECT
            COUNT(*) AS bad_count,
            MIN(date) AS first_timestamp,
            MAX(date) AS last_timestamp,
            MAX(duplicate_count) AS max_occurrences
        FROM duplicate_ranks
        """,
        params,
    ).fetchone()

    bad_count, first_timestamp, last_timestamp, max_occurrences = rows
    if not bad_count:
        return []
    return [
        QualityFinding(
            severity="ERROR",
            table="crypto_universe",
            check_name="duplicate_ranks",
            message=f"{bad_count} duplicate rank groups detected",
            count=int(bad_count),
            first_timestamp=first_timestamp,
            last_timestamp=last_timestamp,
            metadata={"max_occurrences": int(max_occurrences)},
        )
    ]


def _check_universe_invalid_values(
    conn: duckdb.DuckDBPyConnection,
    symbols: Sequence[str] | None,
) -> list[QualityFinding]:
    filter_sql, params = _filters(symbols=symbols, alias="t")
    condition = """
        t.date IS NULL
        OR t.symbol IS NULL
        OR t.rank IS NULL
        OR t.rank < 1
        OR (t.market_cap IS NOT NULL AND (
            NOT isfinite(t.market_cap) OR t.market_cap < 0
        ))
    """
    where_sql = _append_condition(filter_sql, condition)
    rows = conn.execute(
        f"""
        SELECT
            t.symbol,
            COUNT(*) AS bad_count,
            MIN(t.date) AS first_timestamp,
            MAX(t.date) AS last_timestamp
        FROM crypto_universe AS t
        {where_sql}
        GROUP BY t.symbol
        """,
        params,
    ).fetchall()

    return [
        QualityFinding(
            severity="ERROR",
            table="crypto_universe",
            check_name="invalid_values",
            message=f"{bad_count} rows contain null, infinite, or out-of-range values",
            count=int(bad_count),
            symbol=symbol,
            first_timestamp=first_timestamp,
            last_timestamp=last_timestamp,
        )
        for symbol, bad_count, first_timestamp, last_timestamp in rows
    ]


def _check_universe_rank_ordering(conn: duckdb.DuckDBPyConnection) -> list[QualityFinding]:
    row = conn.execute(
        """
        WITH ranked AS (
            SELECT
                date,
                symbol,
                rank,
                market_cap,
                LAG(market_cap) OVER (PARTITION BY date ORDER BY rank) AS prev_market_cap
            FROM crypto_universe
            WHERE market_cap IS NOT NULL AND isfinite(market_cap)
        ),
        violations AS (
            SELECT *
            FROM ranked
            WHERE prev_market_cap IS NOT NULL
              AND market_cap > prev_market_cap
        )
        SELECT
            COUNT(*) AS bad_count,
            MIN(date) AS first_timestamp,
            MAX(date) AS last_timestamp,
            MAX(market_cap - prev_market_cap) AS max_market_cap_inversion
        FROM violations
        """
    ).fetchone()

    bad_count, first_timestamp, last_timestamp, max_market_cap_inversion = row
    if not bad_count:
        return []
    return [
        QualityFinding(
            severity="ERROR",
            table="crypto_universe",
            check_name="rank_market_cap_ordering",
            message=f"{bad_count} rows violate rank ordering by market cap",
            count=int(bad_count),
            first_timestamp=first_timestamp,
            last_timestamp=last_timestamp,
            metadata={"max_market_cap_inversion": float(max_market_cap_inversion)},
        )
    ]


def _normalize_tables(tables: Sequence[str] | None) -> tuple[str, ...]:
    if not tables:
        return SUPPORTED_TABLES
    normalized = tuple(_normalize_values(tables) or ())
    unsupported = sorted(set(normalized) - set(SUPPORTED_TABLES))
    if unsupported:
        valid = ", ".join(SUPPORTED_TABLES)
        raise ValueError(f"Unsupported table(s): {', '.join(unsupported)}. Valid values: {valid}")
    return normalized


def _normalize_values(values: Sequence[str] | None) -> tuple[str, ...] | None:
    if not values:
        return None
    normalized = []
    for value in values:
        for part in str(value).split(","):
            part = part.strip()
            if part:
                normalized.append(part)
    return tuple(dict.fromkeys(normalized)) or None


def _universe_expected_bounds(
    conn: duckdb.DuckDBPyConnection,
    config: QualityConfig,
) -> tuple[datetime, datetime] | None:
    start_dt = _parse_optional_date(config.universe_start_date)
    end_dt = _parse_optional_date(config.universe_end_date)

    if start_dt is not None and end_dt is not None:
        return start_dt, end_dt

    row = conn.execute("SELECT MIN(date), MAX(date) FROM crypto_universe").fetchone()
    if not row or row[0] is None or row[1] is None:
        return None

    return start_dt or _parse_optional_date(row[0]), end_dt or _parse_optional_date(row[1])


def _parse_optional_date(value: str | date | datetime | None) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.replace(hour=0, minute=0, second=0, microsecond=0)
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time())
    return datetime.strptime(str(value), "%Y-%m-%d")


def _table_exists(conn: duckdb.DuckDBPyConnection, table: str) -> bool:
    row = conn.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = ?
        """,
        [table],
    ).fetchone()
    return bool(row and row[0])


def _table_columns(conn: duckdb.DuckDBPyConnection, table: str) -> set[str]:
    rows = conn.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = ?
        """,
        [table],
    ).fetchall()
    return {row[0] for row in rows}


def _universe_identity_columns(conn: duckdb.DuckDBPyConnection) -> tuple[str, ...]:
    columns = _table_columns(conn, "crypto_universe")
    if {"provider", "provider_id", "date"}.issubset(columns):
        return ("provider", "provider_id", "date")
    return ("date", "symbol")


def _universe_date_filter(
    conn: duckdb.DuckDBPyConnection,
    config: QualityConfig,
) -> tuple[str, list[Any]]:
    bounds = _universe_expected_bounds(conn, config)
    if bounds is None:
        return "", []

    start_dt, end_dt = bounds
    if start_dt > end_dt:
        return "WHERE FALSE", []

    return "WHERE date >= ? AND date <= ?", [start_dt.date(), end_dt.date()]


def _filters(
    *,
    symbols: Sequence[str] | None = None,
    intervals: Sequence[str] | None = None,
    alias: str = "",
) -> tuple[str, list[str]]:
    prefix = f"{alias}." if alias else ""
    conditions = []
    params: list[str] = []

    if symbols:
        conditions.append(f"{prefix}symbol IN ({_placeholders(symbols)})")
        params.extend(symbols)
    if intervals:
        conditions.append(f"{prefix}interval IN ({_placeholders(intervals)})")
        params.extend(intervals)

    if not conditions:
        return "", params
    return "WHERE " + " AND ".join(conditions), params


def _append_condition(filter_sql: str, condition: str) -> str:
    clean_condition = " ".join(condition.split())
    if filter_sql:
        return f"{filter_sql} AND ({clean_condition})"
    return f"WHERE {clean_condition}"


def _placeholders(values: Sequence[str]) -> str:
    return ", ".join("?" for _ in values)


def _format_seconds(seconds: int | float) -> str:
    seconds = int(seconds)
    if seconds % 86400 == 0:
        return f"{seconds // 86400}d"
    if seconds % 3600 == 0:
        return f"{seconds // 3600}h"
    if seconds % 60 == 0:
        return f"{seconds // 60}m"
    return f"{seconds}s"


def _finding_sort_key(finding: QualityFinding) -> tuple[int, str, str, str, str]:
    severity_order = 0 if finding.severity == "ERROR" else 1
    return (
        severity_order,
        finding.table,
        finding.symbol or "",
        finding.interval or "",
        finding.check_name,
    )


def _jsonable(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _jsonable(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_jsonable(item) for item in value]
    if isinstance(value, tuple):
        return [_jsonable(item) for item in value]
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


__all__ = [
    "SUPPORTED_TABLES",
    "QualityConfig",
    "QualityFinding",
    "audit_connection",
    "audit_database",
    "findings_to_dataframe",
    "findings_to_jsonable",
    "format_findings",
    "has_errors",
    "has_warnings",
]
