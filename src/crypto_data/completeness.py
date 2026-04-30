"""Database completeness checks used to resume Binance ingestion safely."""

from __future__ import annotations

from datetime import datetime, timedelta

from dateutil.relativedelta import relativedelta

from crypto_data.binance_datasets.base import Period
from crypto_data.intervals import interval_to_seconds
from crypto_data.tables import get_table_spec, is_kline_table


def kline_close_window(start: datetime, end: datetime) -> tuple[datetime, datetime]:
    """
    Return the requested kline close-time window.

    User-facing date ranges are calendar-day inclusive. Because kline rows are
    keyed by candle close time, the first valid close is strictly after the
    start midnight and the final valid close is the midnight after end_date.
    """
    return start, end + timedelta(days=1)


def period_exists_in_db(
    conn,
    table: str,
    symbol: str,
    interval: str | None,
    period: Period,
) -> bool:
    """Return True when a stored period appears complete enough to skip."""
    start, end = _period_bounds(period)

    if is_kline_table(table):
        return _kline_period_exists(conn, table, symbol, interval, start, end)

    return _metric_period_exists(conn, table, symbol, start, end)


def filter_existing_periods(
    conn,
    table: str,
    symbol: str,
    interval: str | None,
    periods: list[Period],
) -> tuple[list[Period], int]:
    """Split periods into missing/downloadable periods and skipped count."""
    missing = []
    skipped = 0

    for period in periods:
        if getattr(period, "replace_existing", False):
            missing.append(period)
            continue

        if period_exists_in_db(conn, table, symbol, interval, period):
            skipped += 1
        else:
            missing.append(period)

    return missing, skipped


def _period_bounds(period: Period) -> tuple[datetime, datetime]:
    if period.is_monthly:
        start = datetime.strptime(period.value, "%Y-%m")
        return start, start + relativedelta(months=1)

    start = datetime.strptime(period.value, "%Y-%m-%d")
    return start, start + relativedelta(days=1)


def _kline_period_exists(
    conn,
    table: str,
    symbol: str,
    interval: str | None,
    start: datetime,
    end: datetime,
) -> bool:
    query = f"""
        SELECT
            MIN(timestamp) as min_ts,
            MAX(timestamp) as max_ts,
            COUNT(*) as row_count
        FROM {table}
        WHERE exchange = ?
          AND symbol = ?
          AND interval = ?
          AND timestamp > ?
          AND timestamp <= ?
    """
    result = conn.execute(query, ["binance", symbol, interval, start, end]).fetchone()

    if not result or result[2] == 0:
        return False

    min_ts, max_ts, row_count = result
    step_seconds = interval_to_seconds(interval)

    if step_seconds is None:
        return True

    observed_seconds = (max_ts - min_ts).total_seconds()
    expected_rows = int(observed_seconds // step_seconds) + 1
    if row_count < expected_rows:
        return False

    edge_tolerance = timedelta(seconds=step_seconds)
    period_start_ceiling = start + edge_tolerance

    covers_period_start = min_ts <= period_start_ceiling
    covers_period_end = max_ts >= end

    return covers_period_start and covers_period_end


def _metric_period_exists(
    conn,
    table: str,
    symbol: str,
    start: datetime,
    end: datetime,
) -> bool:
    query = f"""
        SELECT
            MIN(timestamp) as min_ts,
            MAX(timestamp) as max_ts,
            COUNT(*) as row_count
        FROM {table}
        WHERE exchange = ?
          AND symbol = ?
          AND timestamp >= ?
          AND timestamp < ?
    """
    result = conn.execute(query, ["binance", symbol, start, end]).fetchone()

    if not result or result[2] == 0:
        return False

    min_ts, max_ts, row_count = result
    if row_count < 2:
        return False

    expected_seconds = get_table_spec(table).expected_seconds
    if expected_seconds is None:
        return True

    edge_tolerance = timedelta(seconds=expected_seconds)
    max_gap_tolerance = timedelta(seconds=expected_seconds)

    covers_period_start = min_ts <= start + edge_tolerance
    covers_period_end = max_ts >= end - edge_tolerance
    if not (covers_period_start and covers_period_end):
        return False

    gap_result = conn.execute(
        f"""
        WITH ordered AS (
            SELECT
                timestamp,
                timestamp - LAG(timestamp) OVER (ORDER BY timestamp) AS diff
            FROM {table}
            WHERE exchange = ?
              AND symbol = ?
              AND timestamp >= ?
              AND timestamp < ?
        )
        SELECT MAX(diff)
        FROM ordered
        WHERE diff IS NOT NULL
        """,
        ["binance", symbol, start, end],
    ).fetchone()
    max_gap = gap_result[0] if gap_result else None

    return max_gap is not None and max_gap <= max_gap_tolerance
