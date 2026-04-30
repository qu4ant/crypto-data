"""
Random sampling utilities for database validation.
"""

import random
from dataclasses import dataclass
from datetime import datetime

import duckdb


@dataclass
class OHLCVSample:
    """Sample point for OHLCV validation"""

    exchange: str
    symbol: str
    interval: str
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    quote_volume: float
    trades_count: int | None
    taker_buy_base_volume: float | None
    taker_buy_quote_volume: float | None


@dataclass
class UniverseSample:
    """Sample point for universe validation"""

    date: datetime
    symbol: str
    rank: int
    market_cap: float


def sample_ohlcv(
    db_path: str,
    table: str,  # 'spot' or 'futures'
    n: int = 20,
    symbol: str | None = None,
    interval: str | None = None,
    seed: int | None = None,
) -> list[OHLCVSample]:
    """
    Randomly sample N rows from OHLCV table.

    Approach:
    1. Get total row count (with filters)
    2. Generate N random offsets
    3. Query rows at those offsets
    """
    if seed is not None:
        random.seed(seed)

    if table not in ("spot", "futures"):
        raise ValueError(f"Unsupported table for OHLCV sampling: {table}")

    conn = duckdb.connect(db_path, read_only=True)

    try:
        # Build WHERE clause (parameterized)
        conditions = []
        params = []
        if symbol:
            conditions.append("symbol = ?")
            params.append(symbol)
        if interval:
            conditions.append("interval = ?")
            params.append(interval)

        where_clause = ""
        if conditions:
            where_clause = "WHERE " + " AND ".join(conditions)

        # Get total count
        count_query = f"SELECT COUNT(*) FROM {table} {where_clause}"
        total_count = conn.execute(count_query, params).fetchone()[0]

        if total_count == 0:
            return []

        # Generate random offsets
        n = min(n, total_count)
        offsets = random.sample(range(total_count), n)

        samples = []
        for offset in offsets:
            query = f"""
                SELECT
                    exchange, symbol, interval, timestamp,
                    open, high, low, close,
                    volume, quote_volume, trades_count,
                    taker_buy_base_volume, taker_buy_quote_volume
                FROM {table}
                {where_clause}
                ORDER BY exchange, symbol, interval, timestamp
                LIMIT 1 OFFSET {offset}
            """
            row = conn.execute(query, params).fetchone()
            if row:
                samples.append(
                    OHLCVSample(
                        exchange=row[0],
                        symbol=row[1],
                        interval=row[2],
                        timestamp=row[3],
                        open=float(row[4]),
                        high=float(row[5]),
                        low=float(row[6]),
                        close=float(row[7]),
                        volume=float(row[8]),
                        quote_volume=float(row[9]),
                        trades_count=int(row[10]) if row[10] is not None else None,
                        taker_buy_base_volume=float(row[11]) if row[11] is not None else None,
                        taker_buy_quote_volume=float(row[12]) if row[12] is not None else None,
                    )
                )

        return samples

    finally:
        conn.close()


def sample_ohlcv_ratio_by_symbol(
    db_path: str,
    table: str,  # 'spot' or 'futures'
    sample_ratio: float = 0.2,
    max_symbols: int = 12,
    interval: str | None = None,
    seed: int | None = None,
) -> list[OHLCVSample]:
    """
    Sample OHLCV points with a global ratio budget, then split per symbol.

    Steps:
    1. Select up to `max_symbols` symbols from available data
    2. Compute total points across selected symbols
    3. Allocate `sample_ratio` of that total as a global sampling budget
    4. Split budget across symbols (roughly equal), then sample per symbol
    """
    if seed is not None:
        random.seed(seed)

    if table not in ("spot", "futures"):
        raise ValueError(f"Unsupported table for OHLCV sampling: {table}")
    if sample_ratio <= 0 or sample_ratio > 1:
        raise ValueError(f"sample_ratio must satisfy 0 < ratio <= 1, got {sample_ratio}")
    if max_symbols <= 0:
        raise ValueError(f"max_symbols must be > 0, got {max_symbols}")

    conn = duckdb.connect(db_path, read_only=True)
    try:
        params = []
        where_clause = ""
        if interval:
            where_clause = "WHERE interval = ?"
            params.append(interval)

        symbol_counts = conn.execute(
            f"""
            SELECT symbol, COUNT(*) AS row_count
            FROM {table}
            {where_clause}
            GROUP BY symbol
            ORDER BY symbol
            """,
            params,
        ).fetchall()

        if not symbol_counts:
            return []

        selected = symbol_counts
        if len(selected) > max_symbols:
            selected = random.sample(selected, max_symbols)

        total_points = sum(int(row_count) for _, row_count in selected)
        target_total = max(1, int(total_points * sample_ratio))

        # Ensure at least 1 point per selected symbol when possible.
        if target_total < len(selected):
            selected = random.sample(selected, target_total)

        symbol_count = len(selected)
        if symbol_count == 0:
            return []

        base_per_symbol = target_total // symbol_count
        remainder = target_total % symbol_count

        samples: list[OHLCVSample] = []
        for idx, (symbol, row_count) in enumerate(selected):
            allocated = base_per_symbol + (1 if idx < remainder else 0)
            n_symbol = min(int(row_count), max(1, allocated))

            offsets = random.sample(range(int(row_count)), n_symbol)
            for offset in offsets:
                symbol_params = []
                symbol_where = "WHERE symbol = ?"
                symbol_params.append(symbol)
                if interval:
                    symbol_where += " AND interval = ?"
                    symbol_params.append(interval)

                row = conn.execute(
                    f"""
                    SELECT
                        exchange, symbol, interval, timestamp,
                        open, high, low, close,
                        volume, quote_volume, trades_count,
                        taker_buy_base_volume, taker_buy_quote_volume
                    FROM {table}
                    {symbol_where}
                    ORDER BY exchange, symbol, interval, timestamp
                    LIMIT 1 OFFSET {offset}
                    """,
                    symbol_params,
                ).fetchone()

                if not row:
                    continue

                samples.append(
                    OHLCVSample(
                        exchange=row[0],
                        symbol=row[1],
                        interval=row[2],
                        timestamp=row[3],
                        open=float(row[4]),
                        high=float(row[5]),
                        low=float(row[6]),
                        close=float(row[7]),
                        volume=float(row[8]),
                        quote_volume=float(row[9]),
                        trades_count=int(row[10]) if row[10] is not None else None,
                        taker_buy_base_volume=float(row[11]) if row[11] is not None else None,
                        taker_buy_quote_volume=float(row[12]) if row[12] is not None else None,
                    )
                )

        return samples
    finally:
        conn.close()


def sample_universe(
    db_path: str,
    n: int = 10,
    seed: int | None = None,
) -> list[UniverseSample]:
    """
    Randomly sample N (date, symbol) pairs from crypto_universe.

    Approach:
    1. Get total count
    2. Generate N random offsets
    3. Query rows at those offsets
    """
    if seed is not None:
        random.seed(seed)

    conn = duckdb.connect(db_path, read_only=True)

    try:
        # Get total count
        total_count = conn.execute("SELECT COUNT(*) FROM crypto_universe").fetchone()[0]

        if total_count == 0:
            return []

        # Generate random offsets
        n = min(n, total_count)
        offsets = random.sample(range(total_count), n)

        samples = []
        for offset in offsets:
            query = f"""
                SELECT date, symbol, rank, market_cap
                FROM crypto_universe
                ORDER BY date, rank
                LIMIT 1 OFFSET {offset}
            """
            row = conn.execute(query).fetchone()
            if row:
                samples.append(
                    UniverseSample(
                        date=row[0],
                        symbol=row[1],
                        rank=int(row[2]),
                        market_cap=float(row[3]),
                    )
                )

        return samples

    finally:
        conn.close()


def group_samples_by_date(samples: list[UniverseSample]) -> dict:
    """Group universe samples by date to minimize API calls"""
    grouped = {}
    for sample in samples:
        # Normalize date to date-only (no time component)
        date_key = sample.date.date() if isinstance(sample.date, datetime) else sample.date
        if date_key not in grouped:
            grouped[date_key] = []
        grouped[date_key].append(sample)
    return grouped


def get_table_stats(db_path: str, table: str) -> dict:
    """Get basic statistics about a table for validation planning"""
    conn = duckdb.connect(db_path, read_only=True)

    try:
        stats = {}

        # Total row count
        stats["total_rows"] = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]

        if table in ("spot", "futures"):
            # Distinct symbols
            stats["distinct_symbols"] = conn.execute(
                f"SELECT COUNT(DISTINCT symbol) FROM {table}"
            ).fetchone()[0]

            # Distinct intervals
            stats["distinct_intervals"] = conn.execute(
                f"SELECT COUNT(DISTINCT interval) FROM {table}"
            ).fetchone()[0]

            # Date range
            date_range = conn.execute(
                f"SELECT MIN(timestamp), MAX(timestamp) FROM {table}"
            ).fetchone()
            stats["min_timestamp"] = date_range[0]
            stats["max_timestamp"] = date_range[1]

        elif table == "crypto_universe":
            # Distinct dates
            stats["distinct_dates"] = conn.execute(
                "SELECT COUNT(DISTINCT date) FROM crypto_universe"
            ).fetchone()[0]

            # Distinct symbols
            stats["distinct_symbols"] = conn.execute(
                "SELECT COUNT(DISTINCT symbol) FROM crypto_universe"
            ).fetchone()[0]

        return stats

    finally:
        conn.close()
