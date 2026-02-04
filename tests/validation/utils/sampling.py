"""
Random sampling utilities for database validation.
"""

import random
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional

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
    trades_count: int
    taker_buy_base_volume: float
    taker_buy_quote_volume: float


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
    symbol: Optional[str] = None,
    interval: Optional[str] = None,
    seed: Optional[int] = None,
) -> List[OHLCVSample]:
    """
    Randomly sample N rows from OHLCV table.

    Strategy:
    1. Get total row count (with filters)
    2. Generate N random offsets
    3. Query rows at those offsets
    """
    if seed is not None:
        random.seed(seed)

    conn = duckdb.connect(db_path, read_only=True)

    try:
        # Build WHERE clause
        conditions = []
        if symbol:
            conditions.append(f"symbol = '{symbol}'")
        if interval:
            conditions.append(f"interval = '{interval}'")

        where_clause = ""
        if conditions:
            where_clause = "WHERE " + " AND ".join(conditions)

        # Get total count
        count_query = f"SELECT COUNT(*) FROM {table} {where_clause}"
        total_count = conn.execute(count_query).fetchone()[0]

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
            row = conn.execute(query).fetchone()
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
                        trades_count=int(row[10]),
                        taker_buy_base_volume=float(row[11]),
                        taker_buy_quote_volume=float(row[12]),
                    )
                )

        return samples

    finally:
        conn.close()


def sample_universe(
    db_path: str,
    n: int = 10,
    seed: Optional[int] = None,
) -> List[UniverseSample]:
    """
    Randomly sample N (date, symbol) pairs from crypto_universe.

    Strategy:
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


def group_samples_by_date(samples: List[UniverseSample]) -> dict:
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
