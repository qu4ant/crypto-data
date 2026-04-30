#!/usr/bin/env python3
"""Check whether Binance USD-M futures API has klines missing from the local DB."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import duckdb

DEFAULT_DB_PATH = "crypto_top60_h4_2022_01_2026_04.db"
BINANCE_FUTURES_BASE_URL = "https://fapi.binance.com"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Find local futures kline gaps for one symbol and check whether "
            "Binance USD-M futures REST API returns the missing candles."
        )
    )
    parser.add_argument("--db-path", default=DEFAULT_DB_PATH)
    parser.add_argument("--symbol", default="SOLUSDT")
    parser.add_argument("--interval", default="4h")
    parser.add_argument(
        "--start-close",
        help=(
            "Previous local DB close timestamp before the gap, UTC "
            "(YYYY-MM-DD HH:MM:SS). If omitted, gaps are read from the DB."
        ),
    )
    parser.add_argument(
        "--end-close",
        help=(
            "Next local DB close timestamp after the gap, UTC "
            "(YYYY-MM-DD HH:MM:SS). If omitted, gaps are read from the DB."
        ),
    )
    return parser.parse_args()


def interval_to_milliseconds(interval: str) -> int:
    value = int(interval[:-1])
    unit = interval[-1]
    if unit == "m":
        return value * 60_000
    if unit == "h":
        return value * 3_600_000
    if unit == "d":
        return value * 86_400_000
    raise ValueError(f"Unsupported interval for this quick check: {interval}")


def parse_utc(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")


def to_utc_ms(value: datetime) -> int:
    return int(value.replace(tzinfo=timezone.utc).timestamp() * 1000)


def from_utc_ms(value: int) -> datetime:
    return datetime.fromtimestamp(value / 1000, tz=timezone.utc).replace(tzinfo=None)


def close_key_from_binance_close_ms(close_time_ms: int) -> datetime:
    # Binance closeTime is the final millisecond before the next candle boundary.
    return from_utc_ms(close_time_ms + 1)


def expected_missing_close_keys(
    previous_close: datetime,
    next_close: datetime,
    interval_ms: int,
) -> list[datetime]:
    start_ms = to_utc_ms(previous_close) + interval_ms
    end_ms = to_utc_ms(next_close)
    return [from_utc_ms(ms) for ms in range(start_ms, end_ms, interval_ms)]


def read_local_gaps(db_path: str, symbol: str, interval: str) -> list[tuple[datetime, datetime]]:
    interval_seconds = interval_to_milliseconds(interval) // 1000
    with duckdb.connect(db_path, read_only=True) as conn:
        rows = conn.execute(
            """
            WITH ordered AS (
                SELECT
                    timestamp,
                    LAG(timestamp) OVER (ORDER BY timestamp) AS previous_timestamp,
                    timestamp - LAG(timestamp) OVER (ORDER BY timestamp) AS diff
                FROM futures
                WHERE exchange = 'binance'
                  AND symbol = ?
                  AND interval = ?
            )
            SELECT previous_timestamp, timestamp
            FROM ordered
            WHERE previous_timestamp IS NOT NULL
              AND EXTRACT(epoch FROM diff) > ?
            ORDER BY timestamp
            """,
            [symbol, interval, interval_seconds],
        ).fetchall()
    return [(previous_close, next_close) for previous_close, next_close in rows]


def fetch_futures_klines(
    symbol: str,
    interval: str,
    previous_close: datetime,
    next_close: datetime,
    interval_ms: int,
) -> list[list]:
    start_time = to_utc_ms(previous_close)
    end_time = to_utc_ms(next_close) - interval_ms - 1
    if end_time < start_time:
        return []

    params = {
        "symbol": symbol,
        "interval": interval,
        "startTime": start_time,
        "endTime": end_time,
        "limit": 1500,
    }
    url = f"{BINANCE_FUTURES_BASE_URL}/fapi/v1/klines?{urlencode(params)}"
    # URL is hardcoded https Binance endpoint; no user-controlled scheme.
    request = Request(url, headers={"User-Agent": "crypto-data-gap-check/1.0"})  # noqa: S310
    with urlopen(request, timeout=30) as response:  # noqa: S310
        body = response.read().decode("utf-8")
    return json.loads(body)


def main() -> int:
    args = parse_args()
    interval_ms = interval_to_milliseconds(args.interval)

    if bool(args.start_close) != bool(args.end_close):
        raise ValueError("--start-close and --end-close must be provided together")

    if args.start_close and args.end_close:
        gaps = [(parse_utc(args.start_close), parse_utc(args.end_close))]
    else:
        gaps = read_local_gaps(args.db_path, args.symbol, args.interval)

    if not gaps:
        print(f"No local gaps found for {args.symbol} futures {args.interval}.")
        return 0

    exit_code = 0
    for index, (previous_close, next_close) in enumerate(gaps, start=1):
        expected = expected_missing_close_keys(previous_close, next_close, interval_ms)
        klines = fetch_futures_klines(
            args.symbol,
            args.interval,
            previous_close,
            next_close,
            interval_ms,
        )
        returned_close_keys = [close_key_from_binance_close_ms(int(kline[6])) for kline in klines]
        returned_set = set(returned_close_keys)
        missing_from_api = [timestamp for timestamp in expected if timestamp not in returned_set]

        print()
        print(f"Gap {index}: {args.symbol} futures {args.interval}")
        print(f"  Local previous close: {previous_close}")
        print(f"  Local next close:     {next_close}")
        print(f"  Expected missing:     {len(expected)} candles")
        print(f"  API returned:         {len(returned_close_keys)} candles in gap window")

        if returned_close_keys:
            print(f"  First API close:      {returned_close_keys[0]}")
            print(f"  Last API close:       {returned_close_keys[-1]}")

        if missing_from_api:
            exit_code = 1
            sample = ", ".join(str(timestamp) for timestamp in missing_from_api[:10])
            print(f"  Still missing in API: {len(missing_from_api)} candles")
            print(f"  Missing sample:       {sample}")
        else:
            print("  API has all expected missing candles for this gap.")

    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
