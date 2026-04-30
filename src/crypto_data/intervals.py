"""Shared Binance kline interval metadata."""

from __future__ import annotations

KLINE_INTERVAL_SECONDS: dict[str, int] = {
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

VARIABLE_LENGTH_KLINE_INTERVALS: tuple[str, ...] = ("1M",)

SUPPORTED_KLINE_INTERVALS: tuple[str, ...] = (
    *KLINE_INTERVAL_SECONDS.keys(),
    *VARIABLE_LENGTH_KLINE_INTERVALS,
)


def interval_to_seconds(interval: str | None) -> int | None:
    """Return fixed seconds for a Binance kline interval, or None for variable intervals."""
    if interval is None:
        return None
    return KLINE_INTERVAL_SECONDS.get(interval)
