"""Tests for Binance repair strategies."""

from __future__ import annotations

from datetime import datetime

import pandas as pd
import pytest

from crypto_data.binance_repair import (
    FUNDING_EXPECTED_SECONDS,
    FundingRatesRepairStrategy,
    KlinesRepairStrategy,
)
from crypto_data.enums import DataType, Interval
from crypto_data.gaps import GapBoundary


def _kline_for_close(close_ts: pd.Timestamp, interval_seconds: int = 4 * 3600) -> list:
    close_ts = pd.Timestamp(close_ts).tz_localize("UTC")
    open_ts = close_ts - pd.Timedelta(seconds=interval_seconds)
    open_ms = int(open_ts.timestamp() * 1000)
    close_ms = int(close_ts.timestamp() * 1000) - 1
    return [
        open_ms,
        "1.0",
        "2.0",
        "0.5",
        "1.5",
        "100.0",
        close_ms,
        "150.0",
        42,
        "50.0",
        "75.0",
        "0",
    ]


class _FakeRestClient:
    def __init__(self, payload):
        self._payload = payload
        self.calls: list[str] = []

    async def get_json(self, url):
        self.calls.append(url)
        return self._payload


def test_parse_klines_payload_to_dataframe():
    strategy = KlinesRepairStrategy(data_type=DataType.FUTURES, interval=Interval.HOUR_4)
    payload = [_kline_for_close(pd.Timestamp("2024-01-02T00:00"))]

    df = strategy.parse_payload(payload, symbol="SOLUSDT")

    assert list(df.columns) == [
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
    ]
    assert len(df) == 1
    row = df.iloc[0]
    assert row["exchange"] == "binance"
    assert row["symbol"] == "SOLUSDT"
    assert row["interval"] == "4h"
    assert row["timestamp"] == pd.Timestamp("2024-01-02T00:00")
    assert row["close"] == 1.5


@pytest.mark.asyncio
async def test_fetch_repair_rows_trims_to_exclusive_window():
    strategy = KlinesRepairStrategy(data_type=DataType.FUTURES, interval=Interval.HOUR_4)
    gap = GapBoundary(
        table="futures",
        symbol="SOLUSDT",
        interval="4h",
        prev_close=datetime(2024, 1, 1, 16),
        next_close=datetime(2024, 1, 2, 0),
        expected_seconds=14400,
    )
    payload = [
        _kline_for_close(pd.Timestamp("2024-01-01T16:00")),
        _kline_for_close(pd.Timestamp("2024-01-01T20:00")),
        _kline_for_close(pd.Timestamp("2024-01-02T00:00")),
    ]
    client = _FakeRestClient(payload)

    df = await strategy.fetch_repair_rows(client, gap)

    assert len(df) == 1
    assert df.iloc[0]["timestamp"] == pd.Timestamp("2024-01-01T20:00")
    assert "startTime=1704124800000" in client.calls[0]


@pytest.mark.asyncio
async def test_fetch_repair_rows_off_grid_raises():
    strategy = KlinesRepairStrategy(data_type=DataType.FUTURES, interval=Interval.HOUR_4)
    gap = GapBoundary(
        table="futures",
        symbol="SOLUSDT",
        interval="4h",
        prev_close=datetime(2024, 1, 1, 16, 7),
        next_close=datetime(2024, 1, 2, 0),
        expected_seconds=14400,
    )

    with pytest.raises(ValueError, match="off-grid"):
        await strategy.fetch_repair_rows(_FakeRestClient([]), gap)


@pytest.mark.asyncio
async def test_fetch_repair_rows_too_large_raises():
    strategy = KlinesRepairStrategy(data_type=DataType.FUTURES, interval=Interval.HOUR_4)
    gap = GapBoundary(
        table="futures",
        symbol="SOLUSDT",
        interval="4h",
        prev_close=datetime(2024, 1, 1),
        next_close=datetime(2024, 11, 1),
        expected_seconds=14400,
    )

    with pytest.raises(ValueError, match="too large"):
        await strategy.fetch_repair_rows(_FakeRestClient([]), gap)


def test_parse_funding_payload_to_dataframe():
    strategy = FundingRatesRepairStrategy()
    payload = [
        {
            "symbol": "BTCUSDT",
            "fundingTime": 1704067200000,
            "fundingRate": "0.00012",
            "markPrice": "",
        },
        {
            "symbol": "BTCUSDT",
            "fundingTime": 1704096000000,
            "fundingRate": "-0.00005",
            "markPrice": "42000",
        },
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
        table="funding_rates",
        symbol="BTCUSDT",
        interval=None,
        prev_close=datetime(2024, 1, 2, 0),
        next_close=datetime(2024, 1, 3, 8),
        expected_seconds=FUNDING_EXPECTED_SECONDS,
    )
    payload = [
        {
            "symbol": "BTCUSDT",
            "fundingTime": 1704153600000,
            "fundingRate": "0.0001",
            "markPrice": "",
        },
        {
            "symbol": "BTCUSDT",
            "fundingTime": 1704182400000,
            "fundingRate": "0.0002",
            "markPrice": "",
        },
        {
            "symbol": "BTCUSDT",
            "fundingTime": 1704211200000,
            "fundingRate": "0.0003",
            "markPrice": "",
        },
        {
            "symbol": "BTCUSDT",
            "fundingTime": 1704240000000,
            "fundingRate": "0.0004",
            "markPrice": "",
        },
    ]

    df = await strategy.fetch_repair_rows(_FakeRestClient(payload), gap)

    assert len(df) == 3
    assert all(df["timestamp"] > pd.Timestamp("2024-01-02T00:00"))
    assert all(df["timestamp"] < pd.Timestamp("2024-01-03T08:00"))
