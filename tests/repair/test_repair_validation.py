"""Live Binance REST repair validation, opt-in via --run-validation."""

from __future__ import annotations

import pandas as pd
import pytest

from crypto_data.binance_repair import GapBoundary, KlinesRepairStrategy
from crypto_data.clients.binance_rest import BinanceRestClient
from crypto_data.enums import DataType, Interval


@pytest.mark.validation
@pytest.mark.asyncio
async def test_live_solusdt_futures_4h_gap_returns_candles():
    gap = GapBoundary(
        table="futures",
        symbol="SOLUSDT",
        interval="4h",
        prev_close=pd.Timestamp("2022-03-01T04:00").to_pydatetime(),
        next_close=pd.Timestamp("2022-04-03T04:00").to_pydatetime(),
        expected_seconds=14400,
    )
    strategy = KlinesRepairStrategy(data_type=DataType.FUTURES, interval=Interval.HOUR_4)

    async with BinanceRestClient(max_concurrent=1) as client:
        df = await strategy.fetch_repair_rows(client, gap)

    expected_count = 33 * 24 // 4 - 1
    assert len(df) == expected_count
    assert df["timestamp"].is_monotonic_increasing
    assert df["timestamp"].min() > gap.prev_close
    assert df["timestamp"].max() < gap.next_close
