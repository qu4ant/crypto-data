"""Tests for Binance pipeline REST repair integration."""

from __future__ import annotations

from unittest.mock import patch

from crypto_data import update_binance_market_data
from crypto_data.enums import DataType, Interval


def _close_coroutine(coro, caller_name):
    coro.close()
    return


def test_update_binance_market_data_calls_repair_when_flag_set(tmp_path):
    db_path = tmp_path / "test.db"

    with (
        patch("crypto_data.binance_pipeline.run_async_from_sync") as fake_async,
        patch("crypto_data.binance_pipeline.repair_binance_gaps") as fake_repair,
    ):
        fake_async.side_effect = _close_coroutine
        update_binance_market_data(
            db_path=str(db_path),
            symbols=["BTCUSDT"],
            data_types=[DataType.SPOT],
            start_date="2024-01-01",
            end_date="2024-01-02",
            interval=Interval.HOUR_4,
            repair_gaps_via_api=True,
        )

    fake_repair.assert_called_once_with(
        db_path=str(db_path),
        tables=["spot"],
        symbols=["BTCUSDT"],
        intervals=["4h"],
    )


def test_update_binance_market_data_skips_repair_by_default(tmp_path):
    db_path = tmp_path / "test.db"

    with (
        patch("crypto_data.binance_pipeline.run_async_from_sync") as fake_async,
        patch("crypto_data.binance_pipeline.repair_binance_gaps") as fake_repair,
    ):
        fake_async.side_effect = _close_coroutine
        update_binance_market_data(
            db_path=str(db_path),
            symbols=["BTCUSDT"],
            data_types=[DataType.SPOT],
            start_date="2024-01-01",
            end_date="2024-01-02",
            interval=Interval.HOUR_4,
        )

    fake_repair.assert_not_called()
