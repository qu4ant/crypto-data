"""Tests for create_binance_database repair flag pass-through."""

from __future__ import annotations

from unittest.mock import patch

from crypto_data import create_binance_database


def _close_universe_coroutine(coro, caller_name):
    coro.close()
    return {"by_tag": set(), "by_symbol": set()}


def test_create_binance_database_propagates_repair_flag(tmp_path):
    db_path = str(tmp_path / "test.db")

    with (
        patch(
            "crypto_data.database_builder.run_async_from_sync",
            side_effect=_close_universe_coroutine,
        ),
        patch(
            "crypto_data.database_builder.get_binance_symbols_from_universe",
            return_value=["BTCUSDT"],
        ),
        patch("crypto_data.binance_pipeline.update_binance_market_data") as fake_update,
    ):
        create_binance_database(
            db_path=db_path,
            start_date="2024-01-01",
            end_date="2024-01-02",
            top_n=10,
            repair_gaps_via_api=True,
        )

    assert fake_update.call_args.kwargs["repair_gaps_via_api"] is True
