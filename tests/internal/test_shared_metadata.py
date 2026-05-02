"""Direct tests for shared internal metadata/refactor modules."""

from __future__ import annotations

from datetime import datetime

import pandas as pd
import pytest

from crypto_data.binance_datasets.base import Period
from crypto_data.completeness import (
    filter_existing_periods,
    kline_close_window,
    period_exists_in_db,
)
from crypto_data.database import CryptoDatabase
from crypto_data.db_write import insert_idempotent
from crypto_data.enums import Interval
from crypto_data.intervals import SUPPORTED_KLINE_INTERVALS, interval_to_seconds
from crypto_data.tables import (
    FUNDING_RATES_EXPECTED_SECONDS,
    KLINE_PRIMARY_KEY,
    KLINE_TABLE_COLUMNS,
    OPEN_INTEREST_EXPECTED_SECONDS,
    get_table_spec,
    primary_key_for,
)


def test_interval_metadata_matches_public_enum() -> None:
    assert Interval.MIN_3.value == "3m"
    assert "3m" in SUPPORTED_KLINE_INTERVALS
    assert interval_to_seconds("3m") == 180
    assert interval_to_seconds("1M") is None


def test_table_specs_expose_stable_table_facts() -> None:
    spot = get_table_spec("spot")
    open_interest = get_table_spec("open_interest")
    funding = get_table_spec("funding_rates")

    assert spot.kind == "kline"
    assert spot.primary_key == KLINE_PRIMARY_KEY
    assert spot.columns == KLINE_TABLE_COLUMNS
    assert spot.requires_interval is True
    assert spot.repair_supported is True

    assert open_interest.kind == "metric"
    assert open_interest.expected_seconds == OPEN_INTEREST_EXPECTED_SECONDS
    assert OPEN_INTEREST_EXPECTED_SECONDS == 5 * 60

    assert funding.kind == "metric"
    assert funding.expected_seconds == FUNDING_RATES_EXPECTED_SECONDS
    assert primary_key_for("funding_rates") == ("exchange", "symbol", "timestamp")

    with pytest.raises(ValueError, match="Unsupported table"):
        get_table_spec("unknown")


def test_insert_idempotent_counts_only_new_primary_keys(tmp_path) -> None:
    db = CryptoDatabase(str(tmp_path / "insert.db"))
    try:
        df = pd.DataFrame(
            [
                {
                    "exchange": "binance",
                    "symbol": "BTCUSDT",
                    "interval": "4h",
                    "timestamp": pd.Timestamp("2024-01-01T04:00"),
                    "open": 1.0,
                    "high": 2.0,
                    "low": 0.5,
                    "close": 1.5,
                    "volume": 100.0,
                    "quote_volume": 150.0,
                    "trades_count": 42,
                    "taker_buy_base_volume": 50.0,
                    "taker_buy_quote_volume": 75.0,
                }
            ],
            columns=KLINE_TABLE_COLUMNS,
        )

        assert insert_idempotent(db.conn, "spot", df) == 1
        assert insert_idempotent(db.conn, "spot", df) == 0
        assert db.conn.execute("SELECT COUNT(*) FROM spot").fetchone()[0] == 1
    finally:
        db.close()


def test_insert_idempotent_deduplicates_incoming_primary_keys(tmp_path) -> None:
    db = CryptoDatabase(str(tmp_path / "insert_duplicates.db"))
    try:
        row = {
            "exchange": "binance",
            "symbol": "BTCUSDT",
            "interval": "4h",
            "timestamp": pd.Timestamp("2024-01-01T04:00"),
            "open": 1.0,
            "high": 2.0,
            "low": 0.5,
            "close": 1.5,
            "volume": 100.0,
            "quote_volume": 150.0,
            "trades_count": 42,
            "taker_buy_base_volume": 50.0,
            "taker_buy_quote_volume": 75.0,
        }
        df = pd.DataFrame([row, {**row, "close": 1.6}], columns=KLINE_TABLE_COLUMNS)

        assert insert_idempotent(db.conn, "spot", df) == 1
        assert insert_idempotent(db.conn, "spot", df) == 0
        assert db.conn.execute("SELECT COUNT(*) FROM spot").fetchone()[0] == 1
    finally:
        db.close()


def test_completeness_detects_full_kline_period_and_filters_existing(tmp_path) -> None:
    db = CryptoDatabase(str(tmp_path / "complete.db"))
    try:
        timestamps = pd.date_range("2024-01-01T04:00", "2024-02-01T00:00", freq="4h")
        rows = [
            (
                "binance",
                "BTCUSDT",
                "4h",
                timestamp.to_pydatetime(),
                1.0,
                2.0,
                0.5,
                1.5,
                100.0,
                150.0,
                42,
                50.0,
                75.0,
            )
            for timestamp in timestamps
        ]
        db.conn.executemany("INSERT INTO spot VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)

        period = Period("2024-01")
        assert kline_close_window(datetime(2024, 1, 1), datetime(2024, 1, 31)) == (
            datetime(2024, 1, 1),
            datetime(2024, 2, 1),
        )
        assert period_exists_in_db(db.conn, "spot", "BTCUSDT", "4h", period) is True
        assert filter_existing_periods(db.conn, "spot", "BTCUSDT", "4h", [period]) == ([], 1)
    finally:
        db.close()
