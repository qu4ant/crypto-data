"""Tests for gap enumeration used by quality audits and repair."""

from __future__ import annotations

import pandas as pd
import pytest

from crypto_data.binance_repair import (
    FUNDING_EXPECTED_SECONDS,
    GapBoundary,
    enumerate_kline_gaps,
    enumerate_metric_gaps,
)
from tests.repair.conftest import _insert_funding, _insert_klines


def test_no_rows_returns_empty(empty_db):
    assert enumerate_kline_gaps(empty_db.conn, table="futures") == []


def test_contiguous_rows_no_gap(empty_db):
    timestamps = pd.date_range("2024-01-01T00:00", periods=5, freq="4h")
    _insert_klines(empty_db.conn, "futures", "SOLUSDT", "4h", list(timestamps))

    assert enumerate_kline_gaps(empty_db.conn, table="futures") == []


def test_single_gap_returns_one_boundary(db_with_kline_gap):
    gaps = enumerate_kline_gaps(db_with_kline_gap.conn, table="futures")

    assert len(gaps) == 1
    gap = gaps[0]
    assert isinstance(gap, GapBoundary)
    assert gap.table == "futures"
    assert gap.symbol == "SOLUSDT"
    assert gap.interval == "4h"
    assert gap.prev_close == pd.Timestamp("2024-01-01T16:00").to_pydatetime()
    assert gap.next_close == pd.Timestamp("2024-01-02T16:00").to_pydatetime()
    assert gap.expected_seconds == 14400


def test_multi_symbol_multi_interval(empty_db):
    sol_pre = pd.date_range("2024-01-01T00:00", periods=3, freq="4h")
    sol_post = pd.date_range("2024-01-02T00:00", periods=3, freq="4h")
    btc_pre = pd.date_range("2024-01-01T00:00", periods=3, freq="1h")
    btc_post = pd.date_range("2024-01-01T10:00", periods=3, freq="1h")
    _insert_klines(empty_db.conn, "futures", "SOLUSDT", "4h", list(sol_pre) + list(sol_post))
    _insert_klines(empty_db.conn, "futures", "BTCUSDT", "1h", list(btc_pre) + list(btc_post))

    gaps = enumerate_kline_gaps(empty_db.conn, table="futures")

    assert len(gaps) == 2
    assert {gap.symbol for gap in gaps} == {"SOLUSDT", "BTCUSDT"}


def test_filter_by_symbol(empty_db):
    pre = pd.date_range("2024-01-01T00:00", periods=3, freq="4h")
    post = pd.date_range("2024-01-02T00:00", periods=3, freq="4h")
    _insert_klines(empty_db.conn, "futures", "SOLUSDT", "4h", list(pre) + list(post))
    _insert_klines(empty_db.conn, "futures", "BTCUSDT", "4h", list(pre) + list(post))

    gaps = enumerate_kline_gaps(empty_db.conn, table="futures", symbols=["SOLUSDT"])

    assert len(gaps) == 1
    assert gaps[0].symbol == "SOLUSDT"


def test_filter_by_interval(empty_db):
    pre_4h = pd.date_range("2024-01-01T00:00", periods=3, freq="4h")
    post_4h = pd.date_range("2024-01-02T00:00", periods=3, freq="4h")
    pre_1h = pd.date_range("2024-01-01T00:00", periods=3, freq="1h")
    post_1h = pd.date_range("2024-01-01T10:00", periods=3, freq="1h")
    _insert_klines(empty_db.conn, "futures", "SOLUSDT", "4h", list(pre_4h) + list(post_4h))
    _insert_klines(empty_db.conn, "futures", "SOLUSDT", "1h", list(pre_1h) + list(post_1h))

    gaps = enumerate_kline_gaps(empty_db.conn, table="futures", intervals=["4h"])

    assert len(gaps) == 1
    assert gaps[0].interval == "4h"


def test_unsupported_kline_table_raises(empty_db):
    with pytest.raises(ValueError, match="spot/futures"):
        enumerate_kline_gaps(empty_db.conn, table="open_interest")


def test_funding_no_gap_returns_empty(empty_db):
    timestamps = pd.date_range("2024-01-01T00:00", periods=4, freq="8h")
    _insert_funding(empty_db.conn, "BTCUSDT", list(timestamps))

    assert (
        enumerate_metric_gaps(
            empty_db.conn,
            table="funding_rates",
            expected_seconds=FUNDING_EXPECTED_SECONDS,
        )
        == []
    )


def test_funding_single_gap(db_with_funding_gap):
    gaps = enumerate_metric_gaps(
        db_with_funding_gap.conn,
        table="funding_rates",
        expected_seconds=FUNDING_EXPECTED_SECONDS,
    )

    assert len(gaps) == 1
    gap = gaps[0]
    assert gap.table == "funding_rates"
    assert gap.symbol == "BTCUSDT"
    assert gap.interval is None
    assert gap.prev_close == pd.Timestamp("2024-01-02T00:00").to_pydatetime()
    assert gap.next_close == pd.Timestamp("2024-01-03T08:00").to_pydatetime()


def test_funding_filter_by_symbol(empty_db):
    pre = pd.date_range("2024-01-01T00:00", periods=3, freq="8h")
    post = pd.date_range("2024-01-02T08:00", periods=3, freq="8h")
    _insert_funding(empty_db.conn, "BTCUSDT", list(pre) + list(post))
    _insert_funding(empty_db.conn, "ETHUSDT", list(pre) + list(post))

    gaps = enumerate_metric_gaps(
        empty_db.conn,
        table="funding_rates",
        expected_seconds=FUNDING_EXPECTED_SECONDS,
        symbols=["BTCUSDT"],
    )

    assert len(gaps) == 1
    assert gaps[0].symbol == "BTCUSDT"
