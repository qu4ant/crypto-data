"""Tests for repair_binance_gaps orchestration and reports."""

from __future__ import annotations

from datetime import datetime

import aiohttp
import pandas as pd

from crypto_data.binance_repair import RepairReport, UnrecoverableGap, repair_binance_gaps
from crypto_data.clients.binance_rest import BinanceRateLimitError
from tests.repair.conftest import _insert_funding, _insert_klines


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


def test_repair_report_to_jsonable_round_trip():
    gap = UnrecoverableGap(
        table="futures",
        symbol="SOLUSDT",
        interval="4h",
        prev_close=datetime(2024, 1, 1, 16),
        next_close=datetime(2024, 1, 2, 0),
        missing_count=1,
        reason="partial_fill",
    )
    report = RepairReport(
        inserted_rows={"futures": 5},
        gaps_processed=2,
        gaps_fully_repaired=1,
        unrecoverable_gaps=[gap],
        errors=["network blip"],
        duration_seconds=1.5,
    )

    jsonable = report.to_jsonable()
    summary = report.summary()

    assert jsonable["inserted_rows"] == {"futures": 5}
    assert jsonable["gaps_processed"] == 2
    assert jsonable["unrecoverable_gaps"][0]["reason"] == "partial_fill"
    assert jsonable["unrecoverable_gaps"][0]["prev_close"] == "2024-01-01T16:00:00"
    assert "5" in summary
    assert "partial_fill" in summary


def test_repair_happy_path_single_kline_gap(db_with_kline_gap, mock_rest_client):
    missing_closes = pd.date_range("2024-01-01T20:00", periods=5, freq="4h")
    mock_rest_client.set_response(
        "/fapi/v1/klines",
        [[_kline_for_close(timestamp) for timestamp in missing_closes]],
    )

    report = repair_binance_gaps(
        db_path=str(db_with_kline_gap.db_path),
        tables=["futures"],
        rest_client=mock_rest_client,
    )

    assert report.gaps_processed == 1
    assert report.gaps_fully_repaired == 1
    assert report.inserted_rows == {"futures": 5}
    assert report.unrecoverable_gaps == []
    rows = db_with_kline_gap.conn.execute(
        "SELECT timestamp FROM futures WHERE symbol = 'SOLUSDT' ORDER BY timestamp"
    ).fetchall()
    assert len(rows) == 15


def test_repair_partial_fill_classified(db_with_kline_gap, mock_rest_client):
    missing_closes = pd.date_range("2024-01-01T20:00", periods=2, freq="4h")
    mock_rest_client.set_response(
        "/fapi/v1/klines",
        [[_kline_for_close(timestamp) for timestamp in missing_closes]],
    )

    report = repair_binance_gaps(
        db_path=str(db_with_kline_gap.db_path),
        tables=["futures"],
        rest_client=mock_rest_client,
    )

    assert report.gaps_processed == 1
    assert report.gaps_fully_repaired == 0
    assert report.inserted_rows == {"futures": 2}
    assert len(report.unrecoverable_gaps) == 1
    assert report.unrecoverable_gaps[0].reason == "partial_fill"
    assert report.unrecoverable_gaps[0].missing_count == 3


def test_repair_empty_response_full_partial_fill(db_with_kline_gap, mock_rest_client):
    mock_rest_client.set_response("/fapi/v1/klines", [[]])

    report = repair_binance_gaps(
        db_path=str(db_with_kline_gap.db_path),
        tables=["futures"],
        rest_client=mock_rest_client,
    )

    assert report.inserted_rows == {"futures": 0}
    assert len(report.unrecoverable_gaps) == 1
    assert report.unrecoverable_gaps[0].reason == "partial_fill"
    assert report.unrecoverable_gaps[0].missing_count == 5


def test_repair_network_error_classified(db_with_kline_gap):
    class RaisingClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args) -> None:
            return None

        async def get_json(self, url):
            raise BinanceRateLimitError("simulated 429 exhausted")

    report = repair_binance_gaps(
        db_path=str(db_with_kline_gap.db_path),
        tables=["futures"],
        rest_client=RaisingClient(),
    )

    assert report.inserted_rows == {"futures": 0}
    assert len(report.unrecoverable_gaps) == 1
    assert report.unrecoverable_gaps[0].reason == "network_error"
    assert any("simulated 429" in error for error in report.errors)


def test_repair_1000_prefix_probe(empty_db):
    pre = pd.date_range("2024-01-01T00:00", periods=3, freq="4h")
    post = pd.date_range("2024-01-02T00:00", periods=3, freq="4h")
    _insert_klines(empty_db.conn, "futures", "PEPEUSDT", "4h", list(pre) + list(post))

    class ProbingClient:
        def __init__(self):
            self.calls: list[str] = []
            self.candles = pd.date_range("2024-01-01T12:00", periods=3, freq="4h")

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args) -> None:
            return None

        async def get_json(self, url):
            self.calls.append(url)
            if "symbol=1000PEPEUSDT" in url:
                return [_kline_for_close(timestamp) for timestamp in self.candles]
            raise aiohttp.ClientResponseError(
                request_info=None,
                history=(),
                status=400,
                message="invalid symbol",
            )

    client = ProbingClient()

    report = repair_binance_gaps(
        db_path=str(empty_db.db_path),
        tables=["futures"],
        rest_client=client,
    )

    assert any("1000PEPEUSDT" in call for call in client.calls)
    assert report.inserted_rows == {"futures": 3}
    assert report.gaps_fully_repaired == 0
    assert len(report.unrecoverable_gaps) == 1
    assert report.unrecoverable_gaps[0].symbol == "PEPEUSDT"
    assert report.unrecoverable_gaps[0].missing_count == 3

    pepe_rows = empty_db.conn.execute(
        "SELECT COUNT(*) FROM futures WHERE symbol = 'PEPEUSDT'"
    ).fetchone()[0]
    prefixed_rows = empty_db.conn.execute(
        "SELECT COUNT(*) FROM futures WHERE symbol = '1000PEPEUSDT'"
    ).fetchone()[0]
    assert pepe_rows == 6
    assert prefixed_rows == 3


def test_repair_idempotent_second_run(db_with_kline_gap, mock_rest_client):
    missing_closes = pd.date_range("2024-01-01T20:00", periods=5, freq="4h")
    mock_rest_client.set_response(
        "/fapi/v1/klines",
        [[_kline_for_close(timestamp) for timestamp in missing_closes]],
    )

    first = repair_binance_gaps(
        db_path=str(db_with_kline_gap.db_path),
        tables=["futures"],
        rest_client=mock_rest_client,
    )
    second = repair_binance_gaps(
        db_path=str(db_with_kline_gap.db_path),
        tables=["futures"],
        rest_client=mock_rest_client,
    )

    assert first.inserted_rows["futures"] == 5
    assert second.gaps_processed == 0
    assert second.inserted_rows == {"futures": 0}


def test_repair_filters_by_table(db_with_kline_gap, mock_rest_client):
    pre = pd.date_range("2024-01-01T00:00", periods=4, freq="8h")
    post = pd.date_range("2024-01-03T08:00", periods=4, freq="8h")
    _insert_funding(db_with_kline_gap.conn, "BTCUSDT", list(pre) + list(post))
    missing_closes = pd.date_range("2024-01-01T20:00", periods=5, freq="4h")
    mock_rest_client.set_response(
        "/fapi/v1/klines",
        [[_kline_for_close(timestamp) for timestamp in missing_closes]],
    )

    report = repair_binance_gaps(
        db_path=str(db_with_kline_gap.db_path),
        tables=["futures"],
        rest_client=mock_rest_client,
    )

    assert report.gaps_processed == 1
    assert report.inserted_rows == {"futures": 5}
