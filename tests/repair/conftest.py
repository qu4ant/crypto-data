"""Shared fixtures for Binance repair tests."""

from __future__ import annotations

import pandas as pd
import pytest

from crypto_data.database import CryptoDatabase


def _insert_klines(
    conn,
    table: str,
    symbol: str,
    interval: str,
    timestamps: list[pd.Timestamp],
) -> None:
    rows = [
        (
            "binance",
            symbol,
            interval,
            timestamp.to_pydatetime() if hasattr(timestamp, "to_pydatetime") else timestamp,
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
    conn.executemany(
        f"INSERT INTO {table} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        rows,
    )


def _insert_funding(conn, symbol: str, timestamps: list[pd.Timestamp]) -> None:
    rows = [
        (
            "binance",
            symbol,
            timestamp.to_pydatetime() if hasattr(timestamp, "to_pydatetime") else timestamp,
            0.0001,
        )
        for timestamp in timestamps
    ]
    conn.executemany("INSERT INTO funding_rates VALUES (?, ?, ?, ?)", rows)


class MockRestClient:
    """Drop-in async client returning scripted payloads keyed by URL substring."""

    def __init__(self) -> None:
        self._responses: dict[str, list] = {}
        self.calls: list[str] = []

    def set_response(self, url_substring: str, payloads: list) -> None:
        self._responses[url_substring] = list(payloads)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args) -> None:
        return None

    async def get_json(self, url: str):
        self.calls.append(url)
        for substring, payloads in self._responses.items():
            if substring in url and payloads:
                return payloads.pop(0)
        return []


@pytest.fixture
def empty_db(tmp_path):
    db = CryptoDatabase(str(tmp_path / "test.db"))
    try:
        yield db
    finally:
        db.close()


@pytest.fixture
def db_with_kline_gap(empty_db):
    pre = pd.date_range("2024-01-01T00:00", periods=5, freq="4h")
    post = pd.date_range("2024-01-02T16:00", periods=5, freq="4h")
    _insert_klines(empty_db.conn, "futures", "SOLUSDT", "4h", list(pre) + list(post))
    return empty_db


@pytest.fixture
def db_with_funding_gap(empty_db):
    pre = pd.date_range("2024-01-01T00:00", periods=4, freq="8h")
    post = pd.date_range("2024-01-03T08:00", periods=4, freq="8h")
    _insert_funding(empty_db.conn, "BTCUSDT", list(pre) + list(post))
    return empty_db


@pytest.fixture
def mock_rest_client():
    return MockRestClient()
