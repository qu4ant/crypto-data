"""Tests for the Binance public REST client."""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import patch

import aiohttp
import pytest

from crypto_data.clients.binance_rest import BinanceRateLimitError, BinanceRestClient


def test_spot_klines_url():
    url = BinanceRestClient.build_spot_klines_url(
        symbol="BTCUSDT",
        interval="4h",
        start_ms=1000,
        end_ms=2000,
        limit=500,
    )

    assert url.startswith("https://api.binance.com/api/v3/klines?")
    assert "symbol=BTCUSDT" in url
    assert "interval=4h" in url
    assert "startTime=1000" in url
    assert "endTime=2000" in url
    assert "limit=500" in url


def test_futures_klines_url():
    url = BinanceRestClient.build_futures_klines_url(
        symbol="SOLUSDT",
        interval="4h",
        start_ms=1000,
        end_ms=2000,
        limit=1500,
    )

    assert url.startswith("https://fapi.binance.com/fapi/v1/klines?")
    assert "symbol=SOLUSDT" in url
    assert "limit=1500" in url


def test_funding_rate_url():
    url = BinanceRestClient.build_funding_rate_url(
        symbol="BTCUSDT",
        start_ms=1000,
        end_ms=2000,
        limit=1000,
    )

    assert url.startswith("https://fapi.binance.com/fapi/v1/fundingRate?")
    assert "symbol=BTCUSDT" in url
    assert "limit=1000" in url


class _FakeResponse:
    def __init__(
        self,
        status: int = 200,
        json_body: Any = None,
        headers: dict | None = None,
    ) -> None:
        self.status = status
        self._json_body = json_body if json_body is not None else []
        self.headers = headers or {}
        self.request_info = None

    async def json(self):
        return self._json_body

    async def text(self):
        return ""


class _FakeContext:
    def __init__(self, response_or_error, session: _FakeSession) -> None:
        self._response_or_error = response_or_error
        self._session = session

    async def __aenter__(self):
        self._session.in_flight += 1
        self._session.max_in_flight = max(self._session.max_in_flight, self._session.in_flight)
        if isinstance(self._response_or_error, Exception):
            raise self._response_or_error
        return self._response_or_error

    async def __aexit__(self, *args):
        self._session.in_flight -= 1
        return


class _FakeSession:
    def __init__(self, responses) -> None:
        self._responses = list(responses)
        self.calls: list[str] = []
        self.in_flight = 0
        self.max_in_flight = 0

    def get(self, url, **kwargs):
        self.calls.append(url)
        return _FakeContext(self._responses.pop(0), self)

    async def close(self):
        return None


@pytest.mark.asyncio
async def test_get_json_success_returns_body():
    client = BinanceRestClient(max_concurrent=2)
    fake = _FakeSession([_FakeResponse(200, json_body=[1, 2, 3])])
    client._session = fake

    body = await client.get_json("https://example/x")

    assert body == [1, 2, 3]
    assert fake.calls == ["https://example/x"]


@pytest.mark.asyncio
async def test_semaphore_caps_concurrency():
    client = BinanceRestClient(max_concurrent=2)
    fake = _FakeSession([_FakeResponse(200, json_body=[]) for _ in range(5)])
    client._session = fake

    await asyncio.gather(*(client.get_json(f"https://example/{i}") for i in range(5)))

    assert fake.max_in_flight <= 2


@pytest.mark.asyncio
async def test_429_retries_after_retry_after():
    sleeps: list[float] = []

    async def fake_sleep(seconds):
        sleeps.append(seconds)

    fake = _FakeSession(
        [
            _FakeResponse(429, headers={"Retry-After": "2"}),
            _FakeResponse(200, json_body=[42]),
        ]
    )
    client = BinanceRestClient(max_concurrent=1)
    client._session = fake

    with patch("asyncio.sleep", new=fake_sleep):
        body = await client.get_json("https://example/x")

    assert body == [42]
    assert sleeps == [2.0]


@pytest.mark.asyncio
async def test_429_exhausted_raises_rate_limit_error():
    async def fake_sleep(_):
        return None

    fake = _FakeSession([_FakeResponse(429, headers={"Retry-After": "1"}) for _ in range(6)])
    client = BinanceRestClient(max_concurrent=1)
    client._session = fake

    with patch("asyncio.sleep", new=fake_sleep), pytest.raises(BinanceRateLimitError):
        await client.get_json("https://example/x")


@pytest.mark.asyncio
async def test_5xx_retries_with_backoff_and_succeeds():
    sleeps: list[float] = []

    async def fake_sleep(seconds):
        sleeps.append(seconds)

    fake = _FakeSession(
        [
            _FakeResponse(503),
            _FakeResponse(503),
            _FakeResponse(200, json_body=[7]),
        ]
    )
    client = BinanceRestClient(max_concurrent=1)
    client._session = fake

    with patch("asyncio.sleep", new=fake_sleep):
        body = await client.get_json("https://example/x")

    assert body == [7]
    assert sleeps == [1.0, 2.0]


@pytest.mark.asyncio
async def test_5xx_exhausts_retries_and_raises():
    async def fake_sleep(_):
        return None

    fake = _FakeSession([_FakeResponse(500) for _ in range(4)])
    client = BinanceRestClient(max_concurrent=1)
    client._session = fake

    with patch("asyncio.sleep", new=fake_sleep), pytest.raises(aiohttp.ClientResponseError):
        await client.get_json("https://example/x")


@pytest.mark.asyncio
async def test_aiohttp_client_error_retries_with_backoff_and_succeeds():
    sleeps: list[float] = []

    async def fake_sleep(seconds):
        sleeps.append(seconds)

    fake = _FakeSession(
        [
            aiohttp.ClientConnectionError("disconnect"),
            _FakeResponse(200, json_body=[9]),
        ]
    )
    client = BinanceRestClient(max_concurrent=1)
    client._session = fake

    with patch("asyncio.sleep", new=fake_sleep):
        body = await client.get_json("https://example/x")

    assert body == [9]
    assert sleeps == [1.0]


@pytest.mark.asyncio
async def test_418_retries_once_then_raises():
    async def fake_sleep(_):
        return None

    fake = _FakeSession(
        [
            _FakeResponse(418, headers={"Retry-After": "60"}),
            _FakeResponse(418, headers={"Retry-After": "60"}),
        ]
    )
    client = BinanceRestClient(max_concurrent=1)
    client._session = fake

    with patch("asyncio.sleep", new=fake_sleep), pytest.raises(BinanceRateLimitError):
        await client.get_json("https://example/x")
    assert len(fake.calls) == 2


@pytest.mark.asyncio
async def test_418_then_success():
    async def fake_sleep(_):
        return None

    fake = _FakeSession(
        [
            _FakeResponse(418, headers={"Retry-After": "5"}),
            _FakeResponse(200, json_body=[9]),
        ]
    )
    client = BinanceRestClient(max_concurrent=1)
    client._session = fake

    with patch("asyncio.sleep", new=fake_sleep):
        body = await client.get_json("https://example/x")

    assert body == [9]


@pytest.mark.asyncio
async def test_400_invalid_symbol_no_retry():
    fake = _FakeSession([_FakeResponse(400)])
    client = BinanceRestClient(max_concurrent=1)
    client._session = fake

    with pytest.raises(aiohttp.ClientResponseError) as exc_info:
        await client.get_json("https://example/x")

    assert exc_info.value.status == 400
    assert len(fake.calls) == 1
