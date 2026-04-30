"""Async HTTP client for Binance public REST endpoints."""

from __future__ import annotations

import asyncio
import logging
from typing import Any
from urllib.parse import urlencode

import aiohttp

logger = logging.getLogger(__name__)

SPOT_BASE_URL = "https://api.binance.com"
FUTURES_BASE_URL = "https://fapi.binance.com"


class BinanceRateLimitError(RuntimeError):
    """Raised when Binance rate-limit responses are exhausted."""


class _Retryable5xx(Exception):
    """Internal marker for retryable server responses."""

    def __init__(self, status: int) -> None:
        self.status = status


class BinanceRestClient:
    """Small async transport client for Binance public REST APIs."""

    MAX_429_RETRIES = 5
    MAX_5XX_RETRIES = 3
    MAX_418_RETRIES = 1
    BACKOFF_BASE = 1.0

    def __init__(self, max_concurrent: int = 5) -> None:
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._session: aiohttp.ClientSession | None = None

    async def __aenter__(self) -> BinanceRestClient:
        self._session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._session is not None:
            await self._session.close()
            self._session = None

    @staticmethod
    def build_spot_klines_url(
        symbol: str,
        interval: str,
        start_ms: int,
        end_ms: int,
        limit: int,
    ) -> str:
        params = urlencode(
            {
                "symbol": symbol,
                "interval": interval,
                "startTime": start_ms,
                "endTime": end_ms,
                "limit": limit,
            }
        )
        return f"{SPOT_BASE_URL}/api/v3/klines?{params}"

    @staticmethod
    def build_futures_klines_url(
        symbol: str,
        interval: str,
        start_ms: int,
        end_ms: int,
        limit: int,
    ) -> str:
        params = urlencode(
            {
                "symbol": symbol,
                "interval": interval,
                "startTime": start_ms,
                "endTime": end_ms,
                "limit": limit,
            }
        )
        return f"{FUTURES_BASE_URL}/fapi/v1/klines?{params}"

    @staticmethod
    def build_funding_rate_url(symbol: str, start_ms: int, end_ms: int, limit: int) -> str:
        params = urlencode(
            {
                "symbol": symbol,
                "startTime": start_ms,
                "endTime": end_ms,
                "limit": limit,
            }
        )
        return f"{FUTURES_BASE_URL}/fapi/v1/fundingRate?{params}"

    async def get_json(self, url: str) -> Any:
        """GET a URL and return JSON, with Binance-specific retry behavior."""
        if self._session is None:
            raise RuntimeError("BinanceRestClient must be used as an async context manager")

        attempt_429 = 0
        attempt_418 = 0
        attempt_5xx = 0

        while True:
            try:
                async with self._semaphore, self._session.get(url) as resp:
                    status = resp.status
                    if status == 200:
                        return await resp.json()

                    if status == 429:
                        attempt_429 += 1
                        if attempt_429 > self.MAX_429_RETRIES:
                            raise BinanceRateLimitError(
                                f"429 received {attempt_429} times for {url}"
                            )
                        retry_after = float(resp.headers.get("Retry-After", "1"))
                        logger.warning(
                            "429 from Binance; sleeping %.1fs (attempt %d)",
                            retry_after,
                            attempt_429,
                        )
                        await asyncio.sleep(retry_after)
                        continue

                    if status == 418:
                        attempt_418 += 1
                        if attempt_418 > self.MAX_418_RETRIES:
                            raise BinanceRateLimitError(
                                f"418 received {attempt_418} times for {url}"
                            )
                        retry_after = float(resp.headers.get("Retry-After", "60"))
                        logger.warning(
                            "418 from Binance; sleeping %.1fs (attempt %d)",
                            retry_after,
                            attempt_418,
                        )
                        await asyncio.sleep(retry_after)
                        continue

                    if 500 <= status < 600:
                        raise _Retryable5xx(status)

                    text = await resp.text()
                    raise aiohttp.ClientResponseError(
                        request_info=getattr(resp, "request_info", None),
                        history=(),
                        status=status,
                        message=text,
                    )
            except aiohttp.ClientResponseError:
                raise
            except (_Retryable5xx, aiohttp.ClientError) as exc:
                attempt_5xx += 1
                if attempt_5xx > self.MAX_5XX_RETRIES:
                    if isinstance(exc, _Retryable5xx):
                        raise aiohttp.ClientResponseError(
                            request_info=None,
                            history=(),
                            status=exc.status,
                            message=f"5xx exhausted on {url}",
                        ) from exc
                    raise

                sleep_seconds = self.BACKOFF_BASE * (2 ** (attempt_5xx - 1))
                logger.warning(
                    "Transient Binance REST error %r on %s; backoff %.1fs (attempt %d)",
                    exc,
                    url,
                    sleep_seconds,
                    attempt_5xx,
                )
                await asyncio.sleep(sleep_seconds)
