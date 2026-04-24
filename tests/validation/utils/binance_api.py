"""
Binance REST API client for data validation only.
Uses public endpoints, no authentication required.
"""

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

import aiohttp


@dataclass
class BinanceKline:
    """Binance kline (candlestick) data"""

    open_time: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    close_time: datetime
    quote_volume: float
    trades_count: int
    taker_buy_base_volume: float
    taker_buy_quote_volume: float


def datetime_to_utc_ms(dt: datetime) -> int:
    """
    Convert datetime to UTC milliseconds timestamp.

    If the datetime is naive (no timezone info), it's assumed to be UTC.
    This matches how Binance data is stored in the database.
    """
    if dt.tzinfo is None:
        # Naive datetime - assume UTC
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def interval_to_milliseconds(interval: str) -> int:
    """
    Convert Binance interval string to milliseconds.

    Supports intervals used by this package.
    """
    if not interval or len(interval) < 2:
        raise ValueError(f"Invalid interval: {interval}")

    try:
        value = int(interval[:-1])
    except ValueError as exc:
        raise ValueError(f"Invalid interval: {interval}") from exc

    unit = interval[-1]
    if unit == "m":
        return value * 60_000
    if unit == "h":
        return value * 3_600_000
    if unit == "d":
        return value * 86_400_000
    if unit == "w":
        return value * 7 * 86_400_000
    if unit == "M":
        # Binance month interval is variable length. Validation suite currently
        # targets minute/hour/day data.
        raise ValueError("Month interval is not supported by validation client")

    raise ValueError(f"Unsupported interval unit in: {interval}")


def stored_close_key_to_open_time_ms(timestamp: datetime, interval: str) -> int:
    """
    Convert DB timestamp key to Binance API openTime milliseconds.

    The ingestion pipeline stores candle keys using a normalized close-time key.
    Binance APIs query candles by openTime, so validation must translate.
    """
    close_key_ms = datetime_to_utc_ms(timestamp)
    return close_key_ms - interval_to_milliseconds(interval)


class BinanceAPIError(Exception):
    """Raised when Binance API returns an error"""

    def __init__(self, status_code: int, message: str):
        self.status_code = status_code
        self.message = message
        super().__init__(f"Binance API error {status_code}: {message}")


class BinanceValidationClient:
    """Async client for Binance REST API (validation only)"""

    SPOT_BASE_URL = "https://api.binance.com"
    FUTURES_BASE_URL = "https://fapi.binance.com"

    # 1000-prefix symbols for futures
    KNOWN_1000_PREFIX = {"PEPE", "SHIB", "BONK", "FLOKI", "LUNC", "XEC"}

    def __init__(self, rate_limit_delay: float = 0.1, max_retries: int = 3):
        self.rate_limit_delay = rate_limit_delay
        self.max_retries = max_retries
        self._session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self):
        self._session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, *args):
        if self._session:
            await self._session.close()

    async def _request_with_retry(self, url: str, params: dict) -> list:
        """Make request with retry logic for rate limits"""
        for attempt in range(self.max_retries):
            try:
                async with self._session.get(url, params=params) as response:
                    if response.status == 200:
                        return await response.json()
                    elif response.status == 429:
                        # Rate limited - wait and retry
                        wait_time = 60 if attempt == 0 else 60 * (attempt + 1)
                        await asyncio.sleep(wait_time)
                        continue
                    elif response.status in (500, 503):
                        # Server error - wait and retry
                        await asyncio.sleep(5 * (attempt + 1))
                        continue
                    elif response.status == 400:
                        # Bad request - symbol may be delisted
                        text = await response.text()
                        raise BinanceAPIError(response.status, text)
                    else:
                        text = await response.text()
                        raise BinanceAPIError(response.status, text)
            except aiohttp.ClientError as e:
                if attempt == self.max_retries - 1:
                    raise
                await asyncio.sleep(2 * (attempt + 1))

        raise BinanceAPIError(429, "Max retries exceeded due to rate limiting")

    async def get_spot_kline(
        self, symbol: str, interval: str, timestamp: datetime
    ) -> Optional[BinanceKline]:
        """Fetch single spot kline for exact timestamp"""
        await asyncio.sleep(self.rate_limit_delay)

        # Convert DB close-time key to API openTime in milliseconds.
        timestamp_ms = stored_close_key_to_open_time_ms(timestamp, interval)
        url = f"{self.SPOT_BASE_URL}/api/v3/klines"
        params = {
            "symbol": symbol,
            "interval": interval,
            "startTime": timestamp_ms,
            "endTime": timestamp_ms + 1,  # Single candle
            "limit": 1,
        }

        try:
            data = await self._request_with_retry(url, params)
            if data and len(data) > 0:
                return self._parse_kline(data[0])
            return None
        except BinanceAPIError:
            return None

    async def get_futures_kline(
        self, symbol: str, interval: str, timestamp: datetime
    ) -> Optional[BinanceKline]:
        """Fetch single futures kline for exact timestamp"""
        await asyncio.sleep(self.rate_limit_delay)

        # Handle 1000-prefix mapping
        api_symbol = self._get_futures_api_symbol(symbol)

        # Convert DB close-time key to API openTime in milliseconds.
        timestamp_ms = stored_close_key_to_open_time_ms(timestamp, interval)
        url = f"{self.FUTURES_BASE_URL}/fapi/v1/klines"
        params = {
            "symbol": api_symbol,
            "interval": interval,
            "startTime": timestamp_ms,
            "endTime": timestamp_ms + 1,
            "limit": 1,
        }

        try:
            data = await self._request_with_retry(url, params)
            if data and len(data) > 0:
                return self._parse_kline(data[0])
            return None
        except BinanceAPIError:
            # Try with 1000-prefix if not already tried
            if not api_symbol.startswith("1000"):
                base_symbol = symbol.replace("USDT", "")
                if base_symbol not in self.KNOWN_1000_PREFIX:
                    params["symbol"] = f"1000{symbol}"
                    try:
                        data = await self._request_with_retry(url, params)
                        if data and len(data) > 0:
                            return self._parse_kline(data[0])
                    except BinanceAPIError:
                        pass
            return None

    def _get_futures_api_symbol(self, symbol: str) -> str:
        """Convert DB symbol to Binance Futures API symbol (handle 1000-prefix)"""
        base_symbol = symbol.replace("USDT", "")
        if base_symbol in self.KNOWN_1000_PREFIX:
            return f"1000{symbol}"
        return symbol

    def _parse_kline(self, data: list) -> BinanceKline:
        """Parse Binance kline response to dataclass"""
        # Parse timestamps as naive datetime (represents UTC)
        return BinanceKline(
            open_time=datetime.utcfromtimestamp(data[0] / 1000),
            open=float(data[1]),
            high=float(data[2]),
            low=float(data[3]),
            close=float(data[4]),
            volume=float(data[5]),
            close_time=datetime.utcfromtimestamp(data[6] / 1000),
            quote_volume=float(data[7]),
            trades_count=int(data[8]),
            taker_buy_base_volume=float(data[9]),
            taker_buy_quote_volume=float(data[10]),
        )
