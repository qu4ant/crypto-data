"""
CoinMarketCap API Client (Async)

Handles API communication with CoinMarketCap's historical listings endpoint.
Implements automatic retry logic for rate limits and server errors.
"""

import logging
import asyncio
from typing import Optional
import aiohttp

logger = logging.getLogger(__name__)


class CoinMarketCapClient:
    """
    Async client for CoinMarketCap API.

    Handles:
    - API communication with CoinMarketCap historical listings
    - Automatic retry on rate limits (429) and server errors (500, 503)
    - Configurable retry behavior and timeouts
    - Concurrency control with semaphore

    Example
    -------
    >>> async with CoinMarketCapClient(max_concurrent=5) as client:
    ...     data = await client.get_historical_listings(date='2024-01-01', limit=100)
    >>> # Returns list of coin dictionaries with ranking and market data
    """

    def __init__(
        self,
        api_base: Optional[str] = None,
        max_retries: int = 3,
        rate_limit_wait: int = 60,
        server_error_delay: int = 5,
        timeout: int = 10,
        max_concurrent: int = 10
    ):
        """
        Initialize CoinMarketCap API client.

        Parameters
        ----------
        api_base : str, optional
            API base URL. Defaults to CoinMarketCap's free internal API.
        max_retries : int
            Maximum number of retry attempts (default: 3)
        rate_limit_wait : int
            Wait time in seconds after 429 rate limit error (default: 60)
        server_error_delay : int
            Wait time in seconds after 500/503 server errors (default: 5)
        timeout : int
            Request timeout in seconds (default: 10)
        max_concurrent : int
            Maximum number of concurrent requests (default: 5)
        """
        self.api_base = api_base or 'https://api.coinmarketcap.com/data-api/v3'
        self.max_retries = max_retries
        self.rate_limit_wait = rate_limit_wait
        self.server_error_delay = server_error_delay
        self.timeout = timeout
        self.max_concurrent = max_concurrent

        self._session = None
        self._semaphore = asyncio.Semaphore(max_concurrent)

        logger.debug(f"Initialized CoinMarketCap client: base={self.api_base}, retries={max_retries}, concurrent={max_concurrent}")

    async def __aenter__(self):
        """Async context manager entry."""
        self._session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        if self._session:
            await self._session.close()

    async def get_historical_listings(self, date: str, limit: int) -> list:
        """
        Fetch historical cryptocurrency listings from CoinMarketCap.

        Uses the /cryptocurrency/listings/historical endpoint to get
        top N coins by market cap for a specific date.

        Parameters
        ----------
        date : str
            Date in YYYY-MM-DD format
        limit : int
            Number of coins to fetch (top N by market cap)

        Returns
        -------
        list
            List of coin dictionaries with ranking and market data.
            Each dict contains: symbol, cmcRank, quotes (with marketCap), tags, etc.

        Raises
        ------
        aiohttp.ClientError
            If all retries exhausted or non-retryable error occurs

        Example
        -------
        >>> async with CoinMarketCapClient() as client:
        ...     coins = await client.get_historical_listings('2024-01-01', 100)
        >>> # coins[0] = {'symbol': 'BTC', 'cmcRank': 1, 'quotes': [...], ...}
        """
        url = f"{self.api_base}/cryptocurrency/listings/historical"
        params = {
            'date': date,
            'limit': limit,
            'start': 1,
            'convertId': 2781,  # USD
            'sort': 'cmc_rank',
            'sort_dir': 'asc'
        }

        logger.debug(f"Fetching historical listings: date={date}, limit={limit}")

        response_data = await self._call_with_retry(url, params, self.timeout)

        if 'data' in response_data:
            logger.debug(f"Successfully fetched {len(response_data['data'])} coins")
            return response_data['data']
        else:
            logger.error(f"Unexpected API response format: {response_data}")
            return []

    async def _call_with_retry(self, url: str, params: dict, timeout: int) -> dict:
        """
        Make API call with automatic retry on rate limits and server errors.

        Retry behavior:
        - 429 (Rate Limit): Waits configured rate_limit_wait seconds before retry
        - 500/503 (Server Error): Waits configured server_error_delay seconds before retry
        - Other errors: Raises immediately

        Parameters
        ----------
        url : str
            API endpoint URL
        params : dict
            Query parameters
        timeout : int
            Request timeout in seconds

        Returns
        -------
        dict
            Parsed JSON response

        Raises
        ------
        aiohttp.ClientError
            If all retries exhausted or non-retryable error
        """
        if not self._session:
            raise RuntimeError("Client must be used as async context manager (async with)")

        async with self._semaphore:  # Limit concurrent requests
            for attempt in range(self.max_retries + 1):  # 0 = initial, 1-3 = retries
                try:
                    async with self._session.get(
                        url,
                        params=params,
                        timeout=aiohttp.ClientTimeout(total=timeout)
                    ) as response:
                        # Check for HTTP errors
                        if response.status == 429:
                            # Rate limit error - wait and retry
                            if attempt < self.max_retries:
                                logger.warning(
                                    f"Rate limit hit (429), waiting {self.rate_limit_wait}s before retry "
                                    f"(attempt {attempt + 1}/{self.max_retries})..."
                                )
                                await asyncio.sleep(self.rate_limit_wait)
                                continue
                            else:
                                logger.error(f"Rate limit error - all {self.max_retries} retries exhausted")
                                response.raise_for_status()

                        elif response.status in [500, 503]:
                            # Server errors - wait and retry
                            if attempt < self.max_retries:
                                logger.warning(
                                    f"Server error ({response.status}), waiting {self.server_error_delay}s before retry "
                                    f"(attempt {attempt + 1}/{self.max_retries})..."
                                )
                                await asyncio.sleep(self.server_error_delay)
                                continue
                            else:
                                logger.error(f"Server error - all {self.max_retries} retries exhausted")
                                response.raise_for_status()

                        # Other status codes
                        response.raise_for_status()

                        # Success - parse and return JSON
                        return await response.json()

                except aiohttp.ClientError as e:
                    # Network errors, timeouts, etc.
                    if attempt < self.max_retries:
                        logger.warning(f"Request failed, retrying: {e}")
                        await asyncio.sleep(self.server_error_delay)
                        continue
                    else:
                        logger.error(f"Request failed - all {self.max_retries} retries exhausted: {e}")
                        raise

            # Should never reach here, but just in case
            raise Exception("Unexpected error in retry logic")
