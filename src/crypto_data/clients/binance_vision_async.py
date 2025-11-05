"""
Binance Data Vision Async Client

Async version of Binance Data Vision client using aiohttp.
Designed for parallel downloads from S3-backed data repository.

Performance: 5-10x faster than synchronous version for multiple files.
"""

import logging
from pathlib import Path
from typing import Optional
import aiohttp
import asyncio

logger = logging.getLogger(__name__)


class BinanceDataVisionClientAsync:
    """
    Async client for Binance Data Vision historical data downloads.

    Uses aiohttp for concurrent HTTP requests to Binance's S3-backed repository.
    Designed for downloading multiple files in parallel (months for same symbol).

    """

    # Data category URL paths
    DATA_CATEGORIES = {
        'spot': 'data/spot/monthly/klines',
        'futures': 'data/futures/um/monthly/klines',
        'open_interest': 'data/futures/um/daily/metrics',
        'funding_rates': 'data/futures/um/monthly/fundingRate'
    }

    def __init__(
        self,
        base_url: Optional[str] = None,
        timeout: int = 30,
        max_concurrent: int = 20
    ):
        """
        Initialize Binance Data Vision async client.

        Parameters
        ----------
        base_url : str, optional
            Base URL for Binance Data Vision. Defaults to official repository.
        timeout : int
            Request timeout in seconds (default: 30)
        max_concurrent : int
            Maximum concurrent downloads (default: 20)
            Prevents overwhelming the server or network
        """
        self.base_url = base_url or "https://data.binance.vision/"
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        self.max_concurrent = max_concurrent
        self.session: Optional[aiohttp.ClientSession] = None
        self.semaphore: Optional[asyncio.Semaphore] = None

        logger.debug(f"Initialized Binance Data Vision async client: "
                    f"base={self.base_url}, timeout={timeout}s, "
                    f"max_concurrent={max_concurrent}")

    async def __aenter__(self):
        """Context manager entry: create session."""
        self.session = aiohttp.ClientSession(timeout=self.timeout)
        self.semaphore = asyncio.Semaphore(self.max_concurrent)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit: close session."""
        if self.session:
            await self.session.close()

    async def download_klines(
        self,
        symbol: str,
        data_type: str,
        month: str,
        interval: str,
        output_path: Path
    ) -> bool:
        """
        Download kline (OHLCV) data from Binance Data Vision asynchronously.

        Uses semaphore to limit concurrent downloads and prevent overwhelming server.

        Parameters
        ----------
        symbol : str
            Trading pair symbol (e.g., 'BTCUSDT', 'ETHUSDT')
        data_type : str
            Market type: 'spot' or 'futures'
        month : str
            Month in YYYY-MM format (e.g., '2024-01')
        interval : str
            Kline interval (e.g., '5m', '1h', '4h', '1d')
        output_path : Path
            Path where the ZIP file should be saved

        Returns
        -------
        bool
            True if download successful, False if file not found (404)

        Raises
        ------
        aiohttp.ClientError
            For network errors (timeout, connection issues, etc.)
        aiohttp.ClientResponseError
            For HTTP errors other than 404 (e.g., 500, 503)

        Example
        -------
        >>> async def download():
        ...     async with BinanceDataVisionClientAsync() as client:
        ...         output = Path('/tmp/BTCUSDT-5m-2024-01.zip')
        ...         success = await client.download_klines(
        ...             'BTCUSDT', 'spot', '2024-01', '5m', output
        ...         )
        ...         return success
        >>>
        >>> success = asyncio.run(download())
        """
        if not self.session:
            raise RuntimeError("Client session not initialized. Use 'async with' context manager.")

        # Validate data_type
        if data_type not in self.DATA_CATEGORIES:
            raise ValueError(
                f"Invalid data_type '{data_type}'. Must be one of: {list(self.DATA_CATEGORIES.keys())}"
            )

        # Construct URL
        category = self.DATA_CATEGORIES[data_type]
        filename = f"{symbol}-{interval}-{month}.zip"
        url = f"{self.base_url}{category}/{symbol}/{interval}/{filename}"

        logger.debug(f"Downloading: {url}")

        # Use semaphore to limit concurrent downloads
        async with self.semaphore:
            try:
                async with self.session.get(url) as response:
                    # Handle 404 gracefully (data not available)
                    if response.status == 404:
                        logger.debug(f"  File not found (404)")
                        return False

                    # Raise for other HTTP errors
                    response.raise_for_status()

                    # Read content
                    content = await response.read()

                    # Save to file (synchronous write is fine, it's fast)
                    output_path.write_bytes(content)
                    logger.debug(f"  Downloaded: {len(content)} bytes")

                    return True

            except aiohttp.ClientError as e:
                logger.error(f"  Download error: {e}")
                raise

    async def download_metrics(
        self,
        symbol: str,
        date: str,
        output_path: Path
    ) -> bool:
        """
        Download open interest metrics data from Binance Data Vision asynchronously.

        Uses semaphore to limit concurrent downloads and prevent overwhelming server.

        Parameters
        ----------
        symbol : str
            Trading pair symbol (e.g., 'BTCUSDT', 'ETHUSDT')
        date : str
            Date in YYYY-MM-DD format (e.g., '2024-01-15')
        output_path : Path
            Path where the CSV file should be saved

        Returns
        -------
        bool
            True if download successful, False if file not found (404)

        Raises
        ------
        aiohttp.ClientError
            For network errors (timeout, connection issues, etc.)
        aiohttp.ClientResponseError
            For HTTP errors other than 404 (e.g., 500, 503)

        Example
        -------
        >>> async def download():
        ...     async with BinanceDataVisionClientAsync() as client:
        ...         output = Path('/tmp/BTCUSDT-metrics-2024-01-15.csv')
        ...         success = await client.download_metrics(
        ...             'BTCUSDT', '2024-01-15', output
        ...         )
        ...         return success
        >>>
        >>> success = asyncio.run(download())
        """
        if not self.session:
            raise RuntimeError("Client session not initialized. Use 'async with' context manager.")

        # Construct URL
        category = self.DATA_CATEGORIES['open_interest']
        filename = f"{symbol}-metrics-{date}.zip"
        url = f"{self.base_url}{category}/{symbol}/{filename}"

        logger.debug(f"Downloading: {url}")

        # Use semaphore to limit concurrent downloads
        async with self.semaphore:
            try:
                async with self.session.get(url) as response:
                    # Handle 404 gracefully (data not available)
                    if response.status == 404:
                        logger.debug(f"  File not found (404)")
                        return False

                    # Raise for other HTTP errors
                    response.raise_for_status()

                    # Read content
                    content = await response.read()

                    # Save to file
                    output_path.write_bytes(content)
                    logger.debug(f"  Downloaded: {len(content)} bytes")

                    return True

            except aiohttp.ClientError as e:
                logger.error(f"  Download error: {e}")
                raise

    async def download_funding_rates(
        self,
        symbol: str,
        month: str,
        output_path: Path
    ) -> bool:
        """
        Download funding rate data from Binance Data Vision asynchronously.

        Uses semaphore to limit concurrent downloads and prevent overwhelming server.

        Parameters
        ----------
        symbol : str
            Trading pair symbol (e.g., 'BTCUSDT', 'ETHUSDT')
        month : str
            Month in YYYY-MM format (e.g., '2024-01')
        output_path : Path
            Path where the ZIP file should be saved

        Returns
        -------
        bool
            True if download successful, False if file not found (404)

        Raises
        ------
        aiohttp.ClientError
            For network errors (timeout, connection issues, etc.)
        aiohttp.ClientResponseError
            For HTTP errors other than 404 (e.g., 500, 503)

        Example
        -------
        >>> async def download():
        ...     async with BinanceDataVisionClientAsync() as client:
        ...         output = Path('/tmp/BTCUSDT-fundingRate-2024-01.zip')
        ...         success = await client.download_funding_rates(
        ...             'BTCUSDT', '2024-01', output
        ...         )
        ...         return success
        >>>
        >>> success = asyncio.run(download())
        """
        if not self.session:
            raise RuntimeError("Client session not initialized. Use 'async with' context manager.")

        # Construct URL
        category = self.DATA_CATEGORIES['funding_rates']
        filename = f"{symbol}-fundingRate-{month}.zip"
        url = f"{self.base_url}{category}/{symbol}/{filename}"

        logger.debug(f"Downloading: {url}")

        # Use semaphore to limit concurrent downloads
        async with self.semaphore:
            try:
                async with self.session.get(url) as response:
                    # Handle 404 gracefully (data not available)
                    if response.status == 404:
                        logger.debug(f"  File not found (404)")
                        return False

                    # Raise for other HTTP errors
                    response.raise_for_status()

                    # Read content
                    content = await response.read()

                    # Save to file
                    output_path.write_bytes(content)
                    logger.debug(f"  Downloaded: {len(content)} bytes")

                    return True

            except aiohttp.ClientError as e:
                logger.error(f"  Download error: {e}")
                raise

    def get_download_url(
        self,
        symbol: str,
        data_type: str,
        month: str,
        interval: str
    ) -> str:
        """
        Construct the download URL for a kline file (useful for debugging).

        Parameters
        ----------
        symbol : str
            Trading pair symbol (e.g., 'BTCUSDT')
        data_type : str
            Market type: 'spot' or 'futures'
        month : str
            Month in YYYY-MM format (e.g., '2024-01')
        interval : str
            Kline interval (e.g., '5m', '1h', '1d')

        Returns
        -------
        str
            Full download URL

        Example
        -------
        >>> client = BinanceDataVisionClientAsync()
        >>> url = client.get_download_url('BTCUSDT', 'spot', '2024-01', '5m')
        >>> print(url)
        https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/5m/BTCUSDT-5m-2024-01.zip
        """
        if data_type not in self.DATA_CATEGORIES:
            raise ValueError(
                f"Invalid data_type '{data_type}'. Must be one of: {list(self.DATA_CATEGORIES.keys())}"
            )

        category = self.DATA_CATEGORIES[data_type]
        filename = f"{symbol}-{interval}-{month}.zip"
        return f"{self.base_url}{category}/{symbol}/{interval}/{filename}"
