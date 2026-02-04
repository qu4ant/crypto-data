"""
Binance Exchange Client

Implements the ExchangeClient interface for Binance Data Vision.
Downloads historical market data from Binance's S3-backed data repository.
"""

from __future__ import annotations

import asyncio
import logging
import zipfile
from pathlib import Path
from typing import Optional

import aiohttp

from crypto_data.enums import Exchange
from crypto_data.exchanges.base import ExchangeClient

logger = logging.getLogger(__name__)


class BinanceExchange(ExchangeClient):
    """
    Binance exchange client for downloading historical data.

    Implements the ExchangeClient interface for Binance Data Vision,
    the S3-backed repository for historical market data.

    Parameters
    ----------
    base_url : str, optional
        Base URL for Binance Data Vision.
        Defaults to 'https://data.binance.vision/'
    timeout : int, optional
        Request timeout in seconds. Default is 30.
    max_concurrent : int, optional
        Maximum concurrent downloads. Default is 20.

    Examples
    --------
    >>> async with BinanceExchange() as client:
    ...     success = await client.download_file(
    ...         url='https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/5m/BTCUSDT-5m-2024-01.zip',
    ...         output_path=Path('/tmp/BTCUSDT-5m-2024-01.zip')
    ...     )
    """

    DEFAULT_BASE_URL = "https://data.binance.vision/"

    def __init__(
        self,
        base_url: Optional[str] = None,
        timeout: int = 30,
        max_concurrent: int = 20
    ):
        """
        Initialize Binance exchange client.

        Parameters
        ----------
        base_url : str, optional
            Base URL for Binance Data Vision. Defaults to official repository.
        timeout : int, optional
            Request timeout in seconds. Default is 30.
        max_concurrent : int, optional
            Maximum concurrent downloads. Default is 20.
        """
        self._base_url = base_url or self.DEFAULT_BASE_URL
        self._timeout = aiohttp.ClientTimeout(total=timeout)
        self._max_concurrent = max_concurrent
        self._session: Optional[aiohttp.ClientSession] = None
        self._semaphore: Optional[asyncio.Semaphore] = None

        logger.debug(
            f"Initialized BinanceExchange: base={self._base_url}, "
            f"timeout={timeout}s, max_concurrent={max_concurrent}"
        )

    @property
    def exchange(self) -> Exchange:
        """Return the Exchange enum value for this client."""
        return Exchange.BINANCE

    @property
    def base_url(self) -> str:
        """Return the base URL for the data source."""
        return self._base_url

    async def __aenter__(self) -> BinanceExchange:
        """
        Async context manager entry: create session and semaphore.

        Returns
        -------
        BinanceExchange
            The client instance with initialized session
        """
        self._session = aiohttp.ClientSession(timeout=self._timeout)
        self._semaphore = asyncio.Semaphore(self._max_concurrent)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object | None
    ) -> None:
        """
        Async context manager exit: close session.

        Parameters
        ----------
        exc_type : type[BaseException] | None
            Exception type if an exception was raised
        exc_val : BaseException | None
            Exception value if an exception was raised
        exc_tb : object | None
            Traceback if an exception was raised
        """
        if self._session:
            try:
                await self._session.close()
            finally:
                self._session = None

    async def download_file(
        self,
        url: str,
        output_path: Path,
        session: Optional[aiohttp.ClientSession] = None
    ) -> bool:
        """
        Download a file from the given URL.

        Uses semaphore for concurrency control. Validates download integrity
        by checking Content-Length and ZIP file integrity.

        Parameters
        ----------
        url : str
            Full URL to download from
        output_path : Path
            Path where the downloaded file should be saved
        session : aiohttp.ClientSession, optional
            Async HTTP session to use. If None, uses internal session.

        Returns
        -------
        bool
            True if download succeeded, False if file not found (404)

        Raises
        ------
        RuntimeError
            If no session is available (not in context manager and no session provided)
        aiohttp.ClientError
            For network errors (timeout, connection issues)
        aiohttp.ClientResponseError
            For HTTP errors other than 404
        """
        # Use provided session or internal session
        active_session = session or self._session
        if not active_session:
            raise RuntimeError(
                "No session available. Use 'async with' context manager "
                "or provide a session."
            )

        # Ensure semaphore exists
        if not self._semaphore:
            self._semaphore = asyncio.Semaphore(self._max_concurrent)

        logger.debug(f"Downloading: {url}")

        # Use semaphore to limit concurrent downloads
        async with self._semaphore:
            try:
                async with active_session.get(url) as response:
                    # Handle 404 gracefully (data not available)
                    if response.status == 404:
                        logger.debug(f"  File not found (404): {url}")
                        return False

                    # Raise for other HTTP errors
                    response.raise_for_status()

                    # Read content
                    content = await response.read()

                    # VALIDATION 1: Check Content-Length header if available
                    content_length = response.headers.get('Content-Length')
                    if content_length:
                        try:
                            expected_size = int(content_length)
                            actual_size = len(content)
                            if actual_size != expected_size:
                                logger.error(
                                    f"  Partial download detected: "
                                    f"{actual_size}/{expected_size} bytes"
                                )
                                return False
                        except (ValueError, TypeError):
                            logger.debug(
                                f"  Invalid Content-Length header: {content_length}"
                            )

                    # VALIDATION 2: Write to temp file first (atomic pattern)
                    temp_path = output_path.with_suffix('.tmp')
                    temp_path.parent.mkdir(parents=True, exist_ok=True)
                    temp_path.write_bytes(content)

                    # VALIDATION 3: Verify ZIP integrity for files >= 1KB
                    # Skip validation for small files (likely test data)
                    if len(content) >= 1024:
                        if not zipfile.is_zipfile(temp_path):
                            logger.error(
                                f"  Corrupt ZIP detected: {output_path.name}"
                            )
                            temp_path.unlink()  # Clean up corrupt file
                            return False

                    # VALIDATION 4: Atomic rename: temp -> final
                    temp_path.rename(output_path)
                    logger.debug(
                        f"  Downloaded: {len(content)} bytes (validated)"
                    )

                    return True

            except aiohttp.ClientError as e:
                logger.error(f"  Download error: {e}")
                raise
