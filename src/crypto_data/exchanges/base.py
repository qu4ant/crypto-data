"""
Base Exchange Abstraction

Defines the abstract base class for exchange clients.
Each exchange (Binance, Bybit, Kraken, etc.) implements this interface
to provide consistent download functionality.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import TYPE_CHECKING

from crypto_data.enums import Exchange

if TYPE_CHECKING:
    import aiohttp


class ExchangeClient(ABC):
    """
    Abstract base class for exchange clients.

    Each exchange client implements this interface to provide consistent
    download functionality across different data sources.

    Subclasses must implement:
    - exchange: The Exchange enum value
    - base_url: Base URL for the data source
    - download_file(): Async method to download a file
    - __aenter__(): Async context manager entry
    - __aexit__(): Async context manager exit

    Examples
    --------
    Concrete exchange clients inherit from this class:

    >>> class BinanceClient(ExchangeClient):
    ...     @property
    ...     def exchange(self) -> Exchange:
    ...         return Exchange.BINANCE
    ...     # ... implement other abstract methods
    """

    @property
    @abstractmethod
    def exchange(self) -> Exchange:
        """
        Return the Exchange enum value for this client.

        Returns
        -------
        Exchange
            The exchange this client connects to
        """
        ...

    @property
    @abstractmethod
    def base_url(self) -> str:
        """
        Return the base URL for the data source.

        Returns
        -------
        str
            Base URL for downloading data (e.g., 'https://data.binance.vision')
        """
        ...

    @abstractmethod
    async def download_file(
        self,
        url: str,
        output_path: Path,
        session: aiohttp.ClientSession
    ) -> bool:
        """
        Download a file from the given URL.

        Parameters
        ----------
        url : str
            Full URL to download from
        output_path : Path
            Path where the downloaded file should be saved
        session : aiohttp.ClientSession
            Async HTTP session to use for the download

        Returns
        -------
        bool
            True if download succeeded, False otherwise
        """
        ...

    @abstractmethod
    async def __aenter__(self) -> ExchangeClient:
        """
        Async context manager entry.

        Returns
        -------
        ExchangeClient
            The client instance
        """
        ...

    @abstractmethod
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object | None
    ) -> None:
        """
        Async context manager exit.

        Parameters
        ----------
        exc_type : type[BaseException] | None
            Exception type if an exception was raised
        exc_val : BaseException | None
            Exception value if an exception was raised
        exc_tb : object | None
            Traceback if an exception was raised
        """
        ...
