"""
Tests for BinanceExchange client.

Tests the Binance implementation of the ExchangeClient interface.
"""

import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from crypto_data.enums import Exchange
from crypto_data.exchanges.binance import BinanceExchange


class TestBinanceExchangeInit:
    """Tests for BinanceExchange initialization."""

    def test_default_properties(self):
        """BinanceExchange has correct default properties."""
        client = BinanceExchange()

        assert client.exchange == Exchange.BINANCE
        assert client.base_url == "https://data.binance.vision/"
        assert client._max_concurrent == 20
        assert client._session is None
        assert client._semaphore is None

    def test_custom_base_url(self):
        """BinanceExchange accepts custom base URL."""
        custom_url = "https://custom.binance.test/"
        client = BinanceExchange(base_url=custom_url)

        assert client.base_url == custom_url

    def test_custom_timeout(self):
        """BinanceExchange accepts custom timeout."""
        client = BinanceExchange(timeout=60)

        assert client._timeout.total == 60

    def test_custom_max_concurrent(self):
        """BinanceExchange accepts custom max_concurrent."""
        client = BinanceExchange(max_concurrent=50)

        assert client._max_concurrent == 50


class TestBinanceExchangeContextManager:
    """Tests for BinanceExchange async context manager."""

    @pytest.mark.asyncio
    async def test_creates_session(self):
        """Context manager creates aiohttp session and semaphore."""
        client = BinanceExchange()

        assert client._session is None
        assert client._semaphore is None

        async with client:
            assert client._session is not None
            assert client._semaphore is not None
            assert isinstance(client._semaphore, asyncio.Semaphore)

    @pytest.mark.asyncio
    async def test_closes_session(self):
        """Context manager closes session on exit."""
        client = BinanceExchange()

        async with client:
            session = client._session
            assert session is not None

        # Session should be closed and set to None
        assert client._session is None

    @pytest.mark.asyncio
    async def test_returns_self(self):
        """Context manager returns the client instance."""
        client = BinanceExchange()

        async with client as entered_client:
            assert entered_client is client


class TestBinanceExchangeDownload:
    """Tests for BinanceExchange download_file method."""

    @pytest.mark.asyncio
    async def test_404_returns_false(self):
        """download_file returns False for 404 responses."""
        client = BinanceExchange()

        # Create mock response
        mock_response = AsyncMock()
        mock_response.status = 404
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)

        # Create mock session with async close
        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_response)
        mock_session.close = AsyncMock()

        async with client:
            # Replace session with mock
            client._session = mock_session

            result = await client.download_file(
                url="https://data.binance.vision/test.zip",
                output_path=Path("/tmp/test.zip")
            )

            assert result is False
            mock_session.get.assert_called_once_with(
                "https://data.binance.vision/test.zip"
            )

    @pytest.mark.asyncio
    async def test_successful_download(self, tmp_path):
        """download_file returns True and writes file on success."""
        client = BinanceExchange()

        # Create minimal valid ZIP content (small enough to skip validation)
        zip_content = b"PK\x03\x04" + b"\x00" * 100  # ZIP header

        # Create mock response
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.headers = {'Content-Length': str(len(zip_content))}
        mock_response.read = AsyncMock(return_value=zip_content)
        mock_response.raise_for_status = MagicMock()
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)

        # Create mock session with async close
        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_response)
        mock_session.close = AsyncMock()

        output_path = tmp_path / "test.zip"

        async with client:
            client._session = mock_session

            result = await client.download_file(
                url="https://data.binance.vision/test.zip",
                output_path=output_path
            )

            assert result is True
            assert output_path.exists()
            assert output_path.read_bytes() == zip_content

    @pytest.mark.asyncio
    async def test_partial_download_returns_false(self, tmp_path):
        """download_file returns False when Content-Length doesn't match."""
        client = BinanceExchange()

        # Content is smaller than declared Content-Length
        zip_content = b"PK\x03\x04" + b"\x00" * 50

        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.headers = {'Content-Length': '1000'}  # Claimed size
        mock_response.read = AsyncMock(return_value=zip_content)  # Actual smaller
        mock_response.raise_for_status = MagicMock()
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)

        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_response)
        mock_session.close = AsyncMock()

        output_path = tmp_path / "test.zip"

        async with client:
            client._session = mock_session

            result = await client.download_file(
                url="https://data.binance.vision/test.zip",
                output_path=output_path
            )

            assert result is False
            assert not output_path.exists()

    @pytest.mark.asyncio
    async def test_no_session_raises_error(self):
        """download_file raises RuntimeError when no session is available."""
        client = BinanceExchange()

        with pytest.raises(RuntimeError, match="No session available"):
            await client.download_file(
                url="https://data.binance.vision/test.zip",
                output_path=Path("/tmp/test.zip")
            )

    @pytest.mark.asyncio
    async def test_uses_provided_session(self, tmp_path):
        """download_file uses provided session parameter."""
        client = BinanceExchange()

        zip_content = b"PK\x03\x04" + b"\x00" * 50

        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.headers = {'Content-Length': str(len(zip_content))}
        mock_response.read = AsyncMock(return_value=zip_content)
        mock_response.raise_for_status = MagicMock()
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)

        external_session = MagicMock()
        external_session.get = MagicMock(return_value=mock_response)

        output_path = tmp_path / "test.zip"

        # Don't use context manager - pass session directly
        result = await client.download_file(
            url="https://data.binance.vision/test.zip",
            output_path=output_path,
            session=external_session
        )

        assert result is True
        external_session.get.assert_called_once()


class TestBinanceExchangeInterface:
    """Tests verifying BinanceExchange implements ExchangeClient correctly."""

    def test_implements_exchange_client(self):
        """BinanceExchange is a valid ExchangeClient implementation."""
        from crypto_data.exchanges.base import ExchangeClient

        client = BinanceExchange()
        assert isinstance(client, ExchangeClient)

    def test_has_required_properties(self):
        """BinanceExchange has all required properties."""
        client = BinanceExchange()

        # Properties should not raise
        assert client.exchange is not None
        assert client.base_url is not None

    @pytest.mark.asyncio
    async def test_context_manager_protocol(self):
        """BinanceExchange implements async context manager protocol."""
        client = BinanceExchange()

        # Should have __aenter__ and __aexit__
        assert hasattr(client, '__aenter__')
        assert hasattr(client, '__aexit__')

        # Should work as context manager
        async with client as c:
            assert c is client
