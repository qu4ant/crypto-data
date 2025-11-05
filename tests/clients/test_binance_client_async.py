"""
Tests for BinanceDataVisionClientAsync.

Tests the async Binance Data Vision client for downloading historical OHLCV data.
Uses mocks to avoid real network requests.
"""

import pytest
import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch, MagicMock
import tempfile
import aiohttp

from crypto_data.clients.binance_vision_async import BinanceDataVisionClientAsync


# ============================================================================
# Test Basic Download Functionality
# ============================================================================

class TestBasicDownload:
    """Test basic download functionality with different responses."""

    @pytest.mark.asyncio
    async def test_successful_download_returns_true(self):
        """Test that successful download returns True and writes file."""
        client = BinanceDataVisionClientAsync()

        # Mock session and response
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.read = AsyncMock(return_value=b'fake_zip_content')
        mock_response.raise_for_status = Mock()
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)

        mock_session = AsyncMock()
        mock_session.get = Mock(return_value=mock_response)

        client.session = mock_session
        client.semaphore = asyncio.Semaphore(20)

        # Create temp file
        with tempfile.NamedTemporaryFile(suffix='.zip', delete=False) as f:
            output_path = Path(f.name)

        try:
            # Download
            result = await client.download_klines(
                symbol='BTCUSDT',
                data_type='spot',
                month='2024-01',
                interval='5m',
                output_path=output_path
            )

            # Verify
            assert result is True
            assert output_path.exists()
            assert output_path.read_bytes() == b'fake_zip_content'

        finally:
            # Cleanup
            if output_path.exists():
                output_path.unlink()

    @pytest.mark.asyncio
    async def test_404_returns_false(self):
        """Test that 404 response returns False (data not available)."""
        client = BinanceDataVisionClientAsync()

        # Mock 404 response
        mock_response = AsyncMock()
        mock_response.status = 404
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)

        mock_session = AsyncMock()
        mock_session.get = Mock(return_value=mock_response)

        client.session = mock_session
        client.semaphore = asyncio.Semaphore(20)

        # Download
        with tempfile.NamedTemporaryFile(suffix='.zip', delete=False) as f:
            output_path = Path(f.name)

        try:
            result = await client.download_klines(
                symbol='BTCUSDT',
                data_type='spot',
                month='2024-01',
                interval='5m',
                output_path=output_path
            )

            # Verify
            assert result is False

        finally:
            if output_path.exists():
                output_path.unlink()

    @pytest.mark.asyncio
    async def test_http_error_500_raises(self):
        """Test that HTTP 500 error raises ClientResponseError."""
        client = BinanceDataVisionClientAsync()

        # Mock 500 response
        mock_response = AsyncMock()
        mock_response.status = 500
        mock_response.raise_for_status = Mock(
            side_effect=aiohttp.ClientResponseError(
                request_info=Mock(),
                history=(),
                status=500,
                message='Internal Server Error'
            )
        )
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)

        mock_session = AsyncMock()
        mock_session.get = Mock(return_value=mock_response)

        client.session = mock_session
        client.semaphore = asyncio.Semaphore(20)

        # Download should raise
        with tempfile.NamedTemporaryFile(suffix='.zip') as f:
            output_path = Path(f.name)

        with pytest.raises(aiohttp.ClientResponseError) as exc_info:
            await client.download_klines(
                symbol='BTCUSDT',
                data_type='spot',
                month='2024-01',
                interval='5m',
                output_path=output_path
            )

        assert exc_info.value.status == 500

    @pytest.mark.asyncio
    async def test_http_error_503_raises(self):
        """Test that HTTP 503 error raises ClientResponseError."""
        client = BinanceDataVisionClientAsync()

        # Mock 503 response
        mock_response = AsyncMock()
        mock_response.status = 503
        mock_response.raise_for_status = Mock(
            side_effect=aiohttp.ClientResponseError(
                request_info=Mock(),
                history=(),
                status=503,
                message='Service Unavailable'
            )
        )
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)

        mock_session = AsyncMock()
        mock_session.get = Mock(return_value=mock_response)

        client.session = mock_session
        client.semaphore = asyncio.Semaphore(20)

        # Download should raise
        with tempfile.NamedTemporaryFile(suffix='.zip') as f:
            output_path = Path(f.name)

        with pytest.raises(aiohttp.ClientResponseError) as exc_info:
            await client.download_klines(
                symbol='BTCUSDT',
                data_type='spot',
                month='2024-01',
                interval='5m',
                output_path=output_path
            )

        assert exc_info.value.status == 503

    @pytest.mark.asyncio
    async def test_timeout_raises(self):
        """Test that timeout raises ClientError."""
        client = BinanceDataVisionClientAsync()

        # Mock timeout
        mock_session = AsyncMock()
        mock_session.get = Mock(
            side_effect=aiohttp.ClientError("Timeout")
        )

        client.session = mock_session
        client.semaphore = asyncio.Semaphore(20)

        # Download should raise
        with tempfile.NamedTemporaryFile(suffix='.zip') as f:
            output_path = Path(f.name)

        with pytest.raises(aiohttp.ClientError):
            await client.download_klines(
                symbol='BTCUSDT',
                data_type='spot',
                month='2024-01',
                interval='5m',
                output_path=output_path
            )

    @pytest.mark.asyncio
    async def test_file_written_with_correct_content(self):
        """Test that downloaded file contains correct content."""
        client = BinanceDataVisionClientAsync()

        expected_content = b'PK\x03\x04' + b'a' * 1000  # ZIP header + data

        # Mock response
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.read = AsyncMock(return_value=expected_content)
        mock_response.raise_for_status = Mock()
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)

        mock_session = AsyncMock()
        mock_session.get = Mock(return_value=mock_response)

        client.session = mock_session
        client.semaphore = asyncio.Semaphore(20)

        # Download
        with tempfile.NamedTemporaryFile(suffix='.zip', delete=False) as f:
            output_path = Path(f.name)

        try:
            await client.download_klines(
                symbol='ETHUSDT',
                data_type='futures',
                month='2024-03',
                interval='1h',
                output_path=output_path
            )

            # Verify content
            assert output_path.read_bytes() == expected_content

        finally:
            if output_path.exists():
                output_path.unlink()


# ============================================================================
# Test Context Manager
# ============================================================================

class TestContextManager:
    """Test async context manager functionality."""

    @pytest.mark.asyncio
    async def test_context_manager_creates_closes_session(self):
        """Test that context manager creates and closes session."""
        client = BinanceDataVisionClientAsync()

        # Before entering context
        assert client.session is None
        assert client.semaphore is None

        # Enter context
        async with client:
            # Session and semaphore should be created
            assert client.session is not None
            assert isinstance(client.session, aiohttp.ClientSession)
            assert client.semaphore is not None
            assert isinstance(client.semaphore, asyncio.Semaphore)

        # After exiting context, session should be closed
        # Note: We can't easily test if session is closed without introspection

    @pytest.mark.asyncio
    async def test_call_without_session_raises_runtime_error(self):
        """Test that calling download_klines without session raises RuntimeError."""
        client = BinanceDataVisionClientAsync()

        # Don't initialize session (skip __aenter__)

        with tempfile.NamedTemporaryFile(suffix='.zip') as f:
            output_path = Path(f.name)

        # Should raise RuntimeError
        with pytest.raises(RuntimeError, match="Client session not initialized"):
            await client.download_klines(
                symbol='BTCUSDT',
                data_type='spot',
                month='2024-01',
                interval='5m',
                output_path=output_path
            )


# ============================================================================
# Test URL Construction
# ============================================================================

class TestUrlConstruction:
    """Test URL construction for different data types."""

    def test_spot_url_format_correct(self):
        """Test that spot URL is constructed correctly."""
        client = BinanceDataVisionClientAsync()

        url = client.get_download_url(
            symbol='BTCUSDT',
            data_type='spot',
            month='2024-01',
            interval='5m'
        )

        expected = 'https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/5m/BTCUSDT-5m-2024-01.zip'
        assert url == expected

    def test_futures_url_format_correct(self):
        """Test that futures URL is constructed correctly."""
        client = BinanceDataVisionClientAsync()

        url = client.get_download_url(
            symbol='ETHUSDT',
            data_type='futures',
            month='2024-03',
            interval='1h'
        )

        expected = 'https://data.binance.vision/data/futures/um/monthly/klines/ETHUSDT/1h/ETHUSDT-1h-2024-03.zip'
        assert url == expected

    def test_invalid_data_type_raises_value_error(self):
        """Test that invalid data_type raises ValueError."""
        client = BinanceDataVisionClientAsync()

        # get_download_url should raise ValueError
        with pytest.raises(ValueError, match="Invalid data_type"):
            client.get_download_url(
                symbol='BTCUSDT',
                data_type='invalid',
                month='2024-01',
                interval='5m'
            )

    @pytest.mark.asyncio
    async def test_download_invalid_data_type_raises_value_error(self):
        """Test that download_klines with invalid data_type raises ValueError."""
        client = BinanceDataVisionClientAsync()

        # Initialize session
        client.session = AsyncMock()
        client.semaphore = asyncio.Semaphore(20)

        with tempfile.NamedTemporaryFile(suffix='.zip') as f:
            output_path = Path(f.name)

        # Should raise ValueError
        with pytest.raises(ValueError, match="Invalid data_type"):
            await client.download_klines(
                symbol='BTCUSDT',
                data_type='invalid',
                month='2024-01',
                interval='5m',
                output_path=output_path
            )


# ============================================================================
# Test Concurrency
# ============================================================================

class TestConcurrency:
    """Test concurrency control with semaphore."""

    @pytest.mark.asyncio
    async def test_semaphore_limits_concurrent_downloads(self):
        """Test that semaphore limits concurrent downloads."""
        # Create client with max_concurrent=2
        client = BinanceDataVisionClientAsync(max_concurrent=2)

        async with client:
            # Track concurrent execution
            concurrent_count = 0
            max_concurrent_seen = 0

            async def mock_download():
                nonlocal concurrent_count, max_concurrent_seen

                async with client.semaphore:
                    concurrent_count += 1
                    max_concurrent_seen = max(max_concurrent_seen, concurrent_count)
                    await asyncio.sleep(0.01)  # Simulate work
                    concurrent_count -= 1

            # Launch 5 tasks (more than max_concurrent)
            tasks = [mock_download() for _ in range(5)]
            await asyncio.gather(*tasks)

            # Max concurrent should not exceed 2
            assert max_concurrent_seen <= 2

    @pytest.mark.asyncio
    async def test_parallel_downloads_mixed_results(self):
        """Test that parallel downloads can have mixed success/404 results."""
        client = BinanceDataVisionClientAsync()

        # Create responses: success, 404, success
        responses = []

        # Response 1: Success
        resp1 = AsyncMock()
        resp1.status = 200
        resp1.read = AsyncMock(return_value=b'content1')
        resp1.raise_for_status = Mock()
        resp1.__aenter__ = AsyncMock(return_value=resp1)
        resp1.__aexit__ = AsyncMock(return_value=None)
        responses.append(resp1)

        # Response 2: 404
        resp2 = AsyncMock()
        resp2.status = 404
        resp2.__aenter__ = AsyncMock(return_value=resp2)
        resp2.__aexit__ = AsyncMock(return_value=None)
        responses.append(resp2)

        # Response 3: Success
        resp3 = AsyncMock()
        resp3.status = 200
        resp3.read = AsyncMock(return_value=b'content3')
        resp3.raise_for_status = Mock()
        resp3.__aenter__ = AsyncMock(return_value=resp3)
        resp3.__aexit__ = AsyncMock(return_value=None)
        responses.append(resp3)

        # Mock session to return responses in order
        call_count = 0
        def get_response(url):
            nonlocal call_count
            result = responses[call_count]
            call_count += 1
            return result

        mock_session = AsyncMock()
        mock_session.get = Mock(side_effect=get_response)

        client.session = mock_session
        client.semaphore = asyncio.Semaphore(20)

        # Create temp files
        with tempfile.TemporaryDirectory() as tmpdir:
            paths = [
                Path(tmpdir) / 'file1.zip',
                Path(tmpdir) / 'file2.zip',
                Path(tmpdir) / 'file3.zip'
            ]

            # Download in parallel
            tasks = [
                client.download_klines('BTCUSDT', 'spot', '2024-01', '5m', paths[0]),
                client.download_klines('BTCUSDT', 'spot', '2024-02', '5m', paths[1]),
                client.download_klines('BTCUSDT', 'spot', '2024-03', '5m', paths[2])
            ]

            results = await asyncio.gather(*tasks)

            # Verify results
            assert results == [True, False, True]
            assert paths[0].exists()
            assert not paths[1].exists()  # 404 doesn't write file
            assert paths[2].exists()


# ============================================================================
# Test Configuration
# ============================================================================

class TestConfiguration:
    """Test custom configuration parameters."""

    def test_custom_base_url_and_timeout(self):
        """Test that custom base_url and timeout are applied."""
        custom_url = 'https://custom.binance.mirror.com/'
        custom_timeout = 60

        client = BinanceDataVisionClientAsync(
            base_url=custom_url,
            timeout=custom_timeout
        )

        assert client.base_url == custom_url
        assert client.timeout.total == custom_timeout

    def test_custom_max_concurrent(self):
        """Test that custom max_concurrent is applied."""
        client = BinanceDataVisionClientAsync(max_concurrent=5)

        assert client.max_concurrent == 5

    def test_custom_base_url_affects_url_construction(self):
        """Test that custom base_url affects URL construction."""
        custom_url = 'https://mirror.example.com/'

        client = BinanceDataVisionClientAsync(base_url=custom_url)

        url = client.get_download_url(
            symbol='BTCUSDT',
            data_type='spot',
            month='2024-01',
            interval='5m'
        )

        assert url.startswith(custom_url)
        assert url == 'https://mirror.example.com/data/spot/monthly/klines/BTCUSDT/5m/BTCUSDT-5m-2024-01.zip'


# ============================================================================
# Test Funding Rates Download
# ============================================================================

class TestFundingRatesDownload:
    """Test funding rates download functionality."""

    @pytest.mark.asyncio
    async def test_successful_funding_rates_download(self):
        """Test that successful funding rates download returns True."""
        client = BinanceDataVisionClientAsync()

        # Mock successful response
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.read = AsyncMock(return_value=b'fake_funding_rates_zip')
        mock_response.raise_for_status = Mock()
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)

        mock_session = AsyncMock()
        mock_session.get = Mock(return_value=mock_response)

        client.session = mock_session
        client.semaphore = asyncio.Semaphore(20)

        # Create temp file
        with tempfile.NamedTemporaryFile(suffix='.zip', delete=False) as f:
            output_path = Path(f.name)

        try:
            # Download funding rates
            result = await client.download_funding_rates(
                symbol='BTCUSDT',
                month='2024-01',
                output_path=output_path
            )

            # Verify
            assert result is True
            assert output_path.exists()
            assert output_path.read_bytes() == b'fake_funding_rates_zip'

            # Verify URL was constructed correctly
            expected_url = 'https://data.binance.vision/data/futures/um/monthly/fundingRate/BTCUSDT/BTCUSDT-fundingRate-2024-01.zip'
            mock_session.get.assert_called_once()
            call_args = mock_session.get.call_args[0][0]
            assert call_args == expected_url

        finally:
            if output_path.exists():
                output_path.unlink()

    @pytest.mark.asyncio
    async def test_funding_rates_404_returns_false(self):
        """Test that 404 response returns False for funding rates."""
        client = BinanceDataVisionClientAsync()

        # Mock 404 response
        mock_response = AsyncMock()
        mock_response.status = 404
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)

        mock_session = AsyncMock()
        mock_session.get = Mock(return_value=mock_response)

        client.session = mock_session
        client.semaphore = asyncio.Semaphore(20)

        with tempfile.NamedTemporaryFile(suffix='.zip', delete=False) as f:
            output_path = Path(f.name)

        try:
            result = await client.download_funding_rates(
                symbol='BTCUSDT',
                month='2024-01',
                output_path=output_path
            )

            # Verify - should return False, no file written
            assert result is False

        finally:
            if output_path.exists():
                output_path.unlink()

    @pytest.mark.asyncio
    async def test_funding_rates_http_error_raises(self):
        """Test that HTTP errors raise exception for funding rates."""
        client = BinanceDataVisionClientAsync()

        # Mock 500 response
        mock_response = AsyncMock()
        mock_response.status = 500
        mock_response.raise_for_status = Mock(
            side_effect=aiohttp.ClientResponseError(
                request_info=Mock(),
                history=(),
                status=500,
                message='Internal Server Error'
            )
        )
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)

        mock_session = AsyncMock()
        mock_session.get = Mock(return_value=mock_response)

        client.session = mock_session
        client.semaphore = asyncio.Semaphore(20)

        with tempfile.NamedTemporaryFile(suffix='.zip') as f:
            output_path = Path(f.name)

        # Should raise ClientResponseError
        with pytest.raises(aiohttp.ClientResponseError) as exc_info:
            await client.download_funding_rates(
                symbol='BTCUSDT',
                month='2024-01',
                output_path=output_path
            )

        assert exc_info.value.status == 500

    @pytest.mark.asyncio
    async def test_funding_rates_network_error_raises(self):
        """Test that network errors raise exception for funding rates."""
        client = BinanceDataVisionClientAsync()

        # Mock network error
        mock_session = AsyncMock()
        mock_session.get = Mock(
            side_effect=aiohttp.ClientError("Connection timeout")
        )

        client.session = mock_session
        client.semaphore = asyncio.Semaphore(20)

        with tempfile.NamedTemporaryFile(suffix='.zip') as f:
            output_path = Path(f.name)

        # Should raise ClientError
        with pytest.raises(aiohttp.ClientError):
            await client.download_funding_rates(
                symbol='BTCUSDT',
                month='2024-01',
                output_path=output_path
            )

    @pytest.mark.asyncio
    async def test_funding_rates_without_session_raises(self):
        """Test that calling download_funding_rates without session raises RuntimeError."""
        client = BinanceDataVisionClientAsync()

        # Don't initialize session

        with tempfile.NamedTemporaryFile(suffix='.zip') as f:
            output_path = Path(f.name)

        # Should raise RuntimeError
        with pytest.raises(RuntimeError, match="Client session not initialized"):
            await client.download_funding_rates(
                symbol='BTCUSDT',
                month='2024-01',
                output_path=output_path
            )

    @pytest.mark.asyncio
    async def test_funding_rates_url_construction(self):
        """Test that funding rates URL is constructed correctly."""
        client = BinanceDataVisionClientAsync()

        # Mock response
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.read = AsyncMock(return_value=b'content')
        mock_response.raise_for_status = Mock()
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)

        mock_session = AsyncMock()
        mock_session.get = Mock(return_value=mock_response)

        client.session = mock_session
        client.semaphore = asyncio.Semaphore(20)

        with tempfile.NamedTemporaryFile(suffix='.zip', delete=False) as f:
            output_path = Path(f.name)

        try:
            await client.download_funding_rates(
                symbol='ETHUSDT',
                month='2024-03',
                output_path=output_path
            )

            # Verify URL
            expected_url = 'https://data.binance.vision/data/futures/um/monthly/fundingRate/ETHUSDT/ETHUSDT-fundingRate-2024-03.zip'
            mock_session.get.assert_called_once()
            call_args = mock_session.get.call_args[0][0]
            assert call_args == expected_url

        finally:
            if output_path.exists():
                output_path.unlink()

    @pytest.mark.asyncio
    async def test_funding_rates_parallel_downloads(self):
        """Test parallel funding rates downloads with mixed results."""
        client = BinanceDataVisionClientAsync()

        # Create responses: success, 404, success
        responses = []

        # Response 1: Success
        resp1 = AsyncMock()
        resp1.status = 200
        resp1.read = AsyncMock(return_value=b'content1')
        resp1.raise_for_status = Mock()
        resp1.__aenter__ = AsyncMock(return_value=resp1)
        resp1.__aexit__ = AsyncMock(return_value=None)
        responses.append(resp1)

        # Response 2: 404
        resp2 = AsyncMock()
        resp2.status = 404
        resp2.__aenter__ = AsyncMock(return_value=resp2)
        resp2.__aexit__ = AsyncMock(return_value=None)
        responses.append(resp2)

        # Response 3: Success
        resp3 = AsyncMock()
        resp3.status = 200
        resp3.read = AsyncMock(return_value=b'content3')
        resp3.raise_for_status = Mock()
        resp3.__aenter__ = AsyncMock(return_value=resp3)
        resp3.__aexit__ = AsyncMock(return_value=None)
        responses.append(resp3)

        # Mock session to return responses in order
        call_count = 0
        def get_response(url):
            nonlocal call_count
            result = responses[call_count]
            call_count += 1
            return result

        mock_session = AsyncMock()
        mock_session.get = Mock(side_effect=get_response)

        client.session = mock_session
        client.semaphore = asyncio.Semaphore(20)

        # Create temp files
        with tempfile.TemporaryDirectory() as tmpdir:
            paths = [
                Path(tmpdir) / 'file1.zip',
                Path(tmpdir) / 'file2.zip',
                Path(tmpdir) / 'file3.zip'
            ]

            # Download in parallel
            tasks = [
                client.download_funding_rates('BTCUSDT', '2024-01', paths[0]),
                client.download_funding_rates('BTCUSDT', '2024-02', paths[1]),
                client.download_funding_rates('BTCUSDT', '2024-03', paths[2])
            ]

            results = await asyncio.gather(*tasks)

            # Verify results
            assert results == [True, False, True]
            assert paths[0].exists()
            assert not paths[1].exists()  # 404 doesn't write file
            assert paths[2].exists()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
