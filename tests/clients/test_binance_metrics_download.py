"""
Tests for BinanceDataVisionClientAsync.download_metrics().

Tests the async metrics download method for open interest data.
Uses mocks to avoid real network requests.
"""

import pytest
import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, Mock
import tempfile
import aiohttp

from crypto_data.clients.binance_vision_async import BinanceDataVisionClientAsync


class TestMetricsDownload:
    """Test metrics download functionality with different responses."""

    @pytest.mark.asyncio
    async def test_successful_metrics_download(self):
        """Test that successful download returns True and writes CSV file."""
        client = BinanceDataVisionClientAsync()

        # Mock CSV content
        csv_content = b"""create_time,symbol,sum_open_interest,sum_open_interest_value,count_toptrader_long_short_ratio,sum_toptrader_long_short_ratio,count_long_short_ratio,sum_taker_long_short_vol_ratio
2025-11-02 00:05:00,BTCUSDT,76643.19,8430743345.68,2.05,1.91,1.88,0.76"""

        # Mock session and response
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.read = AsyncMock(return_value=csv_content)
        mock_response.raise_for_status = Mock()
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)

        mock_session = AsyncMock()
        mock_session.get = Mock(return_value=mock_response)

        client.session = mock_session
        client.semaphore = asyncio.Semaphore(20)

        # Create temp file
        with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as f:
            output_path = Path(f.name)

        try:
            # Download
            result = await client.download_metrics(
                symbol='BTCUSDT',
                date='2025-11-02',
                output_path=output_path
            )

            # Verify
            assert result is True
            assert output_path.exists()
            assert output_path.read_bytes() == csv_content

        finally:
            # Cleanup
            if output_path.exists():
                output_path.unlink()

    @pytest.mark.asyncio
    async def test_metrics_404_returns_false(self):
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
        with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as f:
            output_path = Path(f.name)

        try:
            result = await client.download_metrics(
                symbol='BTCUSDT',
                date='2025-11-02',
                output_path=output_path
            )

            # Verify
            assert result is False

        finally:
            if output_path.exists():
                output_path.unlink()

    @pytest.mark.asyncio
    async def test_metrics_http_500_raises(self):
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
        with tempfile.NamedTemporaryFile(suffix='.csv') as f:
            output_path = Path(f.name)

        with pytest.raises(aiohttp.ClientResponseError) as exc_info:
            await client.download_metrics(
                symbol='BTCUSDT',
                date='2025-11-02',
                output_path=output_path
            )

        assert exc_info.value.status == 500

    @pytest.mark.asyncio
    async def test_metrics_timeout_raises(self):
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
        with tempfile.NamedTemporaryFile(suffix='.csv') as f:
            output_path = Path(f.name)

        with pytest.raises(aiohttp.ClientError):
            await client.download_metrics(
                symbol='BTCUSDT',
                date='2025-11-02',
                output_path=output_path
            )

    @pytest.mark.asyncio
    async def test_metrics_csv_content_correct(self):
        """Test that downloaded CSV content matches expected."""
        client = BinanceDataVisionClientAsync()

        # Expected CSV content
        expected_csv = b"""create_time,symbol,sum_open_interest,sum_open_interest_value
2025-11-02 00:05:00,BTCUSDT,76643.19,8430743345.68"""

        # Mock response
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.read = AsyncMock(return_value=expected_csv)
        mock_response.raise_for_status = Mock()
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)

        mock_session = AsyncMock()
        mock_session.get = Mock(return_value=mock_response)

        client.session = mock_session
        client.semaphore = asyncio.Semaphore(20)

        # Download
        with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as f:
            output_path = Path(f.name)

        try:
            result = await client.download_metrics(
                symbol='BTCUSDT',
                date='2025-11-02',
                output_path=output_path
            )

            # Verify content
            assert result is True
            actual_content = output_path.read_bytes()
            assert actual_content == expected_csv

        finally:
            if output_path.exists():
                output_path.unlink()

    @pytest.mark.asyncio
    async def test_metrics_without_session_raises(self):
        """Test that calling without session raises RuntimeError."""
        client = BinanceDataVisionClientAsync()

        # Don't set session (simulate not using context manager)
        with tempfile.NamedTemporaryFile(suffix='.csv') as f:
            output_path = Path(f.name)

        with pytest.raises(RuntimeError, match="Client session not initialized"):
            await client.download_metrics(
                symbol='BTCUSDT',
                date='2025-11-02',
                output_path=output_path
            )


class TestMetricsUrlConstruction:
    """Test URL construction for metrics endpoint."""

    def test_metrics_url_format_correct(self):
        """Test that metrics URL is constructed correctly."""
        client = BinanceDataVisionClientAsync()

        # Note: We can't directly test download_metrics URL, but we can verify
        # the DATA_CATEGORIES includes open_interest
        assert 'open_interest' in client.DATA_CATEGORIES
        assert client.DATA_CATEGORIES['open_interest'] == 'data/futures/um/daily/metrics'

    def test_metrics_url_pattern(self):
        """Test the expected metrics URL pattern."""
        client = BinanceDataVisionClientAsync()

        # Expected pattern:
        # https://data.binance.vision/data/futures/um/daily/metrics/{SYMBOL}/{SYMBOL}-metrics-{DATE}.csv
        expected_base = 'https://data.binance.vision/'
        expected_category = 'data/futures/um/daily/metrics'

        assert client.base_url == expected_base
        assert client.DATA_CATEGORIES['open_interest'] == expected_category


class TestMetricsContextManager:
    """Test metrics download with context manager."""

    @pytest.mark.asyncio
    async def test_context_manager_works_with_metrics(self):
        """Test that context manager works correctly for metrics downloads."""
        with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as f:
            output_path = Path(f.name)

        try:
            # Use client with context manager
            async with BinanceDataVisionClientAsync() as client:
                # Mock response
                mock_response = AsyncMock()
                mock_response.status = 200
                mock_response.read = AsyncMock(return_value=b'csv_content')
                mock_response.raise_for_status = Mock()
                mock_response.__aenter__ = AsyncMock(return_value=mock_response)
                mock_response.__aexit__ = AsyncMock(return_value=None)

                # Replace session's get method
                client.session.get = Mock(return_value=mock_response)

                # Download
                result = await client.download_metrics(
                    symbol='BTCUSDT',
                    date='2025-11-02',
                    output_path=output_path
                )

                assert result is True
                assert output_path.exists()

        finally:
            if output_path.exists():
                output_path.unlink()


class TestMetricsConcurrency:
    """Test concurrent metrics downloads."""

    @pytest.mark.asyncio
    async def test_semaphore_limits_concurrent_downloads(self):
        """Test that semaphore limits concurrent downloads."""
        max_concurrent = 5
        client = BinanceDataVisionClientAsync(max_concurrent=max_concurrent)

        # Verify semaphore is created with correct limit
        async with client:
            assert client.semaphore._value == max_concurrent


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
