"""
Tests for download robustness in binance_vision_async.py

Tests partial download detection, ZIP integrity verification, and atomic write pattern.
"""

import pytest
from crypto_data.enums import DataType, Interval
import asyncio
import zipfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch
from crypto_data.clients.binance_vision_async import BinanceDataVisionClientAsync


@pytest.fixture
def mock_response():
    """Create a mock aiohttp response."""
    response = AsyncMock()
    response.status = 200
    response.headers = {}
    return response


@pytest.fixture
def valid_zip_content(tmp_path):
    """Create valid ZIP file content (> 1KB for validation to trigger)."""
    # Generate 2KB of CSV content to ensure ZIP > 1KB
    csv_rows = ["open_time,open,high,low,close,volume"]
    for i in range(100):  # 100 rows ~ 2KB
        csv_rows.append(f"170406{7200000+i*300000},45000,45100,44900,45050,100")
    csv_content = "\n".join(csv_rows).encode()

    zip_path = tmp_path / "temp.zip"
    with zipfile.ZipFile(zip_path, 'w') as zf:
        zf.writestr("data.csv", csv_content)

    zip_bytes = zip_path.read_bytes()
    assert len(zip_bytes) >= 1024, "ZIP must be >= 1KB for validation to trigger"
    return zip_bytes


@pytest.fixture
def corrupt_zip_content():
    """Create corrupt (non-ZIP) content (> 1KB to trigger validation)."""
    # Generate 2KB of garbage to ensure validation triggers
    return b"This is not a ZIP file, just plain text garbage data " * 50


# =============================================================================
# Content-Length Validation Tests
# =============================================================================

@pytest.mark.asyncio
async def test_partial_download_detected_and_rejected(tmp_path, mock_response, valid_zip_content):
    """Partial download (Content-Length mismatch) should be detected and rejected."""
    output_path = tmp_path / "download.zip"

    # Simulate partial download: Content-Length says 1000 bytes, but only 500 received
    mock_response.headers = {'Content-Length': '1000'}
    mock_response.read = AsyncMock(return_value=valid_zip_content[:500])  # Partial content

    async with BinanceDataVisionClientAsync() as client:
        with patch.object(client.session, 'get') as mock_get:
            mock_get.return_value.__aenter__.return_value = mock_response

            # Download should return False (failed)
            success = await client.download_klines(
                'BTCUSDT', DataType.SPOT, '2024-01', Interval.MIN_5, output_path
            )

    assert success is False, "Partial download should be rejected"
    assert not output_path.exists(), "Partial file should not be saved"


@pytest.mark.asyncio
async def test_full_download_with_content_length_succeeds(tmp_path, mock_response, valid_zip_content):
    """Full download matching Content-Length should succeed."""
    output_path = tmp_path / "download.zip"

    # Simulate complete download: Content-Length matches actual bytes
    mock_response.headers = {'Content-Length': str(len(valid_zip_content))}
    mock_response.read = AsyncMock(return_value=valid_zip_content)

    async with BinanceDataVisionClientAsync() as client:
        with patch.object(client.session, 'get') as mock_get:
            mock_get.return_value.__aenter__.return_value = mock_response

            # Download should succeed
            success = await client.download_klines(
                'BTCUSDT', DataType.SPOT, '2024-01', Interval.MIN_5, output_path
            )

    assert success is True, "Complete download should succeed"
    assert output_path.exists(), "File should be saved"
    assert output_path.read_bytes() == valid_zip_content


@pytest.mark.asyncio
async def test_download_without_content_length_header_succeeds(tmp_path, mock_response, valid_zip_content):
    """Download without Content-Length header should still work (skip size check)."""
    output_path = tmp_path / "download.zip"

    # No Content-Length header (some servers don't provide it)
    mock_response.headers = {}
    mock_response.read = AsyncMock(return_value=valid_zip_content)

    async with BinanceDataVisionClientAsync() as client:
        with patch.object(client.session, 'get') as mock_get:
            mock_get.return_value.__aenter__.return_value = mock_response

            # Download should succeed (skip Content-Length check)
            success = await client.download_klines(
                'BTCUSDT', DataType.SPOT, '2024-01', Interval.MIN_5, output_path
            )

    assert success is True, "Download without Content-Length should succeed"
    assert output_path.exists()


# =============================================================================
# ZIP Integrity Validation Tests
# =============================================================================

@pytest.mark.asyncio
async def test_corrupt_zip_detected_and_rejected(tmp_path, mock_response, corrupt_zip_content):
    """Corrupt ZIP file should be detected and rejected."""
    output_path = tmp_path / "download.zip"

    mock_response.headers = {}
    mock_response.read = AsyncMock(return_value=corrupt_zip_content)

    async with BinanceDataVisionClientAsync() as client:
        with patch.object(client.session, 'get') as mock_get:
            mock_get.return_value.__aenter__.return_value = mock_response

            # Download should return False (corrupt ZIP)
            success = await client.download_klines(
                'BTCUSDT', DataType.SPOT, '2024-01', Interval.MIN_5, output_path
            )

    assert success is False, "Corrupt ZIP should be rejected"
    assert not output_path.exists(), "Corrupt file should not be saved"
    assert not (tmp_path / "download.tmp").exists(), "Temp file should be cleaned up"


@pytest.mark.asyncio
async def test_valid_zip_passes_integrity_check(tmp_path, mock_response, valid_zip_content):
    """Valid ZIP file should pass integrity check."""
    output_path = tmp_path / "download.zip"

    mock_response.headers = {}
    mock_response.read = AsyncMock(return_value=valid_zip_content)

    async with BinanceDataVisionClientAsync() as client:
        with patch.object(client.session, 'get') as mock_get:
            mock_get.return_value.__aenter__.return_value = mock_response

            # Download should succeed
            success = await client.download_klines(
                'BTCUSDT', DataType.SPOT, '2024-01', Interval.MIN_5, output_path
            )

    assert success is True, "Valid ZIP should pass integrity check"
    assert output_path.exists()

    # Verify it's a valid ZIP by opening it
    with zipfile.ZipFile(output_path, 'r') as zf:
        assert len(zf.namelist()) > 0, "ZIP should contain files"


# =============================================================================
# Atomic Write Pattern Tests
# =============================================================================

@pytest.mark.asyncio
async def test_atomic_write_uses_temp_file(tmp_path, mock_response, valid_zip_content):
    """Download should use temp file before atomic rename."""
    output_path = tmp_path / "download.zip"
    temp_path = tmp_path / "download.tmp"

    mock_response.headers = {}
    mock_response.read = AsyncMock(return_value=valid_zip_content)

    # Track if temp file was created
    temp_file_existed = False

    original_write_bytes = Path.write_bytes
    def track_write_bytes(self, data):
        nonlocal temp_file_existed
        if self.suffix == '.tmp':
            temp_file_existed = True
        return original_write_bytes(self, data)

    with patch.object(Path, 'write_bytes', track_write_bytes):
        async with BinanceDataVisionClientAsync() as client:
            with patch.object(client.session, 'get') as mock_get:
                mock_get.return_value.__aenter__.return_value = mock_response

                success = await client.download_klines(
                    'BTCUSDT', DataType.SPOT, '2024-01', Interval.MIN_5, output_path
                )

    assert success is True
    assert temp_file_existed, "Should have used temp file"
    assert output_path.exists(), "Final file should exist"
    assert not temp_path.exists(), "Temp file should be renamed away"


@pytest.mark.asyncio
async def test_temp_file_cleaned_up_on_corruption(tmp_path, mock_response, corrupt_zip_content):
    """Temp file should be cleaned up when corruption is detected."""
    output_path = tmp_path / "download.zip"
    temp_path = tmp_path / "download.tmp"

    mock_response.headers = {}
    mock_response.read = AsyncMock(return_value=corrupt_zip_content)

    async with BinanceDataVisionClientAsync() as client:
        with patch.object(client.session, 'get') as mock_get:
            mock_get.return_value.__aenter__.return_value = mock_response

            success = await client.download_klines(
                'BTCUSDT', DataType.SPOT, '2024-01', Interval.MIN_5, output_path
            )

    assert success is False
    assert not output_path.exists(), "Final file should not exist"
    assert not temp_path.exists(), "Temp file should be cleaned up"


# =============================================================================
# Download Metrics (Open Interest) Tests
# =============================================================================

@pytest.mark.asyncio
async def test_metrics_download_validates_integrity(tmp_path, mock_response, valid_zip_content):
    """Metrics download should also validate ZIP integrity."""
    output_path = tmp_path / "metrics.zip"

    mock_response.headers = {}
    mock_response.read = AsyncMock(return_value=valid_zip_content)

    async with BinanceDataVisionClientAsync() as client:
        with patch.object(client.session, 'get') as mock_get:
            mock_get.return_value.__aenter__.return_value = mock_response

            success = await client.download_metrics(
                'BTCUSDT', '2024-01-01', output_path
            )

    assert success is True
    assert output_path.exists()


@pytest.mark.asyncio
async def test_metrics_download_rejects_corrupt_zip(tmp_path, mock_response, corrupt_zip_content):
    """Metrics download should reject corrupt ZIP."""
    output_path = tmp_path / "metrics.zip"

    mock_response.headers = {}
    mock_response.read = AsyncMock(return_value=corrupt_zip_content)

    async with BinanceDataVisionClientAsync() as client:
        with patch.object(client.session, 'get') as mock_get:
            mock_get.return_value.__aenter__.return_value = mock_response

            success = await client.download_metrics(
                'BTCUSDT', '2024-01-01', output_path
            )

    assert success is False
    assert not output_path.exists()


# =============================================================================
# Download Funding Rates Tests
# =============================================================================

@pytest.mark.asyncio
async def test_funding_rates_download_validates_integrity(tmp_path, mock_response, valid_zip_content):
    """Funding rates download should validate ZIP integrity."""
    output_path = tmp_path / "funding.zip"

    mock_response.headers = {}
    mock_response.read = AsyncMock(return_value=valid_zip_content)

    async with BinanceDataVisionClientAsync() as client:
        with patch.object(client.session, 'get') as mock_get:
            mock_get.return_value.__aenter__.return_value = mock_response

            success = await client.download_funding_rates(
                'BTCUSDT', '2024-01', output_path
            )

    assert success is True
    assert output_path.exists()


@pytest.mark.asyncio
async def test_funding_rates_download_rejects_partial(tmp_path, mock_response, valid_zip_content):
    """Funding rates download should reject partial downloads."""
    output_path = tmp_path / "funding.zip"

    # Content-Length mismatch
    mock_response.headers = {'Content-Length': '10000'}
    mock_response.read = AsyncMock(return_value=valid_zip_content[:500])

    async with BinanceDataVisionClientAsync() as client:
        with patch.object(client.session, 'get') as mock_get:
            mock_get.return_value.__aenter__.return_value = mock_response

            success = await client.download_funding_rates(
                'BTCUSDT', '2024-01', output_path
            )

    assert success is False
    assert not output_path.exists()


# =============================================================================
# Edge Cases
# =============================================================================

@pytest.mark.asyncio
async def test_zero_byte_download_accepted(tmp_path, mock_response):
    """Zero-byte download is accepted (< 1KB, no validation)."""
    output_path = tmp_path / "download.zip"

    mock_response.headers = {}
    mock_response.read = AsyncMock(return_value=b"")  # Empty content

    async with BinanceDataVisionClientAsync() as client:
        with patch.object(client.session, 'get') as mock_get:
            mock_get.return_value.__aenter__.return_value = mock_response

            success = await client.download_klines(
                'BTCUSDT', DataType.SPOT, '2024-01', Interval.MIN_5, output_path
            )

    # Note: Empty files are accepted (skip validation for < 1KB)
    # Real Binance downloads are always > 1KB
    assert success is True, "Zero-byte file accepted (< 1KB threshold)"
    assert output_path.exists()


@pytest.mark.asyncio
async def test_very_small_zip_accepted_if_valid(tmp_path, mock_response):
    """Very small but valid ZIP should be accepted."""
    # Create minimal valid ZIP
    zip_path = tmp_path / "minimal.zip"
    with zipfile.ZipFile(zip_path, 'w') as zf:
        zf.writestr("empty.csv", b"")

    valid_minimal_zip = zip_path.read_bytes()
    output_path = tmp_path / "download.zip"

    mock_response.headers = {}
    mock_response.read = AsyncMock(return_value=valid_minimal_zip)

    async with BinanceDataVisionClientAsync() as client:
        with patch.object(client.session, 'get') as mock_get:
            mock_get.return_value.__aenter__.return_value = mock_response

            success = await client.download_klines(
                'BTCUSDT', DataType.SPOT, '2024-01', Interval.MIN_5, output_path
            )

    assert success is True, "Valid minimal ZIP should be accepted"
    assert output_path.exists()
