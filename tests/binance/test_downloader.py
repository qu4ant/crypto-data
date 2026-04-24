"""
Tests for BinanceDataVisionDownloader

Tests the generic batch downloader with focus on gap detection logic.
Note: Actual downloads are not tested (would require mocking exchange).
"""

import pytest
from unittest.mock import AsyncMock, MagicMock
import io
import zipfile

from crypto_data.binance_downloader import (
    BinanceDataVisionDownloader,
    clear_ticker_mappings,
    get_ticker_mapping,
    set_ticker_mapping,
)
from crypto_data.enums import DataType, Interval
from crypto_data.binance_datasets.base import DownloadResult, Period
from crypto_data.binance_datasets.klines import BinanceKlinesDataset


# -----------------------------------------------------------------------------
# Fixtures
# -----------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def clean_ticker_mappings():
    """Clear ticker mappings before and after each test."""
    clear_ticker_mappings()
    yield
    clear_ticker_mappings()


@pytest.fixture
def klines_dataset():
    """Create a BinanceKlinesDataset for testing."""
    return BinanceKlinesDataset(DataType.SPOT, Interval.MIN_5)


# -----------------------------------------------------------------------------
# TestBinanceDataVisionDownloaderGapDetection
# -----------------------------------------------------------------------------

class TestBinanceDataVisionDownloaderGapDetection:
    """Test gap detection logic."""

    def test_no_gap_all_successful(self, klines_dataset, tmp_path):
        """Test that all results pass through when no gap."""
        # Create mock downloader (we'll test _detect_gaps directly)
        downloader = BinanceDataVisionDownloader(
            dataset=klines_dataset,
            temp_path=tmp_path
        )

        # All successful results
        results = [
            DownloadResult(success=True, symbol='BTCUSDT', data_type=DataType.SPOT, period='2024-01'),
            DownloadResult(success=True, symbol='BTCUSDT', data_type=DataType.SPOT, period='2024-02'),
            DownloadResult(success=True, symbol='BTCUSDT', data_type=DataType.SPOT, period='2024-03'),
            DownloadResult(success=True, symbol='BTCUSDT', data_type=DataType.SPOT, period='2024-04'),
        ]

        filtered = downloader._detect_gaps(results, threshold=3)

        # All 4 results should pass through
        assert len(filtered) == 4

    def test_with_gap_truncates_after_consecutive_failures(self, klines_dataset, tmp_path):
        """Test that results are truncated after consecutive failures."""
        downloader = BinanceDataVisionDownloader(
            dataset=klines_dataset,
            temp_path=tmp_path
        )

        # Success, success, fail, fail, fail (gap detected at index 2)
        results = [
            DownloadResult(success=True, symbol='BTCUSDT', data_type=DataType.SPOT, period='2024-01'),
            DownloadResult(success=True, symbol='BTCUSDT', data_type=DataType.SPOT, period='2024-02'),
            DownloadResult(success=False, symbol='BTCUSDT', data_type=DataType.SPOT, period='2024-03', error='not_found'),
            DownloadResult(success=False, symbol='BTCUSDT', data_type=DataType.SPOT, period='2024-04', error='not_found'),
            DownloadResult(success=False, symbol='BTCUSDT', data_type=DataType.SPOT, period='2024-05', error='not_found'),
        ]

        filtered = downloader._detect_gaps(results, threshold=3)

        # Should be truncated at gap start (index 2)
        assert len(filtered) == 2
        assert filtered[0].period == '2024-01'
        assert filtered[1].period == '2024-02'

    def test_ignores_leading_failures_before_first_success(self, klines_dataset, tmp_path):
        """Test that leading failures (before token launch) are ignored."""
        downloader = BinanceDataVisionDownloader(
            dataset=klines_dataset,
            temp_path=tmp_path
        )

        # fail, fail, fail, success, success (leading failures are OK)
        results = [
            DownloadResult(success=False, symbol='BTCUSDT', data_type=DataType.SPOT, period='2024-01', error='not_found'),
            DownloadResult(success=False, symbol='BTCUSDT', data_type=DataType.SPOT, period='2024-02', error='not_found'),
            DownloadResult(success=False, symbol='BTCUSDT', data_type=DataType.SPOT, period='2024-03', error='not_found'),
            DownloadResult(success=True, symbol='BTCUSDT', data_type=DataType.SPOT, period='2024-04'),
            DownloadResult(success=True, symbol='BTCUSDT', data_type=DataType.SPOT, period='2024-05'),
        ]

        filtered = downloader._detect_gaps(results, threshold=3)

        # All 5 results should pass through (leading failures don't count)
        assert len(filtered) == 5

    def test_gap_with_mixed_failures_after_success(self, klines_dataset, tmp_path):
        """Test gap detection with intermittent failures."""
        downloader = BinanceDataVisionDownloader(
            dataset=klines_dataset,
            temp_path=tmp_path
        )

        # success, fail, success, fail, fail, fail (gap at index 3)
        results = [
            DownloadResult(success=True, symbol='BTCUSDT', data_type=DataType.SPOT, period='2024-01'),
            DownloadResult(success=False, symbol='BTCUSDT', data_type=DataType.SPOT, period='2024-02', error='not_found'),
            DownloadResult(success=True, symbol='BTCUSDT', data_type=DataType.SPOT, period='2024-03'),
            DownloadResult(success=False, symbol='BTCUSDT', data_type=DataType.SPOT, period='2024-04', error='not_found'),
            DownloadResult(success=False, symbol='BTCUSDT', data_type=DataType.SPOT, period='2024-05', error='not_found'),
            DownloadResult(success=False, symbol='BTCUSDT', data_type=DataType.SPOT, period='2024-06', error='not_found'),
        ]

        filtered = downloader._detect_gaps(results, threshold=3)

        # Should truncate at index 3 (first of 3 consecutive failures)
        assert len(filtered) == 3
        assert filtered[0].period == '2024-01'
        assert filtered[1].period == '2024-02'
        assert filtered[2].period == '2024-03'

    def test_no_gap_with_non_not_found_errors(self, klines_dataset, tmp_path):
        """Test that non-404 errors don't trigger gap detection."""
        downloader = BinanceDataVisionDownloader(
            dataset=klines_dataset,
            temp_path=tmp_path
        )

        # success, network_error, network_error, network_error (not gap - different error)
        results = [
            DownloadResult(success=True, symbol='BTCUSDT', data_type=DataType.SPOT, period='2024-01'),
            DownloadResult(success=False, symbol='BTCUSDT', data_type=DataType.SPOT, period='2024-02', error='timeout'),
            DownloadResult(success=False, symbol='BTCUSDT', data_type=DataType.SPOT, period='2024-03', error='timeout'),
            DownloadResult(success=False, symbol='BTCUSDT', data_type=DataType.SPOT, period='2024-04', error='timeout'),
        ]

        filtered = downloader._detect_gaps(results, threshold=3)

        # All results pass through (timeouts != not_found)
        assert len(filtered) == 4

    def test_empty_results(self, klines_dataset, tmp_path):
        """Test gap detection with empty results."""
        downloader = BinanceDataVisionDownloader(
            dataset=klines_dataset,
            temp_path=tmp_path
        )

        results = []
        filtered = downloader._detect_gaps(results, threshold=3)

        assert len(filtered) == 0

    def test_all_failures_no_success(self, klines_dataset, tmp_path):
        """Test gap detection when all results are failures."""
        downloader = BinanceDataVisionDownloader(
            dataset=klines_dataset,
            temp_path=tmp_path
        )

        # All failures - no gap because no first success
        results = [
            DownloadResult(success=False, symbol='BTCUSDT', data_type=DataType.SPOT, period='2024-01', error='not_found'),
            DownloadResult(success=False, symbol='BTCUSDT', data_type=DataType.SPOT, period='2024-02', error='not_found'),
            DownloadResult(success=False, symbol='BTCUSDT', data_type=DataType.SPOT, period='2024-03', error='not_found'),
        ]

        filtered = downloader._detect_gaps(results, threshold=2)

        # All results pass through (no gap because no successful download)
        assert len(filtered) == 3

    def test_threshold_zero_disables_gap_detection(self, klines_dataset, tmp_path):
        """Test that threshold=0 disables gap detection."""
        downloader = BinanceDataVisionDownloader(
            dataset=klines_dataset,
            temp_path=tmp_path
        )

        # Would normally trigger gap at threshold=3
        results = [
            DownloadResult(success=True, symbol='BTCUSDT', data_type=DataType.SPOT, period='2024-01'),
            DownloadResult(success=False, symbol='BTCUSDT', data_type=DataType.SPOT, period='2024-02', error='not_found'),
            DownloadResult(success=False, symbol='BTCUSDT', data_type=DataType.SPOT, period='2024-03', error='not_found'),
            DownloadResult(success=False, symbol='BTCUSDT', data_type=DataType.SPOT, period='2024-04', error='not_found'),
        ]

        filtered = downloader._detect_gaps(results, threshold=0)

        # All results pass through with threshold=0
        assert len(filtered) == 4

    def test_threshold_one_very_strict(self, klines_dataset, tmp_path):
        """Test gap detection with threshold=1 (very strict)."""
        downloader = BinanceDataVisionDownloader(
            dataset=klines_dataset,
            temp_path=tmp_path
        )

        # success, fail triggers gap immediately
        results = [
            DownloadResult(success=True, symbol='BTCUSDT', data_type=DataType.SPOT, period='2024-01'),
            DownloadResult(success=False, symbol='BTCUSDT', data_type=DataType.SPOT, period='2024-02', error='not_found'),
            DownloadResult(success=True, symbol='BTCUSDT', data_type=DataType.SPOT, period='2024-03'),
        ]

        filtered = downloader._detect_gaps(results, threshold=1)

        # Should truncate at index 1
        assert len(filtered) == 1
        assert filtered[0].period == '2024-01'


# -----------------------------------------------------------------------------
# TestBinanceDataVisionDownloaderInit
# -----------------------------------------------------------------------------

class TestBinanceDataVisionDownloaderInit:
    """Test BinanceDataVisionDownloader initialization."""

    def test_init_with_defaults(self, klines_dataset, tmp_path):
        """Test initialization with default concurrency."""
        downloader = BinanceDataVisionDownloader(
            dataset=klines_dataset,
            temp_path=tmp_path
        )

        assert downloader.dataset is klines_dataset
        assert downloader.temp_path == tmp_path
        assert downloader.max_concurrent == klines_dataset.default_max_concurrent

    def test_init_with_custom_concurrency(self, klines_dataset, tmp_path):
        """Test initialization with custom concurrency."""
        downloader = BinanceDataVisionDownloader(
            dataset=klines_dataset,
            temp_path=tmp_path,
            max_concurrent=50
        )

        assert downloader.max_concurrent == 50


# -----------------------------------------------------------------------------
# TestBinanceDataVisionDownloaderDownloadFile
# -----------------------------------------------------------------------------

class TestBinanceDataVisionDownloaderDownloadFile:
    """Test Binance Data Vision file download behavior."""

    @pytest.mark.asyncio
    async def test_404_returns_false(self, klines_dataset, tmp_path):
        downloader = BinanceDataVisionDownloader(dataset=klines_dataset, temp_path=tmp_path)

        mock_response = AsyncMock()
        mock_response.status = 404
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)

        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_response)
        mock_session.close = AsyncMock()

        async with downloader:
            downloader._session = mock_session
            result = await downloader._download_file(
                "https://data.binance.vision/test.zip",
                tmp_path / "test.zip",
            )

        assert result is False

    @pytest.mark.asyncio
    async def test_successful_download_writes_file(self, klines_dataset, tmp_path):
        downloader = BinanceDataVisionDownloader(dataset=klines_dataset, temp_path=tmp_path)
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, mode='w') as zf:
            zf.writestr("test.csv", "a,b\n1,2\n")
        zip_content = buffer.getvalue()

        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.headers = {'Content-Length': str(len(zip_content))}
        mock_response.read = AsyncMock(return_value=zip_content)
        mock_response.raise_for_status = MagicMock()
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)

        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_response)
        mock_session.close = AsyncMock()

        output_path = tmp_path / "test.zip"
        async with downloader:
            downloader._session = mock_session
            result = await downloader._download_file(
                "https://data.binance.vision/test.zip",
                output_path,
            )

        assert result is True
        assert output_path.read_bytes() == zip_content

    @pytest.mark.asyncio
    async def test_partial_download_returns_false(self, klines_dataset, tmp_path):
        downloader = BinanceDataVisionDownloader(dataset=klines_dataset, temp_path=tmp_path)
        zip_content = b"PK\x03\x04" + b"\x00" * 50

        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.headers = {'Content-Length': '1000'}
        mock_response.read = AsyncMock(return_value=zip_content)
        mock_response.raise_for_status = MagicMock()
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)

        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_response)
        mock_session.close = AsyncMock()

        output_path = tmp_path / "test.zip"
        async with downloader:
            downloader._session = mock_session
            result = await downloader._download_file(
                "https://data.binance.vision/test.zip",
                output_path,
            )

        assert result is False
        assert not output_path.exists()

    @pytest.mark.asyncio
    async def test_small_non_zip_payload_returns_false(self, klines_dataset, tmp_path):
        downloader = BinanceDataVisionDownloader(dataset=klines_dataset, temp_path=tmp_path)
        # Small payload that is not a valid ZIP (e.g., tiny HTML error body).
        non_zip_content = b"<html/>"

        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.headers = {'Content-Length': str(len(non_zip_content))}
        mock_response.read = AsyncMock(return_value=non_zip_content)
        mock_response.raise_for_status = MagicMock()
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)

        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_response)
        mock_session.close = AsyncMock()

        output_path = tmp_path / "test.zip"
        temp_path = output_path.with_suffix('.tmp')
        async with downloader:
            downloader._session = mock_session
            result = await downloader._download_file(
                "https://data.binance.vision/test.zip",
                output_path,
            )

        assert result is False
        assert not output_path.exists()
        assert not temp_path.exists()


# -----------------------------------------------------------------------------
# TestTickerMappingCache
# -----------------------------------------------------------------------------

class TestTickerMappingCache:
    """Test ticker mapping cache functions."""

    def test_get_nonexistent_mapping_returns_none(self):
        """Test getting a mapping that doesn't exist."""
        assert get_ticker_mapping('BTCUSDT') is None

    def test_set_and_get_mapping(self):
        """Test setting and getting a mapping."""
        set_ticker_mapping('PEPEUSDT', '1000PEPEUSDT')

        assert get_ticker_mapping('PEPEUSDT') == '1000PEPEUSDT'
        assert get_ticker_mapping('BTCUSDT') is None

    def test_clear_mappings(self):
        """Test clearing all mappings."""
        set_ticker_mapping('PEPEUSDT', '1000PEPEUSDT')
        set_ticker_mapping('SHIBUSDT', '1000SHIBUSDT')

        clear_ticker_mappings()

        assert get_ticker_mapping('PEPEUSDT') is None
        assert get_ticker_mapping('SHIBUSDT') is None

    def test_overwrite_mapping(self):
        """Test overwriting an existing mapping."""
        set_ticker_mapping('PEPEUSDT', '1000PEPEUSDT')
        set_ticker_mapping('PEPEUSDT', 'UPDATED')

        assert get_ticker_mapping('PEPEUSDT') == 'UPDATED'


# -----------------------------------------------------------------------------
# TestDownloadResultIsNotFound
# -----------------------------------------------------------------------------

class TestDownloadResultIsNotFound:
    """Test DownloadResult.is_not_found property."""

    def test_success_is_not_not_found(self):
        """Test that successful results are not 'not_found'."""
        result = DownloadResult(
            success=True,
            symbol='BTCUSDT',
            data_type=DataType.SPOT,
            period='2024-01'
        )
        assert result.is_not_found is False

    def test_failure_with_not_found_error(self):
        """Test that failure with 'not_found' error is detected."""
        result = DownloadResult(
            success=False,
            symbol='BTCUSDT',
            data_type=DataType.SPOT,
            period='2024-01',
            error='not_found'
        )
        assert result.is_not_found is True

    def test_failure_with_other_error(self):
        """Test that failure with other error is not 'not_found'."""
        result = DownloadResult(
            success=False,
            symbol='BTCUSDT',
            data_type=DataType.SPOT,
            period='2024-01',
            error='timeout'
        )
        assert result.is_not_found is False
