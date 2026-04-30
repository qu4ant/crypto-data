"""
Binance Data Vision batch downloader.

Consolidates the 3 download patterns (klines, open_interest, funding_rates)
into a single reusable class with:
- Parallel downloads with asyncio.gather
- Gap detection (consecutive 404s = delisting)
- 1000-prefix auto-discovery (PEPEUSDT -> 1000PEPEUSDT)
"""

from __future__ import annotations

import asyncio
import logging
import threading
import zipfile
from pathlib import Path

import aiohttp

from crypto_data.binance_datasets.base import BinanceDatasetStrategy, DownloadResult, Period
from crypto_data.enums import DataType

logger = logging.getLogger(__name__)

# Global cache for auto-discovered 1000-prefix ticker mappings (e.g., PEPEUSDT -> 1000PEPEUSDT)
# Persists across multiple downloads in the same session to avoid repeated 404s
_ticker_mappings: dict[str, str] = {}
_ticker_mappings_lock = threading.Lock()


def get_ticker_mapping(symbol: str) -> str | None:
    """
    Get cached ticker mapping for a symbol.

    Parameters
    ----------
    symbol : str
        The original symbol (e.g., 'PEPEUSDT')

    Returns
    -------
    Optional[str]
        The mapped symbol if found (e.g., '1000PEPEUSDT'), None otherwise
    """
    with _ticker_mappings_lock:
        return _ticker_mappings.get(symbol)


def set_ticker_mapping(original: str, mapped: str) -> None:
    """
    Cache a ticker mapping.

    Parameters
    ----------
    original : str
        The original symbol (e.g., 'PEPEUSDT')
    mapped : str
        The mapped symbol (e.g., '1000PEPEUSDT')
    """
    with _ticker_mappings_lock:
        _ticker_mappings[original] = mapped


def clear_ticker_mappings() -> None:
    """Clear all cached ticker mappings. Useful for testing."""
    with _ticker_mappings_lock:
        _ticker_mappings.clear()


def _should_retry_with_1000_prefix(data_type: DataType, symbol: str) -> bool:
    """Return True when 1000-prefix futures-style auto-discovery is plausible."""
    if data_type == DataType.SPOT:
        return False
    if symbol.startswith("1000"):
        return False

    base_symbol = symbol.removesuffix("USDT")
    return bool(base_symbol) and not base_symbol[0].isdigit()


class BinanceDataVisionDownloader:
    """
    Binance Data Vision downloader for one dataset.

    Handles:
    - Parallel downloads with configurable concurrency
    - Gap detection (consecutive 404s indicate delisting)
    - 1000-prefix auto-discovery for tokens like PEPE, SHIB, BONK

    Parameters
    ----------
    dataset : BinanceDatasetStrategy
        The Binance dataset handler (klines, open_interest, funding_rates)
    temp_path : Path
        Temporary directory for downloaded files
    max_concurrent : Optional[int]
        Maximum concurrent downloads. Defaults to the dataset default.

    Examples
    --------
    >>> async with BinanceDataVisionDownloader(
    ...     dataset=BinanceKlinesDataset(DataType.SPOT, Interval.MIN_5),
    ...     temp_path=Path('/tmp/downloads')
    ... ) as downloader:
    ...     results = await downloader.download_symbol(
    ...         symbol='BTCUSDT',
    ...         periods=[Period('2024-01'), Period('2024-02')],
    ...         interval='5m'
    ...     )
    """

    def __init__(
        self,
        dataset: BinanceDatasetStrategy,
        temp_path: Path,
        max_concurrent: int | None = None,
        base_url: str = "https://data.binance.vision/",
        timeout: int = 30,
    ):
        """
        Initialize the Binance Data Vision downloader.

        Parameters
        ----------
        dataset : BinanceDatasetStrategy
            The Binance dataset handler (klines, open_interest, funding_rates)
        temp_path : Path
            Temporary directory for downloaded files
        max_concurrent : Optional[int]
            Maximum concurrent downloads. Defaults to the dataset default.
        base_url : str
            Binance Data Vision base URL.
        timeout : int
            Request timeout in seconds.
        """
        self.dataset = dataset
        self.temp_path = temp_path
        self.max_concurrent = max_concurrent or dataset.default_max_concurrent
        self.base_url = base_url
        self._timeout = aiohttp.ClientTimeout(total=timeout)
        self._session: aiohttp.ClientSession | None = None
        self._semaphore: asyncio.Semaphore | None = None

    async def __aenter__(self) -> BinanceDataVisionDownloader:
        """Create the shared HTTP session and concurrency limiter."""
        self._session = aiohttp.ClientSession(timeout=self._timeout)
        self._semaphore = asyncio.Semaphore(self.max_concurrent)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object | None,
    ) -> None:
        """Close the shared HTTP session."""
        if self._session:
            try:
                await self._session.close()
            finally:
                self._session = None
                self._semaphore = None

    async def download_symbol(
        self,
        symbol: str,
        periods: list[Period],
        interval: str | None = None,
        failure_threshold: int = 3,
    ) -> list[DownloadResult]:
        """
        Download all periods for a symbol in parallel.

        Parameters
        ----------
        symbol : str
            Symbol to download (e.g., 'BTCUSDT')
        periods : List[Period]
            List of periods to download
        interval : Optional[str]
            Kline interval (required for klines, ignored for metrics)
        failure_threshold : int
            Stop after N consecutive 404s (gap detection). Default: 3.
            Set to 0 to disable gap detection.

        Returns
        -------
        List[DownloadResult]
            Download results for each period
        """
        if not periods:
            logger.debug(f"No periods to download for {symbol}")
            return []

        # Check for cached 1000-prefix mapping
        cached_mapping = get_ticker_mapping(symbol)
        download_symbol = cached_mapping or symbol

        if cached_mapping:
            logger.debug(f"Using cached mapping: {symbol} -> {download_symbol}")

        # Build download tasks for all periods
        tasks = [self._download_single(download_symbol, period, interval) for period in periods]

        # Execute downloads in parallel
        raw_results = await asyncio.gather(*tasks, return_exceptions=True)

        # Convert exceptions to DownloadResult with error
        results = []
        for i, result in enumerate(raw_results):
            if isinstance(result, Exception):
                period = periods[i]
                logger.error(f"Exception downloading {symbol} {period}: {result}")
                results.append(
                    DownloadResult(
                        success=False,
                        symbol=symbol,
                        data_type=self.dataset.data_type,
                        period=str(period),
                        file_path=None,
                        error=str(result),
                    )
                )
            else:
                # Update symbol to original (for 1000-prefix normalization)
                result_with_original = DownloadResult(
                    success=result.success,
                    symbol=symbol,
                    data_type=result.data_type,
                    period=result.period,
                    file_path=result.file_path,
                    error=result.error,
                )
                results.append(result_with_original)

        # Apply gap detection
        if failure_threshold > 0:
            results = self._detect_gaps(results, failure_threshold)

        # If ALL results are not_found AND no cached mapping, try with 1000-prefix
        if (
            cached_mapping is None
            and _should_retry_with_1000_prefix(self.dataset.data_type, symbol)
            and len(results) > 0
            and all(r.is_not_found for r in results)
        ):
            retry_results = await self._retry_with_prefix(
                symbol, periods, interval, failure_threshold
            )
            if retry_results is not None:
                results = retry_results

        return results

    async def _download_single(
        self, symbol: str, period: Period, interval: str | None = None
    ) -> DownloadResult:
        """
        Download a single period.

        Parameters
        ----------
        symbol : str
            Symbol to download (may include 1000-prefix)
        period : Period
            Period to download
        interval : Optional[str]
            Kline interval (if applicable)

        Returns
        -------
        DownloadResult
            Result of the download attempt
        """
        # Build URL and output path
        url = self.dataset.build_download_url(
            base_url=self.base_url, symbol=symbol, period=period, interval=interval
        )
        filename = self.dataset.build_temp_filename(symbol, period, interval)
        output_path = self.temp_path / filename

        try:
            success = await self._download_file(url, output_path)

            if success:
                return DownloadResult(
                    success=True,
                    symbol=symbol,
                    data_type=self.dataset.data_type,
                    period=str(period),
                    file_path=output_path,
                    error=None,
                )
            # 404 - file not found
            return DownloadResult(
                success=False,
                symbol=symbol,
                data_type=self.dataset.data_type,
                period=str(period),
                file_path=None,
                error="not_found",
            )
        except Exception as e:
            logger.error(f"Download error {symbol} {period}: {e}")
            return DownloadResult(
                success=False,
                symbol=symbol,
                data_type=self.dataset.data_type,
                period=str(period),
                file_path=None,
                error=str(e),
            )

    async def _download_file(self, url: str, output_path: Path) -> bool:
        """
        Download one Binance Data Vision file with basic integrity checks.

        Returns False for 404 and corrupt/partial files. Raises for other HTTP
        and network errors so the caller can record the failure reason.
        """
        if not self._session:
            raise RuntimeError(
                "BinanceDataVisionDownloader must be used as an async context manager"
            )

        if not self._semaphore:
            self._semaphore = asyncio.Semaphore(self.max_concurrent)

        logger.debug(f"Downloading: {url}")

        async with self._semaphore, self._session.get(url) as response:
            if response.status == 404:
                logger.debug(f"  File not found (404): {url}")
                return False

            response.raise_for_status()
            content = await response.read()

            content_length = response.headers.get("Content-Length")
            if content_length:
                try:
                    expected_size = int(content_length)
                except (ValueError, TypeError):
                    logger.debug(f"  Invalid Content-Length header: {content_length}")
                else:
                    if len(content) != expected_size:
                        logger.error(
                            f"  Partial download detected: {len(content)}/{expected_size} bytes"
                        )
                        return False

            temp_path = output_path.with_suffix(".tmp")
            temp_path.parent.mkdir(parents=True, exist_ok=True)
            temp_path.write_bytes(content)

            if not zipfile.is_zipfile(temp_path):
                logger.error(f"  Corrupt ZIP detected: {output_path.name}")
                temp_path.unlink()
                return False

            temp_path.rename(output_path)
            logger.debug(f"  Downloaded: {len(content)} bytes (validated)")
            return True

    def _detect_gaps(self, results: list[DownloadResult], threshold: int) -> list[DownloadResult]:
        """
        Detect gaps in download results and truncate at first gap.

        Gap detection logic:
        1. Find the first successful download (token launch)
        2. Scan chronologically for consecutive not_found after first success
        3. If threshold reached, truncate results at gap start

        Leading failures (before first success) are NOT counted as gaps -
        they represent periods before the token existed.

        Parameters
        ----------
        results : List[DownloadResult]
            Download results to analyze
        threshold : int
            Number of consecutive failures to trigger gap detection

        Returns
        -------
        List[DownloadResult]
            Potentially truncated results
        """
        if not results or threshold <= 0:
            return results

        # Step 1: Find first successful download (token launch)
        first_success_idx = -1
        for i, result in enumerate(results):
            if result.success:
                first_success_idx = i
                break

        # If no successes, return all results
        if first_success_idx == -1:
            return results

        # Step 2: Scan for consecutive not_found after first success
        consecutive_failures = 0
        gap_start_idx = -1

        for i in range(first_success_idx + 1, len(results)):
            result = results[i]

            if result.is_not_found:
                consecutive_failures += 1

                if consecutive_failures == 1:
                    gap_start_idx = i

                if consecutive_failures >= threshold:
                    # Gap detected - truncate at gap start
                    gap_result = results[gap_start_idx]
                    last_success = results[gap_start_idx - 1]
                    logger.warning(
                        f"{gap_result.symbol}: data ends at {last_success.period} "
                        f"({consecutive_failures} consecutive missing periods). "
                        f"Possible causes: delisting, rebrand, or exchange maintenance."
                    )
                    return results[:gap_start_idx]
            else:
                # Reset counter on success or other error
                consecutive_failures = 0
                gap_start_idx = -1

        return results

    async def _retry_with_prefix(
        self, symbol: str, periods: list[Period], interval: str | None, failure_threshold: int
    ) -> list[DownloadResult] | None:
        """
        Retry download with 1000-prefix.

        Used for tokens like PEPE, SHIB, BONK that use 1000PEPEUSDT format.

        Parameters
        ----------
        symbol : str
            Original symbol (e.g., 'PEPEUSDT')
        periods : List[Period]
            Periods to download
        interval : Optional[str]
            Kline interval (if applicable)
        failure_threshold : int
            Gap detection threshold

        Returns
        -------
        Optional[List[DownloadResult]]
            Results if retry succeeded, None if retry also failed
        """
        prefixed_symbol = f"1000{symbol}"
        logger.info(f"Retrying with 1000-prefix: {prefixed_symbol}")

        # Build download tasks
        tasks = [self._download_single(prefixed_symbol, period, interval) for period in periods]

        # Execute downloads
        raw_results = await asyncio.gather(*tasks, return_exceptions=True)

        # Convert exceptions to DownloadResult
        results = []
        for i, result in enumerate(raw_results):
            if isinstance(result, Exception):
                period = periods[i]
                results.append(
                    DownloadResult(
                        success=False,
                        symbol=symbol,  # Use original symbol
                        data_type=self.dataset.data_type,
                        period=str(period),
                        file_path=None,
                        error=str(result),
                    )
                )
            else:
                # Update symbol to original (for normalization)
                results.append(
                    DownloadResult(
                        success=result.success,
                        symbol=symbol,  # Use original symbol
                        data_type=result.data_type,
                        period=result.period,
                        file_path=result.file_path,
                        error=result.error,
                    )
                )

        # Check if any succeeded
        if any(r.success for r in results):
            # Cache the mapping
            set_ticker_mapping(symbol, prefixed_symbol)
            logger.info(f"Auto-discovered mapping: {symbol} -> {prefixed_symbol}")

            # Apply gap detection
            if failure_threshold > 0:
                results = self._detect_gaps(results, failure_threshold)

            return results

        logger.debug(f"Retry with {prefixed_symbol} also failed")
        return None
