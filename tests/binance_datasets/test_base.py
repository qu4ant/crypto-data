"""
Tests for base dataset abstractions.

Tests the Period dataclass, DownloadResult dataclass, and BinanceDatasetStrategy ABC.
"""

from pathlib import Path

import pytest

from crypto_data.binance_datasets.base import (
    BinanceDatasetStrategy,
    DownloadResult,
    Period,
)
from crypto_data.enums import DataType


class TestPeriod:
    """Tests for Period dataclass."""

    def test_monthly_period(self):
        """Monthly period with default is_monthly=True."""
        period = Period("2024-01")
        assert period.value == "2024-01"
        assert period.is_monthly is True

    def test_daily_period(self):
        """Daily period with is_monthly=False."""
        period = Period("2024-01-15", is_monthly=False)
        assert period.value == "2024-01-15"
        assert period.is_monthly is False

    def test_str_returns_value(self):
        """__str__ returns the period value."""
        monthly = Period("2024-06")
        daily = Period("2024-06-15", is_monthly=False)

        assert str(monthly) == "2024-06"
        assert str(daily) == "2024-06-15"


class TestDownloadResult:
    """Tests for DownloadResult dataclass."""

    def test_successful_result(self):
        """Successful download result with file path."""
        result = DownloadResult(
            success=True,
            symbol="BTCUSDT",
            data_type=DataType.SPOT,
            period="2024-01",
            file_path=Path("/tmp/BTCUSDT-5m-2024-01.zip"),
        )

        assert result.success is True
        assert result.symbol == "BTCUSDT"
        assert result.data_type == DataType.SPOT
        assert result.period == "2024-01"
        assert result.file_path == Path("/tmp/BTCUSDT-5m-2024-01.zip")
        assert result.error is None
        assert result.is_not_found is False

    def test_not_found_result(self):
        """Failed download due to 404 (not found)."""
        result = DownloadResult(
            success=False,
            symbol="NEWCOINUSDT",
            data_type=DataType.FUTURES,
            period="2024-01",
            error="not_found",
        )

        assert result.success is False
        assert result.symbol == "NEWCOINUSDT"
        assert result.data_type == DataType.FUTURES
        assert result.period == "2024-01"
        assert result.file_path is None
        assert result.error == "not_found"
        assert result.is_not_found is True

    def test_other_error_result(self):
        """Failed download due to other error (not 404)."""
        result = DownloadResult(
            success=False,
            symbol="BTCUSDT",
            data_type=DataType.OPEN_INTEREST,
            period="2024-01-15",
            error="network_timeout",
        )

        assert result.success is False
        assert result.symbol == "BTCUSDT"
        assert result.data_type == DataType.OPEN_INTEREST
        assert result.error == "network_timeout"
        assert result.is_not_found is False

    def test_is_not_found_requires_both_conditions(self):
        """is_not_found is False if success=True even with error='not_found'."""
        # Edge case: shouldn't happen in practice, but test the logic
        result = DownloadResult(
            success=True,
            symbol="BTCUSDT",
            data_type=DataType.SPOT,
            period="2024-01",
            file_path=Path("/tmp/file.zip"),
            error="not_found",  # This shouldn't happen, but test it
        )

        # is_not_found should be False because success=True
        assert result.is_not_found is False


class TestBinanceDatasetStrategyInterface:
    """Tests for BinanceDatasetStrategy abstract base class."""

    def test_cannot_instantiate_abc_directly(self):
        """BinanceDatasetStrategy cannot be instantiated directly."""
        with pytest.raises(TypeError, match="Can't instantiate abstract class"):
            BinanceDatasetStrategy()

    def test_abstract_methods_list(self):
        """Verify all expected abstract methods are defined."""
        # Get all abstract methods
        abstract_methods = set()
        for name, method in vars(BinanceDatasetStrategy).items():
            if getattr(method, "__isabstractmethod__", False):
                abstract_methods.add(name)

        expected_methods = {
            "data_type",
            "table_name",
            "is_monthly",
            "default_max_concurrent",
            "generate_periods",
            "get_schema",
            "build_download_url",
            "build_temp_filename",
            "parse_csv",
        }

        assert abstract_methods == expected_methods
