"""
Tests for dataset registry.

Tests the get_binance_dataset_strategy() factory function that creates appropriate
datasets based on DataType enums.
"""

import pytest

from crypto_data.binance_datasets import (
    BinanceFundingRatesDataset,
    BinanceKlinesDataset,
    BinanceOpenInterestDataset,
    get_binance_dataset_strategy,
)
from crypto_data.enums import DataType, Interval


class TestGetDatasetSpot:
    """Tests for get_binance_dataset_strategy with SPOT data type."""

    def test_returns_klines_dataset(self):
        """get_binance_dataset_strategy(SPOT, interval) returns BinanceKlinesDataset."""
        dataset = get_binance_dataset_strategy(DataType.SPOT, Interval.MIN_5)

        assert isinstance(dataset, BinanceKlinesDataset)

    def test_dataset_has_spot_data_type(self):
        """Returned dataset has SPOT data type."""
        dataset = get_binance_dataset_strategy(DataType.SPOT, Interval.HOUR_1)

        assert dataset.data_type == DataType.SPOT

    def test_dataset_has_correct_table_name(self):
        """Returned dataset has 'spot' table name."""
        dataset = get_binance_dataset_strategy(DataType.SPOT, Interval.MIN_5)

        assert dataset.table_name == "spot"

    def test_dataset_has_correct_interval(self):
        """Returned dataset has the specified interval."""
        dataset = get_binance_dataset_strategy(DataType.SPOT, Interval.HOUR_4)

        assert dataset.interval == Interval.HOUR_4


class TestGetDatasetFutures:
    """Tests for get_binance_dataset_strategy with FUTURES data type."""

    def test_returns_klines_dataset(self):
        """get_binance_dataset_strategy(FUTURES, interval) returns BinanceKlinesDataset."""
        dataset = get_binance_dataset_strategy(DataType.FUTURES, Interval.MIN_15)

        assert isinstance(dataset, BinanceKlinesDataset)

    def test_dataset_has_futures_data_type(self):
        """Returned dataset has FUTURES data type."""
        dataset = get_binance_dataset_strategy(DataType.FUTURES, Interval.DAY_1)

        assert dataset.data_type == DataType.FUTURES

    def test_dataset_has_correct_table_name(self):
        """Returned dataset has 'futures' table name."""
        dataset = get_binance_dataset_strategy(DataType.FUTURES, Interval.MIN_5)

        assert dataset.table_name == "futures"

    def test_dataset_has_correct_interval(self):
        """Returned dataset has the specified interval."""
        dataset = get_binance_dataset_strategy(DataType.FUTURES, Interval.HOUR_12)

        assert dataset.interval == Interval.HOUR_12


class TestGetDatasetOpenInterest:
    """Tests for get_binance_dataset_strategy with OPEN_INTEREST data type."""

    def test_returns_open_interest_dataset(self):
        """get_binance_dataset_strategy(OPEN_INTEREST) returns BinanceOpenInterestDataset."""
        dataset = get_binance_dataset_strategy(DataType.OPEN_INTEREST)

        assert isinstance(dataset, BinanceOpenInterestDataset)

    def test_dataset_has_open_interest_data_type(self):
        """Returned dataset has OPEN_INTEREST data type."""
        dataset = get_binance_dataset_strategy(DataType.OPEN_INTEREST)

        assert dataset.data_type == DataType.OPEN_INTEREST

    def test_dataset_has_correct_table_name(self):
        """Returned dataset has 'open_interest' table name."""
        dataset = get_binance_dataset_strategy(DataType.OPEN_INTEREST)

        assert dataset.table_name == "open_interest"

    def test_interval_is_ignored(self):
        """Interval parameter is ignored for OPEN_INTEREST."""
        # Should not raise - interval is optional and ignored
        dataset = get_binance_dataset_strategy(DataType.OPEN_INTEREST, Interval.MIN_5)

        assert isinstance(dataset, BinanceOpenInterestDataset)


class TestGetDatasetFundingRates:
    """Tests for get_binance_dataset_strategy with FUNDING_RATES data type."""

    def test_returns_funding_rates_dataset(self):
        """get_binance_dataset_strategy(FUNDING_RATES) returns BinanceFundingRatesDataset."""
        dataset = get_binance_dataset_strategy(DataType.FUNDING_RATES)

        assert isinstance(dataset, BinanceFundingRatesDataset)

    def test_dataset_has_funding_rates_data_type(self):
        """Returned dataset has FUNDING_RATES data type."""
        dataset = get_binance_dataset_strategy(DataType.FUNDING_RATES)

        assert dataset.data_type == DataType.FUNDING_RATES

    def test_dataset_has_correct_table_name(self):
        """Returned dataset has 'funding_rates' table name."""
        dataset = get_binance_dataset_strategy(DataType.FUNDING_RATES)

        assert dataset.table_name == "funding_rates"

    def test_interval_is_ignored(self):
        """Interval parameter is ignored for FUNDING_RATES."""
        # Should not raise - interval is optional and ignored
        dataset = get_binance_dataset_strategy(DataType.FUNDING_RATES, Interval.HOUR_1)

        assert isinstance(dataset, BinanceFundingRatesDataset)


class TestGetDatasetMissingInterval:
    """Tests for missing interval parameter on klines data types."""

    def test_spot_without_interval_raises_value_error(self):
        """get_binance_dataset_strategy(SPOT) without interval raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            get_binance_dataset_strategy(DataType.SPOT)

        assert "interval is required for SPOT data type" in str(exc_info.value)

    def test_futures_without_interval_raises_value_error(self):
        """get_binance_dataset_strategy(FUTURES) without interval raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            get_binance_dataset_strategy(DataType.FUTURES)

        assert "interval is required for FUTURES data type" in str(exc_info.value)
