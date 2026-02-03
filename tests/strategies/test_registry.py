"""
Tests for strategy registry.

Tests the get_strategy() factory function that creates appropriate
strategies based on DataType enums.
"""

import pytest

from crypto_data.enums import DataType, Interval
from crypto_data.strategies import (
    FundingRatesStrategy,
    KlinesStrategy,
    OpenInterestStrategy,
    get_strategy,
)


class TestGetStrategySpot:
    """Tests for get_strategy with SPOT data type."""

    def test_returns_klines_strategy(self):
        """get_strategy(SPOT, interval) returns KlinesStrategy."""
        strategy = get_strategy(DataType.SPOT, Interval.MIN_5)

        assert isinstance(strategy, KlinesStrategy)

    def test_strategy_has_spot_data_type(self):
        """Returned strategy has SPOT data type."""
        strategy = get_strategy(DataType.SPOT, Interval.HOUR_1)

        assert strategy.data_type == DataType.SPOT

    def test_strategy_has_correct_table_name(self):
        """Returned strategy has 'spot' table name."""
        strategy = get_strategy(DataType.SPOT, Interval.MIN_5)

        assert strategy.table_name == 'spot'

    def test_strategy_has_correct_interval(self):
        """Returned strategy has the specified interval."""
        strategy = get_strategy(DataType.SPOT, Interval.HOUR_4)

        assert strategy.interval == Interval.HOUR_4


class TestGetStrategyFutures:
    """Tests for get_strategy with FUTURES data type."""

    def test_returns_klines_strategy(self):
        """get_strategy(FUTURES, interval) returns KlinesStrategy."""
        strategy = get_strategy(DataType.FUTURES, Interval.MIN_15)

        assert isinstance(strategy, KlinesStrategy)

    def test_strategy_has_futures_data_type(self):
        """Returned strategy has FUTURES data type."""
        strategy = get_strategy(DataType.FUTURES, Interval.DAY_1)

        assert strategy.data_type == DataType.FUTURES

    def test_strategy_has_correct_table_name(self):
        """Returned strategy has 'futures' table name."""
        strategy = get_strategy(DataType.FUTURES, Interval.MIN_5)

        assert strategy.table_name == 'futures'

    def test_strategy_has_correct_interval(self):
        """Returned strategy has the specified interval."""
        strategy = get_strategy(DataType.FUTURES, Interval.HOUR_12)

        assert strategy.interval == Interval.HOUR_12


class TestGetStrategyOpenInterest:
    """Tests for get_strategy with OPEN_INTEREST data type."""

    def test_returns_open_interest_strategy(self):
        """get_strategy(OPEN_INTEREST) returns OpenInterestStrategy."""
        strategy = get_strategy(DataType.OPEN_INTEREST)

        assert isinstance(strategy, OpenInterestStrategy)

    def test_strategy_has_open_interest_data_type(self):
        """Returned strategy has OPEN_INTEREST data type."""
        strategy = get_strategy(DataType.OPEN_INTEREST)

        assert strategy.data_type == DataType.OPEN_INTEREST

    def test_strategy_has_correct_table_name(self):
        """Returned strategy has 'open_interest' table name."""
        strategy = get_strategy(DataType.OPEN_INTEREST)

        assert strategy.table_name == 'open_interest'

    def test_interval_is_ignored(self):
        """Interval parameter is ignored for OPEN_INTEREST."""
        # Should not raise - interval is optional and ignored
        strategy = get_strategy(DataType.OPEN_INTEREST, Interval.MIN_5)

        assert isinstance(strategy, OpenInterestStrategy)


class TestGetStrategyFundingRates:
    """Tests for get_strategy with FUNDING_RATES data type."""

    def test_returns_funding_rates_strategy(self):
        """get_strategy(FUNDING_RATES) returns FundingRatesStrategy."""
        strategy = get_strategy(DataType.FUNDING_RATES)

        assert isinstance(strategy, FundingRatesStrategy)

    def test_strategy_has_funding_rates_data_type(self):
        """Returned strategy has FUNDING_RATES data type."""
        strategy = get_strategy(DataType.FUNDING_RATES)

        assert strategy.data_type == DataType.FUNDING_RATES

    def test_strategy_has_correct_table_name(self):
        """Returned strategy has 'funding_rates' table name."""
        strategy = get_strategy(DataType.FUNDING_RATES)

        assert strategy.table_name == 'funding_rates'

    def test_interval_is_ignored(self):
        """Interval parameter is ignored for FUNDING_RATES."""
        # Should not raise - interval is optional and ignored
        strategy = get_strategy(DataType.FUNDING_RATES, Interval.HOUR_1)

        assert isinstance(strategy, FundingRatesStrategy)


class TestGetStrategyMissingInterval:
    """Tests for missing interval parameter on klines data types."""

    def test_spot_without_interval_raises_value_error(self):
        """get_strategy(SPOT) without interval raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            get_strategy(DataType.SPOT)

        assert "interval is required for SPOT data type" in str(exc_info.value)

    def test_futures_without_interval_raises_value_error(self):
        """get_strategy(FUTURES) without interval raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            get_strategy(DataType.FUTURES)

        assert "interval is required for FUTURES data type" in str(exc_info.value)
