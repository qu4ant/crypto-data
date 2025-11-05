"""
Tests for Custom Check Functions

Tests individual check functions used in schemas.
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime

from crypto_data.schemas.checks import (
    check_ohlc_relationships,
    check_price_continuity,
    check_volume_outliers,
    check_timestamp_monotonic,
    check_no_duplicate_ranks_per_date
)


@pytest.mark.schema
class TestOHLCRelationshipsCheck:
    """Tests for OHLC relationships check function"""

    def test_valid_ohlc_passes(self):
        """Test that valid OHLC relationships pass"""
        df = pd.DataFrame({
            'open': [100.0, 200.0],
            'high': [110.0, 210.0],
            'low': [90.0, 190.0],
            'close': [105.0, 205.0]
        })
        assert check_ohlc_relationships(df) is True

    def test_high_less_than_low_fails(self):
        """Test that high < low fails"""
        df = pd.DataFrame({
            'open': [100.0],
            'high': [90.0],  # Less than low
            'low': [95.0],
            'close': [105.0]
        })
        assert check_ohlc_relationships(df) is False

    def test_high_less_than_open_fails(self):
        """Test that high < open fails"""
        df = pd.DataFrame({
            'open': [110.0],  # Greater than high
            'high': [105.0],
            'low': [90.0],
            'close': [100.0]
        })
        assert check_ohlc_relationships(df) is False

    def test_low_greater_than_close_fails(self):
        """Test that low > close fails"""
        df = pd.DataFrame({
            'open': [100.0],
            'high': [110.0],
            'low': [105.0],  # Greater than close
            'close': [100.0]
        })
        assert check_ohlc_relationships(df) is False

    def test_empty_dataframe_passes(self):
        """Test that empty DataFrame passes"""
        df = pd.DataFrame({
            'open': [],
            'high': [],
            'low': [],
            'close': []
        })
        assert check_ohlc_relationships(df) is True


@pytest.mark.schema
class TestPriceContinuityCheck:
    """Tests for price continuity check function"""

    def test_normal_returns_pass(self):
        """Test that normal returns pass"""
        df = pd.DataFrame({
            'close': [100.0, 101.0, 99.5, 100.5, 102.0]
        })
        assert check_price_continuity(df, sigma=5.0) is True

    def test_extreme_jump_fails(self):
        """Test that extreme price jump fails"""
        df = pd.DataFrame({
            'close': [100.0, 101.0, 500.0, 502.0, 505.0]  # Huge jump
        })
        # Should fail (5x jump is way beyond 5 sigma)
        assert check_price_continuity(df, sigma=5.0) is False

    def test_single_row_passes(self):
        """Test that single row passes (no returns to check)"""
        df = pd.DataFrame({
            'close': [100.0]
        })
        assert check_price_continuity(df, sigma=5.0) is True

    def test_empty_dataframe_passes(self):
        """Test that empty DataFrame passes"""
        df = pd.DataFrame({
            'close': []
        })
        assert check_price_continuity(df, sigma=5.0) is True


@pytest.mark.schema
class TestVolumeOutliersCheck:
    """Tests for volume outliers check function"""

    def test_normal_volumes_pass(self):
        """Test that normal volumes pass"""
        df = pd.DataFrame({
            'volume': [100.0, 110.0, 95.0, 105.0, 98.0]
        })
        assert check_volume_outliers(df, iqr_multiplier=3.0) is True

    def test_extreme_outlier_fails(self):
        """Test that extreme outlier fails"""
        df = pd.DataFrame({
            'volume': [100.0, 110.0, 95.0, 10000.0, 98.0]  # Huge outlier
        })
        assert check_volume_outliers(df, iqr_multiplier=3.0) is False


@pytest.mark.schema
class TestTimestampMonotonicCheck:
    """Tests for timestamp monotonic check function"""

    def test_monotonic_timestamps_pass(self):
        """Test that monotonic increasing timestamps pass"""
        df = pd.DataFrame({
            'timestamp': pd.date_range('2024-01-01', periods=5, freq='5T')
        })
        assert check_timestamp_monotonic(df) is True

    def test_decreasing_timestamps_fail(self):
        """Test that decreasing timestamps fail"""
        df = pd.DataFrame({
            'timestamp': [
                pd.Timestamp('2024-01-01 00:00:00'),
                pd.Timestamp('2024-01-01 00:05:00'),
                pd.Timestamp('2024-01-01 00:03:00'),  # Goes backwards
                pd.Timestamp('2024-01-01 00:10:00')
            ]
        })
        assert check_timestamp_monotonic(df) is False


@pytest.mark.schema
class TestRankChecks:
    """Tests for universe rank check functions"""

    def test_no_duplicate_ranks_passes(self):
        """Test that unique ranks pass"""
        df = pd.DataFrame({
            'date': [datetime(2024, 1, 1)] * 3,
            'rank': [1, 2, 3]
        })
        assert check_no_duplicate_ranks_per_date(df) is True

    def test_duplicate_ranks_fails(self):
        """Test that duplicate ranks fail"""
        df = pd.DataFrame({
            'date': [datetime(2024, 1, 1)] * 3,
            'rank': [1, 1, 3]  # Duplicate rank 1
        })
        assert check_no_duplicate_ranks_per_date(df) is False
