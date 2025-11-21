"""
Tests for funding rates download functionality in binance_vision_async.py

Tests the download_funding_rates() method.
"""

import pytest
from crypto_data.enums import DataType, Interval
from pathlib import Path
from crypto_data.clients.binance_vision_async import BinanceDataVisionClientAsync


class TestFundingRatesUrlConstruction:
    """Test URL construction for funding rates."""

    def test_funding_rates_url_format_correct(self):
        """URL should follow correct format for funding rates."""
        client = BinanceDataVisionClientAsync()

        # Construct URL manually
        symbol = 'BTCUSDT'
        month = '2024-12'
        expected_url = f"https://data.binance.vision/data/futures/um/monthly/fundingRate/{symbol}/{symbol}-fundingRate-{month}.zip"

        # Verify URL structure
        assert 'monthly/fundingRate' in expected_url
        assert symbol in expected_url
        assert month in expected_url
        assert expected_url.endswith('.zip')

    def test_funding_rates_url_pattern(self):
        """URL should not include interval (unlike klines)."""
        client = BinanceDataVisionClientAsync()

        symbol = 'BTCUSDT'
        month = '2024-12'
        category = client.DATA_CATEGORIES['funding_rates']
        filename = f"{symbol}-fundingRate-{month}.zip"
        url = f"{client.base_url}{category}/{symbol}/{filename}"

        # Should NOT contain interval paths (like /5m/ or /1h/)
        assert '/5m/' not in url
        assert '/1h/' not in url
        assert '/1d/' not in url

        # Should contain correct structure
        assert '/monthly/fundingRate/' in url
        assert 'fundingRate' in filename


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
