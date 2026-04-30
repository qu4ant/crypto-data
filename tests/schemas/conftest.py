"""
Pytest fixtures for schema tests

Provides sample valid and invalid DataFrames for testing schemas.
"""

from datetime import datetime

import pandas as pd
import pytest

from crypto_data.enums import Interval


@pytest.fixture
def valid_ohlcv_df():
    """Valid OHLCV DataFrame"""
    return pd.DataFrame(
        {
            "exchange": ["binance"] * 5,
            "symbol": ["BTCUSDT"] * 5,
            "interval": [Interval.MIN_5.value] * 5,
            "timestamp": pd.date_range("2024-01-01", periods=5, freq="5min"),
            "open": [50000.0, 50010.0, 50020.0, 50030.0, 50040.0],
            "high": [50100.0, 50110.0, 50120.0, 50130.0, 50140.0],
            "low": [49900.0, 49910.0, 49920.0, 49930.0, 49940.0],
            "close": [50010.0, 50020.0, 50030.0, 50040.0, 50050.0],
            "volume": [100.5, 200.3, 150.7, 180.2, 120.9],
            "quote_volume": [5000000.0, 10000000.0, 7500000.0, 9000000.0, 6000000.0],
            "trades_count": [500, 600, 550, 580, 520],
            "taker_buy_base_volume": [50.2, 100.1, 75.3, 90.1, 60.4],
            "taker_buy_quote_volume": [2500000.0, 5000000.0, 3750000.0, 4500000.0, 3000000.0],
        }
    )


@pytest.fixture
def invalid_ohlcv_high_low():
    """OHLCV DataFrame with high < low violation"""
    return pd.DataFrame(
        {
            "exchange": ["binance"] * 3,
            "symbol": ["BTCUSDT"] * 3,
            "interval": [Interval.MIN_5.value] * 3,
            "timestamp": pd.date_range("2024-01-01", periods=3, freq="5min"),
            "open": [50000.0, 50010.0, 50020.0],
            "high": [50100.0, 49900.0, 50120.0],  # Second row: high < low
            "low": [49900.0, 50000.0, 49920.0],  # Second row: low > high
            "close": [50010.0, 50020.0, 50030.0],
            "volume": [100.5, 200.3, 150.7],
            "quote_volume": [5000000.0, 10000000.0, 7500000.0],
            "trades_count": [500, 600, 550],
            "taker_buy_base_volume": [50.2, 100.1, 75.3],
            "taker_buy_quote_volume": [2500000.0, 5000000.0, 3750000.0],
        }
    )


@pytest.fixture
def invalid_ohlcv_negative_price():
    """OHLCV DataFrame with negative prices"""
    return pd.DataFrame(
        {
            "exchange": ["binance"] * 3,
            "symbol": ["BTCUSDT"] * 3,
            "interval": [Interval.MIN_5.value] * 3,
            "timestamp": pd.date_range("2024-01-01", periods=3, freq="5min"),
            "open": [50000.0, -50010.0, 50020.0],  # Negative price
            "high": [50100.0, 50110.0, 50120.0],
            "low": [49900.0, 49910.0, 49920.0],
            "close": [50010.0, 50020.0, 50030.0],
            "volume": [100.5, 200.3, 150.7],
            "quote_volume": [5000000.0, 10000000.0, 7500000.0],
            "trades_count": [500, 600, 550],
            "taker_buy_base_volume": [50.2, 100.1, 75.3],
            "taker_buy_quote_volume": [2500000.0, 5000000.0, 3750000.0],
        }
    )


@pytest.fixture
def valid_open_interest_df():
    """Valid open interest DataFrame"""
    return pd.DataFrame(
        {
            "exchange": ["binance"] * 5,
            "symbol": ["BTCUSDT"] * 5,
            "timestamp": pd.date_range("2024-01-01", periods=5, freq="1h"),
            "open_interest": [100000.0, 105000.0, 110000.0, 108000.0, 112000.0],
        }
    )


@pytest.fixture
def invalid_open_interest_zero():
    """Open interest DataFrame with zero values (should be filtered during import)"""
    return pd.DataFrame(
        {
            "exchange": ["binance"] * 3,
            "symbol": ["BTCUSDT"] * 3,
            "timestamp": pd.date_range("2024-01-01", periods=3, freq="1h"),
            "open_interest": [100000.0, 0.0, 110000.0],  # Zero value
        }
    )


@pytest.fixture
def valid_funding_rates_df():
    """Valid funding rates DataFrame"""
    return pd.DataFrame(
        {
            "exchange": ["binance"] * 5,
            "symbol": ["BTCUSDT"] * 5,
            "timestamp": pd.date_range("2024-01-01", periods=5, freq="8h"),
            "funding_rate": [0.0001, -0.0001, 0.0002, -0.0003, 0.0001],
        }
    )


@pytest.fixture
def funding_rates_extreme():
    """Funding rates DataFrame with extreme values (>1%)"""
    return pd.DataFrame(
        {
            "exchange": ["binance"] * 10,
            "symbol": ["BTCUSDT"] * 10,
            "timestamp": pd.date_range("2024-01-01", periods=10, freq="8h"),
            "funding_rate": [
                0.0001,
                -0.0001,
                0.0002,
                -0.0002,
                0.0001,
                0.015,
                0.014,
                0.013,
                0.012,
                0.011,
            ],  # Multiple extreme values (last 5), std > 1%
        }
    )


@pytest.fixture
def valid_universe_df():
    """Valid universe DataFrame (v6 schema)"""
    return pd.DataFrame(
        {
            "provider": ["coinmarketcap"] * 6,
            "provider_id": [1, 1027, 1839, 1, 1027, 5426],
            "date": [datetime(2024, 1, 1)] * 3 + [datetime(2024, 2, 1)] * 3,
            "symbol": ["BTC", "ETH", "BNB", "BTC", "ETH", "SOL"],
            "name": ["Bitcoin", "Ethereum", "BNB", "Bitcoin", "Ethereum", "Solana"],
            "slug": ["bitcoin", "ethereum", "bnb", "bitcoin", "ethereum", "solana"],
            "rank": [1, 2, 3, 1, 2, 3],
            "market_cap": [
                800000000000.0,
                400000000000.0,
                80000000000.0,
                850000000000.0,
                420000000000.0,
                90000000000.0,
            ],
            "fully_diluted_market_cap": [None, None, None, None, None, None],
            "circulating_supply": [None, None, None, None, None, None],
            "max_supply": [None, None, None, None, None, None],
            "tags": [
                "currency",
                "smart-contracts",
                "exchange-token",
                "currency",
                "smart-contracts",
                "smart-contracts",
            ],
            "platform": [None, None, None, None, None, None],
            "date_added": [pd.NaT] * 6,
        }
    )


@pytest.fixture
def invalid_universe_duplicate_ranks():
    """Universe DataFrame with duplicate ranks on same date (v6 schema)"""
    return pd.DataFrame(
        {
            "provider": ["coinmarketcap"] * 3,
            "provider_id": [1, 1027, 1839],
            "date": [datetime(2024, 1, 1)] * 3,
            "symbol": ["BTC", "ETH", "BNB"],
            "name": ["Bitcoin", "Ethereum", "BNB"],
            "slug": ["bitcoin", "ethereum", "bnb"],
            "rank": [1, 1, 3],  # Duplicate rank 1
            "market_cap": [800000000000.0, 400000000000.0, 80000000000.0],
            "fully_diluted_market_cap": [None, None, None],
            "circulating_supply": [None, None, None],
            "max_supply": [None, None, None],
            "tags": ["currency", "smart-contracts", "exchange-token"],
            "platform": [None, None, None],
            "date_added": [pd.NaT] * 3,
        }
    )
