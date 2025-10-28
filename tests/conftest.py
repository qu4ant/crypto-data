"""
Pytest configuration and shared fixtures for universe tests.

Provides reusable test data, mocks, and utilities for testing the universe ingestion system.
"""

import pytest
import tempfile
from pathlib import Path
from typing import Dict, List
import pandas as pd
from unittest.mock import MagicMock


# ============================================================================
# Pytest Markers
# ============================================================================

def pytest_configure(config):
    """Register custom pytest markers."""
    config.addinivalue_line("markers", "slow: marks tests as slow (deselect with '-m \"not slow\"')")
    config.addinivalue_line("markers", "integration: marks tests as integration tests")
    config.addinivalue_line("markers", "api: marks tests that interact with external APIs")


# ============================================================================
# Temporary File Fixtures
# ============================================================================

@pytest.fixture
def temp_parquet_path(tmp_path):
    """Provide a temporary Parquet file path that auto-cleans up."""
    return str(tmp_path / "test_universe.parquet")


@pytest.fixture
def temp_dir(tmp_path):
    """Provide a temporary directory path."""
    return tmp_path


# ============================================================================
# Sample Data Fixtures (v3.0.0 Schema)
# ============================================================================

@pytest.fixture
def sample_universe_df():
    """
    Provide a sample universe DataFrame with v3.0.0 schema.

    Schema: date, symbol, rank, market_cap, categories
    """
    return pd.DataFrame({
        'date': pd.to_datetime(['2024-01-01'] * 5),
        'symbol': ['BTC', 'ETH', 'BNB', 'SOL', 'XRP'],
        'rank': [1, 2, 3, 4, 5],
        'market_cap': [
            1200000000000.0,  # $1.2T
            500000000000.0,   # $500B
            100000000000.0,   # $100B
            80000000000.0,    # $80B
            50000000000.0     # $50B
        ],
        'categories': [
            'mineable,pow,sha-256,store-of-value',
            'pos,smart-contracts,ethereum-ecosystem',
            'centralized-exchange,bnb-chain',
            'pos,layer-1,solana-ecosystem',
            'medium-of-exchange,xrp-ecosystem'
        ]
    })


@pytest.fixture
def sample_stablecoin_df():
    """Provide sample data including stablecoins (for filtering tests)."""
    return pd.DataFrame({
        'date': pd.to_datetime(['2024-01-01'] * 3),
        'symbol': ['BTC', 'USDT', 'ETH'],
        'rank': [1, 4, 2],
        'market_cap': [1200000000000.0, 95000000000.0, 500000000000.0],
        'categories': [
            'mineable,pow',
            'stablecoin,asset-backed-stablecoin',  # Should be filtered
            'pos,smart-contracts'
        ]
    })


@pytest.fixture
def sample_multi_month_df():
    """Provide sample data spanning multiple months."""
    dates = ['2024-01-01'] * 2 + ['2024-02-01'] * 2 + ['2024-03-01'] * 2
    return pd.DataFrame({
        'date': pd.to_datetime(dates),
        'symbol': ['BTC', 'ETH'] * 3,
        'rank': [1, 2] * 3,
        'market_cap': [1200000000000.0, 500000000000.0] * 3,
        'categories': ['mineable,pow', 'pos,smart-contracts'] * 3
    })


# ============================================================================
# CoinMarketCap API Response Fixtures
# ============================================================================

@pytest.fixture
def sample_cmc_api_response():
    """
    Provide a sample CoinMarketCap API response.

    Matches the structure from /cryptocurrency/listings/historical endpoint.
    """
    return {
        'data': [
            {
                'symbol': 'BTC',
                'cmcRank': 1,
                'tags': ['mineable', 'pow', 'sha-256'],
                'quotes': [{'marketCap': 1200000000000}]
            },
            {
                'symbol': 'ETH',
                'cmcRank': 2,
                'tags': ['pos', 'smart-contracts'],
                'quotes': [{'marketCap': 500000000000}]
            },
            {
                'symbol': 'USDT',
                'cmcRank': 4,
                'tags': ['stablecoin', 'asset-backed-stablecoin'],
                'quotes': [{'marketCap': 95000000000}]
            }
        ]
    }


@pytest.fixture
def empty_cmc_api_response():
    """Provide an empty CoinMarketCap API response."""
    return {'data': []}


@pytest.fixture
def malformed_cmc_api_response():
    """Provide a malformed CoinMarketCap API response (missing 'data' key)."""
    return {'error': 'Invalid request'}


# ============================================================================
# Mock Factory Fixtures
# ============================================================================

@pytest.fixture
def mock_cmc_response_factory():
    """
    Factory for creating mock CoinMarketCap API responses.

    Usage:
        response = mock_cmc_response_factory(
            symbols=['BTC', 'ETH'],
            ranks=[1, 2],
            market_caps=[1200e9, 500e9],
            tags=[['pow'], ['pos']]
        )
    """
    def factory(
        symbols: List[str],
        ranks: List[int],
        market_caps: List[float],
        tags: List[List[str]] = None
    ) -> Dict:
        """Create a mock API response with specified data."""
        if tags is None:
            tags = [[] for _ in symbols]

        data = []
        for i, symbol in enumerate(symbols):
            data.append({
                'symbol': symbol,
                'cmcRank': ranks[i],
                'tags': tags[i],
                'quotes': [{'marketCap': market_caps[i]}]
            })

        return {'data': data}

    return factory


@pytest.fixture
def mock_requests_get():
    """
    Provide a mock requests.get function.

    Usage in tests:
        with patch('requests.get', mock_requests_get):
            ...
    """
    mock = MagicMock()
    mock.return_value.status_code = 200
    mock.return_value.json.return_value = {'data': []}
    return mock


# ============================================================================
# Config File Fixtures
# ============================================================================

@pytest.fixture
def sample_universe_config():
    """Provide sample universe config content."""
    return """
exclude_categories:
  - stablecoin
  - wrapped-tokens
  - privacy
"""


@pytest.fixture
def empty_universe_config():
    """Provide empty universe config content."""
    return """
exclude_categories: []
"""


@pytest.fixture
def malformed_universe_config():
    """Provide malformed YAML config content."""
    return """
exclude_categories:
  - stablecoin
    - invalid indentation
"""


@pytest.fixture
def temp_config_file(tmp_path, sample_universe_config):
    """Provide a temporary config file with sample content."""
    config_path = tmp_path / "test_config.yaml"
    config_path.write_text(sample_universe_config)
    return str(config_path)


# ============================================================================
# Date/Time Fixtures
# ============================================================================

@pytest.fixture
def sample_dates():
    """Provide a list of sample dates for testing."""
    return ['2024-01-01', '2024-02-01', '2024-03-01']


@pytest.fixture
def sample_date_range():
    """Provide a sample date range (start, end)."""
    return ('2024-01-01', '2024-12-31')


# ============================================================================
# Utility Fixtures
# ============================================================================

@pytest.fixture
def excluded_tags():
    """Provide a list of excluded tags for filtering tests."""
    return ['stablecoin', 'wrapped-tokens', 'privacy', 'tokenized-gold']


@pytest.fixture
def assert_parquet_schema():
    """
    Provide a function to assert Parquet file has correct v3.0.0 schema.

    Usage:
        assert_parquet_schema(parquet_path)
    """
    def checker(parquet_path: str):
        """Check that Parquet file has correct schema."""
        df = pd.read_parquet(parquet_path)

        # Check columns exist
        required_cols = ['date', 'symbol', 'rank', 'market_cap', 'categories']
        for col in required_cols:
            assert col in df.columns, f"Missing column: {col}"

        # Check data types
        assert df['date'].dtype == 'datetime64[ns]', "date should be datetime"
        assert df['symbol'].dtype == 'object', "symbol should be string"
        assert df['rank'].dtype in ['int64', 'int32'], "rank should be integer"
        assert df['market_cap'].dtype == 'float64', "market_cap should be float"
        assert df['categories'].dtype == 'object', "categories should be string"

        return True

    return checker
