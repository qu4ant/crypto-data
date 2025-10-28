"""
Pytest configuration and shared fixtures for universe tests.
"""

import pytest


# ============================================================================
# Pytest Markers
# ============================================================================

def pytest_configure(config):
    """Register custom pytest markers."""
    config.addinivalue_line("markers", "slow: marks tests as slow (deselect with '-m \"not slow\"')")
    config.addinivalue_line("markers", "integration: marks tests as integration tests")
    config.addinivalue_line("markers", "api: marks tests that interact with external APIs")


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
