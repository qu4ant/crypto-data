"""
Pytest configuration and shared fixtures for universe tests.
"""

import os

import pytest

# ============================================================================
# Pytest Markers
# ============================================================================


def pytest_addoption(parser):
    """Add repository-wide pytest CLI options."""
    parser.addoption(
        "--run-validation",
        action="store_true",
        default=False,
        help=(
            "Run external validation tests that may build a temporary DB and "
            "call Binance/CoinMarketCap APIs. These are skipped by default."
        ),
    )


def pytest_configure(config):
    """Register custom pytest markers."""
    config.addinivalue_line(
        "markers", "slow: marks tests as slow (deselect with '-m \"not slow\"')"
    )
    config.addinivalue_line("markers", "integration: marks tests as integration tests")
    config.addinivalue_line("markers", "api: marks tests that interact with external APIs")
    config.addinivalue_line(
        "markers",
        "validation: marks external data validation tests skipped unless --run-validation is set",
    )


def pytest_collection_modifyitems(config, items):
    """Skip network-backed validation tests unless explicitly requested."""
    run_validation = config.getoption("--run-validation") or (
        os.environ.get("CRYPTO_DATA_RUN_VALIDATION") == "1"
    )
    if run_validation:
        return

    skip_validation = pytest.mark.skip(
        reason=(
            "external validation tests are skipped by default; "
            "use --run-validation or CRYPTO_DATA_RUN_VALIDATION=1"
        )
    )
    for item in items:
        if item.get_closest_marker("validation"):
            item.add_marker(skip_validation)


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
        "data": [
            {
                "symbol": "BTC",
                "cmcRank": 1,
                "tags": ["mineable", "pow", "sha-256"],
                "quotes": [{"marketCap": 1200000000000}],
            },
            {
                "symbol": "ETH",
                "cmcRank": 2,
                "tags": ["pos", "smart-contracts"],
                "quotes": [{"marketCap": 500000000000}],
            },
            {
                "symbol": "USDT",
                "cmcRank": 4,
                "tags": ["stablecoin", "asset-backed-stablecoin"],
                "quotes": [{"marketCap": 95000000000}],
            },
        ]
    }


@pytest.fixture
def empty_cmc_api_response():
    """Provide an empty CoinMarketCap API response."""
    return {"data": []}


@pytest.fixture
def malformed_cmc_api_response():
    """Provide a malformed CoinMarketCap API response (missing 'data' key)."""
    return {"error": "Invalid request"}
