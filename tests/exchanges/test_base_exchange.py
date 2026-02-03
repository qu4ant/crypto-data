"""
Tests for base exchange abstraction.

Tests the ExchangeClient ABC.
"""

import pytest

from crypto_data.exchanges.base import ExchangeClient


class TestExchangeClientInterface:
    """Tests for ExchangeClient abstract base class."""

    def test_cannot_instantiate_abc_directly(self):
        """ExchangeClient cannot be instantiated directly."""
        with pytest.raises(TypeError, match="Can't instantiate abstract class"):
            ExchangeClient()

    def test_abstract_methods_list(self):
        """Verify all expected abstract methods are defined."""
        # Get all abstract methods
        abstract_methods = set()
        for name, method in vars(ExchangeClient).items():
            if getattr(method, '__isabstractmethod__', False):
                abstract_methods.add(name)

        expected_methods = {
            'exchange',
            'base_url',
            'download_file',
            '__aenter__',
            '__aexit__',
        }

        assert abstract_methods == expected_methods
