"""
Config loading tests for universe ingestion.

Tests load_universe_config() and related configuration functionality.
"""

import pytest
from pathlib import Path
from crypto_data.utils.config import load_universe_config


class TestConfigLoading:
    """Test config file loading functionality."""

    def test_loads_default_config(self):
        """Test loading default config file."""
        # This tests the actual default config if it exists
        excluded_tags, excluded_symbols = load_universe_config()

        # Should return two lists (even if empty)
        assert isinstance(excluded_tags, list)
        assert isinstance(excluded_symbols, list)

    def test_loads_custom_config(self, temp_config_file):
        """Test loading custom config file."""
        excluded_tags, excluded_symbols = load_universe_config(temp_config_file)

        assert isinstance(excluded_tags, list)
        assert isinstance(excluded_symbols, list)
        assert 'stablecoin' in excluded_tags
        assert 'wrapped-tokens' in excluded_tags
        assert 'privacy' in excluded_tags

    def test_missing_config_returns_empty_list(self, tmp_path):
        """Test that missing config file returns empty lists gracefully."""
        nonexistent_path = tmp_path / "nonexistent.yaml"

        excluded_tags, excluded_symbols = load_universe_config(str(nonexistent_path))

        assert excluded_tags == []
        assert excluded_symbols == []

    def test_empty_config_file(self, tmp_path, empty_universe_config):
        """Test loading empty config file."""
        config_path = tmp_path / "empty_config.yaml"
        config_path.write_text(empty_universe_config)

        excluded_tags, excluded_symbols = load_universe_config(str(config_path))

        assert excluded_tags == []
        assert excluded_symbols == []

    def test_malformed_yaml_returns_empty_list(self, tmp_path):
        """Test that truly malformed YAML returns empty lists gracefully."""
        # Create truly invalid YAML (not parseable)
        malformed_content = """
exclude_categories:
  - stablecoin
  [invalid syntax here
"""
        config_path = tmp_path / "malformed.yaml"
        config_path.write_text(malformed_content)

        excluded_tags, excluded_symbols = load_universe_config(str(config_path))

        # Should handle error gracefully and return empty lists
        assert excluded_tags == []
        assert excluded_symbols == []

    def test_config_with_comments(self, tmp_path):
        """Test loading config with YAML comments."""
        config_content = """
# Universe filtering configuration
exclude_categories:
  - stablecoin  # USD-pegged tokens
  - wrapped-tokens  # Wrapped versions
  # - memes  # Commented out
"""
        config_path = tmp_path / "comments.yaml"
        config_path.write_text(config_content)

        excluded_tags, excluded_symbols = load_universe_config(str(config_path))

        assert 'stablecoin' in excluded_tags
        assert 'wrapped-tokens' in excluded_tags
        assert 'memes' not in excluded_tags  # Commented out

    def test_preserves_tag_case(self, tmp_path):
        """Test that config preserves tag case (though filtering is case-insensitive)."""
        config_content = """
exclude_categories:
  - Stablecoin
  - WRAPPED-TOKENS
  - Privacy
"""
        config_path = tmp_path / "case.yaml"
        config_path.write_text(config_content)

        excluded_tags, excluded_symbols = load_universe_config(str(config_path))

        # Config should preserve original case
        assert 'Stablecoin' in excluded_tags
        assert 'WRAPPED-TOKENS' in excluded_tags
        assert 'Privacy' in excluded_tags


class TestConfigStructure:
    """Test config file structure validation."""

    def test_wrong_key_name_returns_empty(self, tmp_path):
        """Test that wrong key name in config returns empty lists."""
        config_content = """
wrong_key:
  - stablecoin
"""
        config_path = tmp_path / "wrong_key.yaml"
        config_path.write_text(config_content)

        excluded_tags, excluded_symbols = load_universe_config(str(config_path))

        # Should return empty lists if key is wrong
        assert excluded_tags == []
        assert excluded_symbols == []

    def test_non_list_value(self, tmp_path):
        """Test handling of non-list value in config."""
        config_content = """
exclude_categories: "stablecoin"
"""
        config_path = tmp_path / "non_list.yaml"
        config_path.write_text(config_content)

        excluded_tags, excluded_symbols = load_universe_config(str(config_path))

        # Should handle gracefully - tags might be string or list, symbols should be list
        assert isinstance(excluded_tags, (list, str))  # Might return string if not a list
        assert isinstance(excluded_symbols, list)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
