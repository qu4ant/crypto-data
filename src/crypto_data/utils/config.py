"""
Configuration Loading Utilities

Handles loading and parsing of configuration files for the crypto-data package.
"""

import logging
from pathlib import Path
from typing import Optional, List, Tuple
import yaml

logger = logging.getLogger(__name__)

# Project root for config loading
project_root = Path(__file__).parent.parent.parent.parent


def load_universe_config(config_path: Optional[str] = None) -> Tuple[List[str], List[str]]:
    """
    Load universe configuration from config file.

    Filtering is case-insensitive for tags: 'Stablecoin', 'stablecoin', 'STABLECOIN'
    all match CoinMarketCap's 'stablecoin' tag.

    Symbol filtering is case-insensitive: 'LUNA', 'luna', 'Luna' all match.

    Parameters
    ----------
    config_path : str, optional
        Path to universe config file. If None, uses default: config/universe_config.yaml

    Returns
    -------
    tuple
        (excluded_tags, excluded_symbols) - Lists of tags and symbols to exclude
    """
    if config_path is None:
        config_path = project_root / 'config' / 'universe_config.yaml'
    else:
        config_path = Path(config_path)

    if not config_path.exists():
        logger.warning(f"Universe config not found: {config_path}, using no filters")
        return [], []

    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
            excluded_tags = config.get('exclude_categories', [])  # Keep key name for backward compat
            excluded_symbols = config.get('exclude_symbols', [])

            logger.info(f"Loaded {len(excluded_tags)} excluded tags and {len(excluded_symbols)} excluded symbols from config")

            return excluded_tags, excluded_symbols
    except Exception as e:
        logger.warning(f"Failed to load universe config: {e}, using no filters")
        return [], []
