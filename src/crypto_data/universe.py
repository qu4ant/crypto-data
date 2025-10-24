"""
CoinMarketCap Universe Module

Standalone module for fetching cryptocurrency universe rankings from CoinMarketCap
and saving to Parquet files. Completely decoupled from Binance and DuckDB.

This module provides:
- CoinMarketCap API interaction (free historical API)
- Tag-based filtering (stablecoins, wrapped tokens, etc.)
- Parquet file management (append, deduplication, compression)
- Retry logic for rate limits and server errors
"""

import logging
import time
from typing import List, Optional, Dict
from pathlib import Path
import pandas as pd
import requests
import yaml

logger = logging.getLogger(__name__)

# CoinMarketCap API base URL (free internal API)
API_BASE = 'https://api.coinmarketcap.com/data-api/v3'

# Rate limiting configuration
RATE_LIMIT_WAIT = 60  # Wait time after 429 error (seconds)
MAX_RETRIES = 3  # Maximum retry attempts
SERVER_ERROR_DELAY = 5  # Wait time for 500/503 errors (seconds)

# Project root for config loading
PROJECT_ROOT = Path(__file__).parent.parent.parent


def load_config(config_path: Optional[str] = None) -> List[str]:
    """
    Load universe configuration from YAML file.

    Parameters
    ----------
    config_path : str, optional
        Path to config file. If None, uses: config/universe_parquet_config.yaml

    Returns
    -------
    list
        List of tag names to exclude from universe

    Example
    -------
    >>> excluded_tags = load_config()
    >>> print(f"Excluding {len(excluded_tags)} tag categories")
    """
    if config_path is None:
        config_path = PROJECT_ROOT / 'config' / 'universe_parquet_config.yaml'
    else:
        config_path = Path(config_path)

    if not config_path.exists():
        logger.warning(f"Config not found: {config_path}, using no filters")
        return []

    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
            excluded_tags = config.get('exclude_categories', [])
            logger.info(f"Loaded {len(excluded_tags)} excluded tags from config")
            return excluded_tags
    except Exception as e:
        logger.warning(f"Failed to load config: {e}, using no filters")
        return []


def fetch_snapshot(
    date: str,
    top_n: int,
    excluded_tags: Optional[List[str]] = None
) -> pd.DataFrame:
    """
    Fetch universe snapshot from CoinMarketCap API.

    Fetches top N cryptocurrencies by market cap for a specific historical date.
    Filters out coins matching excluded tags (stablecoins, wrapped tokens, etc.).

    Parameters
    ----------
    date : str
        Date in YYYY-MM-DD format
    top_n : int
        Number of top cryptocurrencies to fetch
    excluded_tags : list, optional
        List of tag names to exclude. If None, no filtering applied.

    Returns
    -------
    pd.DataFrame
        DataFrame with columns: date, symbol, rank, market_cap, categories

    Example
    -------
    >>> df = fetch_snapshot('2024-01-01', top_n=100, excluded_tags=['Stablecoins'])
    >>> print(df.head())
    """
    if excluded_tags is None:
        excluded_tags = []

    logger.info(f"Fetching snapshot for {date} (top {top_n})...")

    # Fetch from CoinMarketCap API
    coins = _api_get_historical_listings(date, top_n)
    logger.info(f"  → Received {len(coins)} coins from API")

    # Process and filter coins
    records = []
    filtered_count = 0

    for coin in coins:
        symbol = coin.get('symbol', '').upper()
        rank = coin.get('cmcRank', 0)

        # Get tags (categories)
        tags = coin.get('tags', [])
        tags_str = ','.join(tags) if tags else ''

        # Filter based on excluded tags
        if excluded_tags and any(tag in excluded_tags for tag in tags):
            logger.debug(f"  → Filtered {symbol} (excluded tag)")
            filtered_count += 1
            continue

        # Get market cap from quotes
        market_cap = 0
        quotes = coin.get('quotes', [])
        if quotes and len(quotes) > 0:
            market_cap = quotes[0].get('marketCap', 0)

        record = {
            'date': pd.to_datetime(date),
            'symbol': symbol,
            'rank': rank,
            'market_cap': market_cap,
            'categories': tags_str
        }

        records.append(record)

    df = pd.DataFrame(records)
    logger.info(f"  → Kept {len(df)} coins, filtered {filtered_count}")

    return df


def save_to_parquet(
    df: pd.DataFrame,
    output_path: str,
    compression: str = 'snappy'
) -> None:
    """
    Save DataFrame to Parquet file with append/deduplication logic.

    If file exists:
    - Loads existing data
    - Removes snapshots with same dates as new data (deduplication)
    - Appends new data
    - Saves combined data

    Parameters
    ----------
    df : pd.DataFrame
        Universe data to save (must have 'date' column)
    output_path : str
        Path to output Parquet file
    compression : str
        Compression algorithm (default: 'snappy')

    Example
    -------
    >>> df = fetch_snapshot('2024-01-01', top_n=100)
    >>> save_to_parquet(df, 'crypto_universe.parquet')
    """
    output_path = Path(output_path)

    # If file exists, append with deduplication
    if output_path.exists():
        logger.info(f"Parquet file exists, appending with deduplication...")

        # Load existing data
        df_existing = pd.read_parquet(output_path)

        # Get new dates
        new_dates = df['date'].unique()

        # Remove existing snapshots for these dates
        df_existing = df_existing[~df_existing['date'].isin(new_dates)]

        # Combine
        df_combined = pd.concat([df_existing, df], ignore_index=True)

        # Sort by date and rank
        df_combined = df_combined.sort_values(['date', 'rank']).reset_index(drop=True)

        logger.info(f"  → Removed {len(new_dates)} existing snapshot(s)")
        logger.info(f"  → Total snapshots: {df_combined['date'].nunique()}")
        logger.info(f"  → Total records: {len(df_combined)}")

    else:
        logger.info(f"Creating new Parquet file...")
        df_combined = df.sort_values(['date', 'rank']).reset_index(drop=True)

    # Save to Parquet
    df_combined.to_parquet(
        output_path,
        compression=compression,
        index=False,
        engine='pyarrow'
    )

    logger.info(f"✓ Saved to {output_path}")


def load_universe(parquet_path: str) -> pd.DataFrame:
    """
    Load universe data from Parquet file.

    Parameters
    ----------
    parquet_path : str
        Path to Parquet file

    Returns
    -------
    pd.DataFrame
        Universe data

    Example
    -------
    >>> df = load_universe('crypto_universe.parquet')
    >>> top_50 = df[df['rank'] <= 50]
    """
    return pd.read_parquet(parquet_path)


def snapshot_exists(parquet_path: str, date: str) -> bool:
    """
    Check if snapshot exists for given date in Parquet file.

    Parameters
    ----------
    parquet_path : str
        Path to Parquet file
    date : str
        Date in YYYY-MM-DD format

    Returns
    -------
    bool
        True if snapshot exists, False otherwise

    Example
    -------
    >>> if snapshot_exists('crypto_universe.parquet', '2024-01-01'):
    ...     print("Snapshot already exists")
    """
    parquet_path = Path(parquet_path)

    if not parquet_path.exists():
        return False

    try:
        df = pd.read_parquet(parquet_path)
        date_ts = pd.to_datetime(date)
        return date_ts in df['date'].values
    except Exception as e:
        logger.warning(f"Failed to check snapshot existence: {e}")
        return False


def get_universe_stats(parquet_path: str) -> Dict:
    """
    Get statistics about universe Parquet file.

    Parameters
    ----------
    parquet_path : str
        Path to Parquet file

    Returns
    -------
    dict
        Statistics dictionary with keys:
        - total_records: Total number of records
        - num_snapshots: Number of unique dates
        - date_range: (min_date, max_date)
        - unique_symbols: Number of unique symbols
        - file_size_mb: File size in MB

    Example
    -------
    >>> stats = get_universe_stats('crypto_universe.parquet')
    >>> print(f"Snapshots: {stats['num_snapshots']}")
    """
    parquet_path = Path(parquet_path)

    if not parquet_path.exists():
        return {
            'total_records': 0,
            'num_snapshots': 0,
            'date_range': (None, None),
            'unique_symbols': 0,
            'file_size_mb': 0
        }

    df = pd.read_parquet(parquet_path)
    file_size_mb = parquet_path.stat().st_size / (1024 * 1024)

    return {
        'total_records': len(df),
        'num_snapshots': df['date'].nunique(),
        'date_range': (df['date'].min(), df['date'].max()),
        'unique_symbols': df['symbol'].nunique(),
        'file_size_mb': round(file_size_mb, 2)
    }


# ============================================================================
# Private API helper functions
# ============================================================================

def _api_call_with_retry(url: str, params: dict, timeout: int = 10):
    """
    Make API call with automatic retry on rate limits and server errors.

    Handles:
    - 429 (Rate Limit): Waits 60 seconds before retry
    - 500/503 (Server Error): Waits 5 seconds before retry
    - Other errors: Raises immediately

    Parameters
    ----------
    url : str
        API endpoint URL
    params : dict
        Query parameters
    timeout : int
        Request timeout in seconds

    Returns
    -------
    requests.Response
        Successful response object

    Raises
    ------
    requests.HTTPError
        If all retries exhausted or non-retryable error
    """
    for attempt in range(MAX_RETRIES + 1):  # 0 = initial, 1-3 = retries
        try:
            response = requests.get(url, params=params, timeout=timeout)
            response.raise_for_status()
            return response

        except requests.HTTPError as e:
            status_code = e.response.status_code

            # Rate limit error (429) - wait 60 seconds
            if status_code == 429:
                if attempt < MAX_RETRIES:
                    logger.warning(f"Rate limit hit (429), waiting {RATE_LIMIT_WAIT}s before retry (attempt {attempt + 1}/{MAX_RETRIES})...")
                    time.sleep(RATE_LIMIT_WAIT)
                    continue
                else:
                    logger.error(f"Rate limit error - all {MAX_RETRIES} retries exhausted")
                    raise

            # Server errors (500, 503) - wait 5 seconds
            elif status_code in [500, 503]:
                if attempt < MAX_RETRIES:
                    logger.warning(f"Server error ({status_code}), waiting {SERVER_ERROR_DELAY}s before retry (attempt {attempt + 1}/{MAX_RETRIES})...")
                    time.sleep(SERVER_ERROR_DELAY)
                    continue
                else:
                    logger.error(f"Server error - all {MAX_RETRIES} retries exhausted")
                    raise

            # Other HTTP errors - raise immediately
            else:
                logger.error(f"HTTP error {status_code}: {e}")
                raise

        except requests.RequestException as e:
            # Network errors, timeouts, etc. - raise immediately
            logger.error(f"Request failed: {e}")
            raise

    # Should never reach here
    raise Exception("Unexpected error in retry logic")


def _api_get_historical_listings(date: str, limit: int) -> list:
    """
    Fetch historical cryptocurrency listings from CoinMarketCap.

    Parameters
    ----------
    date : str
        Date in YYYY-MM-DD format
    limit : int
        Number of coins to fetch

    Returns
    -------
    list
        List of coin dictionaries with ranking and market data
    """
    url = f"{API_BASE}/cryptocurrency/listings/historical"
    params = {
        'date': date,
        'limit': limit,
        'start': 1,
        'convertId': 2781,  # USD
        'sort': 'cmc_rank',
        'sort_dir': 'asc'
    }

    response = _api_call_with_retry(url, params, timeout=10)
    data = response.json()

    if 'data' in data:
        return data['data']
    else:
        logger.error(f"Unexpected API response format: {data}")
        return []
