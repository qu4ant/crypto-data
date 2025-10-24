"""
CoinMarketCap Universe Ingestion

Fetches historical universe rankings from CoinMarketCap API and imports to DuckDB.

Uses CoinMarketCap's free internal API for historical market cap snapshots.
No API key required!
"""

import logging
import time
from typing import Optional, List
from pathlib import Path
import pandas as pd
import requests
import duckdb
import yaml

logger = logging.getLogger(__name__)

# CoinMarketCap API base URL (free internal API)
API_BASE = 'https://api.coinmarketcap.com/data-api/v3'

# Rate limiting configuration
RATE_LIMIT_WAIT = 60  # Wait time after 429 error (seconds)
MAX_RETRIES = 3  # Maximum retry attempts
SERVER_ERROR_DELAY = 5  # Wait time for 500/503 errors (seconds)

# Project root for config loading
project_root = Path(__file__).parent.parent.parent.parent


def _load_universe_config(config_path: Optional[str] = None) -> tuple[List[str], bool]:
    """
    Load universe configuration from config file.

    Parameters
    ----------
    config_path : str, optional
        Path to universe config file. If None, uses default: config/universe_config.yaml

    Returns
    -------
    tuple[list, bool]
        (excluded_tags, check_perpetuals)
        - excluded_tags: List of tag names to exclude
        - check_perpetuals: Whether to validate Binance perpetual futures availability
    """
    if config_path is None:
        config_path = project_root / 'config' / 'universe_config.yaml'
    else:
        config_path = Path(config_path)

    if not config_path.exists():
        logger.warning(f"Universe config not found: {config_path}, using no filters")
        return [], True  # Default: check perpetuals

    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
            excluded_tags = config.get('exclude_categories', [])  # Keep key name for backward compat
            check_perpetuals = config.get('check_perpetuals', True)

            logger.info(f"Loaded {len(excluded_tags)} excluded tags from config")
            if check_perpetuals:
                logger.info("Will validate Binance perpetual futures availability")

            return excluded_tags, check_perpetuals
    except Exception as e:
        logger.warning(f"Failed to load universe config: {e}, using no filters")
        return [], True


def _fetch_binance_perpetuals() -> dict:
    """
    Fetch USDT-margined perpetual futures availability from Binance.

    Returns
    -------
    dict
        {symbol: onboard_date_str}
        Example: {'BTC': '2019-09-25', 'ETH': '2019-09-25', ...}
    """
    try:
        url = 'https://fapi.binance.com/fapi/v1/exchangeInfo'
        response = requests.get(url, timeout=10)
        response.raise_for_status()

        data = response.json()
        perpetuals = {}

        for symbol_info in data['symbols']:
            symbol = symbol_info['symbol']

            # Only USDT-margined perpetuals
            if not symbol.endswith('USDT'):
                continue

            # Get base symbol
            base = symbol[:-4]  # Remove USDT

            # Handle multiplier prefixes (e.g., 1000SHIBUSDT → SHIB)
            if base and base[0].isdigit():
                base = ''.join(c for c in base if not c.isdigit())

            # Get onboard date
            onboard = symbol_info.get('onboardDate')
            if onboard:
                from datetime import datetime
                onboard_date = datetime.fromtimestamp(onboard / 1000)
                perpetuals[base] = onboard_date.strftime('%Y-%m-%d')

        logger.info(f"  → Found {len(perpetuals)} USDT perpetuals on Binance")
        return perpetuals

    except Exception as e:
        logger.warning(f"Failed to fetch Binance perpetuals: {e}")
        return {}


def ingest_universe(
    db_path: str,
    date: str,
    top_n: int = 100,
    rate_limit_delay: float = 0.0
):
    """
    Fetch universe snapshot from CoinMarketCap with historical rankings and import to DuckDB.

    Process:
    1. Fetch top N markets from CoinMarketCap historical API (1 API call)
    2. Filter based on excluded tags (stablecoins, wrapped tokens, etc.)
    3. Validate Binance perpetual futures availability (optional)
    4. Transform to DataFrame with all metadata
    5. Import to crypto_universe table

    Total API calls: 1 for CMC + 1 for Binance (if check_perpetuals enabled)

    Parameters
    ----------
    db_path : str
        Path to DuckDB database file
    date : str
        Date of the snapshot (YYYY-MM-DD format)
    top_n : int
        Number of top cryptocurrencies to fetch (default: 100)
    rate_limit_delay : float
        Delay between API calls in seconds (default: 0.0)
        With 0.0, relies on 429 retry logic for automatic throttling

    Example
    -------
    >>> ingest_universe(
    ...     db_path='crypto_universe.db',
    ...     date='2022-01-01',
    ...     top_n=100
    ... )
    """
    logger.info(f"Starting CoinMarketCap universe ingestion")
    logger.info(f"  Database: {db_path}")
    logger.info(f"  Date: {date}")
    logger.info(f"  Top N: {top_n}")

    # Load universe config (excluded tags and perpetual check)
    excluded_tags, check_perpetuals = _load_universe_config()
    if excluded_tags:
        logger.info(f"  Excluding tags: {', '.join(excluded_tags[:5])}{'...' if len(excluded_tags) > 5 else ''}")

    # Fetch Binance perpetuals if enabled
    binance_perpetuals = {}
    if check_perpetuals:
        logger.info(f"Fetching Binance perpetuals data...")
        binance_perpetuals = _fetch_binance_perpetuals()

    # Parse date
    snapshot_date = pd.Timestamp(date)

    # Fetch from CoinMarketCap
    logger.info(f"Fetching historical rankings from CoinMarketCap API...")
    df = _fetch_snapshot(snapshot_date, top_n, excluded_tags, binance_perpetuals, date)

    logger.info(f"Fetched {len(df)} coins")

    # Import to DuckDB
    conn = duckdb.connect(db_path)

    try:
        # Delete existing data for this date (if any)
        conn.execute("""
            DELETE FROM crypto_universe
            WHERE date = ?
        """, [date])

        # Insert new data
        conn.execute("""
            INSERT INTO crypto_universe
            SELECT * FROM df
        """)

        logger.info(f"Successfully imported {len(df)} records to database")

    except Exception as e:
        logger.error(f"Failed to import to database: {e}")
        raise

    finally:
        conn.close()

    logger.info("CoinMarketCap universe ingestion complete")


def _fetch_snapshot(
    date: pd.Timestamp,
    top_n: int,
    excluded_tags: List[str],
    binance_perpetuals: dict,
    date_str: str
) -> pd.DataFrame:
    """
    Fetch universe snapshot from CoinMarketCap API with historical rankings.

    Filters out coins that match any of the excluded tags.
    Validates perpetual availability if binance_perpetuals dict provided.

    Returns
    -------
    pd.DataFrame
        DataFrame with columns: date, symbol, rank, market_cap, categories, exchanges, has_perpetual
    """
    # Fetch from CoinMarketCap historical listings API
    coins = _api_get_historical_listings(date_str, top_n)
    logger.info(f"  → Fetched {len(coins)} coins from CoinMarketCap")

    # Process each coin
    records = []
    filtered_count = 0

    for coin in coins:
        symbol = coin.get('symbol', '').upper()
        rank = coin.get('cmcRank', 0)

        # Get tags (similar to CoinGecko categories)
        tags = coin.get('tags', [])
        tags_str = ','.join(tags) if tags else ''

        # Filter based on excluded tags
        if any(tag in excluded_tags for tag in tags):
            logger.debug(f"  → Filtered {symbol} (excluded tag)")
            filtered_count += 1
            continue

        # Get market cap from quotes
        market_cap = 0
        quotes = coin.get('quotes', [])
        if quotes and len(quotes) > 0:
            market_cap = quotes[0].get('marketCap', 0)

        # Check if coin has perpetuals on Binance
        has_perpetual = None
        if binance_perpetuals:
            if symbol in binance_perpetuals:
                onboard_date = binance_perpetuals[symbol]
                # Check if perpetual was available on target date
                has_perpetual = onboard_date <= date_str
            else:
                has_perpetual = False

        record = {
            'date': date,
            'symbol': symbol,
            'rank': rank,
            'market_cap': market_cap,
            'categories': tags_str,  # Store tags as comma-separated string
            'exchanges': '',  # CMC doesn't provide this
            'has_perpetual': has_perpetual
        }

        records.append(record)

    # Convert to DataFrame
    df = pd.DataFrame(records)

    logger.info(f"  → Kept {len(df)} coins, filtered {filtered_count}")

    return df


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

    # Should never reach here, but just in case
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
