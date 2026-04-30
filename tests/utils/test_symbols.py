"""
Tests for symbol extraction utilities.

Tests get_binance_symbols_from_universe() which extracts symbols from the
crypto_universe table using a UNION approach (all symbols that appeared
in top N at any point during the period).
"""

import pytest
import tempfile
from pathlib import Path

from crypto_data import CryptoDatabase
from crypto_data.utils.symbols import get_binance_symbols_from_universe


@pytest.fixture
def test_db_with_universe():
    """Create temporary database with sample universe data."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / 'test.db')

        # Create database and populate with test data
        db = CryptoDatabase(db_path)

        # Insert sample universe data (v6 schema)
        # BTC: always in top 50 (all months)
        # ETH: always in top 50 (all months)
        # SOL: enters top 50 in March (ranks 51 → 20)
        # DOGE: exits top 50 in February (ranks 45 → 60)
        db.conn.execute("""
            INSERT INTO crypto_universe
            (provider, provider_id, date, symbol, name, slug, rank,
             market_cap, fully_diluted_market_cap,
             circulating_supply, max_supply, tags, platform, date_added)
            VALUES
                ('coinmarketcap', 1,    '2024-01-01', 'BTC',  'Bitcoin',  'bitcoin',  1, 800000000000, NULL, NULL, NULL, '', NULL, NULL),
                ('coinmarketcap', 1027, '2024-01-01', 'ETH',  'Ethereum', 'ethereum', 2, 400000000000, NULL, NULL, NULL, '', NULL, NULL),
                ('coinmarketcap', 74,   '2024-01-01', 'DOGE', 'Dogecoin', 'dogecoin', 45, 10000000000, NULL, NULL, NULL, '', NULL, NULL),

                ('coinmarketcap', 1,    '2024-02-01', 'BTC',  'Bitcoin',  'bitcoin',  1, 850000000000, NULL, NULL, NULL, '', NULL, NULL),
                ('coinmarketcap', 1027, '2024-02-01', 'ETH',  'Ethereum', 'ethereum', 2, 420000000000, NULL, NULL, NULL, '', NULL, NULL),
                ('coinmarketcap', 74,   '2024-02-01', 'DOGE', 'Dogecoin', 'dogecoin', 60, 9000000000,  NULL, NULL, NULL, '', NULL, NULL),

                ('coinmarketcap', 1,    '2024-03-01', 'BTC',  'Bitcoin',  'bitcoin',  1, 900000000000, NULL, NULL, NULL, '', NULL, NULL),
                ('coinmarketcap', 1027, '2024-03-01', 'ETH',  'Ethereum', 'ethereum', 2, 450000000000, NULL, NULL, NULL, '', NULL, NULL),
                ('coinmarketcap', 5426, '2024-03-01', 'SOL',  'Solana',   'solana',   20, 15000000000, NULL, NULL, NULL, '', NULL, NULL),

                ('coinmarketcap', 1,    '2024-04-01', 'BTC',  'Bitcoin',  'bitcoin',  1, 920000000000, NULL, NULL, NULL, '', NULL, NULL),
                ('coinmarketcap', 1027, '2024-04-01', 'ETH',  'Ethereum', 'ethereum', 2, 460000000000, NULL, NULL, NULL, '', NULL, NULL),
                ('coinmarketcap', 5426, '2024-04-01', 'SOL',  'Solana',   'solana',   18, 16000000000, NULL, NULL, NULL, '', NULL, NULL)
        """)

        db.close()

        yield db_path


def test_extracts_symbols_with_usdt_suffix(test_db_with_universe):
    """Test that symbols are extracted with USDT suffix added."""
    symbols = get_binance_symbols_from_universe(
        db_path=test_db_with_universe,
        start_date='2024-01-01',
        end_date='2024-04-30',
        top_n=50
    )

    # Should extract BTC, DOGE, ETH, SOL (alphabetical order)
    assert len(symbols) == 4
    assert all(s.endswith('USDT') for s in symbols)
    assert 'BTCUSDT' in symbols
    assert 'ETHUSDT' in symbols
    assert 'SOLUSDT' in symbols
    assert 'DOGEUSDT' in symbols


def test_union_strategy_captures_entries_and_exits(test_db_with_universe):
    """Test that UNION approach captures symbols that entered or exited top N."""
    symbols = get_binance_symbols_from_universe(
        db_path=test_db_with_universe,
        start_date='2024-01-01',
        end_date='2024-04-30',
        top_n=50
    )

    # DOGE: in top 50 only in January (rank 45), exited in February (rank 60)
    # SOL: entered top 50 in March (rank 20), not in Jan/Feb
    # Both should be included (UNION approach)
    assert 'DOGEUSDT' in symbols, "DOGE should be included (was in top 50 in Jan)"
    assert 'SOLUSDT' in symbols, "SOL should be included (entered top 50 in Mar)"


def test_filters_by_rank_threshold(test_db_with_universe):
    """Test that only symbols within rank threshold are returned."""
    # top_n=10 should exclude DOGE (rank 45), SOL (rank 18-20)
    symbols = get_binance_symbols_from_universe(
        db_path=test_db_with_universe,
        start_date='2024-01-01',
        end_date='2024-04-30',
        top_n=10
    )

    assert 'BTCUSDT' in symbols  # rank 1
    assert 'ETHUSDT' in symbols  # rank 2
    assert 'DOGEUSDT' not in symbols  # rank 45
    assert 'SOLUSDT' not in symbols  # rank 18-20


def test_filters_by_date_range(test_db_with_universe):
    """Test that date range filters are applied correctly."""
    # Only January data
    symbols = get_binance_symbols_from_universe(
        db_path=test_db_with_universe,
        start_date='2024-01-01',
        end_date='2024-01-31',
        top_n=50
    )

    assert 'BTCUSDT' in symbols
    assert 'ETHUSDT' in symbols
    assert 'DOGEUSDT' in symbols
    assert 'SOLUSDT' not in symbols  # SOL only appears in March+


def test_empty_universe_returns_empty_list(test_db_with_universe):
    """Test that function returns empty list when no data matches filters."""
    # Request data from year 2025 (no data exists)
    symbols = get_binance_symbols_from_universe(
        db_path=test_db_with_universe,
        start_date='2025-01-01',
        end_date='2025-12-31',
        top_n=50
    )

    assert symbols == []
    assert isinstance(symbols, list)


def test_returns_empty_on_missing_database():
    """Test that function returns empty list when database doesn't exist."""
    symbols = get_binance_symbols_from_universe(
        db_path='/nonexistent/path/to/database.db',
        start_date='2024-01-01',
        end_date='2024-12-31',
        top_n=50
    )

    assert symbols == []
    assert isinstance(symbols, list)


def test_symbols_are_sorted_alphabetically(test_db_with_universe):
    """Test that returned symbols are sorted alphabetically."""
    symbols = get_binance_symbols_from_universe(
        db_path=test_db_with_universe,
        start_date='2024-01-01',
        end_date='2024-04-30',
        top_n=50
    )

    # Should be: BTC, DOGE, ETH, SOL (alphabetical)
    assert symbols == sorted(symbols)
    assert symbols == ['BTCUSDT', 'DOGEUSDT', 'ETHUSDT', 'SOLUSDT']


def test_handles_symbols_with_special_characters(test_db_with_universe):
    """Test that function handles symbols with numbers/special chars correctly."""
    db = CryptoDatabase(test_db_with_universe)

    # Add a symbol with numbers (e.g., "1INCH")
    db.conn.execute("""
        INSERT INTO crypto_universe
        (provider, provider_id, date, symbol, name, slug, rank,
         market_cap, fully_diluted_market_cap,
         circulating_supply, max_supply, tags, platform, date_added)
        VALUES ('coinmarketcap', 8104, '2024-01-01', '1INCH', '1inch Network', '1inch',
                30, 5000000000, NULL, NULL, NULL, '', NULL, NULL)
    """)
    db.close()

    symbols = get_binance_symbols_from_universe(
        db_path=test_db_with_universe,
        start_date='2024-01-01',
        end_date='2024-01-31',
        top_n=50
    )

    assert '1INCHUSDT' in symbols


def test_excludes_synthetic_assets_by_default(test_db_with_universe):
    """Default symbol extraction should filter old unfiltered universe rows."""
    db = CryptoDatabase(test_db_with_universe)
    db.conn.execute("""
        INSERT INTO crypto_universe
        (provider, provider_id, date, symbol, name, slug, rank,
         market_cap, fully_diluted_market_cap,
         circulating_supply, max_supply, tags, platform, date_added)
        VALUES
            ('coinmarketcap', 825,   '2024-01-01', 'USDT', 'Tether USDt',     'tether',          3, 95000000000, NULL, NULL, NULL, 'stablecoin,asset-backed-stablecoin', NULL, NULL),
            ('coinmarketcap', 3717,  '2024-01-01', 'WBTC', 'Wrapped Bitcoin', 'wrapped-bitcoin', 4, 12000000000, NULL, NULL, NULL, 'wrapped-tokens',                     NULL, NULL),
            ('coinmarketcap', 4705,  '2024-01-01', 'PAXG', 'PAX Gold',        'pax-gold',        5, 1000000000,  NULL, NULL, NULL, 'tokenized-gold',                     NULL, NULL),
            ('coinmarketcap', 12345, '2024-01-01', 'TSLA', 'Tesla Tokenized', 'tesla-tokenized', 6, 900000000,   NULL, NULL, NULL, 'tokenized-stock',                    NULL, NULL)
    """)
    db.close()

    symbols = get_binance_symbols_from_universe(
        db_path=test_db_with_universe,
        start_date='2024-01-01',
        end_date='2024-01-31',
        top_n=50
    )

    assert 'BTCUSDT' in symbols
    assert 'USDTUSDT' not in symbols
    assert 'WBTCUSDT' not in symbols
    assert 'PAXGUSDT' not in symbols
    assert 'TSLAUSDT' not in symbols


def test_symbol_extraction_allows_explicit_filter_opt_out(test_db_with_universe):
    """Passing empty filter lists should disable the package defaults."""
    db = CryptoDatabase(test_db_with_universe)
    db.conn.execute("""
        INSERT INTO crypto_universe
        (provider, provider_id, date, symbol, name, slug, rank,
         market_cap, fully_diluted_market_cap,
         circulating_supply, max_supply, tags, platform, date_added)
        VALUES ('coinmarketcap', 825, '2024-01-01', 'USDT', 'Tether USDt', 'tether',
                3, 95000000000, NULL, NULL, NULL, 'stablecoin', NULL, NULL)
    """)
    db.close()

    symbols = get_binance_symbols_from_universe(
        db_path=test_db_with_universe,
        start_date='2024-01-01',
        end_date='2024-01-31',
        top_n=50,
        exclude_tags=[],
        exclude_symbols=[],
    )

    assert 'USDTUSDT' in symbols


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
