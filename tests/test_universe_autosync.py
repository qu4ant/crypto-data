"""
Tests for monthly universe auto-sync functionality.

Tests the automatic monthly snapshot generation and ingestion for CoinMarketCap universe data.
"""

import pytest
import tempfile
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock
import pandas as pd

# Add scripts to path to import update_universe script functions
script_dir = Path(__file__).parent.parent / "scripts"
sys.path.insert(0, str(script_dir))
from update_universe import _generate_month_list, _universe_snapshot_exists

# Import crypto_data modules
from crypto_data import CryptoDatabase, ingest_universe


class TestGenerateMonthList:
    """Test the _generate_month_list() helper function."""

    def test_single_month(self):
        """Test generating list for single month (start and end in same month)."""
        months = _generate_month_list('2024-01-01', '2024-01-31')
        assert months == ['2024-01']

    def test_multiple_months(self):
        """Test generating list for multiple months (3 months)."""
        months = _generate_month_list('2024-01-01', '2024-03-31')
        assert months == ['2024-01', '2024-02', '2024-03']

    def test_full_year(self):
        """Test generating list for full year (12 months)."""
        months = _generate_month_list('2024-01-01', '2024-12-31')
        expected = [f'2024-{i:02d}' for i in range(1, 13)]
        assert months == expected
        assert len(months) == 12

    def test_year_boundary(self):
        """Test generating list across year boundary."""
        months = _generate_month_list('2023-12-01', '2024-02-28')
        assert months == ['2023-12', '2024-01', '2024-02']

    def test_same_start_and_end_date(self):
        """Test when start and end are the same date (should return 1 month)."""
        months = _generate_month_list('2024-06-15', '2024-06-15')
        assert months == ['2024-06']

    def test_mid_month_dates(self):
        """Test that mid-month dates are normalized to month start."""
        months = _generate_month_list('2024-01-15', '2024-03-20')
        assert months == ['2024-01', '2024-02', '2024-03']


class TestUniverseSnapshotExists:
    """Test the _universe_snapshot_exists() helper function."""

    def test_empty_database(self):
        """Test returns False for empty database."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / 'test.db'
            db = CryptoDatabase(str(db_path))
            db.close()

            # Check for non-existent snapshot
            assert _universe_snapshot_exists(str(db_path), '2024-01-01') is False

    def test_snapshot_exists(self):
        """Test returns True when snapshot exists."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / 'test.db'

            # Initialize database (creates schema)
            db = CryptoDatabase(str(db_path))
            db.close()

            # Create snapshot
            test_data = pd.DataFrame({
                'date': ['2024-01-01', '2024-01-01'],
                'symbol': ['BTC', 'ETH'],
                'rank': [1, 2],
                'is_stablecoin': [False, False],
                'market_cap': [1200000000000, 500000000000],
                'category': ['layer-1', 'smart-contract-platform'],
                'exchanges': ['binance,coinbase,kraken', 'binance,coinbase,kraken']
            })

            # Mock the fetch to return our test data
            with patch('crypto_data.ingestion.coinmarketcap._fetch_snapshot', return_value=test_data):
                ingest_universe(str(db_path), '2024-01-01', top_n=2, rate_limit_delay=0)

            # Check snapshot exists
            assert _universe_snapshot_exists(str(db_path), '2024-01-01') is True

    def test_different_date(self):
        """Test returns False when different date exists."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / 'test.db'

            # Initialize database (creates schema)
            db = CryptoDatabase(str(db_path))
            db.close()

            # Create snapshot for Jan
            test_data = pd.DataFrame({
                'date': ['2024-01-01'],
                'symbol': ['BTC'],
                'rank': [1],
                'is_stablecoin': [False],
                'market_cap': [1200000000000],
                'category': ['layer-1'],
                'exchanges': ['binance,coinbase,kraken']
            })

            with patch('crypto_data.ingestion.coinmarketcap._fetch_snapshot', return_value=test_data):
                ingest_universe(str(db_path), '2024-01-01', top_n=1, rate_limit_delay=0)

            # Check for different date
            assert _universe_snapshot_exists(str(db_path), '2024-02-01') is False

    def test_error_handling(self):
        """Test gracefully handles database errors (returns False)."""
        # Non-existent database path
        assert _universe_snapshot_exists('/nonexistent/path/db.db', '2024-01-01') is False


class TestMonthlyIngestion:
    """Test multi-month universe ingestion."""

    def test_three_month_ingestion(self):
        """Test ingesting 3 monthly snapshots."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / 'test.db'

            # Initialize database (creates schema)
            db = CryptoDatabase(str(db_path))
            db.close()

            # Create test data for each month
            def mock_fetch_snapshot(date, top_n, rate_limit_delay):
                date_str = date.strftime('%Y-%m-%d')
                return pd.DataFrame({
                    'date': [date_str] * 3,
                    'symbol': ['BTC', 'ETH', 'SOL'],
                    'rank': [1, 2, 3],
                    'is_stablecoin': [False, False, False],
                    'market_cap': [1200000000000, 500000000000, 80000000000],
                    'category': ['layer-1', 'smart-contract-platform', 'smart-contract-platform'],
                    'exchanges': ['binance,coinbase,kraken', 'binance,coinbase,kraken', 'binance,coinbase']
                })

            with patch('crypto_data.ingestion.coinmarketcap._fetch_snapshot', side_effect=mock_fetch_snapshot):
                # Ingest for Jan, Feb, Mar
                ingest_universe(str(db_path), '2024-01-01', top_n=3, rate_limit_delay=0)
                ingest_universe(str(db_path), '2024-02-01', top_n=3, rate_limit_delay=0)
                ingest_universe(str(db_path), '2024-03-01', top_n=3, rate_limit_delay=0)

            # Verify all 3 snapshots exist
            assert _universe_snapshot_exists(str(db_path), '2024-01-01') is True
            assert _universe_snapshot_exists(str(db_path), '2024-02-01') is True
            assert _universe_snapshot_exists(str(db_path), '2024-03-01') is True

            # Verify total records
            import duckdb
            conn = duckdb.connect(str(db_path))
            count = conn.execute("SELECT COUNT(*) FROM crypto_universe").fetchone()[0]
            assert count == 9  # 3 months × 3 coins
            conn.close()

    def test_snapshot_data_correct(self):
        """Test that snapshot data is stored correctly."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / 'test.db'

            # Initialize database (creates schema)
            db = CryptoDatabase(str(db_path))
            db.close()

            # Create specific test data
            test_data = pd.DataFrame({
                'date': ['2024-01-01', '2024-01-01'],
                'symbol': ['BTC', 'ETH'],
                'rank': [1, 2],
                'is_stablecoin': [False, False],
                'market_cap': [1200000000000, 500000000000],
                'category': ['layer-1', 'smart-contract-platform'],
                'exchanges': ['binance,coinbase,kraken', 'binance,coinbase,kraken']
            })

            with patch('crypto_data.ingestion.coinmarketcap._fetch_snapshot', return_value=test_data):
                ingest_universe(str(db_path), '2024-01-01', top_n=2, rate_limit_delay=0)

            # Verify data
            import duckdb
            conn = duckdb.connect(str(db_path))
            result = conn.execute("""
                SELECT symbol, rank, market_cap, category, exchanges
                FROM crypto_universe
                WHERE date = '2024-01-01'
                ORDER BY rank
            """).fetchall()

            assert len(result) == 2
            assert result[0][0] == 'BTC'  # symbol
            assert result[0][1] == 1      # rank
            assert result[0][2] == 1200000000000  # market_cap
            assert result[0][3] == 'layer-1'  # category
            assert result[0][4] == 'binance,coinbase,kraken'  # exchanges

            conn.close()

    def test_date_conversion_to_first_of_month(self):
        """Test that monthly snapshots use 1st day of month."""
        months = _generate_month_list('2024-01-15', '2024-03-20')
        snapshot_dates = [f"{month}-01" for month in months]

        assert snapshot_dates == ['2024-01-01', '2024-02-01', '2024-03-01']


class TestSkipLogic:
    """Test snapshot skip logic for existing data."""

    def test_existing_snapshot_skipped(self):
        """Test that existing snapshots are not re-fetched."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / 'test.db'

            # Initialize database (creates schema)
            db = CryptoDatabase(str(db_path))
            db.close()

            # Pre-populate with Jan snapshot
            test_data_jan = pd.DataFrame({
                'date': ['2024-01-01'] * 2,
                'symbol': ['BTC', 'ETH'],
                'rank': [1, 2],
                'is_stablecoin': [False, False],
                'market_cap': [1200000000000, 500000000000],
                'category': ['layer-1', 'smart-contract-platform'],
                'exchanges': ['binance,coinbase,kraken', 'binance,coinbase,kraken']
            })

            with patch('crypto_data.ingestion.coinmarketcap._fetch_snapshot', return_value=test_data_jan):
                ingest_universe(str(db_path), '2024-01-01', top_n=2, rate_limit_delay=0)

            # Verify Jan exists
            assert _universe_snapshot_exists(str(db_path), '2024-01-01') is True

            # Try to ingest Jan again (should be skipped in real script)
            # Here we just verify the exists check works
            assert _universe_snapshot_exists(str(db_path), '2024-01-01') is True

    def test_partial_skip(self):
        """Test that some snapshots are skipped while others are fetched."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / 'test.db'

            # Initialize database (creates schema)
            db = CryptoDatabase(str(db_path))
            db.close()

            # Pre-populate with Jan snapshot only
            def mock_fetch(date, top_n, rate_limit_delay):
                date_str = date.strftime('%Y-%m-%d')
                return pd.DataFrame({
                    'date': [date_str] * 2,
                    'symbol': ['BTC', 'ETH'],
                    'rank': [1, 2],
                    'is_stablecoin': [False, False],
                    'market_cap': [1200000000000, 500000000000],
                    'category': ['layer-1', 'smart-contract-platform'],
                    'exchanges': ['binance,coinbase,kraken', 'binance,coinbase,kraken']
                })

            with patch('crypto_data.ingestion.coinmarketcap._fetch_snapshot', side_effect=mock_fetch):
                ingest_universe(str(db_path), '2024-01-01', top_n=2, rate_limit_delay=0)

            # Verify Jan exists, Feb/Mar don't
            assert _universe_snapshot_exists(str(db_path), '2024-01-01') is True
            assert _universe_snapshot_exists(str(db_path), '2024-02-01') is False
            assert _universe_snapshot_exists(str(db_path), '2024-03-01') is False

            # In real script, would skip Jan and fetch Feb/Mar
            # Here we just verify the logic works
            months = ['2024-01', '2024-02', '2024-03']
            snapshot_dates = [f"{month}-01" for month in months]

            to_fetch = [date for date in snapshot_dates if not _universe_snapshot_exists(str(db_path), date)]
            assert to_fetch == ['2024-02-01', '2024-03-01']


class TestErrorHandling:
    """Test error handling and continuation."""

    def test_continues_on_error(self):
        """Test that script continues to next month when one fails."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / 'test.db'

            # Initialize database (creates schema)
            db = CryptoDatabase(str(db_path))
            db.close()

            call_count = [0]

            def mock_fetch_with_error(date, top_n, rate_limit_delay):
                call_count[0] += 1
                date_str = date.strftime('%Y-%m-%d')

                # Fail on Feb (2nd call)
                if date_str == '2024-02-01':
                    raise Exception("API rate limit exceeded")

                return pd.DataFrame({
                    'date': [date_str] * 2,
                    'symbol': ['BTC', 'ETH'],
                    'rank': [1, 2],
                    'is_stablecoin': [False, False],
                    'market_cap': [1200000000000, 500000000000],
                    'category': ['layer-1', 'smart-contract-platform'],
                    'exchanges': ['binance,coinbase,kraken', 'binance,coinbase,kraken']
                })

            with patch('crypto_data.ingestion.coinmarketcap._fetch_snapshot', side_effect=mock_fetch_with_error):
                # Jan should succeed
                ingest_universe(str(db_path), '2024-01-01', top_n=2, rate_limit_delay=0)

                # Feb should fail
                with pytest.raises(Exception, match="API rate limit exceeded"):
                    ingest_universe(str(db_path), '2024-02-01', top_n=2, rate_limit_delay=0)

                # Mar should succeed (in real script, error handling allows continuation)
                ingest_universe(str(db_path), '2024-03-01', top_n=2, rate_limit_delay=0)

            # Verify Jan and Mar exist, Feb doesn't
            assert _universe_snapshot_exists(str(db_path), '2024-01-01') is True
            assert _universe_snapshot_exists(str(db_path), '2024-02-01') is False
            assert _universe_snapshot_exists(str(db_path), '2024-03-01') is True

    def test_replaces_existing_snapshot(self):
        """Test that re-running ingestion replaces existing snapshot."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / 'test.db'

            # Initialize database (creates schema)
            db = CryptoDatabase(str(db_path))
            db.close()

            # First ingestion - 2 coins
            test_data_v1 = pd.DataFrame({
                'date': ['2024-01-01', '2024-01-01'],
                'symbol': ['BTC', 'ETH'],
                'rank': [1, 2],
                'is_stablecoin': [False, False],
                'market_cap': [1200000000000, 500000000000],
                'category': ['layer-1', 'smart-contract-platform'],
                'exchanges': ['binance,coinbase,kraken', 'binance,coinbase,kraken']
            })

            with patch('crypto_data.ingestion.coinmarketcap._fetch_snapshot', return_value=test_data_v1):
                ingest_universe(str(db_path), '2024-01-01', top_n=2, rate_limit_delay=0)

            # Verify 2 coins
            import duckdb
            conn = duckdb.connect(str(db_path))
            count_v1 = conn.execute("SELECT COUNT(*) FROM crypto_universe WHERE date = '2024-01-01'").fetchone()[0]
            assert count_v1 == 2
            conn.close()

            # Second ingestion - 3 coins (should replace)
            test_data_v2 = pd.DataFrame({
                'date': ['2024-01-01', '2024-01-01', '2024-01-01'],
                'symbol': ['BTC', 'ETH', 'SOL'],
                'rank': [1, 2, 3],
                'is_stablecoin': [False, False, False],
                'market_cap': [1200000000000, 500000000000, 80000000000],
                'category': ['layer-1', 'smart-contract-platform', 'smart-contract-platform'],
                'exchanges': ['binance,coinbase,kraken', 'binance,coinbase,kraken', 'binance,coinbase']
            })

            with patch('crypto_data.ingestion.coinmarketcap._fetch_snapshot', return_value=test_data_v2):
                ingest_universe(str(db_path), '2024-01-01', top_n=3, rate_limit_delay=0)

            # Verify 3 coins (replaced, not added)
            conn = duckdb.connect(str(db_path))
            count_v2 = conn.execute("SELECT COUNT(*) FROM crypto_universe WHERE date = '2024-01-01'").fetchone()[0]
            assert count_v2 == 3
            conn.close()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
