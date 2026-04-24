"""
Tests for atomic transaction handling in universe ingestion.

Validates that DELETE + INSERT operations are atomic and that ROLLBACK
only occurs when the transaction has not been committed.
"""

import pytest
import asyncio
import tempfile
from pathlib import Path
import pandas as pd
from unittest.mock import patch, MagicMock, AsyncMock

from crypto_data import CryptoDatabase
from crypto_data.database_builder import update_coinmarketcap_universe


def test_universe_rollback_on_error():
    """Test that errors during API fetch raise RuntimeError when ALL fetches fail."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / 'test.db'

        # Create database with initial data
        db = CryptoDatabase(str(db_path))
        conn = db.conn

        initial_data = pd.DataFrame([
            {'date': pd.Timestamp('2024-01-01'), 'symbol': 'BTC', 'rank': 1, 'market_cap': 1000000, 'categories': ''}
        ])
        conn.execute("INSERT INTO crypto_universe SELECT * FROM initial_data")
        db.close()

        # Mock API to simulate error
        with patch('crypto_data.database_builder.CoinMarketCapClient') as MockClient:
            mock_instance = AsyncMock()
            mock_instance.__aenter__.return_value = mock_instance
            mock_instance.__aexit__.return_value = None
            mock_instance.get_historical_listings = AsyncMock(side_effect=Exception("API error"))
            MockClient.return_value = mock_instance

            # When ALL fetches fail, should raise RuntimeError
            with pytest.raises(RuntimeError, match="All .* universe fetches failed"):
                asyncio.run(update_coinmarketcap_universe(
                    db_path=str(db_path),
                    dates=['2024-01-01'],
                    top_n=1,
                    skip_existing=False,
                ))

        # Verify original data still there (no new data due to error)
        db = CryptoDatabase(str(db_path))
        result = db.execute("SELECT COUNT(*) FROM crypto_universe WHERE date = '2024-01-01'").fetchone()
        db.close()

        assert result[0] == 1, "Original data should still be present"


def test_universe_no_rollback_after_commit():
    """Test that committed flag prevents ROLLBACK after successful COMMIT."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / 'test.db'

        # Create database with initial data
        db = CryptoDatabase(str(db_path))
        conn = db.conn

        initial_data = pd.DataFrame([
            {'date': pd.Timestamp('2024-01-01'), 'symbol': 'OLD', 'rank': 1, 'market_cap': 1000, 'categories': ''}
        ])
        conn.execute("INSERT INTO crypto_universe SELECT * FROM initial_data")
        db.close()

        # Mock API to return new data
        new_data = [
            {'symbol': 'BTC', 'cmcRank': 1, 'quotes': [{'marketCap': 2000000}], 'tags': []}
        ]

        with patch('crypto_data.database_builder.CoinMarketCapClient') as MockClient:
            mock_instance = AsyncMock()
            mock_instance.__aenter__.return_value = mock_instance
            mock_instance.__aexit__.return_value = None
            mock_instance.get_historical_listings = AsyncMock(return_value=new_data)
            MockClient.return_value = mock_instance

            # Run ingestion - this should successfully commit
            asyncio.run(update_coinmarketcap_universe(
                db_path=str(db_path),
                dates=['2024-01-01'],
                top_n=1,
                skip_existing=False,
            ))

        # Verify data was committed and replaced (not OLD, but BTC)
        db = CryptoDatabase(str(db_path))
        result = db.execute("""
            SELECT symbol FROM crypto_universe WHERE date = '2024-01-01'
        """).fetchone()
        db.close()

        assert result[0] == 'BTC', "Data should be committed and replaced"


def test_universe_idempotent():
    """Test that running ingestion twice with same data is idempotent."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / 'test.db'

        # Mock API
        new_data = [
            {'symbol': 'BTC', 'cmcRank': 1, 'quotes': [{'marketCap': 1000000}], 'tags': []},
            {'symbol': 'ETH', 'cmcRank': 2, 'quotes': [{'marketCap': 500000}], 'tags': []}
        ]

        with patch('crypto_data.database_builder.CoinMarketCapClient') as MockClient:
            mock_instance = AsyncMock()
            mock_instance.__aenter__.return_value = mock_instance
            mock_instance.__aexit__.return_value = None
            mock_instance.get_historical_listings = AsyncMock(return_value=new_data)
            MockClient.return_value = mock_instance

            # Run twice
            asyncio.run(update_coinmarketcap_universe(db_path=str(db_path), dates=['2024-01-01'], top_n=2))
            asyncio.run(update_coinmarketcap_universe(db_path=str(db_path), dates=['2024-01-01'], top_n=2))

        # Verify only 2 records (not 4)
        db = CryptoDatabase(str(db_path))
        result = db.execute("SELECT COUNT(*) FROM crypto_universe WHERE date = '2024-01-01'").fetchone()
        db.close()

        assert result[0] == 2, "Should replace, not append"


def test_update_coinmarketcap_universe_excludes_synthetic_assets_by_default():
    """Default universe ingestion should exclude stable, wrapped, and tokenized assets."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / 'test.db'

        new_data = [
            {'symbol': 'BTC', 'cmcRank': 1, 'quotes': [{'marketCap': 1000000}], 'tags': ['mineable']},
            {'symbol': 'USDT', 'cmcRank': 2, 'quotes': [{'marketCap': 900000}], 'tags': ['stablecoin']},
            {'symbol': 'WBTC', 'cmcRank': 3, 'quotes': [{'marketCap': 800000}], 'tags': ['wrapped-tokens']},
            {'symbol': 'PAXG', 'cmcRank': 4, 'quotes': [{'marketCap': 700000}], 'tags': ['tokenized-gold']},
            {'symbol': 'TSLA', 'cmcRank': 5, 'quotes': [{'marketCap': 600000}], 'tags': ['tokenized-stock']},
        ]

        with patch('crypto_data.database_builder.CoinMarketCapClient') as MockClient:
            mock_instance = AsyncMock()
            mock_instance.__aenter__.return_value = mock_instance
            mock_instance.__aexit__.return_value = None
            mock_instance.get_historical_listings = AsyncMock(return_value=new_data)
            MockClient.return_value = mock_instance

            exclusions = asyncio.run(update_coinmarketcap_universe(
                db_path=str(db_path),
                dates=['2024-01-01'],
                top_n=5,
            ))

        db = CryptoDatabase(str(db_path))
        rows = db.execute("""
            SELECT symbol FROM crypto_universe WHERE date = '2024-01-01' ORDER BY symbol
        """).fetchall()
        db.close()

        assert [row[0] for row in rows] == ['BTC']
        assert exclusions['by_tag'] == {'USDT', 'WBTC', 'PAXG', 'TSLA'}


def test_update_coinmarketcap_universe_allows_explicit_filter_opt_out():
    """Passing empty filter lists should disable the package defaults."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / 'test.db'

        new_data = [
            {'symbol': 'USDT', 'cmcRank': 1, 'quotes': [{'marketCap': 1000000}], 'tags': ['stablecoin']},
        ]

        with patch('crypto_data.database_builder.CoinMarketCapClient') as MockClient:
            mock_instance = AsyncMock()
            mock_instance.__aenter__.return_value = mock_instance
            mock_instance.__aexit__.return_value = None
            mock_instance.get_historical_listings = AsyncMock(return_value=new_data)
            MockClient.return_value = mock_instance

            asyncio.run(update_coinmarketcap_universe(
                db_path=str(db_path),
                dates=['2024-01-01'],
                top_n=1,
                exclude_tags=[],
                exclude_symbols=[],
            ))

        db = CryptoDatabase(str(db_path))
        result = db.execute("""
            SELECT symbol FROM crypto_universe WHERE date = '2024-01-01'
        """).fetchone()
        db.close()

        assert result[0] == 'USDT'


def test_universe_transaction_atomicity():
    """
    Test transaction atomicity: DELETE + INSERT are atomic.

    Validates that if we manually trigger a ROLLBACK after DELETE,
    the original data is preserved (simulating what happens if INSERT fails).
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / 'test.db'

        # Create database with initial data
        db = CryptoDatabase(str(db_path))
        conn = db.conn

        initial_data = pd.DataFrame([
            {'date': pd.Timestamp('2024-01-01'), 'symbol': 'BTC', 'rank': 1, 'market_cap': 1000000, 'categories': 'original'},
            {'date': pd.Timestamp('2024-01-01'), 'symbol': 'ETH', 'rank': 2, 'market_cap': 500000, 'categories': 'original'}
        ])
        conn.execute("INSERT INTO crypto_universe SELECT * FROM initial_data")

        # Test Case 1: Transaction with ROLLBACK - data should be preserved
        conn.execute("BEGIN TRANSACTION")
        conn.execute("DELETE FROM crypto_universe WHERE date = '2024-01-01'")

        # Verify data is "gone" within the transaction
        result_during = conn.execute("SELECT COUNT(*) FROM crypto_universe WHERE date = '2024-01-01'").fetchone()
        assert result_during[0] == 0, "Data should appear deleted within transaction"

        # Now ROLLBACK (simulating INSERT failure)
        conn.execute("ROLLBACK")

        # Verify data is RESTORED after rollback
        result_after = conn.execute("SELECT COUNT(*) FROM crypto_universe WHERE date = '2024-01-01'").fetchone()
        assert result_after[0] == 2, "Data should be restored after ROLLBACK"

        # Test Case 2: Transaction with COMMIT - data should be deleted
        conn.execute("BEGIN TRANSACTION")
        conn.execute("DELETE FROM crypto_universe WHERE date = '2024-01-01'")

        # New data to insert
        new_data = pd.DataFrame([
            {'date': pd.Timestamp('2024-01-01'), 'symbol': 'SOL', 'rank': 1, 'market_cap': 2000000, 'categories': 'new'}
        ])
        conn.execute("INSERT INTO crypto_universe SELECT * FROM new_data")
        conn.execute("COMMIT")

        # Verify data is replaced after commit
        result_final = conn.execute("""
            SELECT symbol FROM crypto_universe WHERE date = '2024-01-01'
        """).fetchone()
        assert result_final[0] == 'SOL', "Data should be replaced after COMMIT"

        db.close()


def test_update_coinmarketcap_universe_accepts_dates_kwarg():
    """update_coinmarketcap_universe should accept canonical YYYY-MM-DD date list."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / 'test.db'

        new_data = [
            {'symbol': 'BTC', 'cmcRank': 1, 'quotes': [{'marketCap': 1000000}], 'tags': []}
        ]

        with patch('crypto_data.database_builder.CoinMarketCapClient') as MockClient:
            mock_instance = AsyncMock()
            mock_instance.__aenter__.return_value = mock_instance
            mock_instance.__aexit__.return_value = None
            mock_instance.get_historical_listings = AsyncMock(return_value=new_data)
            MockClient.return_value = mock_instance

            asyncio.run(update_coinmarketcap_universe(
                db_path=str(db_path),
                dates=['2024-01-01'],
                top_n=1,
            ))

            mock_instance.get_historical_listings.assert_awaited_once_with('2024-01-01', 1)


def test_update_coinmarketcap_universe_requires_dates():
    """The CMC universe API requires explicit YYYY-MM-DD snapshot dates."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / 'test.db'

        with pytest.raises(ValueError, match="`dates` must be provided"):
            asyncio.run(update_coinmarketcap_universe(
                db_path=str(db_path),
                top_n=1,
            ))


def test_update_coinmarketcap_universe_skip_existing_filters_present_dates():
    """skip_existing=True should avoid API calls for existing snapshot dates."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / 'test.db'

        db = CryptoDatabase(str(db_path))
        db.conn.execute("""
            INSERT INTO crypto_universe (date, symbol, rank, market_cap, categories)
            VALUES ('2024-01-01', 'BTC', 1, 1000000, NULL)
        """)
        db.close()

        new_data = [
            {'symbol': 'ETH', 'cmcRank': 1, 'quotes': [{'marketCap': 500000}], 'tags': []}
        ]

        with patch('crypto_data.database_builder.CoinMarketCapClient') as MockClient:
            mock_instance = AsyncMock()
            mock_instance.__aenter__.return_value = mock_instance
            mock_instance.__aexit__.return_value = None
            mock_instance.get_historical_listings = AsyncMock(return_value=new_data)
            MockClient.return_value = mock_instance

            asyncio.run(update_coinmarketcap_universe(
                db_path=str(db_path),
                dates=['2024-01-01', '2024-01-02'],
                top_n=1,
                skip_existing=True,
            ))

            mock_instance.get_historical_listings.assert_awaited_once_with('2024-01-02', 1)


def test_update_coinmarketcap_universe_skip_existing_is_date_only():
    """skip_existing=True should not refresh existing dates when top_n changes."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / 'test.db'

        db = CryptoDatabase(str(db_path))
        db.conn.execute("""
            INSERT INTO crypto_universe (date, symbol, rank, market_cap, categories)
            VALUES ('2024-01-01', 'BTC', 1, 1000000, NULL)
        """)
        db.close()

        new_data = [
            {'symbol': 'BTC', 'cmcRank': 1, 'quotes': [{'marketCap': 1000000}], 'tags': []},
            {'symbol': 'ETH', 'cmcRank': 2, 'quotes': [{'marketCap': 500000}], 'tags': []}
        ]

        with patch('crypto_data.database_builder.CoinMarketCapClient') as MockClient:
            mock_instance = AsyncMock()
            mock_instance.__aenter__.return_value = mock_instance
            mock_instance.__aexit__.return_value = None
            mock_instance.get_historical_listings = AsyncMock(return_value=new_data)
            MockClient.return_value = mock_instance

            asyncio.run(update_coinmarketcap_universe(
                db_path=str(db_path),
                dates=['2024-01-01'],
                top_n=2,
                skip_existing=True,
            ))

            mock_instance.get_historical_listings.assert_not_awaited()

        db = CryptoDatabase(str(db_path))
        result = db.execute("""
            SELECT COUNT(*) FROM crypto_universe WHERE date = '2024-01-01'
        """).fetchone()
        db.close()

        assert result[0] == 1


def test_update_coinmarketcap_universe_skip_existing_disabled_fetches_all():
    """skip_existing=False should fetch all requested dates, including existing ones."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / 'test.db'

        db = CryptoDatabase(str(db_path))
        db.conn.execute("""
            INSERT INTO crypto_universe (date, symbol, rank, market_cap, categories)
            VALUES ('2024-01-01', 'BTC', 1, 1000000, NULL)
        """)
        db.close()

        new_data = [
            {'symbol': 'ETH', 'cmcRank': 1, 'quotes': [{'marketCap': 500000}], 'tags': []}
        ]

        with patch('crypto_data.database_builder.CoinMarketCapClient') as MockClient:
            mock_instance = AsyncMock()
            mock_instance.__aenter__.return_value = mock_instance
            mock_instance.__aexit__.return_value = None
            mock_instance.get_historical_listings = AsyncMock(return_value=new_data)
            MockClient.return_value = mock_instance

            asyncio.run(update_coinmarketcap_universe(
                db_path=str(db_path),
                dates=['2024-01-01', '2024-01-02'],
                top_n=1,
                skip_existing=False,
            ))

            assert mock_instance.get_historical_listings.await_count == 2


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
