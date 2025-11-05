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
from crypto_data.ingestion import ingest_universe


def test_universe_rollback_on_error():
    """Test that errors during API fetch are logged (batch version doesn't raise)."""
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
        with patch('crypto_data.ingestion.CoinMarketCapClient') as MockClient:
            mock_instance = AsyncMock()
            mock_instance.__aenter__.return_value = mock_instance
            mock_instance.__aexit__.return_value = None
            mock_instance.get_historical_listings = AsyncMock(side_effect=Exception("API error"))
            MockClient.return_value = mock_instance

            # Batch version logs error and continues (doesn't raise)
            asyncio.run(ingest_universe(
                db_path=str(db_path),
                months=['2024-01'],
                top_n=1
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

        with patch('crypto_data.ingestion.CoinMarketCapClient') as MockClient:
            mock_instance = AsyncMock()
            mock_instance.__aenter__.return_value = mock_instance
            mock_instance.__aexit__.return_value = None
            mock_instance.get_historical_listings = AsyncMock(return_value=new_data)
            MockClient.return_value = mock_instance

            # Run ingestion - this should successfully commit
            asyncio.run(ingest_universe(
                db_path=str(db_path),
                months=['2024-01'],
                top_n=1
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

        with patch('crypto_data.ingestion.CoinMarketCapClient') as MockClient:
            mock_instance = AsyncMock()
            mock_instance.__aenter__.return_value = mock_instance
            mock_instance.__aexit__.return_value = None
            mock_instance.get_historical_listings = AsyncMock(return_value=new_data)
            MockClient.return_value = mock_instance

            # Run twice
            asyncio.run(ingest_universe(db_path=str(db_path), months=['2024-01'], top_n=2))
            asyncio.run(ingest_universe(db_path=str(db_path), months=['2024-01'], top_n=2))

        # Verify only 2 records (not 4)
        db = CryptoDatabase(str(db_path))
        result = db.execute("SELECT COUNT(*) FROM crypto_universe WHERE date = '2024-01-01'").fetchone()
        db.close()

        assert result[0] == 2, "Should replace, not append"


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


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
