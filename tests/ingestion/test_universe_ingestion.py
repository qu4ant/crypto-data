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


def test_universe_transaction_atomic():
    """Test that universe update is atomic (DELETE + INSERT in one transaction)."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / 'test.db'

        # Create database
        db = CryptoDatabase(str(db_path))
        conn = db.conn

        # Insert initial data
        initial_data = pd.DataFrame([
            {'date': pd.Timestamp('2024-01-01'), 'symbol': 'BTC', 'rank': 1, 'market_cap': 1000000, 'categories': ''},
            {'date': pd.Timestamp('2024-01-01'), 'symbol': 'ETH', 'rank': 2, 'market_cap': 500000, 'categories': ''}
        ])
        conn.execute("INSERT INTO crypto_universe SELECT * FROM initial_data")

        # Verify initial state
        result = conn.execute("SELECT COUNT(*) FROM crypto_universe WHERE date = '2024-01-01'").fetchone()
        assert result[0] == 2

        db.close()

        # Mock the CoinMarketCap API to return new data
        new_data = [
            {'symbol': 'BTC', 'cmcRank': 1, 'quotes': [{'marketCap': 1100000}], 'tags': []},
            {'symbol': 'SOL', 'cmcRank': 2, 'quotes': [{'marketCap': 600000}], 'tags': []}
        ]

        with patch('crypto_data.ingestion.CoinMarketCapClient') as MockClient:
            mock_instance = AsyncMock()
            mock_instance.__aenter__.return_value = mock_instance
            mock_instance.__aexit__.return_value = None
            mock_instance.get_historical_listings = AsyncMock(return_value=new_data)
            MockClient.return_value = mock_instance

            # Run ingestion (should replace old data atomically)
            asyncio.run(ingest_universe(
                db_path=str(db_path),
                months=['2024-01'],
                top_n=2
            ))

        # Verify data was replaced (not appended)
        db = CryptoDatabase(str(db_path))
        result = db.execute("SELECT symbol, rank FROM crypto_universe WHERE date = '2024-01-01' ORDER BY rank").fetchall()
        db.close()

        assert len(result) == 2
        assert result[0][0] == 'BTC'  # BTC still rank 1
        assert result[1][0] == 'SOL'  # SOL replaced ETH at rank 2


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


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
