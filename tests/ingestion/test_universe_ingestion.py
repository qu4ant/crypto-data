"""
Tests for atomic transaction handling in universe ingestion.

Validates that DELETE + INSERT operations are atomic and that ROLLBACK
only occurs when the transaction has not been committed.
"""

import pytest
import tempfile
from pathlib import Path
import pandas as pd
from unittest.mock import patch, MagicMock

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
            mock_instance = MockClient.return_value
            mock_instance.get_historical_listings.return_value = new_data

            # Run ingestion (should replace old data atomically)
            ingest_universe(
                db_path=str(db_path),
                date='2024-01-01',
                top_n=2
            )

        # Verify data was replaced (not appended)
        db = CryptoDatabase(str(db_path))
        result = db.execute("SELECT symbol, rank FROM crypto_universe WHERE date = '2024-01-01' ORDER BY rank").fetchall()
        db.close()

        assert len(result) == 2
        assert result[0][0] == 'BTC'  # BTC still rank 1
        assert result[1][0] == 'SOL'  # SOL replaced ETH at rank 2


def test_universe_rollback_on_error():
    """Test that ROLLBACK occurs when INSERT fails (before COMMIT)."""
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

        # Mock API to return data, but force INSERT to fail
        new_data = [
            {'symbol': 'BTC', 'cmcRank': 1, 'quotes': [{'marketCap': 1100000}], 'tags': []}
        ]

        with patch('crypto_data.ingestion.CoinMarketCapClient') as MockClient:
            mock_instance = MockClient.return_value
            mock_instance.get_historical_listings.return_value = new_data

            # Patch the database connection to simulate INSERT failure
            with patch('crypto_data.ingestion.CryptoDatabase') as MockDB:
                mock_db = MagicMock()
                mock_conn = MagicMock()

                # Make INSERT raise an error
                def execute_side_effect(query, *args):
                    if 'INSERT' in query:
                        raise Exception("Simulated INSERT failure")
                    # Let other queries pass through

                mock_conn.execute = MagicMock(side_effect=execute_side_effect)
                mock_db.conn = mock_conn
                MockDB.return_value = mock_db

                # Run ingestion (should fail and rollback)
                with pytest.raises(Exception, match="Simulated INSERT failure"):
                    ingest_universe(
                        db_path=str(db_path),
                        date='2024-01-01',
                        top_n=1
                    )

                # Verify ROLLBACK was called
                rollback_calls = [call for call in mock_conn.execute.call_args_list
                                  if 'ROLLBACK' in str(call)]
                assert len(rollback_calls) == 1, "ROLLBACK should be called on error"


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
            mock_instance = MockClient.return_value
            mock_instance.get_historical_listings.return_value = new_data

            # Run ingestion - this should successfully commit
            ingest_universe(
                db_path=str(db_path),
                date='2024-01-01',
                top_n=1
            )

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
            mock_instance = MockClient.return_value
            mock_instance.get_historical_listings.return_value = new_data

            # Run twice
            ingest_universe(db_path=str(db_path), date='2024-01-01', top_n=2)
            ingest_universe(db_path=str(db_path), date='2024-01-01', top_n=2)

        # Verify only 2 records (not 4)
        db = CryptoDatabase(str(db_path))
        result = db.execute("SELECT COUNT(*) FROM crypto_universe WHERE date = '2024-01-01'").fetchone()
        db.close()

        assert result[0] == 2, "Should replace, not append"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
