"""
Basic smoke tests for DuckDB database functionality.
"""

import tempfile
from pathlib import Path

import pytest

from crypto_data import CryptoDatabase


def test_database_creation():
    """Test that database can be created and tables exist."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as f:
        db_path = f.name
    # File is deleted when context exits, leaving just the path

    try:
        # Create database
        db = CryptoDatabase(db_path)

        # Verify tables exist
        tables = db.execute("SHOW TABLES").fetchall()
        table_names = [row[0] for row in tables]

        assert "spot" in table_names
        assert "futures" in table_names
        assert "crypto_universe" in table_names

        # Verify spot schema
        spot_schema = db.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'spot'
        """).fetchall()

        spot_columns = [row[0] for row in spot_schema]

        assert "exchange" in spot_columns
        assert "symbol" in spot_columns
        assert "timestamp" in spot_columns
        assert "open" in spot_columns
        assert "high" in spot_columns
        assert "low" in spot_columns
        assert "close" in spot_columns
        assert "volume" in spot_columns

        # Verify crypto_universe schema (v6.0.0)
        universe_schema = db.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'crypto_universe'
        """).fetchall()

        universe_columns = [row[0] for row in universe_schema]

        expected_columns = {
            "provider",
            "provider_id",
            "date",
            "symbol",
            "name",
            "slug",
            "rank",
            "market_cap",
            "fully_diluted_market_cap",
            "circulating_supply",
            "max_supply",
            "tags",
            "platform",
            "date_added",
        }
        assert expected_columns.issubset(set(universe_columns)), (
            f"Missing columns: {expected_columns - set(universe_columns)}"
        )
        # Old column should be gone:
        assert "categories" not in universe_columns

        db.close()

    finally:
        # Cleanup
        if Path(db_path).exists():
            Path(db_path).unlink()


def test_database_context_manager():
    """Test database can be used as context manager."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as f:
        db_path = f.name
    # File is deleted when context exits, leaving just the path

    try:
        with CryptoDatabase(db_path) as db:
            tables = db.execute("SHOW TABLES").fetchall()
            assert len(tables) >= 3  # spot, futures, crypto_universe

    finally:
        if Path(db_path).exists():
            Path(db_path).unlink()


def test_execute_query_with_results():
    """Test execute() method with queries that return results."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"

        try:
            db = CryptoDatabase(str(db_path))

            # Insert test data with the v6 schema
            db.conn.execute("""
                INSERT INTO crypto_universe
                (provider, provider_id, date, symbol, name, slug, rank,
                 market_cap, fully_diluted_market_cap,
                 circulating_supply, max_supply, tags, platform, date_added)
                VALUES
                    ('coinmarketcap', 1,    '2024-01-01', 'BTC', 'Bitcoin',  'bitcoin',  1, 800000000000, 900000000000, 19000000, 21000000, NULL, NULL, '2010-07-13'),
                    ('coinmarketcap', 1027, '2024-01-01', 'ETH', 'Ethereum', 'ethereum', 2, 400000000000, 400000000000, 120000000, NULL, NULL, NULL, '2015-08-07')
            """)

            # Query using execute() method
            result = db.execute("""
                SELECT symbol, rank, market_cap
                FROM crypto_universe
                WHERE date = '2024-01-01'
                ORDER BY rank
            """).fetchall()

            # Verify results
            assert len(result) == 2
            assert result[0][0] == "BTC"  # First symbol
            assert result[0][1] == 1  # Rank
            assert result[1][0] == "ETH"  # Second symbol
            assert result[1][1] == 2  # Rank

            db.close()

        finally:
            if db_path.exists():
                db_path.unlink()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
