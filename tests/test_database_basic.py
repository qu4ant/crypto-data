"""
Basic smoke tests for DuckDB database functionality.
"""

import pytest
import tempfile
from pathlib import Path

from crypto_data import CryptoDatabase


def test_database_creation():
    """Test that database can be created and tables exist."""
    with tempfile.NamedTemporaryFile(suffix='.db', delete=True) as f:
        db_path = f.name
    # File is deleted when context exits, leaving just the path

    try:
        # Create database
        db = CryptoDatabase(db_path)

        # Verify tables exist
        tables = db.execute("SHOW TABLES").fetchall()
        table_names = [row[0] for row in tables]

        assert 'binance_spot' in table_names
        assert 'binance_futures' in table_names
        assert 'crypto_universe' in table_names

        # Verify schemas
        universe_schema = db.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'crypto_universe'
        """).fetchall()

        universe_columns = [row[0] for row in universe_schema]

        assert 'date' in universe_columns
        assert 'symbol' in universe_columns
        assert 'rank' in universe_columns
        assert 'market_cap' in universe_columns
        assert 'categories' in universe_columns
        assert 'exchanges' in universe_columns
        assert 'has_perpetual' in universe_columns

        db.close()

    finally:
        # Cleanup
        if Path(db_path).exists():
            Path(db_path).unlink()


def test_database_stats_empty():
    """Test that stats work on empty database."""
    with tempfile.NamedTemporaryFile(suffix='.db', delete=True) as f:
        db_path = f.name
    # File is deleted when context exits, leaving just the path

    try:
        db = CryptoDatabase(db_path)
        stats = db.get_table_stats()

        # Should have entries for all tables
        assert 'binance_spot' in stats
        assert 'binance_futures' in stats
        assert 'crypto_universe' in stats

        # All should be empty
        assert stats['binance_spot']['records'] == 0
        assert stats['binance_futures']['records'] == 0
        assert stats['crypto_universe']['records'] == 0

        db.close()

    finally:
        if Path(db_path).exists():
            Path(db_path).unlink()


def test_database_context_manager():
    """Test database can be used as context manager."""
    with tempfile.NamedTemporaryFile(suffix='.db', delete=True) as f:
        db_path = f.name
    # File is deleted when context exits, leaving just the path

    try:
        with CryptoDatabase(db_path) as db:
            tables = db.execute("SHOW TABLES").fetchall()
            assert len(tables) >= 3  # At least 3 tables

    finally:
        if Path(db_path).exists():
            Path(db_path).unlink()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
