"""
Tests for 1000-prefix symbol normalization.

Validates that symbols like 1000PEPEUSDT are correctly normalized to PEPEUSDT
when stored in the database, ensuring consistency across spot and futures data.
"""

import pytest
import tempfile
from pathlib import Path
import duckdb
import pandas as pd
import zipfile

from crypto_data import CryptoDatabase
from crypto_data.utils.database import import_to_duckdb


def create_test_csv_zip(path: Path, symbol: str, timestamp_ms: int = 1704067200000):
    """
    Create a test ZIP file with CSV data.

    Parameters
    ----------
    path : Path
        Output path for ZIP file
    symbol : str
        Symbol name (used in filename, not in data)
    timestamp_ms : int
        Timestamp in milliseconds (default: 2024-01-01 00:00:00)
    """
    csv_content = f"{timestamp_ms},100.0,101.0,99.0,100.5,1000.0,{timestamp_ms + 300000},100500.0,50,500.0,50250.0,0\n"

    # Create temporary CSV
    csv_path = path.parent / f"{symbol}-5m-2024-01.csv"
    csv_path.write_text(csv_content)

    # Create ZIP
    with zipfile.ZipFile(path, 'w') as zf:
        zf.write(csv_path, csv_path.name)

    # Clean up CSV
    csv_path.unlink()


def test_1000prefix_normalization():
    """Test that 1000PEPEUSDT is stored as PEPEUSDT in database."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / 'test.db'
        zip_path = Path(tmpdir) / '1000PEPEUSDT-5m-2024-01.zip'

        # Create test data
        create_test_csv_zip(zip_path, '1000PEPEUSDT')

        # Create database and import
        db = CryptoDatabase(str(db_path))
        conn = db.conn

        try:
            import_to_duckdb(
                conn=conn,
                file_path=zip_path,
                symbol='1000PEPEUSDT',
                data_type='futures',
                interval='5m',
                original_symbol='PEPEUSDT'  # Key: normalize to original
            )

            # Verify data is stored with original symbol
            result = conn.execute("""
                SELECT symbol, close, volume
                FROM futures
                WHERE interval = '5m'
            """).fetchall()

            assert len(result) == 1
            assert result[0][0] == 'PEPEUSDT'  # Should be normalized
            assert result[0][1] == 100.5  # close price
            assert result[0][2] == 1000.0  # volume

        finally:
            db.close()

def test_normal_symbol_unchanged():
    """Test that normal symbols (not 1000-prefix) are unchanged."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / 'test.db'
        zip_path = Path(tmpdir) / 'BTCUSDT-5m-2024-01.zip'

        # Create test data
        create_test_csv_zip(zip_path, 'BTCUSDT')

        # Create database and import
        db = CryptoDatabase(str(db_path))
        conn = db.conn

        try:
            # Import without original_symbol (should use symbol as-is)
            import_to_duckdb(
                conn=conn,
                file_path=zip_path,
                symbol='BTCUSDT',
                data_type='spot',
                interval='5m'
                # No original_symbol parameter
            )

            # Verify data is stored with same symbol
            result = conn.execute("""
                SELECT symbol
                FROM spot
                WHERE interval = '5m'
            """).fetchall()

            assert len(result) == 1
            assert result[0][0] == 'BTCUSDT'  # Unchanged

        finally:
            db.close()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
