"""
Tests for timestamp format auto-detection (milliseconds vs microseconds).

Validates that the import function correctly detects and converts both
timestamp formats used by Binance Data Vision.
"""

import pytest
from crypto_data.enums import DataType, Interval
import tempfile
from pathlib import Path
import duckdb
import zipfile
from datetime import datetime

from crypto_data import CryptoDatabase
from crypto_data.utils.database import import_to_duckdb


def create_test_csv_zip_with_timestamps(
    path: Path,
    symbol: str,
    timestamps: list,
    use_header: bool = False
):
    """
    Create a test ZIP file with CSV data containing specific timestamps.

    Parameters
    ----------
    path : Path
        Output path for ZIP file
    symbol : str
        Symbol name
    timestamps : list
        List of timestamp values (in ms or µs)
    use_header : bool
        Whether to include header row
    """
    # Build CSV content
    lines = []

    if use_header:
        lines.append("open_time,open,high,low,close,volume,close_time,quote_volume,trades_count,taker_buy_base_volume,taker_buy_quote_volume,ignore")

    for ts in timestamps:
        # Each row: open_time, OHLCV data, close_time (same as open_time + 5min)
        close_ts = ts + 300000 if ts < 5e12 else ts + 300000000  # Add 5 min in correct unit
        line = f"{ts},100.0,101.0,99.0,100.5,1000.0,{close_ts},100500.0,50,500.0,50250.0,0"
        lines.append(line)

    csv_content = "\n".join(lines) + "\n"

    # Create temporary CSV
    csv_path = path.parent / f"{symbol}-5m-2024-01.csv"
    csv_path.write_text(csv_content)

    # Create ZIP
    with zipfile.ZipFile(path, 'w') as zf:
        zf.write(csv_path, csv_path.name)

    # Clean up CSV
    csv_path.unlink()


def test_timestamp_milliseconds_detection():
    """Test that millisecond timestamps (13 digits) are correctly converted."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / 'test.db'
        zip_path = Path(tmpdir) / 'BTCUSDT-5m-2024-01.zip'

        # Millisecond timestamps (2024-01-01 00:00:00, 00:05:00, 00:10:00)
        # Note: Timestamp is based on close_time (which is open_time + 5min)
        timestamps_ms = [
            1704067200000,  # 2024-01-01 00:00:00 (open) → close at 00:05:00
            1704067500000,  # 2024-01-01 00:05:00 (open) → close at 00:10:00
            1704067800000,  # 2024-01-01 00:10:00 (open) → close at 00:15:00
        ]

        create_test_csv_zip_with_timestamps(zip_path, 'BTCUSDT', timestamps_ms)

        # Import data
        db = CryptoDatabase(str(db_path))
        conn = db.conn

        try:
            import_to_duckdb(
                conn=conn,
                file_path=zip_path,
                symbol='BTCUSDT',
                data_type=DataType.SPOT,
                interval=Interval.MIN_5
            )

            # Verify timestamps are correctly converted
            result = conn.execute("""
                SELECT timestamp
                FROM spot
                ORDER BY timestamp
            """).fetchall()

            assert len(result) == 3

            # Check timestamps (based on close_time)
            assert result[0][0] == datetime(2024, 1, 1, 0, 5, 0)
            assert result[1][0] == datetime(2024, 1, 1, 0, 10, 0)
            assert result[2][0] == datetime(2024, 1, 1, 0, 15, 0)

        finally:
            db.close()


def test_timestamp_microseconds_detection():
    """Test that microsecond timestamps (16 digits) are correctly converted."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / 'test.db'
        zip_path = Path(tmpdir) / 'BTCUSDT-5m-2025-01.zip'

        # Microsecond timestamps (2025-01-01 00:00:00, 00:05:00, 00:10:00)
        # Note: Binance switched from ms to µs between 2024 and 2025
        # Timestamp is based on close_time (which is open_time + 5min)
        timestamps_us = [
            1735689600000000,  # 2025-01-01 00:00:00 (open) → close at 00:05:00
            1735689900000000,  # 2025-01-01 00:05:00 (open) → close at 00:10:00
            1735690200000000,  # 2025-01-01 00:10:00 (open) → close at 00:15:00
        ]

        create_test_csv_zip_with_timestamps(zip_path, 'BTCUSDT', timestamps_us)

        # Import data
        db = CryptoDatabase(str(db_path))
        conn = db.conn

        try:
            import_to_duckdb(
                conn=conn,
                file_path=zip_path,
                symbol='BTCUSDT',
                data_type=DataType.SPOT,
                interval=Interval.MIN_5
            )

            # Verify timestamps are correctly converted
            result = conn.execute("""
                SELECT timestamp
                FROM spot
                ORDER BY timestamp
            """).fetchall()

            assert len(result) == 3

            # Check timestamps (based on close_time)
            assert result[0][0] == datetime(2025, 1, 1, 0, 5, 0)
            assert result[1][0] == datetime(2025, 1, 1, 0, 10, 0)
            assert result[2][0] == datetime(2025, 1, 1, 0, 15, 0)

        finally:
            db.close()


def test_timestamp_threshold_boundary():
    """Test timestamp detection at the threshold boundary (5e12)."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / 'test.db'
        zip_path = Path(tmpdir) / 'TEST-5m-boundary.zip'

        # Timestamps around the threshold (5e12 = ~2128 AD)
        # Below threshold: milliseconds, above: microseconds
        timestamps = [
            4999999999999,  # Just below threshold → milliseconds
            5000000000000,  # Exactly at threshold → microseconds
            5000000000001,  # Just above threshold → microseconds
        ]

        create_test_csv_zip_with_timestamps(zip_path, 'TEST', timestamps)

        # Import data
        db = CryptoDatabase(str(db_path))
        conn = db.conn

        try:
            import_to_duckdb(
                conn=conn,
                file_path=zip_path,
                symbol='TEST',
                data_type=DataType.SPOT,
                interval=Interval.MIN_5
            )

            # Verify all timestamps are valid (no NaT)
            result = conn.execute("""
                SELECT timestamp
                FROM spot
                ORDER BY timestamp
            """).fetchall()

            assert len(result) == 3

            # All should be valid timestamps (not None/NaT)
            for row in result:
                assert row[0] is not None
                assert isinstance(row[0], datetime)

            # Verify chronological order is maintained
            assert result[0][0] < result[1][0] < result[2][0]

        finally:
            db.close()


def test_timestamp_mixed_format_error():
    """Test that mixing ms and µs in same file is handled gracefully."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / 'test.db'
        zip_path = Path(tmpdir) / 'MIXED-5m-2024.zip'

        # Mixed formats (should not happen in practice, but test robustness)
        timestamps_mixed = [
            1704067200000,      # Milliseconds (2024-01-01)
            1735689600000000,   # Microseconds (2025-01-01)
        ]

        create_test_csv_zip_with_timestamps(zip_path, 'MIXED', timestamps_mixed)

        # Import data
        db = CryptoDatabase(str(db_path))
        conn = db.conn

        try:
            import_to_duckdb(
                conn=conn,
                file_path=zip_path,
                symbol='MIXED',
                data_type=DataType.SPOT,
                interval=Interval.MIN_5
            )

            # Verify both timestamps are valid (even if logically inconsistent)
            result = conn.execute("""
                SELECT timestamp
                FROM spot
                ORDER BY timestamp
            """).fetchall()

            assert len(result) == 2

            # Both should be valid
            assert result[0][0] is not None
            assert result[1][0] is not None

            # First should be 2024, second should be 2025
            assert result[0][0].year == 2024
            assert result[1][0].year == 2025

        finally:
            db.close()


def test_timestamp_with_header():
    """Test that timestamp detection works with CSV headers."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / 'test.db'
        zip_path = Path(tmpdir) / 'HEADER-5m-2024.zip'

        timestamps_ms = [1704067200000, 1704067500000]  # open times

        create_test_csv_zip_with_timestamps(
            zip_path, 'HEADER', timestamps_ms, use_header=True
        )

        # Import data
        db = CryptoDatabase(str(db_path))
        conn = db.conn

        try:
            import_to_duckdb(
                conn=conn,
                file_path=zip_path,
                symbol='HEADER',
                data_type=DataType.SPOT,
                interval=Interval.MIN_5
            )

            # Verify timestamps are correctly converted
            result = conn.execute("""
                SELECT timestamp
                FROM spot
                ORDER BY timestamp
            """).fetchall()

            assert len(result) == 2
            # Timestamps based on close_time (open + 5min)
            assert result[0][0] == datetime(2024, 1, 1, 0, 5, 0)
            assert result[1][0] == datetime(2024, 1, 1, 0, 10, 0)

        finally:
            db.close()


def test_timestamp_rounding_to_full_seconds():
    """Test that Binance timestamps ending in .999 are rounded up to full seconds."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / 'test.db'
        zip_path_ms = Path(tmpdir) / 'ROUND-5m-2024-ms.zip'
        zip_path_us = Path(tmpdir) / 'ROUND-5m-2025-us.zip'

        # Test milliseconds: close_time ending in .999
        # Note: timestamps parameter is open_time, close_time = open_time + 5min (300000ms)
        # 2024-01-01 00:00:00 = 1704067200000 ms
        # To get close_time = 00:04:59.999, we need open_time = 00:04:59.999 - 5min = 23:59:59.999 (Dec 31)
        # 2024-01-01 00:04:59.999 (close_time) → should round to 00:05:00.000
        # 2024-01-01 00:09:59.999 (close_time) → should round to 00:10:00.000
        # These simulate real Binance data where close_time = 03:59:59.999, 07:59:59.999, etc.
        timestamps_ms = [
            1704067199999,  # open_time: 00:00:00 - 1ms → close_time: 00:04:59.999 → rounds to 00:05:00
            1704067499999,  # open_time: 00:04:59.999 → close_time: 00:09:59.999 → rounds to 00:10:00
        ]

        # Test microseconds: close_time ending in .999999
        # 2025-01-01 00:00:00 = 1735689600000000 μs
        # Note: Function adds 300000000 μs (5 minutes) to open_time to get close_time
        # 2025-01-01 00:14:59.999999 (close_time) → should round to 00:15:00.000
        # 2025-01-01 00:19:59.999999 (close_time) → should round to 00:20:00.000
        timestamps_us = [
            1735690199999999,  # open_time: 00:09:59.999999 → close_time: 00:14:59.999999 → rounds to 00:15:00
            1735690499999999,  # open_time: 00:14:59.999999 → close_time: 00:19:59.999999 → rounds to 00:20:00
        ]

        # Create test data for milliseconds
        create_test_csv_zip_with_timestamps(zip_path_ms, 'ROUND', timestamps_ms)

        # Import and verify milliseconds rounding
        db = CryptoDatabase(str(db_path))
        conn = db.conn

        try:
            import_to_duckdb(
                conn=conn,
                file_path=zip_path_ms,
                symbol='ROUNDMS',
                data_type=DataType.SPOT,
                interval=Interval.MIN_5
            )

            result_ms = conn.execute("""
                SELECT timestamp
                FROM spot
                WHERE symbol = 'ROUNDMS'
                ORDER BY timestamp
            """).fetchall()

            assert len(result_ms) == 2

            # Verify rounding: .999 → full second
            assert result_ms[0][0] == datetime(2024, 1, 1, 0, 5, 0)
            assert result_ms[1][0] == datetime(2024, 1, 1, 0, 10, 0)

            # Verify no sub-second precision (microseconds should be 0)
            assert result_ms[0][0].microsecond == 0
            assert result_ms[1][0].microsecond == 0

            # Create and test microseconds data
            create_test_csv_zip_with_timestamps(zip_path_us, 'ROUND', timestamps_us)

            import_to_duckdb(
                conn=conn,
                file_path=zip_path_us,
                symbol='ROUNDUS',
                data_type=DataType.SPOT,
                interval=Interval.MIN_5
            )

            result_us = conn.execute("""
                SELECT timestamp
                FROM spot
                WHERE symbol = 'ROUNDUS'
                ORDER BY timestamp
            """).fetchall()

            assert len(result_us) == 2

            # Verify rounding: .999999 → full second
            assert result_us[0][0] == datetime(2025, 1, 1, 0, 15, 0)
            assert result_us[1][0] == datetime(2025, 1, 1, 0, 20, 0)

            # Verify no sub-second precision
            assert result_us[0][0].microsecond == 0
            assert result_us[1][0].microsecond == 0

        finally:
            db.close()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
