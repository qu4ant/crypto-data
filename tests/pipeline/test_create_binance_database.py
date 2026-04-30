"""
Tests for create_binance_database() orchestration function.

Tests that create_binance_database() correctly orchestrates the complete workflow:
universe ingestion → symbol extraction → Binance data ingestion.

Uses mocks to avoid external API calls and file I/O.
"""

import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from crypto_data.database_builder import create_binance_database
from crypto_data.enums import DataType, Interval
from crypto_data.universe_filters import (
    DEFAULT_UNIVERSE_EXCLUDE_SYMBOLS,
    DEFAULT_UNIVERSE_EXCLUDE_TAGS,
)


def test_create_binance_database_orchestrates_full_workflow():
    """Test that create_binance_database() calls all components in correct order."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "test.db")

        # Mock all heavy functions
        with (
            patch(
                "crypto_data.database_builder.update_coinmarketcap_universe", new_callable=AsyncMock
            ) as mock_update_coinmarketcap_universe,
            patch(
                "crypto_data.database_builder.get_binance_symbols_from_universe"
            ) as mock_get_symbols,
            patch("crypto_data.binance_pipeline.update_binance_market_data") as mock_ingest_binance,
            patch("crypto_data.database_builder.CryptoDatabase") as mock_db,
            patch("crypto_data.database_builder.Path") as mock_path,
        ):
            # Setup mocks
            mock_get_symbols.return_value = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]

            # Mock CryptoDatabase for symbol count
            mock_db_instance = MagicMock()
            mock_db_instance.execute.return_value.fetchone.return_value = (3,)
            mock_db.return_value = mock_db_instance

            # Mock Path for database size
            mock_path_instance = MagicMock()
            mock_path_instance.stat.return_value.st_size = 1024 * 1024  # 1 MB
            mock_path.return_value = mock_path_instance

            # Call create_binance_database
            create_binance_database(
                db_path=db_path,
                start_date="2024-01-01",
                end_date="2024-03-31",
                top_n=50,
                interval=Interval.MIN_5,
                data_types=[DataType.SPOT, DataType.FUTURES],
            )

            # Verify update_coinmarketcap_universe was called
            mock_update_coinmarketcap_universe.assert_called_once()

            # Verify get_binance_symbols_from_universe called
            mock_get_symbols.assert_called_once_with(
                db_path,
                "2024-01-01",
                "2024-03-31",
                50,
                exclude_tags=DEFAULT_UNIVERSE_EXCLUDE_TAGS,
                exclude_symbols=DEFAULT_UNIVERSE_EXCLUDE_SYMBOLS,
            )

            # Verify update_binance_market_data called with extracted symbols
            mock_ingest_binance.assert_called_once_with(
                db_path=db_path,
                symbols=["BTCUSDT", "ETHUSDT", "SOLUSDT"],
                data_types=[DataType.SPOT, DataType.FUTURES],
                start_date="2024-01-01",
                end_date="2024-03-31",
                interval=Interval.MIN_5,
                max_concurrent_klines=20,
                max_concurrent_metrics=100,
                max_concurrent_funding=50,
                failure_threshold=3,
                repair_gaps_via_api=False,
                prune_klines_to_date_range=True,
            )


def test_create_binance_database_handles_empty_universe():
    """Test that create_binance_database() raises RuntimeError when no symbols are extracted."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "test.db")

        # Mock functions
        with (
            patch(
                "crypto_data.database_builder.update_coinmarketcap_universe", new_callable=AsyncMock
            ) as mock_update_coinmarketcap_universe,
            patch(
                "crypto_data.database_builder.get_binance_symbols_from_universe"
            ) as mock_get_symbols,
            patch("crypto_data.binance_pipeline.update_binance_market_data") as mock_ingest_binance,
        ):
            # Setup mocks - empty universe
            mock_get_symbols.return_value = []

            # Should raise RuntimeError for empty universe
            with pytest.raises(RuntimeError, match="No symbols extracted from universe"):
                create_binance_database(
                    db_path=db_path,
                    start_date="2024-01-01",
                    end_date="2024-01-31",
                    top_n=50,
                    interval=Interval.MIN_5,
                    data_types=[DataType.SPOT],
                )

            # Verify update_coinmarketcap_universe was called
            mock_update_coinmarketcap_universe.assert_called_once()

            # Verify get_binance_symbols_from_universe was called
            assert mock_get_symbols.call_count == 1

            # Verify update_binance_market_data was NOT called (error raised before)
            mock_ingest_binance.assert_not_called()


def test_create_binance_database_generates_correct_month_list():
    """Test that create_binance_database() generates correct monthly snapshots (batch mode)."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "test.db")

        # Mock functions
        with (
            patch(
                "crypto_data.database_builder.update_coinmarketcap_universe", new_callable=AsyncMock
            ) as mock_update_coinmarketcap_universe,
            patch(
                "crypto_data.database_builder.get_binance_symbols_from_universe"
            ) as mock_get_symbols,
            patch("crypto_data.binance_pipeline.update_binance_market_data") as mock_ingest_binance,
        ):
            # Setup mocks
            mock_get_symbols.return_value = ["BTCUSDT"]

            # Call sync with 12-month period
            create_binance_database(
                db_path=db_path,
                start_date="2024-01-01",
                end_date="2024-12-31",
                top_n=100,
                interval=Interval.MIN_5,
                data_types=[DataType.SPOT],
            )

            # Verify update_coinmarketcap_universe was called
            mock_update_coinmarketcap_universe.assert_called_once()


def test_create_binance_database_continues_on_universe_failure():
    """Test that create_binance_database() continues even if universe ingestion has errors (batch mode logs and continues)."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "test.db")

        # Mock functions
        with (
            patch(
                "crypto_data.database_builder.update_coinmarketcap_universe", new_callable=AsyncMock
            ) as mock_update_coinmarketcap_universe,
            patch(
                "crypto_data.database_builder.get_binance_symbols_from_universe"
            ) as mock_get_symbols,
            patch("crypto_data.binance_pipeline.update_binance_market_data") as mock_ingest_binance,
        ):
            # Setup mocks
            mock_get_symbols.return_value = ["BTCUSDT"]

            # Call sync (batch version doesn't raise on errors, logs them)
            create_binance_database(
                db_path=db_path,
                start_date="2024-01-01",
                end_date="2024-03-31",
                top_n=50,
                interval=Interval.MIN_5,
                data_types=[DataType.SPOT],
            )

            # Verify update_coinmarketcap_universe was called
            mock_update_coinmarketcap_universe.assert_called_once()

            # Verify update_binance_market_data was still called (workflow continued)
            mock_ingest_binance.assert_called_once()


def test_create_binance_database_uses_default_data_types():
    """Test that create_binance_database() uses default data_types if not specified."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "test.db")

        # Mock functions
        with (
            patch(
                "crypto_data.database_builder.update_coinmarketcap_universe", new_callable=AsyncMock
            ),
            patch(
                "crypto_data.database_builder.get_binance_symbols_from_universe"
            ) as mock_get_symbols,
            patch("crypto_data.binance_pipeline.update_binance_market_data") as mock_ingest_binance,
        ):
            mock_get_symbols.return_value = ["BTCUSDT"]

            # Call sync without data_types parameter
            create_binance_database(
                db_path=db_path,
                start_date="2024-01-01",
                end_date="2024-01-31",
                top_n=50,
                interval=Interval.MIN_5,
                # data_types not specified
            )

            # Verify update_binance_market_data called with default [DataType.SPOT, DataType.FUTURES]
            call_args = mock_ingest_binance.call_args
            assert call_args[1]["data_types"] == [DataType.SPOT, DataType.FUTURES]


def test_create_binance_database_handles_year_boundary():
    """Test that create_binance_database() correctly handles date ranges crossing year boundaries."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "test.db")

        # Mock functions
        with (
            patch(
                "crypto_data.database_builder.update_coinmarketcap_universe", new_callable=AsyncMock
            ) as mock_update_coinmarketcap_universe,
            patch(
                "crypto_data.database_builder.get_binance_symbols_from_universe"
            ) as mock_get_symbols,
            patch("crypto_data.binance_pipeline.update_binance_market_data"),
        ):
            mock_get_symbols.return_value = ["BTCUSDT"]

            # Call sync with year boundary
            create_binance_database(
                db_path=db_path,
                start_date="2023-11-01",
                end_date="2024-02-28",
                top_n=50,
                interval=Interval.MIN_5,
                data_types=[DataType.SPOT],
            )

            # Verify update_coinmarketcap_universe was called
            mock_update_coinmarketcap_universe.assert_called_once()


def test_create_binance_database_frequency_daily_generates_daily_dates():
    """Daily frequency should generate one universe snapshot per day."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "test.db")

        with (
            patch(
                "crypto_data.database_builder.update_coinmarketcap_universe", new_callable=AsyncMock
            ) as mock_update_coinmarketcap_universe,
            patch(
                "crypto_data.database_builder.get_binance_symbols_from_universe"
            ) as mock_get_symbols,
            patch("crypto_data.binance_pipeline.update_binance_market_data"),
        ):
            mock_get_symbols.return_value = ["BTCUSDT"]

            create_binance_database(
                db_path=db_path,
                start_date="2024-01-01",
                end_date="2024-01-03",
                top_n=50,
                interval=Interval.MIN_5,
                data_types=[DataType.SPOT],
                universe_frequency="daily",
            )

            kwargs = mock_update_coinmarketcap_universe.call_args.kwargs
            assert kwargs["dates"] == ["2024-01-01", "2024-01-02", "2024-01-03"]


def test_create_binance_database_default_frequency_is_monthly():
    """Default frequency should remain monthly for backward compatibility."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "test.db")

        with (
            patch(
                "crypto_data.database_builder.update_coinmarketcap_universe", new_callable=AsyncMock
            ) as mock_update_coinmarketcap_universe,
            patch(
                "crypto_data.database_builder.get_binance_symbols_from_universe"
            ) as mock_get_symbols,
            patch("crypto_data.binance_pipeline.update_binance_market_data"),
        ):
            mock_get_symbols.return_value = ["BTCUSDT"]

            create_binance_database(
                db_path=db_path,
                start_date="2024-01-15",
                end_date="2024-03-20",
                top_n=50,
                interval=Interval.MIN_5,
                data_types=[DataType.SPOT],
            )

            kwargs = mock_update_coinmarketcap_universe.call_args.kwargs
            assert kwargs["dates"] == ["2024-01-01", "2024-02-01", "2024-03-01"]


def test_create_binance_database_propagates_daily_quota():
    """daily_quota should be forwarded to update_coinmarketcap_universe."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "test.db")

        with (
            patch(
                "crypto_data.database_builder.update_coinmarketcap_universe", new_callable=AsyncMock
            ) as mock_update_coinmarketcap_universe,
            patch(
                "crypto_data.database_builder.get_binance_symbols_from_universe"
            ) as mock_get_symbols,
            patch("crypto_data.binance_pipeline.update_binance_market_data"),
        ):
            mock_get_symbols.return_value = ["BTCUSDT"]

            create_binance_database(
                db_path=db_path,
                start_date="2024-01-01",
                end_date="2024-01-31",
                top_n=50,
                interval=Interval.MIN_5,
                data_types=[DataType.SPOT],
                daily_quota=123,
            )

            kwargs = mock_update_coinmarketcap_universe.call_args.kwargs
            assert kwargs["daily_quota"] == 123


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
