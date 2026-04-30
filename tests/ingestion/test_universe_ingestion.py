"""
Tests for atomic transaction handling in universe ingestion.

Validates that DELETE + INSERT operations are atomic and that ROLLBACK
only occurs when the transaction has not been committed.
"""

import asyncio
import datetime
import logging
import tempfile
from dataclasses import dataclass
from pathlib import Path
from unittest.mock import AsyncMock, patch

import duckdb
import pytest

from crypto_data import CryptoDatabase
from crypto_data.database_builder import _get_existing_universe_dates, update_coinmarketcap_universe

# =============================================================================
# Fixture helpers — v6 schema
# =============================================================================


@dataclass(frozen=True)
class UniverseRowFixture:
    """Test fixture for a single crypto_universe v6 row.

    All fields are required so test intent stays explicit at the call site.
    The fixture only covers the identity + selection columns; nullable
    metadata (slug, FDMC, supplies, tags, platform, date_added) is set to
    NULL in the helper.
    """

    provider_id: int
    date: str
    symbol: str
    name: str
    rank: int
    market_cap: float


def _insert_universe_row(conn: duckdb.DuckDBPyConnection, row: UniverseRowFixture) -> None:
    """Insert a single universe row into a freshly created v6 schema."""
    conn.execute(
        """
        INSERT INTO crypto_universe
        (provider, provider_id, date, symbol, name, slug, rank,
         market_cap, fully_diluted_market_cap,
         circulating_supply, max_supply, tags, platform, date_added)
        VALUES
        ('coinmarketcap', ?, ?, ?, ?, NULL, ?, ?, NULL, NULL, NULL, '', NULL, NULL)
        """,
        [row.provider_id, row.date, row.symbol, row.name, row.rank, row.market_cap],
    )


# =============================================================================
# Tests
# =============================================================================


def test_get_existing_universe_dates_missing_database_returns_empty():
    """A first-run database path should not block universe ingestion startup."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "missing.db"

        assert _get_existing_universe_dates(str(db_path)) == set()


def test_get_existing_universe_dates_missing_table_returns_empty():
    """An existing DB without crypto_universe is an expected startup state."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        conn = duckdb.connect(str(db_path))
        conn.execute("CREATE TABLE unrelated_table (date DATE)")
        conn.close()

        assert _get_existing_universe_dates(str(db_path)) == set()


def test_get_existing_universe_dates_unexpected_connect_failure_propagates():
    """Unexpected DuckDB/runtime failures should not be treated as no existing dates."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        conn = duckdb.connect(str(db_path))
        conn.close()

        with patch(
            "crypto_data.database_builder.duckdb.connect",
            side_effect=RuntimeError("connect failed"),
        ):
            with pytest.raises(RuntimeError, match="connect failed"):
                _get_existing_universe_dates(str(db_path))


def test_get_existing_universe_dates_unexpected_query_failure_propagates():
    """Unexpected query failures should not be treated as no existing dates."""

    class FailingConnection:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def execute(self, query):
            raise RuntimeError("query failed")

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        conn = duckdb.connect(str(db_path))
        conn.close()

        with patch("crypto_data.database_builder.duckdb.connect", return_value=FailingConnection()):
            with pytest.raises(RuntimeError, match="query failed"):
                _get_existing_universe_dates(str(db_path))


def test_universe_rollback_on_error():
    """Test that errors during API fetch raise RuntimeError when ALL fetches fail."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"

        # Create database with initial data
        db = CryptoDatabase(str(db_path))
        conn = db.conn

        _insert_universe_row(
            conn,
            UniverseRowFixture(
                provider_id=1,
                date="2024-01-01",
                symbol="BTC",
                name="Bitcoin",
                rank=1,
                market_cap=1_000_000.0,
            ),
        )
        db.close()

        # Mock API to simulate error
        with patch("crypto_data.database_builder.CoinMarketCapClient") as MockClient:
            mock_instance = AsyncMock()
            mock_instance.__aenter__.return_value = mock_instance
            mock_instance.__aexit__.return_value = None
            mock_instance.get_historical_listings = AsyncMock(side_effect=Exception("API error"))
            MockClient.return_value = mock_instance

            # When ALL fetches fail, should raise RuntimeError
            with pytest.raises(RuntimeError, match="All .* universe fetches failed"):
                asyncio.run(
                    update_coinmarketcap_universe(
                        db_path=str(db_path),
                        dates=["2024-01-01"],
                        top_n=1,
                        skip_existing=False,
                    )
                )

        # Verify original data still there (no new data due to error)
        db = CryptoDatabase(str(db_path))
        result = db.execute(
            "SELECT COUNT(*) FROM crypto_universe WHERE date = '2024-01-01'"
        ).fetchone()
        db.close()

        assert result[0] == 1, "Original data should still be present"


def test_universe_no_rollback_after_commit():
    """Test that committed flag prevents ROLLBACK after successful COMMIT."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"

        # Create database with initial data
        db = CryptoDatabase(str(db_path))
        conn = db.conn

        _insert_universe_row(
            conn,
            UniverseRowFixture(
                provider_id=99001,
                date="2024-01-01",
                symbol="OLD",
                name="Legacy Asset",
                rank=1,
                market_cap=1_000.0,
            ),
        )
        db.close()

        # Mock API to return new data
        new_data = [
            {
                "id": 1,
                "symbol": "BTC",
                "name": "Bitcoin",
                "cmcRank": 1,
                "quotes": [{"marketCap": 2000000}],
                "tags": [],
            }
        ]

        with patch("crypto_data.database_builder.CoinMarketCapClient") as MockClient:
            mock_instance = AsyncMock()
            mock_instance.__aenter__.return_value = mock_instance
            mock_instance.__aexit__.return_value = None
            mock_instance.get_historical_listings = AsyncMock(return_value=new_data)
            MockClient.return_value = mock_instance

            # Run ingestion - this should successfully commit
            asyncio.run(
                update_coinmarketcap_universe(
                    db_path=str(db_path),
                    dates=["2024-01-01"],
                    top_n=1,
                    skip_existing=False,
                )
            )

        # Verify data was committed and replaced (not OLD, but BTC)
        db = CryptoDatabase(str(db_path))
        result = db.execute("""
            SELECT symbol FROM crypto_universe WHERE date = '2024-01-01'
        """).fetchone()
        db.close()

        assert result[0] == "BTC", "Data should be committed and replaced"


def test_update_coinmarketcap_universe_write_failure_rolls_back_and_raises():
    """Unexpected write failures should rollback the current date and abort the batch."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"

        conn = duckdb.connect(str(db_path))
        conn.execute("""
            CREATE TABLE crypto_universe (
                provider                 VARCHAR  NOT NULL CHECK (provider = 'coinmarketcap'),
                provider_id              BIGINT   NOT NULL,
                date                     DATE     NOT NULL,
                symbol                   VARCHAR  NOT NULL CHECK (symbol != 'ETH'),
                name                     VARCHAR  NOT NULL,
                slug                     VARCHAR,
                rank                     INTEGER  NOT NULL,
                market_cap               DOUBLE,
                fully_diluted_market_cap DOUBLE,
                circulating_supply       DOUBLE,
                max_supply               DOUBLE,
                tags                     VARCHAR,
                platform                 VARCHAR,
                date_added               DATE,
                PRIMARY KEY (provider, provider_id, date)
            )
        """)
        _insert_universe_row(
            conn,
            UniverseRowFixture(
                provider_id=99002,
                date="2024-01-02",
                symbol="OLD",
                name="Legacy Asset",
                rank=1,
                market_cap=1_000.0,
            ),
        )
        conn.close()

        async def fake_historical_listings(date_str, top_n):
            if date_str == "2024-01-01":
                return [
                    {
                        "id": 1,
                        "symbol": "BTC",
                        "name": "Bitcoin",
                        "cmcRank": 1,
                        "quotes": [{"marketCap": 2000000}],
                        "tags": [],
                    }
                ]
            return [
                {
                    "id": 1027,
                    "symbol": "ETH",
                    "name": "Ethereum",
                    "cmcRank": 1,
                    "quotes": [{"marketCap": 1000000}],
                    "tags": [],
                }
            ]

        with patch("crypto_data.database_builder.CoinMarketCapClient") as MockClient:
            mock_instance = AsyncMock()
            mock_instance.__aenter__.return_value = mock_instance
            mock_instance.__aexit__.return_value = None
            mock_instance.get_historical_listings = AsyncMock(side_effect=fake_historical_listings)
            MockClient.return_value = mock_instance

            with pytest.raises(duckdb.ConstraintException):
                asyncio.run(
                    update_coinmarketcap_universe(
                        db_path=str(db_path),
                        dates=["2024-01-01", "2024-01-02"],
                        top_n=1,
                        skip_existing=False,
                    )
                )

        conn = duckdb.connect(str(db_path))
        rows = conn.execute("""
            SELECT date, symbol
            FROM crypto_universe
            ORDER BY date, symbol
        """).fetchall()
        conn.close()

        assert rows == [
            (datetime.date(2024, 1, 1), "BTC"),
            (datetime.date(2024, 1, 2), "OLD"),
        ]


def test_universe_idempotent():
    """Test that running ingestion twice with same data is idempotent."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"

        # Mock API
        new_data = [
            {
                "id": 1,
                "symbol": "BTC",
                "name": "Bitcoin",
                "cmcRank": 1,
                "quotes": [{"marketCap": 1000000}],
                "tags": [],
            },
            {
                "id": 1027,
                "symbol": "ETH",
                "name": "Ethereum",
                "cmcRank": 2,
                "quotes": [{"marketCap": 500000}],
                "tags": [],
            },
        ]

        with patch("crypto_data.database_builder.CoinMarketCapClient") as MockClient:
            mock_instance = AsyncMock()
            mock_instance.__aenter__.return_value = mock_instance
            mock_instance.__aexit__.return_value = None
            mock_instance.get_historical_listings = AsyncMock(return_value=new_data)
            MockClient.return_value = mock_instance

            # Run twice
            asyncio.run(
                update_coinmarketcap_universe(db_path=str(db_path), dates=["2024-01-01"], top_n=2)
            )
            asyncio.run(
                update_coinmarketcap_universe(db_path=str(db_path), dates=["2024-01-01"], top_n=2)
            )

        # Verify only 2 records (not 4)
        db = CryptoDatabase(str(db_path))
        result = db.execute(
            "SELECT COUNT(*) FROM crypto_universe WHERE date = '2024-01-01'"
        ).fetchone()
        db.close()

        assert result[0] == 2, "Should replace, not append"


def test_update_coinmarketcap_universe_excludes_synthetic_assets_by_default():
    """Default universe ingestion should exclude stable, wrapped, and tokenized assets."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"

        new_data = [
            {
                "id": 1,
                "symbol": "BTC",
                "name": "Bitcoin",
                "cmcRank": 1,
                "quotes": [{"marketCap": 1000000}],
                "tags": ["mineable"],
            },
            {
                "id": 825,
                "symbol": "USDT",
                "name": "Tether USDt",
                "cmcRank": 2,
                "quotes": [{"marketCap": 900000}],
                "tags": ["stablecoin"],
            },
            {
                "id": 3717,
                "symbol": "WBTC",
                "name": "Wrapped Bitcoin",
                "cmcRank": 3,
                "quotes": [{"marketCap": 800000}],
                "tags": ["wrapped-tokens"],
            },
            {
                "id": 4705,
                "symbol": "PAXG",
                "name": "PAX Gold",
                "cmcRank": 4,
                "quotes": [{"marketCap": 700000}],
                "tags": ["tokenized-gold"],
            },
            {
                "id": 12345,
                "symbol": "TSLA",
                "name": "Tesla Tokenized",
                "cmcRank": 5,
                "quotes": [{"marketCap": 600000}],
                "tags": ["tokenized-stock"],
            },
        ]

        with patch("crypto_data.database_builder.CoinMarketCapClient") as MockClient:
            mock_instance = AsyncMock()
            mock_instance.__aenter__.return_value = mock_instance
            mock_instance.__aexit__.return_value = None
            mock_instance.get_historical_listings = AsyncMock(return_value=new_data)
            MockClient.return_value = mock_instance

            exclusions = asyncio.run(
                update_coinmarketcap_universe(
                    db_path=str(db_path),
                    dates=["2024-01-01"],
                    top_n=5,
                )
            )

        db = CryptoDatabase(str(db_path))
        rows = db.execute("""
            SELECT symbol FROM crypto_universe WHERE date = '2024-01-01' ORDER BY symbol
        """).fetchall()
        db.close()

        assert [row[0] for row in rows] == ["BTC"]
        assert exclusions["by_tag"] == {"USDT", "WBTC", "PAXG", "TSLA"}


def test_update_coinmarketcap_universe_allows_explicit_filter_opt_out():
    """Passing empty filter lists should disable the package defaults."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"

        new_data = [
            {
                "id": 825,
                "symbol": "USDT",
                "name": "Tether USDt",
                "cmcRank": 1,
                "quotes": [{"marketCap": 1000000}],
                "tags": ["stablecoin"],
            },
        ]

        with patch("crypto_data.database_builder.CoinMarketCapClient") as MockClient:
            mock_instance = AsyncMock()
            mock_instance.__aenter__.return_value = mock_instance
            mock_instance.__aexit__.return_value = None
            mock_instance.get_historical_listings = AsyncMock(return_value=new_data)
            MockClient.return_value = mock_instance

            asyncio.run(
                update_coinmarketcap_universe(
                    db_path=str(db_path),
                    dates=["2024-01-01"],
                    top_n=1,
                    exclude_tags=[],
                    exclude_symbols=[],
                )
            )

        db = CryptoDatabase(str(db_path))
        result = db.execute("""
            SELECT symbol FROM crypto_universe WHERE date = '2024-01-01'
        """).fetchone()
        db.close()

        assert result[0] == "USDT"


def test_update_coinmarketcap_universe_rejects_invalid_snapshot_data():
    """Malformed CMC rows should not replace an existing universe snapshot."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"

        db = CryptoDatabase(str(db_path))
        _insert_universe_row(
            db.conn,
            UniverseRowFixture(
                provider_id=1,
                date="2024-01-01",
                symbol="BTC",
                name="Bitcoin",
                rank=1,
                market_cap=1_000_000.0,
            ),
        )
        db.close()

        invalid_data = [
            # Missing cmcRank and marketCap should fail schema validation
            # instead of being coerced to rank=0 / market_cap=0.
            {"id": 1027, "symbol": "ETH", "name": "Ethereum", "quotes": [{}], "tags": []}
        ]

        with patch("crypto_data.database_builder.CoinMarketCapClient") as MockClient:
            mock_instance = AsyncMock()
            mock_instance.__aenter__.return_value = mock_instance
            mock_instance.__aexit__.return_value = None
            mock_instance.get_historical_listings = AsyncMock(return_value=invalid_data)
            MockClient.return_value = mock_instance

            with pytest.raises(RuntimeError, match="All .* universe fetches failed"):
                asyncio.run(
                    update_coinmarketcap_universe(
                        db_path=str(db_path),
                        dates=["2024-01-01"],
                        top_n=1,
                        skip_existing=False,
                    )
                )

        db = CryptoDatabase(str(db_path))
        result = db.execute("""
            SELECT symbol, rank, market_cap
            FROM crypto_universe
            WHERE date = '2024-01-01'
        """).fetchone()
        db.close()

        assert result == ("BTC", 1, 1000000.0)


def test_update_coinmarketcap_universe_warns_on_duplicate_symbols(caplog):
    """Duplicate CMC symbols are deduplicated deterministically and logged."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        caplog.set_level(logging.WARNING)

        # Both rows share id=5426 so the v6 dedup key (provider, provider_id, date) triggers.
        duplicate_data = [
            {
                "id": 5426,
                "symbol": "SOL",
                "name": "Solana",
                "cmcRank": 10,
                "quotes": [{"marketCap": 1000}],
                "tags": [],
            },
            {
                "id": 5426,
                "symbol": "SOL",
                "name": "Solana",
                "cmcRank": 20,
                "quotes": [{"marketCap": 500}],
                "tags": [],
            },
        ]

        with patch("crypto_data.database_builder.CoinMarketCapClient") as MockClient:
            mock_instance = AsyncMock()
            mock_instance.__aenter__.return_value = mock_instance
            mock_instance.__aexit__.return_value = None
            mock_instance.get_historical_listings = AsyncMock(return_value=duplicate_data)
            MockClient.return_value = mock_instance

            asyncio.run(
                update_coinmarketcap_universe(
                    db_path=str(db_path),
                    dates=["2024-01-01"],
                    top_n=2,
                    skip_existing=False,
                )
            )

        db = CryptoDatabase(str(db_path))
        rows = db.execute("""
            SELECT symbol, rank, market_cap
            FROM crypto_universe
            WHERE date = '2024-01-01'
        """).fetchall()
        db.close()

        assert rows == [("SOL", 10, 1000.0)]
        assert "Dropped 1 duplicate universe rows" in caplog.text


def test_universe_transaction_atomicity():
    """
    Test transaction atomicity: DELETE + INSERT are atomic.

    Validates that if we manually trigger a ROLLBACK after DELETE,
    the original data is preserved (simulating what happens if INSERT fails).
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"

        # Create database with initial data
        db = CryptoDatabase(str(db_path))
        conn = db.conn

        _insert_universe_row(
            conn,
            UniverseRowFixture(
                provider_id=1,
                date="2024-01-01",
                symbol="BTC",
                name="Bitcoin",
                rank=1,
                market_cap=1_000_000.0,
            ),
        )
        _insert_universe_row(
            conn,
            UniverseRowFixture(
                provider_id=1027,
                date="2024-01-01",
                symbol="ETH",
                name="Ethereum",
                rank=2,
                market_cap=500_000.0,
            ),
        )

        # Test Case 1: Transaction with ROLLBACK - data should be preserved
        conn.execute("BEGIN TRANSACTION")
        conn.execute("DELETE FROM crypto_universe WHERE date = '2024-01-01'")

        # Verify data is "gone" within the transaction
        result_during = conn.execute(
            "SELECT COUNT(*) FROM crypto_universe WHERE date = '2024-01-01'"
        ).fetchone()
        assert result_during[0] == 0, "Data should appear deleted within transaction"

        # Now ROLLBACK (simulating INSERT failure)
        conn.execute("ROLLBACK")

        # Verify data is RESTORED after rollback
        result_after = conn.execute(
            "SELECT COUNT(*) FROM crypto_universe WHERE date = '2024-01-01'"
        ).fetchone()
        assert result_after[0] == 2, "Data should be restored after ROLLBACK"

        # Test Case 2: Transaction with COMMIT - data should be deleted
        conn.execute("BEGIN TRANSACTION")
        conn.execute("DELETE FROM crypto_universe WHERE date = '2024-01-01'")

        # New data to insert
        _insert_universe_row(
            conn,
            UniverseRowFixture(
                provider_id=5426,
                date="2024-01-01",
                symbol="SOL",
                name="Solana",
                rank=1,
                market_cap=2_000_000.0,
            ),
        )
        conn.execute("COMMIT")

        # Verify data is replaced after commit
        result_final = conn.execute("""
            SELECT symbol FROM crypto_universe WHERE date = '2024-01-01'
        """).fetchone()
        assert result_final[0] == "SOL", "Data should be replaced after COMMIT"

        db.close()


def test_update_coinmarketcap_universe_accepts_dates_kwarg():
    """update_coinmarketcap_universe should accept canonical YYYY-MM-DD date list."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"

        new_data = [
            {
                "id": 1,
                "symbol": "BTC",
                "name": "Bitcoin",
                "cmcRank": 1,
                "quotes": [{"marketCap": 1000000}],
                "tags": [],
            }
        ]

        with patch("crypto_data.database_builder.CoinMarketCapClient") as MockClient:
            mock_instance = AsyncMock()
            mock_instance.__aenter__.return_value = mock_instance
            mock_instance.__aexit__.return_value = None
            mock_instance.get_historical_listings = AsyncMock(return_value=new_data)
            MockClient.return_value = mock_instance

            asyncio.run(
                update_coinmarketcap_universe(
                    db_path=str(db_path),
                    dates=["2024-01-01"],
                    top_n=1,
                )
            )

            mock_instance.get_historical_listings.assert_awaited_once_with("2024-01-01", 1)


def test_update_coinmarketcap_universe_requires_dates():
    """The CMC universe API requires explicit YYYY-MM-DD snapshot dates."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"

        with pytest.raises(ValueError, match="`dates` must be provided"):
            asyncio.run(
                update_coinmarketcap_universe(
                    db_path=str(db_path),
                    top_n=1,
                )
            )


def test_update_coinmarketcap_universe_skip_existing_filters_present_dates():
    """skip_existing=True should avoid API calls for existing snapshot dates."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"

        db = CryptoDatabase(str(db_path))
        _insert_universe_row(
            db.conn,
            UniverseRowFixture(
                provider_id=1,
                date="2024-01-01",
                symbol="BTC",
                name="Bitcoin",
                rank=1,
                market_cap=1_000_000.0,
            ),
        )
        db.close()

        new_data = [
            {
                "id": 1027,
                "symbol": "ETH",
                "name": "Ethereum",
                "cmcRank": 1,
                "quotes": [{"marketCap": 500000}],
                "tags": [],
            }
        ]

        with patch("crypto_data.database_builder.CoinMarketCapClient") as MockClient:
            mock_instance = AsyncMock()
            mock_instance.__aenter__.return_value = mock_instance
            mock_instance.__aexit__.return_value = None
            mock_instance.get_historical_listings = AsyncMock(return_value=new_data)
            MockClient.return_value = mock_instance

            asyncio.run(
                update_coinmarketcap_universe(
                    db_path=str(db_path),
                    dates=["2024-01-01", "2024-01-02"],
                    top_n=1,
                    skip_existing=True,
                )
            )

            mock_instance.get_historical_listings.assert_awaited_once_with("2024-01-02", 1)


def test_update_coinmarketcap_universe_skip_existing_is_date_only():
    """skip_existing=True should not refresh existing dates when top_n changes."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"

        db = CryptoDatabase(str(db_path))
        _insert_universe_row(
            db.conn,
            UniverseRowFixture(
                provider_id=1,
                date="2024-01-01",
                symbol="BTC",
                name="Bitcoin",
                rank=1,
                market_cap=1_000_000.0,
            ),
        )
        db.close()

        new_data = [
            {
                "id": 1,
                "symbol": "BTC",
                "name": "Bitcoin",
                "cmcRank": 1,
                "quotes": [{"marketCap": 1000000}],
                "tags": [],
            },
            {
                "id": 1027,
                "symbol": "ETH",
                "name": "Ethereum",
                "cmcRank": 2,
                "quotes": [{"marketCap": 500000}],
                "tags": [],
            },
        ]

        with patch("crypto_data.database_builder.CoinMarketCapClient") as MockClient:
            mock_instance = AsyncMock()
            mock_instance.__aenter__.return_value = mock_instance
            mock_instance.__aexit__.return_value = None
            mock_instance.get_historical_listings = AsyncMock(return_value=new_data)
            MockClient.return_value = mock_instance

            asyncio.run(
                update_coinmarketcap_universe(
                    db_path=str(db_path),
                    dates=["2024-01-01"],
                    top_n=2,
                    skip_existing=True,
                )
            )

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
        db_path = Path(tmpdir) / "test.db"

        db = CryptoDatabase(str(db_path))
        _insert_universe_row(
            db.conn,
            UniverseRowFixture(
                provider_id=1,
                date="2024-01-01",
                symbol="BTC",
                name="Bitcoin",
                rank=1,
                market_cap=1_000_000.0,
            ),
        )
        db.close()

        new_data = [
            {
                "id": 1027,
                "symbol": "ETH",
                "name": "Ethereum",
                "cmcRank": 1,
                "quotes": [{"marketCap": 500000}],
                "tags": [],
            }
        ]

        with patch("crypto_data.database_builder.CoinMarketCapClient") as MockClient:
            mock_instance = AsyncMock()
            mock_instance.__aenter__.return_value = mock_instance
            mock_instance.__aexit__.return_value = None
            mock_instance.get_historical_listings = AsyncMock(return_value=new_data)
            MockClient.return_value = mock_instance

            asyncio.run(
                update_coinmarketcap_universe(
                    db_path=str(db_path),
                    dates=["2024-01-01", "2024-01-02"],
                    top_n=1,
                    skip_existing=False,
                )
            )

            assert mock_instance.get_historical_listings.await_count == 2


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
