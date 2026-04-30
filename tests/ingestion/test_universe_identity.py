"""Identity test: stable provider_id survives in-listing symbol renames.

Note: cross-listing rebrands (e.g., MATIC id=3890 → POL id=28321) are NOT
unified by provider_id alone — those produce two distinct ids on CMC and
remain the consumer's UNION concern. This file tests only the in-listing
case, where CMC keeps `id` constant while displayed `symbol` changes.
"""

import tempfile
from pathlib import Path

import duckdb
import pytest

from crypto_data import CryptoDatabase
from tests.ingestion.test_universe_ingestion import (
    UniverseRowFixture,
    _insert_universe_row,
)


_SYNTHETIC_ID: int = 99_999
_DEFAULT_RANK: int = 1
_DEFAULT_MARKET_CAP: float = 1_000_000.0


def test_same_provider_id_with_different_symbols_across_dates() -> None:
    """Two snapshots for the same listing with different tickers form one series."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / 'test.db'
        db = CryptoDatabase(str(db_path))

        # Synthetic asset: provider_id=_SYNTHETIC_ID. Symbol observed as 'OLD'
        # on day1, then renamed by the provider (in-listing) to 'NEW' on day2.
        _insert_universe_row(db.conn, UniverseRowFixture(
            provider_id=_SYNTHETIC_ID, date='2024-01-01', symbol='OLD',
            name='Synthetic Asset', rank=_DEFAULT_RANK, market_cap=_DEFAULT_MARKET_CAP,
        ))
        _insert_universe_row(db.conn, UniverseRowFixture(
            provider_id=_SYNTHETIC_ID, date='2024-01-02', symbol='NEW',
            name='Synthetic Asset', rank=_DEFAULT_RANK, market_cap=_DEFAULT_MARKET_CAP,
        ))

        distinct_ids = db.conn.execute("""
            SELECT COUNT(DISTINCT provider_id) FROM crypto_universe
            WHERE provider_id = ?
        """, [_SYNTHETIC_ID]).fetchone()[0]
        assert distinct_ids == 1

        symbols_by_date = db.conn.execute("""
            SELECT date, symbol FROM crypto_universe
            WHERE provider_id = ? ORDER BY date
        """, [_SYNTHETIC_ID]).fetchall()
        assert [row[1] for row in symbols_by_date] == ['OLD', 'NEW']

        db.close()


def test_pk_rejects_duplicate_provider_id_date() -> None:
    """The PK (provider, provider_id, date) blocks a same-day duplicate."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / 'test.db'
        db = CryptoDatabase(str(db_path))

        _insert_universe_row(db.conn, UniverseRowFixture(
            provider_id=_SYNTHETIC_ID, date='2024-01-01', symbol='X',
            name='X', rank=_DEFAULT_RANK, market_cap=_DEFAULT_MARKET_CAP,
        ))
        with pytest.raises(duckdb.ConstraintException):
            _insert_universe_row(db.conn, UniverseRowFixture(
                provider_id=_SYNTHETIC_ID, date='2024-01-01', symbol='Y',
                name='X', rank=_DEFAULT_RANK, market_cap=_DEFAULT_MARKET_CAP,
            ))

        db.close()


def test_distinct_provider_ids_remain_separate_series() -> None:
    """Cross-listing rebrand: same symbol later, different provider_id → two series.
    This documents what provider_id does NOT unify (consumer must UNION manually)."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / 'test.db'
        db = CryptoDatabase(str(db_path))

        _insert_universe_row(db.conn, UniverseRowFixture(
            provider_id=3890, date='2024-01-01', symbol='MATIC',
            name='Polygon', rank=_DEFAULT_RANK, market_cap=_DEFAULT_MARKET_CAP,
        ))
        _insert_universe_row(db.conn, UniverseRowFixture(
            provider_id=28321, date='2024-09-01', symbol='POL',
            name='POL (prev. MATIC)', rank=_DEFAULT_RANK, market_cap=_DEFAULT_MARKET_CAP,
        ))

        distinct_ids = db.execute("""
            SELECT COUNT(DISTINCT provider_id) FROM crypto_universe
        """).fetchone()[0]
        assert distinct_ids == 2

        db.close()
