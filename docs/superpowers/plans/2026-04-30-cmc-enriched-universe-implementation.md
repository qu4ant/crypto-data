# CMC Enriched Universe Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the 5-column `crypto_universe` schema with a 14-column v6.0.0 schema that captures CMC `id`, supply, FDMC, slug, name, platform, and `date_added`, identified by `(provider, provider_id, date)`.

**Architecture:** Single fat snapshot table (no normalization). Identity composite is `(provider='coinmarketcap', provider_id=cmc_id, date)`. Pandera schema gates ingestion. No automated migration — v5 databases must be deleted by the user. Spec: `docs/superpowers/specs/2026-04-30-cmc-enriched-universe-design.md`.

**Tech Stack:** Python, DuckDB, pandas, Pandera, pytest, aiohttp.

---

## File Structure

**Files modified:**
- `src/crypto_data/database.py` — `_create_crypto_universe` DDL (lines 163-182)
- `src/crypto_data/schemas/universe.py` — `UNIVERSE_SCHEMA` Pandera definition
- `src/crypto_data/database_builder.py` — `UNIVERSE_COLUMNS` constant (line 38) and record dict in `_fetch_snapshot` (lines 117-152)
- `src/crypto_data/utils/symbols.py` — `categories` → `tags` rename (line 89, 101-103)
- `src/crypto_data/__init__.py` — `__version__` bump to `6.0.0` (line 65)
- `tests/database/test_database_basic.py` — column assertions and INSERT fixtures
- `tests/ingestion/test_universe_ingestion.py` — INSERT fixtures and mock CMC payloads
- `CLAUDE.md` — schema documentation
- `README.md`, `README.fr.md` — v6 breaking note

**Files created:**
- `tests/ingestion/test_universe_parsing.py` — unit tests for `_fetch_snapshot` record extraction
- `tests/ingestion/test_universe_schema.py` — Pandera contract tests
- `tests/ingestion/test_universe_identity.py` — in-listing-rename use case test

---

## Task 1: Add Pandera schema tests (TDD red)

**Files:**
- Create: `tests/ingestion/test_universe_schema.py`

- [ ] **Step 1: Write the failing tests**

```python
"""Pandera contract tests for the v6.0.0 UNIVERSE_SCHEMA."""

import pandas as pd
import pandera.pandas as pa
import pytest

from crypto_data.schemas.universe import UNIVERSE_SCHEMA


def _valid_row(**overrides: object) -> dict:
    base = {
        'provider': 'coinmarketcap',
        'provider_id': 1,
        'date': pd.Timestamp('2024-01-01'),
        'symbol': 'BTC',
        'name': 'Bitcoin',
        'slug': 'bitcoin',
        'rank': 1,
        'market_cap': 1_000_000.0,
        'fully_diluted_market_cap': 1_100_000.0,
        'circulating_supply': 19_000_000.0,
        'max_supply': 21_000_000.0,
        'tags': 'mineable,pow',
        'platform': None,
        'date_added': pd.Timestamp('2010-07-13'),
    }
    base.update(overrides)
    return base


def test_valid_dataframe_passes():
    df = pd.DataFrame([_valid_row()])
    UNIVERSE_SCHEMA.validate(df)


def test_provider_must_be_coinmarketcap():
    df = pd.DataFrame([_valid_row(provider='coingecko')])
    with pytest.raises(pa.errors.SchemaError):
        UNIVERSE_SCHEMA.validate(df)


def test_circulating_supply_cannot_exceed_max_supply():
    df = pd.DataFrame([_valid_row(circulating_supply=22_000_000.0, max_supply=21_000_000.0)])
    with pytest.raises(pa.errors.SchemaError):
        UNIVERSE_SCHEMA.validate(df)


def test_circulating_le_max_skipped_when_either_null():
    df = pd.DataFrame([_valid_row(max_supply=None)])
    UNIVERSE_SCHEMA.validate(df)


def test_duplicate_identity_rejected():
    df = pd.DataFrame([_valid_row(), _valid_row()])
    with pytest.raises(pa.errors.SchemaError):
        UNIVERSE_SCHEMA.validate(df)


def test_distinct_dates_same_id_allowed():
    df = pd.DataFrame([
        _valid_row(date=pd.Timestamp('2024-01-01')),
        _valid_row(date=pd.Timestamp('2024-01-02')),
    ])
    UNIVERSE_SCHEMA.validate(df)


def test_distinct_provider_ids_same_date_allowed():
    df = pd.DataFrame([
        _valid_row(provider_id=1, symbol='BTC'),
        _valid_row(provider_id=1027, symbol='ETH', name='Ethereum', slug='ethereum'),
    ])
    UNIVERSE_SCHEMA.validate(df)


def test_provider_id_must_be_positive():
    df = pd.DataFrame([_valid_row(provider_id=0)])
    with pytest.raises(pa.errors.SchemaError):
        UNIVERSE_SCHEMA.validate(df)


def test_rank_must_be_positive():
    df = pd.DataFrame([_valid_row(rank=0)])
    with pytest.raises(pa.errors.SchemaError):
        UNIVERSE_SCHEMA.validate(df)


def test_name_required():
    row = _valid_row()
    row['name'] = None
    df = pd.DataFrame([row])
    with pytest.raises(pa.errors.SchemaError):
        UNIVERSE_SCHEMA.validate(df)


def test_supplies_cannot_be_negative():
    df = pd.DataFrame([_valid_row(circulating_supply=-1.0)])
    with pytest.raises(pa.errors.SchemaError):
        UNIVERSE_SCHEMA.validate(df)


def test_market_cap_can_be_null():
    df = pd.DataFrame([_valid_row(market_cap=None)])
    UNIVERSE_SCHEMA.validate(df)


def test_fdmc_below_market_cap_does_not_raise():
    """§3.4: we deliberately do not enforce FDMC >= MC (CMC publishes from
    out-of-sync supply snapshots; rejecting a date for that would be fragile)."""
    df = pd.DataFrame([_valid_row(market_cap=1_000_000.0, fully_diluted_market_cap=900_000.0)])
    UNIVERSE_SCHEMA.validate(df)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/ingestion/test_universe_schema.py -v`

Expected: All tests fail with `SchemaError` complaining about column `categories` not found, or columns like `provider`, `provider_id`, `name` not present in the schema.

- [ ] **Step 3: Commit failing tests**

```bash
git add tests/ingestion/test_universe_schema.py
git commit -m "test: add v6 UNIVERSE_SCHEMA Pandera contract tests (TDD red)"
```

---

## Task 2: Rewrite `UNIVERSE_SCHEMA` (TDD green)

**Files:**
- Modify: `src/crypto_data/schemas/universe.py` (full rewrite of the schema definition)

- [ ] **Step 1: Replace the schema with the v6 definition**

Open `src/crypto_data/schemas/universe.py` and replace its entire content with:

```python
"""
Crypto Universe Schema (v6.0.0)

Pandera schema for validating CoinMarketCap-enriched universe ranking data.

Identity is `(provider, provider_id, date)`. `symbol` is mutable / not identity.
"""

import pandas as pd
import pandera.pandas as pa
from pandera.pandas import Column, Check, DataFrameSchema


def _check_circulating_le_max(df: pd.DataFrame) -> bool:
    """circulating_supply <= max_supply when both columns are non-null."""
    mask = df['circulating_supply'].notna() & df['max_supply'].notna()
    if not mask.any():
        return True
    return bool((df.loc[mask, 'circulating_supply'] <= df.loc[mask, 'max_supply']).all())


UNIVERSE_SCHEMA = DataFrameSchema(
    columns={
        # Identity
        'provider': Column(
            str,
            checks=[Check.equal_to('coinmarketcap', error="provider must be 'coinmarketcap'")],
            nullable=False,
            description="Data provider identifier",
        ),
        'provider_id': Column(
            'Int64',
            checks=[Check.greater_than_or_equal_to(1, error="provider_id must be >= 1")],
            nullable=False,
            description="Provider-internal asset id (e.g., CMC id)",
        ),
        'date': Column(
            'datetime64[ns]',
            nullable=False,
            description="Snapshot date",
        ),

        # Display / linking
        'symbol': Column(
            str,
            checks=[
                Check.str_matches(r'^[A-Z0-9]+$'),
                Check.str_length(min_value=1, max_value=20),
            ],
            nullable=False,
            description="Base asset symbol (mutable, not identity)",
        ),
        'name': Column(
            str,
            checks=[Check.str_length(min_value=1, max_value=100)],
            nullable=False,
            description="Human-readable asset name",
        ),
        'slug': Column(str, nullable=True, description="URL-style identifier"),

        # Universe selection
        'rank': Column(
            'Int64',
            checks=[Check.greater_than_or_equal_to(1, error="rank must be >= 1")],
            nullable=False,
            description="Provider rank (1 = highest market cap)",
        ),
        'market_cap': Column(
            float,
            checks=[Check.greater_than_or_equal_to(0)],
            nullable=True,
            description="Market capitalization in USD",
        ),
        'fully_diluted_market_cap': Column(
            float,
            checks=[Check.greater_than_or_equal_to(0)],
            nullable=True,
            description="Fully diluted market cap in USD",
        ),

        # Anti-shitcoin (supply)
        'circulating_supply': Column(
            float,
            checks=[Check.greater_than_or_equal_to(0)],
            nullable=True,
        ),
        'max_supply': Column(
            float,
            checks=[Check.greater_than_or_equal_to(0)],
            nullable=True,
        ),

        # Metadata
        'tags': Column(str, nullable=True, description="Comma-separated CMC tags"),
        'platform': Column(str, nullable=True, description="Chain platform name; NULL for L1s"),
        'date_added': Column('datetime64[ns]', nullable=True, description="Listing date"),
    },
    checks=[
        Check(
            _check_circulating_le_max,
            name='circulating_le_max',
            error="circulating_supply must not exceed max_supply",
        ),
    ],
    strict=True,
    coerce=True,
    ordered=False,
    unique=['provider', 'provider_id', 'date'],
    description="CMC enriched universe schema (v6.0.0)",
)


def validate_universe_dataframe(
    df: pd.DataFrame,
    *,
    strict: bool = True,
) -> pd.DataFrame | pa.errors.SchemaErrors:
    """Validate a universe DataFrame against UNIVERSE_SCHEMA.

    Args:
        df: DataFrame to validate.
        strict: If True, raise on first error. If False, accumulate errors lazily
            and return them as a SchemaErrors object instead of a DataFrame.

    Returns:
        The validated DataFrame on success, or a SchemaErrors object when
        strict=False and validation failed.
    """
    if strict:
        return UNIVERSE_SCHEMA.validate(df, lazy=False)
    try:
        return UNIVERSE_SCHEMA.validate(df, lazy=True)
    except pa.errors.SchemaErrors as e:
        return e


__all__ = ['UNIVERSE_SCHEMA', 'validate_universe_dataframe']
```

- [ ] **Step 2: Run schema tests to verify they pass**

Run: `uv run pytest tests/ingestion/test_universe_schema.py -v`

Expected: All 13 tests pass.

- [ ] **Step 3: Run the full schemas test directory to confirm nothing else broke**

Run: `uv run pytest tests/schemas/ -v`

Expected: All pass (or no tests in that dir — that's fine; this is a sanity check).

- [ ] **Step 4: Commit**

```bash
git add src/crypto_data/schemas/universe.py
git commit -m "feat(schema): rewrite UNIVERSE_SCHEMA for v6 enriched universe"
```

---

## Task 3: Update DDL in `database.py` and adapt `test_database_basic.py`

**Files:**
- Modify: `src/crypto_data/database.py:163-182` (the `_create_crypto_universe` method)
- Modify: `tests/database/test_database_basic.py` (column assertions + INSERT fixture)

- [ ] **Step 1: Update the test expectations first (TDD red)**

Open `tests/database/test_database_basic.py`. Replace the `crypto_universe` schema check block (around lines 48-61) with:

```python
        # Verify crypto_universe schema (v6.0.0)
        universe_schema = db.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'crypto_universe'
        """).fetchall()

        universe_columns = [row[0] for row in universe_schema]

        expected_columns = {
            'provider', 'provider_id', 'date', 'symbol', 'name', 'slug',
            'rank', 'market_cap', 'fully_diluted_market_cap',
            'circulating_supply', 'max_supply',
            'tags', 'platform', 'date_added',
        }
        assert expected_columns.issubset(set(universe_columns)), (
            f"Missing columns: {expected_columns - set(universe_columns)}"
        )
        # Old column should be gone:
        assert 'categories' not in universe_columns
```

Then update the INSERT fixture in `test_execute_query_with_results` (around lines 96-102) to:

```python
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
```

- [ ] **Step 2: Run the test to confirm it fails**

Run: `uv run pytest tests/database/test_database_basic.py -v`

Expected: `test_database_creation` fails because the actual DDL still emits the old 5-column schema, so `expected_columns` is not a subset.

- [ ] **Step 3: Replace the DDL in `database.py`**

In `src/crypto_data/database.py`, replace the entire `_create_crypto_universe` method (lines 163-182) with:

```python
    def _create_crypto_universe(self):
        """Create crypto_universe table for CoinMarketCap rankings (v6.0.0)."""
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS crypto_universe (
                provider                 VARCHAR  NOT NULL CHECK (provider = 'coinmarketcap'),
                provider_id              BIGINT   NOT NULL,
                date                     DATE     NOT NULL,
                symbol                   VARCHAR  NOT NULL,
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

        # Index for universe selection (top-N by date)
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_universe_date_rank
            ON crypto_universe(date, rank)
        """)

        # Index for Binance joins (u.symbol || 'USDT' = s.symbol)
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_universe_date_symbol
            ON crypto_universe(date, symbol)
        """)

        logger.debug("Created crypto_universe table (v6.0.0)")
```

- [ ] **Step 4: Run `test_database_basic.py` to confirm it now passes**

Run: `uv run pytest tests/database/test_database_basic.py -v`

Expected: All 3 tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/crypto_data/database.py tests/database/test_database_basic.py
git commit -m "feat(db): emit v6 crypto_universe DDL with provider identity"
```

---

## Task 4: Add ingestion parsing tests (TDD red)

**Files:**
- Create: `tests/ingestion/test_universe_parsing.py`

- [ ] **Step 1: Write the failing parsing tests**

```python
"""Unit tests for record extraction from CMC payloads (v6.0.0).

Exercises `_fetch_snapshot` indirectly through `update_coinmarketcap_universe`
with mocked CMC client, focusing on the shape and field mapping of records
written to the database.
"""

import asyncio
import tempfile
from pathlib import Path
from unittest.mock import patch, AsyncMock

import pytest

from crypto_data import CryptoDatabase
from crypto_data.database_builder import update_coinmarketcap_universe


def _full_btc_payload() -> dict:
    return {
        'id': 1,
        'name': 'Bitcoin',
        'symbol': 'BTC',
        'slug': 'bitcoin',
        'cmcRank': 1,
        'circulatingSupply': 19_000_000.0,
        'totalSupply': 19_000_000.0,
        'maxSupply': 21_000_000.0,
        'dateAdded': '2010-07-13T00:00:00.000Z',
        'tags': ['mineable', 'pow'],
        'platform': None,
        'quotes': [{
            'marketCap': 1_000_000_000_000.0,
            'fullyDilutedMarketCap': 1_050_000_000_000.0,
            'price': 50_000.0,
        }],
    }


def _full_token_payload() -> dict:
    return {
        'id': 1027,
        'name': 'Ethereum',
        'symbol': 'ETH',
        'slug': 'ethereum',
        'cmcRank': 2,
        'circulatingSupply': 120_000_000.0,
        'totalSupply': 120_000_000.0,
        'maxSupply': None,
        'dateAdded': '2015-08-07T00:00:00.000Z',
        'tags': ['smart-contracts'],
        'platform': None,
        'quotes': [{
            'marketCap': 300_000_000_000.0,
            'fullyDilutedMarketCap': 300_000_000_000.0,
            'price': 2_500.0,
        }],
    }


def _erc20_payload() -> dict:
    return {
        'id': 7083,
        'name': 'Uniswap',
        'symbol': 'UNI',
        'slug': 'uniswap',
        'cmcRank': 30,
        'circulatingSupply': 600_000_000.0,
        'maxSupply': 1_000_000_000.0,
        'dateAdded': '2020-09-17T00:00:00.000Z',
        'tags': ['defi', 'dex'],
        'platform': {
            'id': 1027, 'name': 'Ethereum', 'symbol': 'ETH',
            'slug': 'ethereum', 'token_address': '0x1f9...',
        },
        'quotes': [{
            'marketCap': 4_000_000_000.0,
            'fullyDilutedMarketCap': 6_000_000_000.0,
            'price': 6.5,
        }],
    }


def _ingest(payload_list: list[dict], dates: list[str], top_n: int) -> list[tuple]:
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / 'test.db'
        with patch('crypto_data.database_builder.CoinMarketCapClient') as MockClient:
            inst = AsyncMock()
            inst.__aenter__.return_value = inst
            inst.__aexit__.return_value = None
            inst.get_historical_listings = AsyncMock(return_value=payload_list)
            MockClient.return_value = inst
            asyncio.run(update_coinmarketcap_universe(
                db_path=str(db_path),
                dates=dates,
                top_n=top_n,
                exclude_tags=[],
                exclude_symbols=[],
                skip_existing=False,
            ))
        db = CryptoDatabase(str(db_path))
        rows = db.execute("""
            SELECT provider, provider_id, date, symbol, name, slug, rank,
                   market_cap, fully_diluted_market_cap,
                   circulating_supply, max_supply,
                   tags, platform, date_added
            FROM crypto_universe
            ORDER BY rank
        """).fetchall()
        db.close()
        return rows


def test_full_payload_extracts_all_fields():
    rows = _ingest([_full_btc_payload()], dates=['2024-01-01'], top_n=1)
    assert len(rows) == 1
    (provider, provider_id, date, symbol, name, slug, rank,
     mc, fdmc, circ, maxs, tags, platform, date_added) = rows[0]
    assert provider == 'coinmarketcap'
    assert provider_id == 1
    assert symbol == 'BTC'
    assert name == 'Bitcoin'
    assert slug == 'bitcoin'
    assert rank == 1
    assert mc == 1_000_000_000_000.0
    assert fdmc == 1_050_000_000_000.0
    assert circ == 19_000_000.0
    assert maxs == 21_000_000.0
    assert tags == 'mineable,pow'
    assert platform is None
    assert str(date_added) == '2010-07-13'


def test_platform_name_extracted_for_erc20():
    rows = _ingest([_erc20_payload()], dates=['2024-01-01'], top_n=1)
    assert len(rows) == 1
    assert rows[0][12] == 'Ethereum'  # platform column index


def test_max_supply_null_preserved():
    rows = _ingest([_full_token_payload()], dates=['2024-01-01'], top_n=1)
    assert len(rows) == 1
    assert rows[0][10] is None  # max_supply column index


def test_empty_quotes_yields_null_market_cap_and_fdmc():
    payload = _full_btc_payload()
    payload['quotes'] = []
    rows = _ingest([payload], dates=['2024-01-01'], top_n=1)
    assert len(rows) == 1
    assert rows[0][7] is None   # market_cap
    assert rows[0][8] is None   # fully_diluted_market_cap


def test_empty_tags_stored_as_empty_string():
    payload = _full_btc_payload()
    payload['tags'] = []
    rows = _ingest([payload], dates=['2024-01-01'], top_n=1)
    assert len(rows) == 1
    assert rows[0][11] == ''  # tags


def test_missing_date_added_yields_null():
    payload = _full_btc_payload()
    payload.pop('dateAdded')
    rows = _ingest([payload], dates=['2024-01-01'], top_n=1)
    assert len(rows) == 1
    assert rows[0][13] is None  # date_added


def test_date_added_iso_string_parsed_correctly():
    payload = _full_btc_payload()
    payload['dateAdded'] = '2017-04-21T00:00:00.000Z'
    rows = _ingest([payload], dates=['2024-01-01'], top_n=1)
    assert str(rows[0][13]) == '2017-04-21'
```

- [ ] **Step 2: Run the new parsing tests to confirm they fail**

Run: `uv run pytest tests/ingestion/test_universe_parsing.py -v`

Expected: tests fail because the current `_fetch_snapshot` produces only 5-column records — DuckDB INSERT either errors on column count mismatch or on Pandera validation rejecting the partial record.

- [ ] **Step 3: Commit failing tests**

```bash
git add tests/ingestion/test_universe_parsing.py
git commit -m "test: add CMC payload parsing tests for v6 enriched fields (TDD red)"
```

---

## Task 5: Update `database_builder.py` parsing (TDD green)

**Files:**
- Modify: `src/crypto_data/database_builder.py:38` (`UNIVERSE_COLUMNS`) and `lines 117-152` (record construction in `_fetch_snapshot`)

- [ ] **Step 1: Update `UNIVERSE_COLUMNS`**

In `src/crypto_data/database_builder.py`, replace line 38:

```python
UNIVERSE_COLUMNS = ['date', 'symbol', 'rank', 'market_cap', 'categories']
```

With:

```python
UNIVERSE_COLUMNS = [
    'provider', 'provider_id', 'date', 'symbol', 'name', 'slug', 'rank',
    'market_cap', 'fully_diluted_market_cap',
    'circulating_supply', 'max_supply',
    'tags', 'platform', 'date_added',
]
```

- [ ] **Step 2: Update the record-building block in `_fetch_snapshot`**

In the same file, replace the per-coin block (currently around lines 117-152, the `for coin in coins:` loop body up to and including `records.append(record)`) with:

```python
    for coin in coins:
        symbol = str(coin.get('symbol') or '').upper()
        rank = coin.get('cmcRank')

        tags = coin.get('tags') or []
        tags_str = ','.join(tags)

        if has_excluded_tag(tags, excluded_tags_lower):
            logger.debug(f"  → Filtered {symbol} (excluded tag)")
            excluded_by_tag.add(symbol)
            continue

        if has_excluded_symbol(symbol, excluded_symbols_upper):
            logger.debug(f"  → Filtered {symbol} (blacklisted symbol)")
            excluded_by_symbol.add(symbol)
            continue

        quotes = coin.get('quotes') or []
        first_quote = quotes[0] if quotes else {}
        market_cap = first_quote.get('marketCap')
        fdmc = first_quote.get('fullyDilutedMarketCap')

        platform_field = coin.get('platform') or {}
        platform_name = platform_field.get('name') if isinstance(platform_field, dict) else None

        # `errors='coerce'` returns NaT for None or unparseable values, which
        # plays cleanly with the nullable datetime64[ns] column downstream.
        date_added = pd.to_datetime(coin.get('dateAdded'), errors='coerce')

        records.append({
            'provider': 'coinmarketcap',
            'provider_id': int(coin['id']),
            'date': date,
            'symbol': symbol,
            'name': coin.get('name') or '',
            'slug': coin.get('slug'),
            'rank': rank,
            'market_cap': market_cap,
            'fully_diluted_market_cap': fdmc,
            'circulating_supply': coin.get('circulatingSupply'),
            'max_supply': coin.get('maxSupply'),
            'tags': tags_str,
            'platform': platform_name,
            'date_added': date_added,
        })
```

- [ ] **Step 3: Update the dedup sort key**

Still in `_fetch_snapshot`, the post-build dedup currently sorts on `['symbol', 'rank', 'market_cap']` and dedups on `['date', 'symbol']`. The PK is now `(provider, provider_id, date)`, so:

Find the block around lines 158-172 and replace it with:

```python
    df = pd.DataFrame(records, columns=UNIVERSE_COLUMNS)
    if not df.empty:
        before_dedup = len(df)
        df = df.sort_values(
            by=['provider_id', 'rank', 'market_cap'],
            ascending=[True, True, False],
            na_position='last',
        ).drop_duplicates(subset=['provider', 'provider_id', 'date'], keep='first')
        dropped = before_dedup - len(df)
        if dropped:
            logger.warning(
                "Dropped %s duplicate universe rows for %s on key ['provider', 'provider_id', 'date']",
                dropped,
                date_str,
            )
```

- [ ] **Step 4: Update the docstring of `_fetch_snapshot`**

Find the `Returns` section in the `_fetch_snapshot` docstring (around line 96-100) and replace the columns list:

```python
    Returns
    -------
    Tuple[pd.DataFrame, Set[str], Set[str]]
        - DataFrame with v6 universe columns (see UNIVERSE_COLUMNS)
        - Set of symbols excluded by tags
        - Set of symbols excluded by symbol blacklist
```

- [ ] **Step 5: Run the parsing tests to confirm they pass**

Run: `uv run pytest tests/ingestion/test_universe_parsing.py -v`

Expected: All 7 tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/crypto_data/database_builder.py
git commit -m "feat(ingest): extract enriched CMC fields into v6 universe records"
```

---

## Task 6: Update existing `test_universe_ingestion.py` fixtures

**Files:**
- Modify: `tests/ingestion/test_universe_ingestion.py`

- [ ] **Step 1: Update raw `INSERT` fixtures**

Several tests in `test_universe_ingestion.py` insert legacy-shaped rows directly via `conn.execute("INSERT INTO crypto_universe SELECT * FROM initial_data")` using a 5-column DataFrame. These will fail under the v6 DDL.

Add the following dataclass + helper near the top of the file (after the existing imports). All 6 fields are required — no `None`-as-control-signal, no magic-number fallbacks:

```python
from dataclasses import dataclass
import duckdb


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
```

Then replace each legacy-style fixture by constructing a `UniverseRowFixture` and calling `_insert_universe_row`. Use this stable id mapping (matching Step 2 mock payloads): BTC=1/Bitcoin, ETH=1027/Ethereum, SOL=5426/Solana, OLD=99001/Legacy Asset.

- `test_universe_rollback_on_error` (around line 28-32): replace the 4-line DataFrame+INSERT with
  ```python
  _insert_universe_row(conn, UniverseRowFixture(
      provider_id=1, date='2024-01-01', symbol='BTC',
      name='Bitcoin', rank=1, market_cap=1_000_000.0,
  ))
  ```
- `test_universe_no_rollback_after_commit` (around line 65-72):
  ```python
  _insert_universe_row(conn, UniverseRowFixture(
      provider_id=99001, date='2024-01-01', symbol='OLD',
      name='Legacy Asset', rank=1, market_cap=1_000.0,
  ))
  ```
- `test_universe_transaction_atomicity` (around line 299-303): replace the 2-row DataFrame block with two `_insert_universe_row(conn, UniverseRowFixture(...))` calls (BTC id=1, ETH id=1027, both at `date='2024-01-01'`, ranks 1/2, market caps 1_000_000 / 500_000). Update the `new_data` block (line 325-328) similarly with `UniverseRowFixture(provider_id=5426, date='2024-01-01', symbol='SOL', name='Solana', rank=1, market_cap=2_000_000.0)`.
- `test_update_coinmarketcap_universe_rejects_invalid_snapshot_data` (around line 211-214):
  ```python
  _insert_universe_row(db.conn, UniverseRowFixture(
      provider_id=1, date='2024-01-01', symbol='BTC',
      name='Bitcoin', rank=1, market_cap=1_000_000.0,
  ))
  ```
- `test_update_coinmarketcap_universe_skip_existing_filters_present_dates` (around line 383-386): same as above (BTC id=1).
- `test_update_coinmarketcap_universe_skip_existing_is_date_only` (around line 416-419): same as above (BTC id=1).
- `test_update_coinmarketcap_universe_skip_existing_disabled_fetches_all` (around line 458-461): same as above (BTC id=1).

- [ ] **Step 2: Update mock CMC payloads to include `id` and `name`**

Every mock payload list has dicts of the form `{'symbol': 'BTC', 'cmcRank': 1, 'quotes': [...], 'tags': []}`. The new parser requires `id` and `name`. Update each payload to add those fields. Example for `test_universe_no_rollback_after_commit`:

```python
new_data = [
    {'id': 1, 'symbol': 'BTC', 'name': 'Bitcoin', 'cmcRank': 1,
     'quotes': [{'marketCap': 2000000}], 'tags': []}
]
```

Apply the same pattern (adding `id` and `name`) to every `new_data = [...]` block in the file. Use a stable mapping `symbol → (id, name)`:
- BTC → `(1, 'Bitcoin')`
- ETH → `(1027, 'Ethereum')`
- USDT → `(825, 'Tether USDt')`
- WBTC → `(3717, 'Wrapped Bitcoin')`
- PAXG → `(4705, 'PAX Gold')`
- TSLA → `(12345, 'Tesla Tokenized')`
- SOL → `(5426, 'Solana')`
- OLD → `(99001, 'Legacy Asset')` (synthetic)

**Important — `test_update_coinmarketcap_universe_warns_on_duplicate_symbols`:** the two SOL records in `duplicate_data` must share **the same** `id=5426` and `name='Solana'`. This is required because the v6 dedup key is `(provider, provider_id, date)`, so the two rows need the same `provider_id` for the dedup to actually trigger. Otherwise both rows would survive and the assertion `rows == [('SOL', 10, 1000.0)]` would fail.

The `test_update_coinmarketcap_universe_rejects_invalid_snapshot_data` test (line 216-220) intentionally provides a malformed payload missing `cmcRank`. **Keep it malformed** but add `id` and `name` so the parser fails on rank/cap validation rather than on KeyError for `id`:
```python
invalid_data = [
    {'id': 1027, 'symbol': 'ETH', 'name': 'Ethereum', 'quotes': [{}], 'tags': []}
]
```

- [ ] **Step 3: Update the existing assertion in `test_update_coinmarketcap_universe_rejects_invalid_snapshot_data`**

The final assertion `assert result == ('BTC', 1, 1000000.0)` selects `(symbol, rank, market_cap)`. Leave the SELECT statement and the assertion as-is — the column names are still valid in v6.

- [ ] **Step 4: Run the full ingestion test directory**

Run: `uv run pytest tests/ingestion/ -v`

Expected: All tests pass.

- [ ] **Step 5: Commit**

```bash
git add tests/ingestion/test_universe_ingestion.py
git commit -m "test: adapt universe ingestion fixtures to v6 schema"
```

---

## Task 7: Rename `categories` → `tags` in `utils/symbols.py`

**Files:**
- Modify: `src/crypto_data/utils/symbols.py:89, 101-103`

- [ ] **Step 1: Update the SELECT statement and loop variable name**

In `src/crypto_data/utils/symbols.py`, replace lines 88-104 (the SELECT block and the exclusion loop):

```python
            result = conn.execute("""
                SELECT DISTINCT symbol, tags
                FROM crypto_universe
                WHERE date >= ?
                    AND date <= ?
                    AND rank <= ?
                ORDER BY symbol
            """, [start_date, end_date, top_n]).fetchall()

        # If any historical row for a symbol has an excluded tag, exclude the
        # symbol from the download superset.
        symbol_exclusions = {}
        for symbol, tags in result:
            symbol_exclusions.setdefault(symbol, False)
            if has_excluded_tag(tags, exclude_tags):
                symbol_exclusions[symbol] = True
```

- [ ] **Step 2: Run the symbol-extraction tests**

Run: `uv run pytest tests/utils/ tests/ingestion/test_universe_ingestion.py tests/pipeline/ -v`

Expected: All pass. (If any test referenced `categories` directly in a SELECT, fix it the same way.)

- [ ] **Step 3: Commit**

```bash
git add src/crypto_data/utils/symbols.py
git commit -m "refactor(symbols): rename categories column reference to tags"
```

---

## Task 8: Add the in-listing-rename identity test

**Files:**
- Create: `tests/ingestion/test_universe_identity.py`

- [ ] **Step 1: Write the test**

```python
"""Identity test: stable provider_id survives in-listing symbol renames.

Note: cross-listing rebrands (e.g., MATIC id=3890 → POL id=28321) are NOT
unified by provider_id alone — those produce two distinct ids on CMC and
remain the consumer's UNION concern. This file tests only the in-listing
case, where CMC keeps `id` constant while displayed `symbol` changes.
"""

import tempfile
from pathlib import Path

import duckdb
import pandera.pandas as pa  # noqa: F401  (kept for consistency with other test files)
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

        distinct_ids = db.execute("""
            SELECT COUNT(DISTINCT provider_id) FROM crypto_universe
            WHERE provider_id = ?
        """, [_SYNTHETIC_ID]).fetchone()[0]
        assert distinct_ids == 1

        symbols_by_date = db.execute("""
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
```

- [ ] **Step 2: Run the test**

Run: `uv run pytest tests/ingestion/test_universe_identity.py -v`

Expected: All 3 tests pass.

- [ ] **Step 3: Commit**

```bash
git add tests/ingestion/test_universe_identity.py
git commit -m "test: add identity test for stable provider_id across symbol rename"
```

---

## Task 9: Update documentation (CLAUDE.md, README, README.fr)

**Files:**
- Modify: `CLAUDE.md` (Database Schema section + SQL Notes)
- Modify: `README.md`, `README.fr.md` (breaking-change note)

- [ ] **Step 1: Update `CLAUDE.md` Database Schema section**

Find the Database Schema table (search for `crypto_universe | (date, symbol)`) and replace the `crypto_universe` row with:

```markdown
| `crypto_universe` | `(provider, provider_id, date)` | CoinMarketCap-enriched snapshots; columns: `provider`, `provider_id` (cmc_id), `date`, `symbol` (mutable, not identity), `name`, `slug`, `rank`, `market_cap`, `fully_diluted_market_cap`, `circulating_supply`, `max_supply`, `tags`, `platform`, `date_added` |
```

Then in the **SQL Notes** code block, update the comment and example:

```sql
-- Universe JOIN: u.symbol || 'USDT' = s.symbol  (symbol is mutable, not identity)
-- Track an asset across symbol renames via provider_id.
-- Check top N status (don't use spot/futures for this):
SELECT date, symbol, rank FROM crypto_universe WHERE provider_id = 11419 AND rank <= 50;
```

(Replace the previous example that used `symbol = 'TON'` — the new form demonstrates the new identity primitive. `11419` is the CMC id for TON; the example is illustrative.)

- [ ] **Step 2: Add a top-of-section breaking-change note to `README.md`**

Find the section that introduces the package (near the top, likely under a "What's new" or "Quick Start" header). Insert before it:

```markdown
> **v6.0.0 — breaking schema change.** The `crypto_universe` table now uses `(provider, provider_id, date)` as primary key and stores 14 enriched fields including `name`, `slug`, `fully_diluted_market_cap`, supply fields, `platform`, and `date_added`. **Delete pre-v6 `.db` files** and re-ingest with your existing scripts.
```

- [ ] **Step 3: Add the same note (translated) to `README.fr.md`**

```markdown
> **v6.0.0 — changement de schéma cassant.** La table `crypto_universe` utilise désormais `(provider, provider_id, date)` comme clé primaire et stocke 14 champs enrichis dont `name`, `slug`, `fully_diluted_market_cap`, supply, `platform` et `date_added`. **Supprimez les fichiers `.db` antérieurs à v6** et relancez l'ingestion avec vos scripts existants.
```

- [ ] **Step 4: Commit**

```bash
git add CLAUDE.md README.md README.fr.md
git commit -m "docs: document v6 enriched universe schema and breaking change"
```

---

## Task 10: Bump version, run full suite, finalize

**Files:**
- Modify: `src/crypto_data/__init__.py:65` (`__version__`)

- [ ] **Step 1: Bump version**

In `src/crypto_data/__init__.py`, replace:
```python
__version__ = "5.0.0"
```
with:
```python
__version__ = "6.0.0"
```

Also update the docstring header (around line 1) — the line `Crypto Data Infrastructure Package v5.0.0` becomes `Crypto Data Infrastructure Package v6.0.0`. Add a new BREAKING CHANGE block above the existing v5 one:

```python
"""
Crypto Data Infrastructure Package v6.0.0

Pure data ingestion pipeline for cryptocurrency data.
Downloads from Binance Data Vision and CoinMarketCap → Populates DuckDB.

BREAKING CHANGE (v6.0.0): Enriched CMC universe schema
- crypto_universe primary key: (provider, provider_id, date)
- 14 columns: provider, provider_id, date, symbol, name, slug, rank,
  market_cap, fully_diluted_market_cap, circulating_supply, max_supply,
  tags, platform, date_added
- 'categories' column is renamed to 'tags' (matches CMC API field)
- Pre-v6 databases must be deleted and re-ingested

BREAKING CHANGE (v5.0.0): Type-safe enums
...
"""
```

(Keep the rest of the docstring unchanged.)

- [ ] **Step 2: Run the full test suite**

Run: `uv run pytest tests/ -v`

Expected: All tests pass. If any unrelated test fails, investigate before continuing.

- [ ] **Step 3: Commit**

```bash
git add src/crypto_data/__init__.py
git commit -m "chore: bump version to 6.0.0 for enriched CMC universe schema"
```

- [ ] **Step 4: Final sanity check**

Run a one-shot end-to-end smoke (no real network):

```bash
uv run python -c "
from crypto_data import CryptoDatabase
import tempfile, pathlib
with tempfile.TemporaryDirectory() as t:
    p = pathlib.Path(t) / 'smoke.db'
    db = CryptoDatabase(str(p))
    cols = [r[0] for r in db.execute(\"SELECT column_name FROM information_schema.columns WHERE table_name = 'crypto_universe'\").fetchall()]
    print('columns:', sorted(cols))
    assert 'provider_id' in cols and 'fully_diluted_market_cap' in cols and 'tags' in cols
    print('OK')
"
```

Expected output ends with `OK`.
