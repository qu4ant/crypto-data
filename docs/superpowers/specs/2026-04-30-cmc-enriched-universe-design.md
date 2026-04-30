# CMC Enriched Universe — Design Spec

**Date:** 2026-04-30
**Status:** Approved (brainstorming validated)
**Target version:** crypto-data v6.0.0 (breaking schema change)

## 1. Motivation

The current `crypto_universe` table stores only five columns (`date`, `symbol`, `rank`, `market_cap`, `categories`) and discards the rest of the CoinMarketCap historical-listings payload. Three concrete problems follow from this:

1. **No stable provider identity.** `(date, symbol)` is the primary key, but `symbol` is mutable on CMC's side. Within a single CMC listing, the displayed ticker can change while `id` stays constant (in-listing rename). Without storing `cmc_id`, two snapshots of the same listing before and after such a change cannot be linked, and downstream code is forced to assume `symbol` is identity. Note: cross-listing rebrands where CMC issues a brand-new `id` (e.g., MATIC `id=3890` → POL `id=28321`) are **not** unified by `provider_id` alone — those still require the user-side UNION pattern documented in `CLAUDE.md`, and (optionally, in a future spec) an external alias map. v6.0.0 only fixes the in-listing case.
2. **No anti-shitcoin signals.** Supply-related fields (`circulating_supply`, `max_supply`, `fully_diluted_market_cap`) are the standard discriminants for low-float / high-future-dilution risk. None are stored.
3. **No descriptive metadata.** `name`, `slug`, `platform` (chain), `date_added` are useful for filtering (e.g., exclude assets younger than 6 months, group by L1) and for audit / display.

This spec defines a v6.0.0 schema that captures these fields while keeping the package's "ingestion-only, query in SQL" philosophy intact.

## 2. Goals & Non-Goals

**Goals**
- Add stable provider identity via `(provider, provider_id)`.
- Capture the supply / FDMC fields needed for shitcoin filtering.
- Add descriptive metadata (`name`, `slug`, `platform`, `date_added`).
- Rename the misnamed `categories` column to `tags` (matches CMC's API field).

**Non-Goals**
- Multi-provider support beyond schema readiness. Only CoinMarketCap is ingested in v6.0.0; `provider` is a constant `'coinmarketcap'` enforced by a `CHECK` constraint. Adding a second provider is a future spec.
- Asset-level normalization. We keep a single fat snapshot table (one row per `(provider, provider_id, date)`); we do **not** introduce separate `assets` / `asset_metadata_history` tables.
- Migration of existing v5 databases. The user will delete pre-v6 `.db` files and re-ingest.
- Storing fields already available elsewhere (`price`, `volume_24h`, `high_24h`, `low_24h` come from Binance OHLCV) or trivially derivable (`percent_change_*`, `ath`, `atl`).

## 3. Schema

### 3.1 DDL

```sql
CREATE TABLE crypto_universe (
    -- Identity
    provider                 VARCHAR  NOT NULL CHECK (provider = 'coinmarketcap'),
    provider_id              BIGINT   NOT NULL,
    date                     DATE     NOT NULL,

    -- Display / linking
    symbol                   VARCHAR  NOT NULL,   -- mutable, NOT identity
    name                     VARCHAR  NOT NULL,
    slug                     VARCHAR,

    -- Universe selection
    rank                     INTEGER  NOT NULL,
    market_cap               DOUBLE,
    fully_diluted_market_cap DOUBLE,

    -- Anti-shitcoin (supply)
    circulating_supply       DOUBLE,
    max_supply               DOUBLE,

    -- Metadata
    tags                     VARCHAR,             -- comma-separated
    platform                 VARCHAR,             -- chain name, NULL for L1s
    date_added               DATE,

    PRIMARY KEY (provider, provider_id, date)
);

CREATE INDEX idx_universe_date_rank   ON crypto_universe(date, rank);
CREATE INDEX idx_universe_date_symbol ON crypto_universe(date, symbol);
```

### 3.2 Column rationale

| Column | Source CMC field | Nullable | Notes |
|---|---|---|---|
| `provider` | (constant) | No | Future-proofs multi-provider; enforced by CHECK |
| `provider_id` | `id` | No | Stable identity; survives symbol changes |
| `date` | request param | No | Snapshot date |
| `symbol` | `symbol` | No | Uppercased; mutable; used for Binance joins |
| `name` | `name` | No | Display |
| `slug` | `slug` | Yes | URL identifier; defensively nullable |
| `rank` | `cmcRank` | No | Universe selection key |
| `market_cap` | `quotes[0].marketCap` | Yes | Sizing |
| `fully_diluted_market_cap` | `quotes[0].fullyDilutedMarketCap` | Yes | **Primary anti-shitcoin signal** |
| `circulating_supply` | `circulatingSupply` | Yes | Numerator of float ratio |
| `max_supply` | `maxSupply` | Yes | Denominator; commonly null on CMC |
| `tags` | `tags` (joined `,`) | Yes | Filter source for `exclude_tags` |
| `platform` | `platform.name` | Yes | NULL for BTC, ETH, other L1s |
| `date_added` | `dateAdded` (cast to DATE) | Yes | Anti-pump filter (exclude very new coins) |

### 3.3 Index rationale

- `(date, rank)` — already present in v5; supports `WHERE date = ? AND rank <= N` (universe extraction).
- `(date, symbol)` — new; supports the Binance join `u.symbol || 'USDT' = s.symbol` documented in `CLAUDE.md`. Without this, every Binance-side query forces a full scan.
- The PK `(provider, provider_id, date)` covers identity-based queries (e.g., "all snapshots for cmc_id=3890").

### 3.4 Pandera schema (`schemas/universe.py`)

`UNIVERSE_SCHEMA` is rewritten with the 14 columns above and the following dataframe-level checks:

- `unique=['provider', 'provider_id', 'date']` (PK)
- `provider == 'coinmarketcap'` row-level
- `provider_id >= 1`, `rank >= 1`, supplies `>= 0` when non-null
- `circulating_supply <= max_supply` when both non-null (mathematically inviolable)
- `name` non-null (CMC always provides it)
- We deliberately do **not** check `fully_diluted_market_cap >= market_cap`. Although true by construction at any single instant, CMC can publish MC and FDMC computed from slightly out-of-sync supply snapshots; rejecting an entire date for that data-staleness would be fragile.

## 4. Ingestion

### 4.1 Parsing (`database_builder.filter_universe_data`)

The current per-coin record-builder (`database_builder.py:119-149`) is replaced with the following, while keeping the existing `exclude_tags` / `exclude_symbols` filtering logic and the post-build deterministic dedup (`sort_values` + `drop_duplicates`):

```python
records.append({
    'provider': 'coinmarketcap',
    'provider_id': int(coin['id']),
    'date': pd.Timestamp(date_str),
    'symbol': str(coin.get('symbol') or '').upper(),
    'name': coin.get('name') or '',
    'slug': coin.get('slug'),
    'rank': coin.get('cmcRank'),
    'market_cap': quotes[0].get('marketCap') if quotes else None,
    'fully_diluted_market_cap': quotes[0].get('fullyDilutedMarketCap') if quotes else None,
    'circulating_supply': coin.get('circulatingSupply'),
    'max_supply': coin.get('maxSupply'),
    'tags': ','.join(coin.get('tags') or []),
    'platform': (coin.get('platform') or {}).get('name'),
    'date_added': pd.to_datetime(coin.get('dateAdded')).date() if coin.get('dateAdded') else None,
})
```

`UNIVERSE_COLUMNS` becomes the 14-tuple matching the DDL order.

### 4.2 Insertion path

The atomic-per-date insertion path in `update_coinmarketcap_universe` is unchanged:

```sql
DELETE FROM crypto_universe WHERE date = ?;
INSERT INTO crypto_universe SELECT * FROM df_new;
```

The `INSERT ... SELECT *` keeps working because the DataFrame column order matches the DDL column order (we control both).

`_get_existing_dates` (the `skip_existing` check) is unchanged — it queries `DISTINCT date` only.

### 4.3 Exclusion filters

`has_excluded_tag(tags_list, excluded_tags_lower)` and `has_excluded_symbol(symbol, excluded_symbols_upper)` are unchanged — they operate on the in-memory `tags` list and `symbol` string before the record is appended.

## 5. Downstream impact

### 5.1 `utils/symbols.py:89` — column rename

```python
# Before
SELECT DISTINCT symbol, categories FROM crypto_universe ...
# After
SELECT DISTINCT symbol, tags FROM crypto_universe ...
```

The downstream tag-filtering logic (`has_excluded_tag(tags_string, exclude_tags)`) is unchanged — the column still contains a comma-separated string.

### 5.2 `quality.py`

No changes. All queries reference `date`, `symbol`, `rank`, `market_cap` — all preserved.

### 5.3 `database.py`

`_create_crypto_universe` is rewritten to emit the v6 DDL. No migration helper, no ALTER, no legacy table. v5 databases must be deleted by the user before first v6 use.

### 5.4 Public API (`__init__.py`)

No new exports. Users continue to query the table directly in SQL. The new columns are documented in `CLAUDE.md`.

### 5.5 `CLAUDE.md`

Update the **Database Schema** section: replace the `crypto_universe` row description with the 14-column list, and update the SQL Notes example to reference `tags` instead of `categories`.

## 6. Migration

**No automated migration.** v6.0.0 is a breaking schema change. Action required from users:

1. Delete pre-v6 `.db` files.
2. Re-run their CMC ingestion scripts (e.g., `scripts/download_top60_h4_spot_futures_fundings_2022_01_2026_04.py`).

The CMC daily quota (200 calls / 24h) makes a full re-ingestion of one year of daily snapshots take ~2 days. Weekly/monthly rebalances are well under one day.

A one-line note in the `README` (English + French) and the `__init__.py` `__version__` bump (`5.x.x` → `6.0.0`) communicate the break.

## 7. Testing

### 7.1 Update existing tests

- `tests/database/test_database_basic.py` — verify the 14 columns appear in `INFORMATION_SCHEMA`; remove the `'categories' in universe_columns` assertion.
- `tests/ingestion/test_universe_ingestion.py` — update all `INSERT` fixtures to include the new fields; PK becomes `(provider, provider_id, date)`.

### 7.2 New test files

**`tests/ingestion/test_universe_parsing.py`** — unit tests on `filter_universe_data`:
- Full payload → all 14 fields extracted correctly.
- `platform: null` (BTC, ETH) → `platform = None`.
- `maxSupply: null` (typical for most tokens) → `max_supply = None`.
- `quotes: []` → `market_cap = None`, `fully_diluted_market_cap = None`.
- `tags: []` → `tags = ''`.
- `dateAdded` parsing handles both ISO timestamps and date-only strings.

**`tests/ingestion/test_universe_schema.py`** — Pandera:
- A valid DataFrame passes.
- `provider != 'coinmarketcap'` → SchemaError.
- `circulating_supply > max_supply` (both non-null) → SchemaError.
- Duplicate `(provider, provider_id, date)` → SchemaError.
- A row with `fully_diluted_market_cap < market_cap` does **not** raise (intentional, see §3.4).

**`tests/ingestion/test_universe_identity.py`** — the in-listing-rename use case (synthetic data, no real-world claim):
- Insert two snapshots of a synthetic asset with the same `provider_id` but different `symbol` strings on `date1` and `date2`.
- Assert that `GROUP BY provider_id` returns a single continuous series with the symbol change observable across the date axis.
- Assert that the same `(provider_id, date)` pair cannot be inserted twice (PK uniqueness).
- Explicitly **not** tested here: cross-listing rebrands such as MATIC→POL — those produce two distinct `provider_id`s on CMC and remain the consumer's UNION concern.

### 7.3 Untouched

`quality.py` tests (preserved column set), all `binance_*` tests (orthogonal).

## 8. Versioning

- Bump `crypto_data.__version__` to `6.0.0`.
- Add a `CHANGELOG` / `README` note: *"v6.0.0 introduces enriched CMC universe (14 columns including provider_id, FDMC, supply, platform). Breaking schema — delete pre-v6 databases and re-ingest."*

## 9. Open Questions

None.
