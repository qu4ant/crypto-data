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
