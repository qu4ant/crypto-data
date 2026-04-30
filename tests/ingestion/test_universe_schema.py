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
