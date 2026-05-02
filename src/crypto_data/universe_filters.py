"""
Shared CoinMarketCap universe filtering rules.

The defaults keep synthetic/pegged assets out of both universe ingestion and
download symbol extraction: stablecoins, wrapped assets, and explicit
tokenized commodities/gold tags.
"""

from collections.abc import Iterable, Sequence

DEFAULT_UNIVERSE_EXCLUDE_TAGS = [
    "stablecoin",
    "asset-backed-stablecoin",
    "algorithmic-stablecoin",
    "usd-stablecoin",
    "fiat-stablecoin",
    "eur-stablecoin",
    "wrapped-tokens",
    "tokenized-gold",
    "tokenized-commodities",
]

DEFAULT_UNIVERSE_EXCLUDE_SYMBOLS: list[str] = []


def resolve_exclude_tags(exclude_tags: Sequence[str] | None) -> list[str]:
    """Return default tags unless the caller explicitly provides a list."""
    if exclude_tags is None:
        return list(DEFAULT_UNIVERSE_EXCLUDE_TAGS)
    return list(exclude_tags)


def resolve_exclude_symbols(exclude_symbols: Sequence[str] | None) -> list[str]:
    """Return default symbols unless the caller explicitly provides a list."""
    if exclude_symbols is None:
        return list(DEFAULT_UNIVERSE_EXCLUDE_SYMBOLS)
    return list(exclude_symbols)


def _normalize_tag(tag: str) -> str:
    return tag.strip().lower()


def _iter_tags(tags: object) -> Iterable[str]:
    if tags is None:
        return []
    if isinstance(tags, str):
        return [tag.strip() for tag in tags.split(",") if tag.strip()]
    if isinstance(tags, Iterable):
        return [str(tag).strip() for tag in tags if str(tag).strip()]
    return []


def has_excluded_tag(tags: object, exclude_tags: Sequence[str]) -> bool:
    """
    Return True when a CMC tag exactly matches an excluded tag.

    Matching is case-insensitive and exact to avoid excluding protocols or
    infrastructure assets tagged with broad categories such as
    ``stablecoin-protocol`` or ``tokenized-stock``.
    """
    excluded_tags = {_normalize_tag(tag) for tag in exclude_tags if tag}
    if not excluded_tags:
        return False

    for tag in _iter_tags(tags):
        normalized_tag = _normalize_tag(tag)
        if normalized_tag in excluded_tags:
            return True
    return False


def has_excluded_symbol(symbol: str, exclude_symbols: Sequence[str]) -> bool:
    """Return True when a symbol is explicitly excluded."""
    excluded_symbols = {excluded.upper() for excluded in exclude_symbols}
    return symbol.upper() in excluded_symbols
