"""
Shared CoinMarketCap universe filtering rules.

The defaults keep synthetic/pegged assets out of both universe ingestion and
download symbol extraction: stablecoins, wrapped assets, and tokenized assets.
"""

from collections.abc import Iterable
from typing import List, Optional, Sequence


DEFAULT_UNIVERSE_EXCLUDE_TAGS = [
    "stablecoin",
    "wrapped-tokens",
    "tokenized",
    "tokenised",
]

DEFAULT_UNIVERSE_EXCLUDE_SYMBOLS: List[str] = []


def resolve_exclude_tags(exclude_tags: Optional[Sequence[str]]) -> List[str]:
    """Return default tags unless the caller explicitly provides a list."""
    if exclude_tags is None:
        return list(DEFAULT_UNIVERSE_EXCLUDE_TAGS)
    return list(exclude_tags)


def resolve_exclude_symbols(exclude_symbols: Optional[Sequence[str]]) -> List[str]:
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
    Return True when a CMC tag matches an excluded tag or tag family.

    Matching is case-insensitive and substring-based so ``tokenized`` excludes
    ``tokenized-gold`` / ``tokenized-stock`` without maintaining every subtype.
    """
    excluded_tags = [_normalize_tag(tag) for tag in exclude_tags if tag]
    if not excluded_tags:
        return False

    for tag in _iter_tags(tags):
        normalized_tag = _normalize_tag(tag)
        if any(excluded in normalized_tag for excluded in excluded_tags):
            return True
    return False


def has_excluded_symbol(symbol: str, exclude_symbols: Sequence[str]) -> bool:
    """Return True when a symbol is explicitly excluded."""
    excluded_symbols = {excluded.upper() for excluded in exclude_symbols}
    return symbol.upper() in excluded_symbols
