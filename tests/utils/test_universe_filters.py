"""Tests for shared universe filtering rules."""

from crypto_data.universe_filters import DEFAULT_UNIVERSE_EXCLUDE_TAGS, has_excluded_tag


def test_default_exclude_tags_match_exact_synthetic_assets():
    assert has_excluded_tag(["stablecoin"], DEFAULT_UNIVERSE_EXCLUDE_TAGS)
    assert has_excluded_tag(["usd-stablecoin"], DEFAULT_UNIVERSE_EXCLUDE_TAGS)
    assert has_excluded_tag(["wrapped-tokens"], DEFAULT_UNIVERSE_EXCLUDE_TAGS)
    assert has_excluded_tag(["tokenized-gold"], DEFAULT_UNIVERSE_EXCLUDE_TAGS)
    assert has_excluded_tag(["tokenized-commodities"], DEFAULT_UNIVERSE_EXCLUDE_TAGS)


def test_default_exclude_tags_do_not_match_broad_categories():
    assert not has_excluded_tag(["stablecoin-protocol"], DEFAULT_UNIVERSE_EXCLUDE_TAGS)
    assert not has_excluded_tag(["tokenized-stock"], DEFAULT_UNIVERSE_EXCLUDE_TAGS)
    assert not has_excluded_tag(["tokenized-assets"], DEFAULT_UNIVERSE_EXCLUDE_TAGS)


def test_exclude_tag_matching_is_case_insensitive_and_accepts_csv_strings():
    assert has_excluded_tag("DeFi, USD-Stablecoin", DEFAULT_UNIVERSE_EXCLUDE_TAGS)
    assert not has_excluded_tag("DeFi, Stablecoin-Protocol", DEFAULT_UNIVERSE_EXCLUDE_TAGS)
