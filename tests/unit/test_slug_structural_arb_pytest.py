from __future__ import annotations

import pytest

from executor.slug_structural_arb import (
    SlugMarket,
    SlugResolutionConfig,
    SlugSubscriptionPlan,
    auto_pair_slugs,
    build_slug_pairs,
    build_structural_arb_candidates,
    fetch_markets_for_slug,
    parse_slug_pairs,
)


pytestmark = pytest.mark.unit


def test_auto_pair_slugs_matches_between_and_non_between_same_theme_date() -> None:
    slugs = (
        "bitcoin-price-on-march-22",
        "bitcoin-price-between-march-22",
        "eth-price-on-march-22",
        "eth-price-between-march-22",
    )

    pairs = auto_pair_slugs(slugs)

    assert ("bitcoin-price-on-march-22", "bitcoin-price-between-march-22") in pairs
    assert ("eth-price-on-march-22", "eth-price-between-march-22") in pairs


def test_auto_pair_slugs_matches_price_and_above_same_theme_date() -> None:
    slugs = (
        "bitcoin-above-on-march-22",
        "bitcoin-price-on-march-22",
    )

    pairs = auto_pair_slugs(slugs)

    assert ("bitcoin-above-on-march-22", "bitcoin-price-on-march-22") in pairs


def test_build_slug_pairs_merges_explicit_and_auto() -> None:
    plan = SlugSubscriptionPlan(
        slugs=(
            "bitcoin-price-on-march-22",
            "bitcoin-price-between-march-22",
            "eth-price-on-march-22",
            "eth-price-between-march-22",
        ),
        explicit_pairs=(("manual-a", "manual-b"),),
        auto_pair=True,
        match_mode="text",
    )

    pairs = build_slug_pairs(plan)

    assert ("manual-a", "manual-b") in pairs
    assert ("bitcoin-price-on-march-22", "bitcoin-price-between-march-22") in pairs


def test_parse_slug_pairs_accepts_pipe_comma_format() -> None:
    parsed = parse_slug_pairs("a|b,c|d, invalid, e|f")
    assert parsed == (("a", "b"), ("c", "d"), ("e", "f"))


def test_structural_candidates_text_match_filters_pairs() -> None:
    markets_by_slug = {
        "bitcoin-price-on-march-22": (
            SlugMarket(
                slug="bitcoin-price-on-march-22",
                market_id="mkt-a",
                question="Will Bitcoin close above 90000 on March 22?",
                token_ids=("111", "222"),
                active=True,
                accepting_orders=True,
                closed=False,
                yes_price_cents=53,
            ),
        ),
        "bitcoin-price-between-march-22": (
            SlugMarket(
                slug="bitcoin-price-between-march-22",
                market_id="mkt-b",
                question="Will Bitcoin close between 88000 and 92000 on March 22?",
                token_ids=("333", "444"),
                active=True,
                accepting_orders=True,
                closed=False,
                yes_price_cents=61,
            ),
            SlugMarket(
                slug="bitcoin-price-between-march-22",
                market_id="mkt-c",
                question="Will Ethereum close above 5000 on March 22?",
                token_ids=("555", "666"),
                active=True,
                accepting_orders=True,
                closed=False,
                yes_price_cents=20,
            ),
        ),
    }

    candidates = build_structural_arb_candidates(
        pairs=(("bitcoin-price-on-march-22", "bitcoin-price-between-march-22"),),
        markets_by_slug=markets_by_slug,
        match_mode="text",
        min_similarity=0.20,
    )

    market_pairs = {(item.left_market.market_id, item.right_market.market_id) for item in candidates}
    assert ("mkt-a", "mkt-b") in market_pairs
    assert ("mkt-a", "mkt-c") not in market_pairs


def test_structural_candidates_strict_requires_two_strikes() -> None:
    markets_by_slug = {
        "bitcoin-price-on-march-22": (
            SlugMarket(
                slug="bitcoin-price-on-march-22",
                market_id="above-88000",
                question="Will Bitcoin close above 88000 on March 22?",
                token_ids=("111", "222"),
                active=True,
                accepting_orders=True,
                closed=False,
                yes_price_cents=58,
            ),
            SlugMarket(
                slug="bitcoin-price-on-march-22",
                market_id="above-92000",
                question="Will Bitcoin close above 92000 on March 22?",
                token_ids=("333", "444"),
                active=True,
                accepting_orders=True,
                closed=False,
                yes_price_cents=39,
            ),
        ),
        "bitcoin-price-between-march-22": (
            SlugMarket(
                slug="bitcoin-price-between-march-22",
                market_id="between-88000-92000",
                question="Will Bitcoin close between 88000 and 92000 on March 22?",
                token_ids=("555", "666"),
                active=True,
                accepting_orders=True,
                closed=False,
                yes_price_cents=22,
            ),
        ),
    }

    candidates = build_structural_arb_candidates(
        pairs=(("bitcoin-price-on-march-22", "bitcoin-price-between-march-22"),),
        markets_by_slug=markets_by_slug,
        match_mode="strict",
    )

    assert len(candidates) == 1
    candidate = candidates[0]
    assert candidate.match_mode == "strict"
    assert candidate.lower_strike_market is not None
    assert candidate.upper_strike_market is not None
    assert candidate.range_market is not None
    assert candidate.lower_strike_market.market_id == "above-88000"
    assert candidate.upper_strike_market.market_id == "above-92000"
    assert candidate.range_market.market_id == "between-88000-92000"


def test_structural_candidates_strict_skips_when_upper_strike_missing() -> None:
    markets_by_slug = {
        "bitcoin-price-on-march-22": (
            SlugMarket(
                slug="bitcoin-price-on-march-22",
                market_id="above-88000",
                question="Will Bitcoin close above 88000 on March 22?",
                token_ids=("111", "222"),
                active=True,
                accepting_orders=True,
                closed=False,
                yes_price_cents=58,
            ),
        ),
        "bitcoin-price-between-march-22": (
            SlugMarket(
                slug="bitcoin-price-between-march-22",
                market_id="between-88000-92000",
                question="Will Bitcoin close between 88000 and 92000 on March 22?",
                token_ids=("555", "666"),
                active=True,
                accepting_orders=True,
                closed=False,
                yes_price_cents=22,
            ),
        ),
    }

    candidates = build_structural_arb_candidates(
        pairs=(("bitcoin-price-on-march-22", "bitcoin-price-between-march-22"),),
        markets_by_slug=markets_by_slug,
        match_mode="strict",
    )

    assert candidates == ()


def test_fetch_markets_for_slug_falls_back_to_events_when_markets_endpoint_empty(monkeypatch) -> None:
    config = SlugResolutionConfig(
        gamma_api_url="https://gamma-api.polymarket.com",
        timeout_ms=1000,
        include_only_active_tradable=False,
    )

    calls: list[str] = []

    def fake_http_get_json(url: str, *, timeout_ms: int) -> object:
        _ = timeout_ms
        calls.append(url)
        if "/markets?" in url:
            return []
        if "/events?" in url:
            return [
                {
                    "slug": "bitcoin-above-on-march-22",
                    "markets": [
                        {
                            "conditionId": "cond-1",
                            "question": "Will the price of Bitcoin be above $62000 on March 22?",
                            "clobTokenIds": '["yes-token","no-token"]',
                            "outcomes": '["Yes","No"]',
                            "outcomePrices": '["0.65","0.35"]',
                            "active": True,
                            "closed": False,
                        }
                    ],
                }
            ]
        raise AssertionError(f"unexpected url: {url}")

    monkeypatch.setattr("executor.slug_structural_arb._http_get_json", fake_http_get_json)

    markets = fetch_markets_for_slug("bitcoin-above-on-march-22", config)

    assert len(markets) == 1
    assert markets[0].market_id == "cond-1"
    assert markets[0].yes_price_cents == 65
    assert markets[0].no_price_cents == 35
    assert any("/markets?" in url for url in calls)
    assert any("/events?" in url for url in calls)
