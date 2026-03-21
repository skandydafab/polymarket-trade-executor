from __future__ import annotations

import json

import pytest

from examples.run_executor_from_env import (
    _ArbRejectionLogger,
    _ArbResearchLogger,
    _ObservedOpportunity,
    _RejectedOpportunity,
    _candidate_to_opportunity,
    _collect_observed_opportunities,
    _strict_candidate_to_opportunity,
)
from executor.planner import ExecutionPlanner
from executor.slug_structural_arb import SlugMarket, StructuralArbCandidate
from tests.helpers.factories import make_default_planner_config


pytestmark = pytest.mark.unit


def test_strict_candidate_considers_no_outcomes_when_present() -> None:
    lower = SlugMarket(
        slug="bitcoin-price-on-march-22",
        market_id="above-88000",
        question="Will Bitcoin close above 88000 on March 22?",
        token_ids=("l_yes", "l_no"),
        active=True,
        accepting_orders=True,
        closed=False,
        yes_price_cents=80,
        no_price_cents=20,
        yes_token_id="l_yes",
        no_token_id="l_no",
    )
    upper = SlugMarket(
        slug="bitcoin-price-on-march-22",
        market_id="above-92000",
        question="Will Bitcoin close above 92000 on March 22?",
        token_ids=("u_yes", "u_no"),
        active=True,
        accepting_orders=True,
        closed=False,
        yes_price_cents=10,
        no_price_cents=90,
        yes_token_id="u_yes",
        no_token_id="u_no",
    )
    between = SlugMarket(
        slug="bitcoin-price-between-march-22",
        market_id="between-88000-92000",
        question="Will Bitcoin close between 88000 and 92000 on March 22?",
        token_ids=("r_yes", "r_no"),
        active=True,
        accepting_orders=True,
        closed=False,
        yes_price_cents=65,
        no_price_cents=35,
        yes_token_id="r_yes",
        no_token_id="r_no",
    )

    candidate = StructuralArbCandidate(
        slug_left=lower.slug,
        slug_right=between.slug,
        left_market=lower,
        right_market=between,
        similarity=1.0,
        match_mode="strict",
        lower_strike_market=lower,
        upper_strike_market=upper,
        range_market=between,
        strike_low=8_800_000,
        strike_high=9_200_000,
        boundary_relation="above",
    )

    converted = _strict_candidate_to_opportunity(candidate, now_ns=1_000)

    assert converted.success
    opportunity = converted.opportunity
    assert opportunity is not None
    snapshots = converted.snapshots
    assert len(opportunity.legs) == 3
    token_ids = {leg.token_id for leg in opportunity.legs}
    assert "l_no" in token_ids
    assert "r_no" in token_ids
    assert len(snapshots) == 3


def test_strict_candidate_falls_back_to_yes_only_when_no_missing() -> None:
    lower = SlugMarket(
        slug="bitcoin-price-on-march-22",
        market_id="above-88000",
        question="Will Bitcoin close above 88000 on March 22?",
        token_ids=("l_yes",),
        active=True,
        accepting_orders=True,
        closed=False,
        yes_price_cents=62,
    )
    upper = SlugMarket(
        slug="bitcoin-price-on-march-22",
        market_id="above-92000",
        question="Will Bitcoin close above 92000 on March 22?",
        token_ids=("u_yes",),
        active=True,
        accepting_orders=True,
        closed=False,
        yes_price_cents=30,
    )
    between = SlugMarket(
        slug="bitcoin-price-between-march-22",
        market_id="between-88000-92000",
        question="Will Bitcoin close between 88000 and 92000 on March 22?",
        token_ids=("r_yes",),
        active=True,
        accepting_orders=True,
        closed=False,
        yes_price_cents=20,
    )

    candidate = StructuralArbCandidate(
        slug_left=lower.slug,
        slug_right=between.slug,
        left_market=lower,
        right_market=between,
        similarity=1.0,
        match_mode="strict",
        lower_strike_market=lower,
        upper_strike_market=upper,
        range_market=between,
        strike_low=8_800_000,
        strike_high=9_200_000,
        boundary_relation="above",
    )

    converted = _strict_candidate_to_opportunity(candidate, now_ns=2_000)

    assert converted.success
    opportunity = converted.opportunity
    assert opportunity is not None
    snapshots = converted.snapshots
    assert len(opportunity.legs) == 3
    token_ids = {leg.token_id for leg in opportunity.legs}
    assert token_ids == {"l_yes", "u_yes", "r_yes"}
    assert len(snapshots) == 3


def test_candidate_conversion_never_returns_none_for_invalid_strict_shape() -> None:
    lower = SlugMarket(
        slug="bitcoin-price-on-march-22",
        market_id="above-88000",
        question="Will Bitcoin close above 88000 on March 22?",
        token_ids=("l_yes",),
        active=True,
        accepting_orders=True,
        closed=False,
        yes_price_cents=62,
    )
    between = SlugMarket(
        slug="bitcoin-price-between-march-22",
        market_id="between-88000-92000",
        question="Will Bitcoin close between 88000 and 92000 on March 22?",
        token_ids=("r_yes",),
        active=True,
        accepting_orders=True,
        closed=False,
        yes_price_cents=20,
    )

    candidate = StructuralArbCandidate(
        slug_left=lower.slug,
        slug_right=between.slug,
        left_market=lower,
        right_market=between,
        similarity=1.0,
        match_mode="strict",
        lower_strike_market=lower,
        upper_strike_market=None,
        range_market=between,
        strike_low=8_800_000,
        strike_high=9_200_000,
        boundary_relation="above",
    )

    converted = _candidate_to_opportunity(candidate, now_ns=10_000)

    assert not converted.success
    assert converted.opportunity is None
    assert converted.reason_code == "CONVERT_INVALID_RELATION"
    assert converted.reason is not None


def test_collect_rejects_locked_book_as_invalid(monkeypatch: pytest.MonkeyPatch) -> None:
    lower = SlugMarket(
        slug="bitcoin-price-on-march-22",
        market_id="above-88000",
        question="Will Bitcoin close above 88000 on March 22?",
        token_ids=("l_yes",),
        active=True,
        accepting_orders=True,
        closed=False,
        yes_price_cents=62,
    )
    upper = SlugMarket(
        slug="bitcoin-price-on-march-22",
        market_id="above-92000",
        question="Will Bitcoin close above 92000 on March 22?",
        token_ids=("u_yes",),
        active=True,
        accepting_orders=True,
        closed=False,
        yes_price_cents=30,
    )
    between = SlugMarket(
        slug="bitcoin-price-between-march-22",
        market_id="between-88000-92000",
        question="Will Bitcoin close between 88000 and 92000 on March 22?",
        token_ids=("r_yes",),
        active=True,
        accepting_orders=True,
        closed=False,
        yes_price_cents=20,
    )

    candidate = StructuralArbCandidate(
        slug_left=lower.slug,
        slug_right=between.slug,
        left_market=lower,
        right_market=between,
        similarity=1.0,
        match_mode="strict",
        lower_strike_market=lower,
        upper_strike_market=upper,
        range_market=between,
        strike_low=8_800_000,
        strike_high=9_200_000,
        boundary_relation="above",
    )

    planner = ExecutionPlanner(make_default_planner_config())

    def _locked_book(
        *,
        api_url: str,
        token_id: str,
        timeout_ms: int,
        cache: dict[str, tuple[int, int, int, int] | None],
    ) -> tuple[int, int, int, int]:
        _ = (api_url, token_id, timeout_ms, cache)
        return (60, 100, 60, 100)

    monkeypatch.setattr("examples.run_executor_from_env._fetch_public_top_of_book", _locked_book)

    observations, rejections = _collect_observed_opportunities(
        candidates=[candidate],
        planner=planner,
        poll_ts_ns=123_000,
        min_edge_bps=1,
        use_public_orderbook=True,
        public_book_api_url="https://example.invalid",
        public_book_timeout_ms=500,
    )

    assert observations == []
    assert len(rejections) == 1
    payload = rejections[0].payload
    assert payload["reason_code"] == "LOCKED_BOOK"
    assert payload["diagnostic_class"] == "INVALID_BOOK"
    assert payload["executable_edge_bps"] is None
    assert payload["leg_checks"]


def test_collect_rejects_price_protection_before_planner(monkeypatch: pytest.MonkeyPatch) -> None:
    low_price = SlugMarket(
        slug="bitcoin-above-on-march-22",
        market_id="above-80000",
        question="Will Bitcoin close above 80000 on March 22?",
        token_ids=("low_yes",),
        active=True,
        accepting_orders=True,
        closed=False,
        yes_price_cents=20,
        yes_token_id="low_yes",
    )
    high_price = SlugMarket(
        slug="bitcoin-above-on-march-22",
        market_id="above-90000",
        question="Will Bitcoin close above 90000 on March 22?",
        token_ids=("high_yes",),
        active=True,
        accepting_orders=True,
        closed=False,
        yes_price_cents=80,
        yes_token_id="high_yes",
    )

    candidate = StructuralArbCandidate(
        slug_left=low_price.slug,
        slug_right=high_price.slug,
        left_market=low_price,
        right_market=high_price,
        similarity=1.0,
        match_mode="legacy",
    )

    planner = ExecutionPlanner(make_default_planner_config())

    def _wide_buy_ask(
        *,
        api_url: str,
        token_id: str,
        timeout_ms: int,
        cache: dict[str, tuple[int, int, int, int] | None],
    ) -> tuple[int, int, int, int]:
        _ = (api_url, token_id, timeout_ms, cache)
        return (79, 100, 80, 100)

    monkeypatch.setattr("examples.run_executor_from_env._fetch_public_top_of_book", _wide_buy_ask)

    observations, rejections = _collect_observed_opportunities(
        candidates=[candidate],
        planner=planner,
        poll_ts_ns=456_000,
        min_edge_bps=1,
        use_public_orderbook=True,
        public_book_api_url="https://example.invalid",
        public_book_timeout_ms=500,
    )

    assert observations == []
    assert len(rejections) == 1
    payload = rejections[0].payload
    assert payload["reason_code"] == "PRICE_PROTECTION_FAIL"
    assert payload["diagnostic_class"] == "PRICE_PROTECTION"
    assert payload["executable_edge_bps"] is not None
    assert payload["leg_checks"]


def test_research_logger_tracks_lifecycle_and_duration(tmp_path) -> None:
    lower = SlugMarket(
        slug="bitcoin-price-on-march-22",
        market_id="above-88000",
        question="Will Bitcoin close above 88000 on March 22?",
        token_ids=("l_yes",),
        active=True,
        accepting_orders=True,
        closed=False,
        yes_price_cents=62,
    )
    upper = SlugMarket(
        slug="bitcoin-price-on-march-22",
        market_id="above-92000",
        question="Will Bitcoin close above 92000 on March 22?",
        token_ids=("u_yes",),
        active=True,
        accepting_orders=True,
        closed=False,
        yes_price_cents=30,
    )
    between = SlugMarket(
        slug="bitcoin-price-between-march-22",
        market_id="between-88000-92000",
        question="Will Bitcoin close between 88000 and 92000 on March 22?",
        token_ids=("r_yes",),
        active=True,
        accepting_orders=True,
        closed=False,
        yes_price_cents=20,
    )

    candidate = StructuralArbCandidate(
        slug_left=lower.slug,
        slug_right=between.slug,
        left_market=lower,
        right_market=between,
        similarity=1.0,
        match_mode="strict",
        lower_strike_market=lower,
        upper_strike_market=upper,
        range_market=between,
        strike_low=8_800_000,
        strike_high=9_200_000,
        boundary_relation="above",
    )

    converted = _strict_candidate_to_opportunity(candidate, now_ns=5_000)
    assert converted.success
    opportunity = converted.opportunity
    assert opportunity is not None
    snapshots = converted.snapshots

    logger = _ArbResearchLogger(
        jsonl_path=tmp_path / "events.jsonl",
        csv_path=tmp_path / "windows.csv",
    )

    base_payload = {
        "window_key": "window-1",
        "relation_id": "slugpair:a|b",
        "asset": "bitcoin",
        "slug_left": candidate.slug_left,
        "slug_right": candidate.slug_right,
        "boundary_relation": "above",
        "strike_low_x100": candidate.strike_low,
        "strike_high_x100": candidate.strike_high,
        "expected_edge_bps": 25,
        "max_fillable_profit_cents": 8,
        "max_fillable_units": 4,
        "price_source": "gamma_fallback",
    }

    opened, closed = logger.observe(
        [
            _ObservedOpportunity(
                window_key="window-1",
                relation_id="slugpair:a|b",
                observed_ts_ns=1_000_000_000,
                payload=dict(base_payload),
                opportunity=opportunity,
                snapshots=snapshots,
            )
        ],
        observed_ts_ns=1_000_000_000,
    )
    assert len(opened) == 1
    assert closed == 0

    updated_payload = dict(base_payload)
    updated_payload["max_fillable_profit_cents"] = 12
    updated_payload["expected_edge_bps"] = 31
    opened, closed = logger.observe(
        [
            _ObservedOpportunity(
                window_key="window-1",
                relation_id="slugpair:a|b",
                observed_ts_ns=1_600_000_000,
                payload=updated_payload,
                opportunity=opportunity,
                snapshots=snapshots,
            )
        ],
        observed_ts_ns=1_600_000_000,
    )
    assert opened == []
    assert closed == 0

    opened, closed = logger.observe([], observed_ts_ns=2_200_000_000)
    assert opened == []
    assert closed == 1

    logger.close()

    json_lines = (tmp_path / "events.jsonl").read_text(encoding="utf-8").splitlines()
    assert len(json_lines) == 3
    event_types = [json.loads(line)["event_type"] for line in json_lines]
    assert event_types == ["opened", "update", "closed"]

    csv_lines = (tmp_path / "windows.csv").read_text(encoding="utf-8").splitlines()
    assert len(csv_lines) == 2
    assert "peak_profit_cents" in csv_lines[0]
    assert ",12," in csv_lines[1]


def test_rejection_logger_writes_separate_jsonl(tmp_path) -> None:
    logger = _ArbRejectionLogger(jsonl_path=tmp_path / "rejections.jsonl")

    logger.write_batch(
        [
            _RejectedOpportunity(
                observed_ts_ns=1_000_000_000,
                relation_id="slugpair:a|b",
                window_key="slugpair:a|b|candidate:m1|m2",
                candidate_market_ids=("m1", "m2"),
                payload={
                    "asset": "bitcoin",
                    "rejection_code": "PRICE_PROTECTION",
                    "rejection_reason": "buy protection too tight",
                },
            )
        ]
    )
    logger.close()

    lines = (tmp_path / "rejections.jsonl").read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1

    record = json.loads(lines[0])
    assert record["event_type"] == "rejected"
    assert record["relation_id"] == "slugpair:a|b"
    assert record["rejection_code"] == "PRICE_PROTECTION"
    assert record["candidate_market_ids"] == ["m1", "m2"]
