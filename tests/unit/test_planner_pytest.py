from __future__ import annotations

from dataclasses import replace

import pytest

from executor.planner import ExecutionPlanner, PlannerRejectCode
from tests.helpers.factories import make_default_planner_config, make_opportunity, make_snapshots


pytestmark = pytest.mark.unit


def test_stale_data_rejection(now_ns: int) -> None:
    config = replace(make_default_planner_config(), max_snapshot_age_ms=50)
    planner = ExecutionPlanner(config)
    opportunity = make_opportunity(now_ns=now_ns)
    snapshots = make_snapshots(now_ns=now_ns, snapshot_age_ms=120)

    decision = planner.plan(opportunity, snapshots, now_ns)

    assert decision.plan is None
    assert decision.rejection is not None
    assert decision.rejection.code == PlannerRejectCode.STALE_SNAPSHOT


def test_edge_erosion_between_detection_and_submission_rejected(now_ns: int) -> None:
    config = replace(
        make_default_planner_config(),
        min_expected_profit_cents=0,
        min_expected_edge_bps=40,
    )
    planner = ExecutionPlanner(config)
    opportunity = make_opportunity(
        now_ns=now_ns,
        expected_edge_bps=120,
        buy_reference_price_ticks=100,
        sell_reference_price_ticks=106,
    )
    snapshots = make_snapshots(
        now_ns=now_ns,
        buy_best_ask_ticks=101,
        sell_best_bid_ticks=101,
        sell_best_ask_ticks=102,
    )

    decision = planner.plan(opportunity, snapshots, now_ns)

    assert decision.plan is None
    assert decision.rejection is not None
    assert decision.rejection.code == PlannerRejectCode.EDGE_BELOW_THRESHOLD


def test_package_sizing_reduced_by_displayed_depth(now_ns: int) -> None:
    config = replace(make_default_planner_config(), scale_down_to_common_size=True)
    planner = ExecutionPlanner(config)
    opportunity = make_opportunity(
        now_ns=now_ns,
        target_package_units=20,
        min_package_units=5,
        max_package_units=30,
    )
    snapshots = make_snapshots(
        now_ns=now_ns,
        buy_best_ask_size=7,
        sell_best_bid_size=12,
    )

    decision = planner.plan(opportunity, snapshots, now_ns)

    assert decision.plan is not None
    plan = decision.plan
    assert plan.package_units == 7
    assert all(leg.quantity >= 7 for leg in plan.legs)
