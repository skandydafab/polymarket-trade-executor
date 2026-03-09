from __future__ import annotations

from dataclasses import replace

import pytest

from executor.planner import ExecutionPlanner, PlannerRejectCode
from executor.risk import ExecutionRiskManager, RiskReasonCode
from tests.helpers.factories import make_default_planner_config, make_default_risk_config, make_opportunity, make_snapshots


pytestmark = pytest.mark.failure_mode


def test_stale_data_is_rejected_conservatively(now_ns: int) -> None:
    planner = ExecutionPlanner(replace(make_default_planner_config(), max_snapshot_age_ms=25))
    opportunity = make_opportunity(now_ns=now_ns)
    snapshots = make_snapshots(now_ns=now_ns, snapshot_age_ms=100)

    decision = planner.plan(opportunity, snapshots, now_ns)

    assert decision.plan is None
    assert decision.rejection is not None
    assert decision.rejection.code == PlannerRejectCode.STALE_SNAPSHOT


def test_edge_erosion_causes_submission_abort(now_ns: int) -> None:
    planner = ExecutionPlanner(
        replace(
            make_default_planner_config(),
            min_expected_profit_cents=0,
            min_expected_edge_bps=60,
        )
    )
    opportunity = make_opportunity(now_ns=now_ns, expected_edge_bps=150)
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


def test_repeated_venue_ambiguity_triggers_auto_halt(now_ns: int) -> None:
    manager = ExecutionRiskManager(
        replace(
            make_default_risk_config(),
            repeated_venue_ambiguity_threshold=2,
            enable_auto_kill_switch=True,
        )
    )

    first = manager.record_venue_ambiguity(now_ns=now_ns, detail="submit timeout", package_id="pkg-a")
    second = manager.record_venue_ambiguity(now_ns=now_ns + 1, detail="cancel timeout", package_id="pkg-b")

    assert first is None
    assert second is not None
    assert second.code == RiskReasonCode.REPEATED_VENUE_AMBIGUITY_THRESHOLD
    assert second.should_halt_trading
    assert manager.is_trading_halted
