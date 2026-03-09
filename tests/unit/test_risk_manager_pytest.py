from __future__ import annotations

from dataclasses import replace

import pytest

from executor.risk import ExecutionRiskManager, RiskReasonCode
from executor.state_machine import PackageState, VenueFillEvent
from tests.helpers.factories import (
    make_default_risk_config,
    make_package_execution,
    make_plan,
    make_pretrade_request,
)


pytestmark = pytest.mark.unit


def test_second_leg_timeout_protection(now_ns: int) -> None:
    config = replace(
        make_default_risk_config(),
        second_leg_timeout_ms=100,
        max_partial_fill_exposure_cents=1_000_000,
    )
    manager = ExecutionRiskManager(config)
    plan = make_plan(now_ns=now_ns)
    register = manager.register_package(
        make_pretrade_request(package_id="pkg-risk-1", relation_id="rel-1", plan=plan),
        now_ns,
    )

    package = make_package_execution(
        package_id="pkg-risk-1",
        opportunity_id=plan.opportunity_id,
        now_ns=now_ns,
    )
    package = replace(package, state=PackageState.EXECUTING, updated_ts_ns=now_ns)

    _ = manager.on_state_event(
        package,
        VenueFillEvent(
            package_id="pkg-risk-1",
            leg_id="leg-buy",
            client_order_id="cid-buy",
            fill_qty=10,
            fill_price_ticks=100,
            cumulative_qty=10,
            ts_ns=now_ns + 1,
        ),
        now_ns + 1,
    )
    decisions = manager.evaluate_active_risk(now_ns + 200 * 1_000_000)

    assert register.allowed
    assert any(item.code == RiskReasonCode.SECOND_LEG_TIMEOUT for item in decisions)


def test_manual_kill_switch_blocks_new_trades(now_ns: int) -> None:
    manager = ExecutionRiskManager(make_default_risk_config())
    _ = manager.activate_manual_kill_switch("operator halt", now_ns)

    plan = make_plan(now_ns=now_ns)
    decision = manager.validate_pre_trade(
        make_pretrade_request(package_id="pkg-risk-2", relation_id="rel-2", plan=plan),
        now_ns + 1,
    )

    assert manager.is_trading_halted
    assert not decision.allowed
    assert decision.code == RiskReasonCode.MANUAL_KILL_SWITCH


def test_cooldown_after_failed_package(now_ns: int) -> None:
    config = replace(make_default_risk_config(), relation_cooldown_ms=1_000, market_cooldown_ms=1_000)
    manager = ExecutionRiskManager(config)

    plan_a = make_plan(now_ns=now_ns)
    register = manager.register_package(
        make_pretrade_request(package_id="pkg-risk-3a", relation_id="rel-cool", plan=plan_a),
        now_ns,
    )
    _ = manager.close_package(package_id="pkg-risk-3a", now_ns=now_ns + 1, success=False, realized_pnl_cents=-10)

    blocked_ts = now_ns + 500 * 1_000_000
    blocked_plan = make_plan(now_ns=blocked_ts)
    blocked = manager.validate_pre_trade(
        make_pretrade_request(package_id="pkg-risk-3b", relation_id="rel-cool", plan=blocked_plan),
        blocked_ts,
    )

    allowed_ts = now_ns + 1_100 * 1_000_000
    allowed_plan = make_plan(now_ns=allowed_ts)
    allowed = manager.validate_pre_trade(
        make_pretrade_request(package_id="pkg-risk-3c", relation_id="rel-cool", plan=allowed_plan),
        allowed_ts,
    )

    assert register.allowed
    assert blocked.code in {RiskReasonCode.RELATION_COOLDOWN_ACTIVE, RiskReasonCode.MARKET_COOLDOWN_ACTIVE}
    assert allowed.allowed
