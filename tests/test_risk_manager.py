from __future__ import annotations

from dataclasses import replace
import unittest

from executor.planner import ExecutionPlan, PlannedLeg
from executor.risk import (
    ExecutionRiskManager,
    PreTradeRiskRequest,
    RiskManagerConfig,
    RiskReasonCode,
)
from executor.state_machine import (
    LegExecution,
    OrderState,
    PackageState,
    Side,
    TimeInForce,
    VenueFillEvent,
    VenueRejectEvent,
    create_package_execution,
)


class ExecutionRiskManagerTests(unittest.TestCase):
    def _plan(
        self,
        *,
        now_ns: int,
        buy_price: int = 100,
        sell_price: int = 105,
        qty: int = 10,
        snapshot_age_ms: int = 5,
        available_units: int = 100,
        opportunity_id: str = "opp-1",
    ) -> ExecutionPlan:
        snapshot_ts = now_ns - snapshot_age_ms * 1_000_000
        legs = (
            PlannedLeg(
                leg_id="leg-buy",
                market_id="mkt-a",
                token_id="token-a",
                side=Side.BUY,
                submission_rank=1,
                quantity=qty,
                executable_price_ticks=buy_price,
                limit_price_ticks=buy_price,
                tick_size_ticks=1,
                timeout_ms=500,
                tif=TimeInForce.IOC,
                snapshot_ts_ns=snapshot_ts,
                available_units_at_plan=available_units,
            ),
            PlannedLeg(
                leg_id="leg-sell",
                market_id="mkt-b",
                token_id="token-b",
                side=Side.SELL,
                submission_rank=2,
                quantity=qty,
                executable_price_ticks=sell_price,
                limit_price_ticks=sell_price,
                tick_size_ticks=1,
                timeout_ms=500,
                tif=TimeInForce.IOC,
                snapshot_ts_ns=snapshot_ts,
                available_units_at_plan=available_units,
            ),
        )
        total_notional = qty * buy_price + qty * sell_price
        expected_net_profit = qty * (sell_price - buy_price)
        edge_bps = (expected_net_profit * 10_000) // total_notional
        return ExecutionPlan(
            plan_id=f"plan-{opportunity_id}",
            opportunity_id=opportunity_id,
            created_ts_ns=now_ns,
            expires_at_ns=now_ns + 5_000_000_000,
            package_timeout_ms=1_500,
            package_units=qty,
            tif=TimeInForce.IOC,
            total_shares=qty * 2,
            total_notional_cents=total_notional,
            expected_gross_profit_cents=expected_net_profit,
            expected_fee_cents=0,
            expected_net_profit_cents=expected_net_profit,
            expected_net_edge_bps=edge_bps,
            legs=legs,
        )

    def _request(self, package_id: str, relation_id: str, plan: ExecutionPlan) -> PreTradeRiskRequest:
        return PreTradeRiskRequest(package_id=package_id, relation_id=relation_id, plan=plan)

    def _package_execution(self, *, package_id: str, opportunity_id: str, now_ns: int, state: PackageState) -> object:
        package = create_package_execution(
            package_id=package_id,
            opportunity_id=opportunity_id,
            created_ts_ns=now_ns,
            legs=(
                LegExecution(
                    leg_id="leg-buy",
                    market_id="mkt-a",
                    side=Side.BUY,
                    intended_qty=10,
                    limit_price_ticks=100,
                ),
                LegExecution(
                    leg_id="leg-sell",
                    market_id="mkt-b",
                    side=Side.SELL,
                    intended_qty=10,
                    limit_price_ticks=105,
                ),
            ),
        )
        return replace(package, state=state, updated_ts_ns=now_ns)

    def test_pretrade_rejects_max_notional_per_package(self) -> None:
        now_ns = 1_000_000_000_000
        config = RiskManagerConfig(max_notional_per_package_cents=1_000)
        manager = ExecutionRiskManager(config)
        plan = self._plan(now_ns=now_ns, buy_price=100, sell_price=105, qty=10)

        decision = manager.validate_pre_trade(self._request("pkg-1", "rel-1", plan), now_ns)

        self.assertFalse(decision.allowed)
        self.assertEqual(decision.code, RiskReasonCode.MAX_NOTIONAL_PER_PACKAGE)

    def test_stale_leg_reject_and_repeated_stale_auto_halt(self) -> None:
        now_ns = 1_000_000_000_000
        config = RiskManagerConfig(
            max_leg_snapshot_age_ms=50,
            repeated_stale_data_threshold=2,
        )
        manager = ExecutionRiskManager(config)
        stale_plan = self._plan(now_ns=now_ns, snapshot_age_ms=100)

        first = manager.validate_pre_trade(self._request("pkg-1", "rel-1", stale_plan), now_ns)
        second = manager.validate_pre_trade(self._request("pkg-2", "rel-2", stale_plan), now_ns + 1)

        self.assertEqual(first.code, RiskReasonCode.STALE_LEG_DATA)
        self.assertEqual(second.code, RiskReasonCode.REPEATED_STALE_DATA_THRESHOLD)
        self.assertTrue(manager.is_trading_halted)

    def test_conflicting_relation_blocked_when_active(self) -> None:
        now_ns = 1_000_000_000_000
        manager = ExecutionRiskManager(RiskManagerConfig())
        plan_a = self._plan(now_ns=now_ns, opportunity_id="opp-a")
        plan_b = self._plan(now_ns=now_ns + 1, opportunity_id="opp-b")

        first = manager.register_package(self._request("pkg-1", "rel-shared", plan_a), now_ns)
        second = manager.validate_pre_trade(
            self._request("pkg-2", "rel-shared", plan_b),
            now_ns + 1,
        )

        self.assertTrue(first.allowed)
        self.assertEqual(second.code, RiskReasonCode.CONFLICTING_ACTIVE_PACKAGE)

    def test_relation_cooldown_after_failed_package(self) -> None:
        now_ns = 1_000_000_000_000
        config = RiskManagerConfig(relation_cooldown_ms=1_000, market_cooldown_ms=1_000)
        manager = ExecutionRiskManager(config)
        plan_a = self._plan(now_ns=now_ns, opportunity_id="opp-a")
        blocked_check_ts = now_ns + 500 * 1_000_000
        allowed_check_ts = now_ns + 1_100 * 1_000_000
        plan_b = self._plan(now_ns=blocked_check_ts, opportunity_id="opp-b")
        plan_c = self._plan(now_ns=allowed_check_ts, opportunity_id="opp-c")

        register = manager.register_package(self._request("pkg-1", "rel-cool", plan_a), now_ns)
        _ = manager.close_package(package_id="pkg-1", now_ns=now_ns + 10, success=False)

        blocked = manager.validate_pre_trade(
            self._request("pkg-2", "rel-cool", plan_b),
            blocked_check_ts,
        )
        allowed = manager.validate_pre_trade(
            self._request("pkg-3", "rel-cool", plan_c),
            allowed_check_ts,
        )

        self.assertTrue(register.allowed)
        self.assertEqual(blocked.code, RiskReasonCode.RELATION_COOLDOWN_ACTIVE)
        self.assertTrue(allowed.allowed)

    def test_partial_fill_exposure_and_second_leg_timeout(self) -> None:
        now_ns = 1_000_000_000_000
        config = RiskManagerConfig(
            max_partial_fill_exposure_cents=500,
            second_leg_timeout_ms=100,
        )
        manager = ExecutionRiskManager(config)
        plan = self._plan(now_ns=now_ns)
        req = self._request("pkg-1", "rel-1", plan)
        manager.register_package(req, now_ns)

        package = self._package_execution(
            package_id="pkg-1",
            opportunity_id=plan.opportunity_id,
            now_ns=now_ns,
            state=PackageState.EXECUTING,
        )
        decisions = manager.on_state_event(
            package,
            VenueFillEvent(
                package_id="pkg-1",
                leg_id="leg-buy",
                client_order_id="cid-1",
                fill_qty=10,
                fill_price_ticks=100,
                ts_ns=now_ns,
                cumulative_qty=10,
            ),
            now_ns,
        )
        timeout_decisions = manager.evaluate_active_risk(now_ns + 200 * 1_000_000)

        self.assertTrue(any(d.code == RiskReasonCode.MAX_PARTIAL_FILL_EXPOSURE for d in decisions))
        self.assertTrue(any(d.code == RiskReasonCode.SECOND_LEG_TIMEOUT for d in timeout_decisions))

    def test_daily_loss_cap_triggers_auto_kill(self) -> None:
        now_ns = 1_000_000_000_000
        config = RiskManagerConfig(daily_loss_cap_cents=1_000)
        manager = ExecutionRiskManager(config)
        plan = self._plan(now_ns=now_ns)

        manager.register_package(self._request("pkg-1", "rel-1", plan), now_ns)
        decisions = manager.close_package(
            package_id="pkg-1",
            now_ns=now_ns + 1,
            success=False,
            realized_pnl_cents=-1_500,
        )

        self.assertTrue(any(d.code == RiskReasonCode.DAILY_LOSS_CAP for d in decisions))
        self.assertTrue(manager.is_trading_halted)

    def test_repeated_reject_threshold_triggers_auto_kill(self) -> None:
        now_ns = 1_000_000_000_000
        config = RiskManagerConfig(repeated_reject_threshold=2)
        manager = ExecutionRiskManager(config)
        plan = self._plan(now_ns=now_ns)
        manager.register_package(self._request("pkg-1", "rel-1", plan), now_ns)

        package = self._package_execution(
            package_id="pkg-1",
            opportunity_id=plan.opportunity_id,
            now_ns=now_ns,
            state=PackageState.EXECUTING,
        )

        manager.on_state_event(
            package,
            VenueRejectEvent(
                package_id="pkg-1",
                leg_id="leg-buy",
                client_order_id="cid-1",
                reason="reject-1",
                ts_ns=now_ns,
            ),
            now_ns,
        )
        second = manager.on_state_event(
            package,
            VenueRejectEvent(
                package_id="pkg-1",
                leg_id="leg-sell",
                client_order_id="cid-2",
                reason="reject-2",
                ts_ns=now_ns + 1,
            ),
            now_ns + 1,
        )

        self.assertTrue(any(d.code == RiskReasonCode.REPEATED_REJECT_THRESHOLD for d in second))
        self.assertTrue(manager.is_trading_halted)

    def test_rebuild_from_recovered_state_rehydrates_exposure_and_relation_guards(self) -> None:
        now_ns = 1_000_000_000_000
        manager = ExecutionRiskManager(RiskManagerConfig())

        package = create_package_execution(
            package_id="pkg-recovered",
            opportunity_id="opp-recovered",
            created_ts_ns=now_ns - 50_000_000,
            legs=(
                LegExecution(
                    leg_id="leg-buy",
                    market_id="mkt-a",
                    side=Side.BUY,
                    intended_qty=10,
                    limit_price_ticks=100,
                ),
                LegExecution(
                    leg_id="leg-sell",
                    market_id="mkt-b",
                    side=Side.SELL,
                    intended_qty=10,
                    limit_price_ticks=105,
                ),
            ),
        )
        package = replace(
            package,
            state=PackageState.EXECUTING,
            updated_ts_ns=now_ns - 10_000_000,
            legs=(
                LegExecution(
                    leg_id="leg-buy",
                    market_id="mkt-a",
                    side=Side.BUY,
                    intended_qty=10,
                    limit_price_ticks=100,
                    state=OrderState.PARTIALLY_FILLED,
                    client_order_id="cid-buy",
                    filled_qty=4,
                    avg_fill_price_ticks=100,
                    submit_ts_ns=now_ns - 45_000_000,
                    ack_ts_ns=now_ns - 44_000_000,
                ),
                LegExecution(
                    leg_id="leg-sell",
                    market_id="mkt-b",
                    side=Side.SELL,
                    intended_qty=10,
                    limit_price_ticks=105,
                    state=OrderState.WORKING,
                    client_order_id="cid-sell",
                    submit_ts_ns=now_ns - 45_000_000,
                    ack_ts_ns=now_ns - 44_000_000,
                ),
            ),
        )

        decisions = manager.rebuild_from_recovered_state(
            active_packages={"pkg-recovered": package},
            relation_by_package={"pkg-recovered": "rel-recovered"},
            now_ns=now_ns,
        )

        self.assertEqual(decisions, tuple())
        snapshot = manager.exposure.snapshot()
        self.assertEqual(snapshot.active_package_count, 1)
        self.assertEqual(snapshot.total_open_notional_cents, 2_050)
        self.assertEqual(snapshot.open_directional_imbalance_by_market.get("mkt-a"), 4)

        candidate_plan = self._plan(now_ns=now_ns + 1, opportunity_id="opp-next")
        blocked = manager.validate_pre_trade(
            self._request("pkg-next", "rel-recovered", candidate_plan),
            now_ns + 1,
        )
        self.assertEqual(blocked.code, RiskReasonCode.CONFLICTING_ACTIVE_PACKAGE)


if __name__ == "__main__":
    unittest.main()