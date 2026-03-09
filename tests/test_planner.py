from __future__ import annotations

import unittest
from dataclasses import replace

from executor.planner import (
    ExecutionPlanner,
    Opportunity,
    OpportunityLeg,
    PlannerConfig,
    PlannerRejectCode,
    PricingSnapshot,
)
from executor.state_machine import Side


class ExecutionPlannerTests(unittest.TestCase):
    def _base_config(self) -> PlannerConfig:
        return PlannerConfig(
            aggressiveness_bps=0,
            max_cross_bps_from_touch=200,
            default_max_slippage_bps=500,
            max_snapshot_age_ms=250,
            min_confidence=0.8,
            scale_down_to_common_size=True,
            min_expected_profit_cents=1,
            min_expected_edge_bps=1,
            additional_cost_bps=0,
            fixed_cost_cents=0,
            default_leg_timeout_ms=800,
            package_timeout_ms=1500,
            plan_ttl_ms=400,
            min_total_notional_cents=1,
            max_total_notional_cents=5_000_000,
            min_package_units=1,
            max_package_units=1000,
            max_shares_per_leg=100_000,
            max_total_shares=200_000,
        )

    def _base_opportunity(self, now_ns: int) -> Opportunity:
        return Opportunity(
            opportunity_id="opp-1",
            detected_ts_ns=now_ns - 1_000_000,
            expires_at_ns=now_ns + 5_000_000_000,
            expected_edge_bps=80,
            confidence=0.95,
            target_package_units=10,
            min_package_units=5,
            max_package_units=20,
            legs=(
                OpportunityLeg(
                    leg_id="leg-buy",
                    market_id="mkt-a",
                    token_id="token-a",
                    side=Side.BUY,
                    quantity_ratio=1,
                    reference_price_ticks=100,
                    fee_bps=0,
                ),
                OpportunityLeg(
                    leg_id="leg-sell",
                    market_id="mkt-b",
                    token_id="token-b",
                    side=Side.SELL,
                    quantity_ratio=1,
                    reference_price_ticks=106,
                    fee_bps=0,
                ),
            ),
        )

    def _base_snapshots(self, now_ns: int) -> dict[str, PricingSnapshot]:
        return {
            "mkt-a": PricingSnapshot(
                market_id="mkt-a",
                ts_ns=now_ns - 10_000_000,
                best_bid_ticks=99,
                best_bid_size=1000,
                best_ask_ticks=100,
                best_ask_size=1000,
                tick_size_ticks=1,
            ),
            "mkt-b": PricingSnapshot(
                market_id="mkt-b",
                ts_ns=now_ns - 10_000_000,
                best_bid_ticks=106,
                best_bid_size=1000,
                best_ask_ticks=107,
                best_ask_size=1000,
                tick_size_ticks=1,
            ),
        }

    def test_profitable_package_returns_plan(self) -> None:
        now_ns = 1_000_000_000_000
        planner = ExecutionPlanner(self._base_config())
        opportunity = self._base_opportunity(now_ns)
        snapshots = self._base_snapshots(now_ns)

        decision = planner.plan(opportunity, snapshots, now_ns)

        self.assertTrue(decision.accepted)
        self.assertIsNotNone(decision.plan)
        self.assertIsNone(decision.rejection)
        assert decision.plan is not None
        self.assertEqual(decision.plan.package_units, 10)
        self.assertGreater(decision.plan.expected_net_profit_cents, 0)
        self.assertEqual(len(decision.plan.legs), 2)

    def test_rejected_due_to_stale_leg(self) -> None:
        now_ns = 1_000_000_000_000
        config = self._base_config()
        planner = ExecutionPlanner(config)
        opportunity = self._base_opportunity(now_ns)
        snapshots = self._base_snapshots(now_ns)
        snapshots["mkt-b"] = PricingSnapshot(
            market_id="mkt-b",
            ts_ns=now_ns - (config.max_snapshot_age_ms + 1) * 1_000_000,
            best_bid_ticks=106,
            best_bid_size=1000,
            best_ask_ticks=107,
            best_ask_size=1000,
            tick_size_ticks=1,
        )

        decision = planner.plan(opportunity, snapshots, now_ns)

        self.assertFalse(decision.accepted)
        self.assertIsNotNone(decision.rejection)
        assert decision.rejection is not None
        self.assertEqual(decision.rejection.code, PlannerRejectCode.STALE_SNAPSHOT)
        self.assertEqual(decision.rejection.leg_id, "leg-sell")

    def test_rejected_due_to_insufficient_size_when_no_scaling(self) -> None:
        now_ns = 1_000_000_000_000
        config = replace(self._base_config(), scale_down_to_common_size=False)
        planner = ExecutionPlanner(config)
        opportunity = self._base_opportunity(now_ns)
        snapshots = self._base_snapshots(now_ns)
        snapshots["mkt-a"] = PricingSnapshot(
            market_id="mkt-a",
            ts_ns=now_ns - 10_000_000,
            best_bid_ticks=99,
            best_bid_size=1000,
            best_ask_ticks=100,
            best_ask_size=4,
            tick_size_ticks=1,
        )

        decision = planner.plan(opportunity, snapshots, now_ns)

        self.assertFalse(decision.accepted)
        self.assertIsNotNone(decision.rejection)
        assert decision.rejection is not None
        self.assertEqual(decision.rejection.code, PlannerRejectCode.INSUFFICIENT_SIZE)

    def test_rejected_due_to_net_edge_below_threshold(self) -> None:
        now_ns = 1_000_000_000_000
        config = replace(
            self._base_config(),
            min_expected_profit_cents=0,
            min_expected_edge_bps=50,
        )
        planner = ExecutionPlanner(config)
        opportunity = Opportunity(
            opportunity_id="opp-low-edge",
            detected_ts_ns=now_ns - 1_000_000,
            expires_at_ns=now_ns + 5_000_000_000,
            expected_edge_bps=3,
            confidence=0.95,
            target_package_units=10,
            min_package_units=5,
            max_package_units=20,
            legs=(
                OpportunityLeg(
                    leg_id="leg-buy",
                    market_id="mkt-a",
                    token_id="token-a",
                    side=Side.BUY,
                    quantity_ratio=1,
                    reference_price_ticks=100,
                ),
                OpportunityLeg(
                    leg_id="leg-sell",
                    market_id="mkt-b",
                    token_id="token-b",
                    side=Side.SELL,
                    quantity_ratio=1,
                    reference_price_ticks=100,
                ),
            ),
        )
        snapshots = {
            "mkt-a": PricingSnapshot(
                market_id="mkt-a",
                ts_ns=now_ns - 10_000_000,
                best_bid_ticks=99,
                best_bid_size=1000,
                best_ask_ticks=100,
                best_ask_size=1000,
                tick_size_ticks=1,
            ),
            "mkt-b": PricingSnapshot(
                market_id="mkt-b",
                ts_ns=now_ns - 10_000_000,
                best_bid_ticks=100,
                best_bid_size=1000,
                best_ask_ticks=101,
                best_ask_size=1000,
                tick_size_ticks=1,
            ),
        }

        decision = planner.plan(opportunity, snapshots, now_ns)

        self.assertFalse(decision.accepted)
        self.assertIsNotNone(decision.rejection)
        assert decision.rejection is not None
        self.assertEqual(decision.rejection.code, PlannerRejectCode.EDGE_BELOW_THRESHOLD)

    def test_price_rounding_behavior(self) -> None:
        now_ns = 1_000_000_000_000
        config = replace(
            self._base_config(),
            aggressiveness_bps=150,
            max_cross_bps_from_touch=500,
            default_max_slippage_bps=5_000,
            min_expected_profit_cents=0,
            min_expected_edge_bps=0,
        )
        planner = ExecutionPlanner(config)
        opportunity = Opportunity(
            opportunity_id="opp-round",
            detected_ts_ns=now_ns - 1_000_000,
            expires_at_ns=now_ns + 5_000_000_000,
            expected_edge_bps=1,
            confidence=0.95,
            target_package_units=1,
            min_package_units=1,
            max_package_units=1,
            legs=(
                OpportunityLeg(
                    leg_id="buy-leg",
                    market_id="mkt-buy",
                    token_id="token-buy",
                    side=Side.BUY,
                    quantity_ratio=1,
                    reference_price_ticks=150,
                ),
                OpportunityLeg(
                    leg_id="sell-leg",
                    market_id="mkt-sell",
                    token_id="token-sell",
                    side=Side.SELL,
                    quantity_ratio=1,
                    reference_price_ticks=80,
                ),
            ),
        )
        snapshots = {
            "mkt-buy": PricingSnapshot(
                market_id="mkt-buy",
                ts_ns=now_ns - 1_000_000,
                best_bid_ticks=100,
                best_bid_size=100,
                best_ask_ticks=101,
                best_ask_size=100,
                tick_size_ticks=5,
            ),
            "mkt-sell": PricingSnapshot(
                market_id="mkt-sell",
                ts_ns=now_ns - 1_000_000,
                best_bid_ticks=109,
                best_bid_size=100,
                best_ask_ticks=110,
                best_ask_size=100,
                tick_size_ticks=5,
            ),
        }

        decision = planner.plan(opportunity, snapshots, now_ns)

        self.assertTrue(decision.accepted)
        assert decision.plan is not None
        by_leg = {leg.leg_id: leg for leg in decision.plan.legs}
        self.assertEqual(by_leg["buy-leg"].limit_price_ticks, 105)
        self.assertEqual(by_leg["sell-leg"].limit_price_ticks, 105)


if __name__ == "__main__":
    unittest.main()