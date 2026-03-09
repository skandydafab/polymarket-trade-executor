from __future__ import annotations

from dataclasses import replace

from executor.planner import (
    ExecutionPlan,
    ExecutionPlanner,
    Opportunity,
    OpportunityLeg,
    PlannedLeg,
    PlannerConfig,
    PricingSnapshot,
)
from executor.risk import PreTradeRiskRequest, RiskManagerConfig
from executor.state_machine import LegExecution, PackageExecution, Side, TimeInForce, create_package_execution


def make_default_planner_config() -> PlannerConfig:
    return PlannerConfig(
        aggressiveness_bps=0,
        max_cross_bps_from_touch=200,
        default_max_slippage_bps=2_000,
        max_snapshot_age_ms=200,
        min_confidence=0.80,
        scale_down_to_common_size=True,
        min_expected_profit_cents=1,
        min_expected_edge_bps=1,
        additional_cost_bps=0,
        fixed_cost_cents=0,
        default_leg_timeout_ms=800,
        package_timeout_ms=1_500,
        plan_ttl_ms=400,
        min_total_notional_cents=1,
        max_total_notional_cents=10_000_000,
        min_package_units=1,
        max_package_units=50_000,
        max_shares_per_leg=100_000,
        max_total_shares=200_000,
        tif=TimeInForce.IOC,
    )


def make_default_risk_config() -> RiskManagerConfig:
    return RiskManagerConfig(
        max_notional_per_package_cents=5_000_000,
        max_notional_per_market_cents=7_000_000,
        max_total_open_notional_cents=15_000_000,
        max_simultaneous_active_packages=100,
        min_expected_net_profit_cents=1,
        min_displayed_executable_size_shares=1,
        max_leg_snapshot_age_ms=500,
        relation_cooldown_ms=1_000,
        market_cooldown_ms=1_000,
        second_leg_timeout_ms=200,
        max_partial_fill_exposure_cents=50_000,
        max_open_directional_imbalance_shares=100_000,
        package_execution_deadline_ms=5_000,
        failure_rate_window_ms=60_000,
        failure_rate_halt_threshold=0.70,
        failure_rate_min_sample=3,
        enable_auto_kill_switch=True,
        daily_loss_cap_cents=200_000,
        repeated_reject_threshold=5,
        repeated_venue_ambiguity_threshold=3,
        repeated_stale_data_threshold=3,
        repeated_event_window_ms=60_000,
    )


def make_opportunity(
    *,
    now_ns: int,
    opportunity_id: str = "opp-1",
    confidence: float = 0.95,
    target_package_units: int = 10,
    min_package_units: int = 5,
    max_package_units: int = 20,
    buy_market_id: str = "mkt-a",
    sell_market_id: str = "mkt-b",
    buy_token_id: str = "token-a",
    sell_token_id: str = "token-b",
    buy_reference_price_ticks: int = 100,
    sell_reference_price_ticks: int = 106,
    buy_quantity_ratio: int = 1,
    sell_quantity_ratio: int = 1,
    expected_edge_bps: int = 50,
) -> Opportunity:
    return Opportunity(
        opportunity_id=opportunity_id,
        detected_ts_ns=now_ns - 1_000_000,
        expires_at_ns=now_ns + 5_000_000_000,
        expected_edge_bps=expected_edge_bps,
        confidence=confidence,
        target_package_units=target_package_units,
        min_package_units=min_package_units,
        max_package_units=max_package_units,
        legs=(
            OpportunityLeg(
                leg_id="leg-buy",
                market_id=buy_market_id,
                token_id=buy_token_id,
                side=Side.BUY,
                quantity_ratio=buy_quantity_ratio,
                reference_price_ticks=buy_reference_price_ticks,
            ),
            OpportunityLeg(
                leg_id="leg-sell",
                market_id=sell_market_id,
                token_id=sell_token_id,
                side=Side.SELL,
                quantity_ratio=sell_quantity_ratio,
                reference_price_ticks=sell_reference_price_ticks,
            ),
        ),
    )


def make_snapshots(
    *,
    now_ns: int,
    buy_market_id: str = "mkt-a",
    sell_market_id: str = "mkt-b",
    buy_best_ask_ticks: int = 100,
    buy_best_ask_size: int = 1_000,
    buy_best_bid_ticks: int = 99,
    buy_best_bid_size: int = 1_000,
    sell_best_bid_ticks: int = 106,
    sell_best_bid_size: int = 1_000,
    sell_best_ask_ticks: int = 107,
    sell_best_ask_size: int = 1_000,
    snapshot_age_ms: int = 10,
    tick_size_ticks: int = 1,
) -> dict[str, PricingSnapshot]:
    ts_ns = now_ns - snapshot_age_ms * 1_000_000
    return {
        buy_market_id: PricingSnapshot(
            market_id=buy_market_id,
            ts_ns=ts_ns,
            best_bid_ticks=buy_best_bid_ticks,
            best_bid_size=buy_best_bid_size,
            best_ask_ticks=buy_best_ask_ticks,
            best_ask_size=buy_best_ask_size,
            tick_size_ticks=tick_size_ticks,
        ),
        sell_market_id: PricingSnapshot(
            market_id=sell_market_id,
            ts_ns=ts_ns,
            best_bid_ticks=sell_best_bid_ticks,
            best_bid_size=sell_best_bid_size,
            best_ask_ticks=sell_best_ask_ticks,
            best_ask_size=sell_best_ask_size,
            tick_size_ticks=tick_size_ticks,
        ),
    }


def make_plan(
    *,
    now_ns: int,
    planner: ExecutionPlanner | None = None,
    opportunity: Opportunity | None = None,
    snapshots: dict[str, PricingSnapshot] | None = None,
) -> ExecutionPlan:
    active_planner = planner or ExecutionPlanner(make_default_planner_config())
    active_opportunity = opportunity or make_opportunity(now_ns=now_ns)
    active_snapshots = snapshots or make_snapshots(now_ns=now_ns)
    decision = active_planner.plan(active_opportunity, active_snapshots, now_ns)
    if decision.plan is None:
        raise AssertionError(f"expected plan but got rejection: {decision.rejection}")
    return decision.plan


def make_pretrade_request(
    *,
    package_id: str,
    relation_id: str,
    plan: ExecutionPlan,
) -> PreTradeRiskRequest:
    return PreTradeRiskRequest(package_id=package_id, relation_id=relation_id, plan=plan)


def make_package_execution(
    *,
    package_id: str,
    opportunity_id: str,
    now_ns: int,
    quantity: int = 10,
    buy_market_id: str = "mkt-a",
    sell_market_id: str = "mkt-b",
    buy_limit: int = 100,
    sell_limit: int = 106,
) -> PackageExecution:
    return create_package_execution(
        package_id=package_id,
        opportunity_id=opportunity_id,
        created_ts_ns=now_ns,
        legs=(
            LegExecution(
                leg_id="leg-buy",
                market_id=buy_market_id,
                side=Side.BUY,
                intended_qty=quantity,
                limit_price_ticks=buy_limit,
            ),
            LegExecution(
                leg_id="leg-sell",
                market_id=sell_market_id,
                side=Side.SELL,
                intended_qty=quantity,
                limit_price_ticks=sell_limit,
            ),
        ),
    )


def with_plan_edge_erosion(plan: ExecutionPlan, *, sell_limit_ticks: int) -> ExecutionPlan:
    updated_legs: list[PlannedLeg] = []
    for leg in plan.legs:
        if leg.side == Side.SELL:
            updated_legs.append(
                replace(
                    leg,
                    executable_price_ticks=sell_limit_ticks,
                    limit_price_ticks=sell_limit_ticks,
                )
            )
        else:
            updated_legs.append(leg)

    total_notional = sum(item.quantity * item.limit_price_ticks for item in updated_legs)
    gross_profit = 0
    for item in updated_legs:
        leg_notional = item.quantity * item.limit_price_ticks
        gross_profit += leg_notional if item.side == Side.SELL else -leg_notional
    net_edge_bps = (gross_profit * 10_000) // total_notional if total_notional > 0 else 0

    return replace(
        plan,
        total_notional_cents=total_notional,
        expected_gross_profit_cents=gross_profit,
        expected_net_profit_cents=gross_profit,
        expected_net_edge_bps=net_edge_bps,
        legs=tuple(updated_legs),
    )
