from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Mapping

from .state_machine import Side, TimeInForce


_BPS_SCALE = 10_000
_NS_PER_MS = 1_000_000


class PlannerRejectCode(str, Enum):
    INVALID_OPPORTUNITY = "INVALID_OPPORTUNITY"
    OPPORTUNITY_EXPIRED = "OPPORTUNITY_EXPIRED"
    LOW_CONFIDENCE = "LOW_CONFIDENCE"
    MISSING_SNAPSHOT = "MISSING_SNAPSHOT"
    STALE_SNAPSHOT = "STALE_SNAPSHOT"
    UNCERTAIN_SNAPSHOT = "UNCERTAIN_SNAPSHOT"
    INVALID_BOOK = "INVALID_BOOK"
    PRICE_PROTECTION = "PRICE_PROTECTION"
    INSUFFICIENT_SIZE = "INSUFFICIENT_SIZE"
    SHARE_CAP_EXCEEDED = "SHARE_CAP_EXCEEDED"
    NOTIONAL_OUT_OF_BOUNDS = "NOTIONAL_OUT_OF_BOUNDS"
    PROFIT_BELOW_THRESHOLD = "PROFIT_BELOW_THRESHOLD"
    EDGE_BELOW_THRESHOLD = "EDGE_BELOW_THRESHOLD"


@dataclass(frozen=True, slots=True)
class PlannerRejection:
    code: PlannerRejectCode
    reason: str
    ts_ns: int
    leg_id: str | None = None


@dataclass(frozen=True, slots=True)
class OpportunityLeg:
    leg_id: str
    market_id: str
    token_id: str
    side: Side
    quantity_ratio: int
    reference_price_ticks: int
    max_slippage_bps: int | None = None
    fee_bps: int = 0
    order_timeout_ms: int | None = None


@dataclass(frozen=True, slots=True)
class Opportunity:
    opportunity_id: str
    detected_ts_ns: int
    expires_at_ns: int
    expected_edge_bps: int
    confidence: float
    target_package_units: int
    min_package_units: int
    max_package_units: int
    legs: tuple[OpportunityLeg, ...]


@dataclass(frozen=True, slots=True)
class PricingSnapshot:
    market_id: str
    ts_ns: int
    best_bid_ticks: int
    best_bid_size: int
    best_ask_ticks: int
    best_ask_size: int
    tick_size_ticks: int = 1
    is_uncertain: bool = False


@dataclass(frozen=True, slots=True)
class PlannedLeg:
    leg_id: str
    market_id: str
    token_id: str
    side: Side
    submission_rank: int
    quantity: int
    executable_price_ticks: int
    limit_price_ticks: int
    tick_size_ticks: int
    timeout_ms: int
    tif: TimeInForce
    snapshot_ts_ns: int
    available_units_at_plan: int


@dataclass(frozen=True, slots=True)
class ExecutionPlan:
    plan_id: str
    opportunity_id: str
    created_ts_ns: int
    expires_at_ns: int
    package_timeout_ms: int
    package_units: int
    tif: TimeInForce
    total_shares: int
    total_notional_cents: int
    expected_gross_profit_cents: int
    expected_fee_cents: int
    expected_net_profit_cents: int
    expected_net_edge_bps: int
    legs: tuple[PlannedLeg, ...]


@dataclass(frozen=True, slots=True)
class PlannerConfig:
    aggressiveness_bps: int = 20
    max_cross_bps_from_touch: int = 50
    default_max_slippage_bps: int = 80
    max_snapshot_age_ms: int = 200
    min_confidence: float = 0.9
    scale_down_to_common_size: bool = True
    min_expected_profit_cents: int = 1
    min_expected_edge_bps: int = 1
    additional_cost_bps: int = 0
    fixed_cost_cents: int = 0
    default_leg_timeout_ms: int = 800
    package_timeout_ms: int = 1200
    plan_ttl_ms: int = 400
    min_total_notional_cents: int = 1
    max_total_notional_cents: int = 2_000_000
    min_package_units: int = 1
    max_package_units: int = 10_000
    max_shares_per_leg: int = 10_000
    max_total_shares: int = 30_000
    tif: TimeInForce = TimeInForce.IOC
    reject_crossed_book: bool = True


@dataclass(frozen=True, slots=True)
class PlannerDecision:
    plan: ExecutionPlan | None
    rejection: PlannerRejection | None

    @property
    def accepted(self) -> bool:
        return self.plan is not None


@dataclass(frozen=True, slots=True)
class _LegPlanInputs:
    leg: OpportunityLeg
    snapshot: PricingSnapshot
    executable_price_ticks: int
    limit_price_ticks: int
    available_units: int


class ExecutionPlanner:
    """Deterministic hot-path planner for structural package execution.

    Contract:
    - input: validated detector opportunity + latest executable book view
    - output: execution plan or explicit rejection reason
    - no side effects
    """

    __slots__ = ("_config",)

    def __init__(self, config: PlannerConfig) -> None:
        self._validate_config(config)
        self._config = config

    @property
    def config(self) -> PlannerConfig:
        return self._config

    def plan(
        self,
        opportunity: Opportunity,
        snapshots: Mapping[str, PricingSnapshot],
        now_ns: int,
    ) -> PlannerDecision:
        invalid = self._validate_opportunity(opportunity, now_ns)
        if invalid is not None:
            return invalid

        leg_inputs: list[_LegPlanInputs] = []
        for leg in opportunity.legs:
            snapshot = snapshots.get(leg.market_id)
            if snapshot is None:
                return self._reject(
                    PlannerRejectCode.MISSING_SNAPSHOT,
                    f"missing snapshot for market_id={leg.market_id}",
                    now_ns,
                    leg.leg_id,
                )

            snapshot_reject = self._validate_snapshot(leg, snapshot, now_ns)
            if snapshot_reject is not None:
                return snapshot_reject

            executable_price = snapshot.best_ask_ticks if leg.side == Side.BUY else snapshot.best_bid_ticks
            displayed_size = snapshot.best_ask_size if leg.side == Side.BUY else snapshot.best_bid_size
            available_units = displayed_size // leg.quantity_ratio

            if available_units <= 0:
                return self._reject(
                    PlannerRejectCode.INSUFFICIENT_SIZE,
                    (
                        f"displayed size too small for leg_id={leg.leg_id}, "
                        f"displayed={displayed_size}, ratio={leg.quantity_ratio}"
                    ),
                    now_ns,
                    leg.leg_id,
                )

            price_or_reject = self._compute_marketable_limit(leg, snapshot, executable_price, now_ns)
            if isinstance(price_or_reject, PlannerDecision):
                return price_or_reject

            leg_inputs.append(
                _LegPlanInputs(
                    leg=leg,
                    snapshot=snapshot,
                    executable_price_ticks=executable_price,
                    limit_price_ticks=price_or_reject,
                    available_units=available_units,
                )
            )

        units_or_reject = self._choose_package_units(opportunity, leg_inputs, now_ns)
        if isinstance(units_or_reject, PlannerDecision):
            return units_or_reject
        package_units = units_or_reject

        plan_or_reject = self._build_plan(opportunity, leg_inputs, package_units, now_ns)
        if isinstance(plan_or_reject, PlannerDecision):
            return plan_or_reject
        return PlannerDecision(plan=plan_or_reject, rejection=None)

    def _build_plan(
        self,
        opportunity: Opportunity,
        leg_inputs: list[_LegPlanInputs],
        package_units: int,
        now_ns: int,
    ) -> ExecutionPlan | PlannerDecision:
        total_notional = 0
        total_shares = 0
        gross_profit = 0
        fee_cents = 0

        for item in leg_inputs:
            qty = package_units * item.leg.quantity_ratio
            notional = qty * item.limit_price_ticks
            total_notional += notional
            total_shares += qty
            gross_profit += notional if item.leg.side == Side.SELL else -notional
            fee_bps_total = item.leg.fee_bps + self._config.additional_cost_bps
            if fee_bps_total > 0:
                fee_cents += _ceil_div(notional * fee_bps_total, _BPS_SCALE)

        fee_cents += self._config.fixed_cost_cents
        net_profit = gross_profit - fee_cents

        if total_notional <= 0:
            return self._reject(
                PlannerRejectCode.NOTIONAL_OUT_OF_BOUNDS,
                "computed notional is zero",
                now_ns,
            )

        net_edge_bps = (net_profit * _BPS_SCALE) // total_notional
        if net_profit < self._config.min_expected_profit_cents:
            return self._reject(
                PlannerRejectCode.PROFIT_BELOW_THRESHOLD,
                (
                    f"net profit {net_profit} below threshold "
                    f"{self._config.min_expected_profit_cents}"
                ),
                now_ns,
            )

        if net_edge_bps < self._config.min_expected_edge_bps:
            return self._reject(
                PlannerRejectCode.EDGE_BELOW_THRESHOLD,
                (
                    f"net edge {net_edge_bps} bps below threshold "
                    f"{self._config.min_expected_edge_bps} bps"
                ),
                now_ns,
            )

        # Leg ordering is deterministic and prioritizes constrained liquidity first
        # to maximize probability of completing all legs under competition.
        ordered_inputs = sorted(
            leg_inputs,
            key=lambda item: (item.available_units, item.leg.leg_id),
        )

        planned_legs: list[PlannedLeg] = []
        for rank, item in enumerate(ordered_inputs, start=1):
            planned_legs.append(
                PlannedLeg(
                    leg_id=item.leg.leg_id,
                    market_id=item.leg.market_id,
                    token_id=item.leg.token_id,
                    side=item.leg.side,
                    submission_rank=rank,
                    quantity=package_units * item.leg.quantity_ratio,
                    executable_price_ticks=item.executable_price_ticks,
                    limit_price_ticks=item.limit_price_ticks,
                    tick_size_ticks=item.snapshot.tick_size_ticks,
                    timeout_ms=item.leg.order_timeout_ms or self._config.default_leg_timeout_ms,
                    tif=self._config.tif,
                    snapshot_ts_ns=item.snapshot.ts_ns,
                    available_units_at_plan=item.available_units,
                )
            )

        expiry_ns = min(
            opportunity.expires_at_ns,
            now_ns + self._config.plan_ttl_ms * _NS_PER_MS,
        )
        if expiry_ns <= now_ns:
            return self._reject(
                PlannerRejectCode.OPPORTUNITY_EXPIRED,
                "plan expiry already elapsed",
                now_ns,
            )

        return ExecutionPlan(
            plan_id=f"{opportunity.opportunity_id}:{now_ns}",
            opportunity_id=opportunity.opportunity_id,
            created_ts_ns=now_ns,
            expires_at_ns=expiry_ns,
            package_timeout_ms=self._config.package_timeout_ms,
            package_units=package_units,
            tif=self._config.tif,
            total_shares=total_shares,
            total_notional_cents=total_notional,
            expected_gross_profit_cents=gross_profit,
            expected_fee_cents=fee_cents,
            expected_net_profit_cents=net_profit,
            expected_net_edge_bps=net_edge_bps,
            legs=tuple(planned_legs),
        )

    def _choose_package_units(
        self,
        opportunity: Opportunity,
        leg_inputs: list[_LegPlanInputs],
        now_ns: int,
    ) -> int | PlannerDecision:
        configured_min = max(opportunity.min_package_units, self._config.min_package_units)
        configured_max = min(opportunity.max_package_units, self._config.max_package_units)
        target_units = min(opportunity.target_package_units, configured_max)

        if configured_min <= 0 or configured_max <= 0:
            return self._reject(
                PlannerRejectCode.INVALID_OPPORTUNITY,
                "invalid min/max package units",
                now_ns,
            )
        if configured_min > configured_max:
            return self._reject(
                PlannerRejectCode.INVALID_OPPORTUNITY,
                "min package units exceed max package units",
                now_ns,
            )

        liquidity_cap = min(item.available_units for item in leg_inputs)
        if not self._config.scale_down_to_common_size and target_units > liquidity_cap:
            return self._reject(
                PlannerRejectCode.INSUFFICIENT_SIZE,
                (
                    f"target units={target_units} exceed common executable units "
                    f"{liquidity_cap}"
                ),
                now_ns,
            )

        units = min(target_units, liquidity_cap) if self._config.scale_down_to_common_size else target_units
        if units < configured_min:
            return self._reject(
                PlannerRejectCode.INSUFFICIENT_SIZE,
                (
                    f"executable units={units} below required minimum units "
                    f"{configured_min}"
                ),
                now_ns,
            )

        shares_cap_per_leg = min(self._config.max_shares_per_leg // item.leg.quantity_ratio for item in leg_inputs)
        total_ratio = sum(item.leg.quantity_ratio for item in leg_inputs)
        total_shares_cap = self._config.max_total_shares // total_ratio
        share_units_cap = min(shares_cap_per_leg, total_shares_cap)

        if units > share_units_cap:
            if not self._config.scale_down_to_common_size:
                return self._reject(
                    PlannerRejectCode.SHARE_CAP_EXCEEDED,
                    (
                        f"units={units} exceed share-cap units={share_units_cap} "
                        f"(max_shares_per_leg={self._config.max_shares_per_leg}, "
                        f"max_total_shares={self._config.max_total_shares})"
                    ),
                    now_ns,
                )
            units = share_units_cap

        per_unit_notional = sum(item.limit_price_ticks * item.leg.quantity_ratio for item in leg_inputs)
        notional_units_cap = self._config.max_total_notional_cents // per_unit_notional if per_unit_notional > 0 else 0
        if notional_units_cap <= 0:
            return self._reject(
                PlannerRejectCode.NOTIONAL_OUT_OF_BOUNDS,
                "max_total_notional_cents too small for one package unit",
                now_ns,
            )

        if units > notional_units_cap:
            if not self._config.scale_down_to_common_size:
                return self._reject(
                    PlannerRejectCode.NOTIONAL_OUT_OF_BOUNDS,
                    (
                        f"units={units} exceed notional-cap units={notional_units_cap} "
                        f"(max_total_notional={self._config.max_total_notional_cents})"
                    ),
                    now_ns,
                )
            units = notional_units_cap

        if units < configured_min:
            return self._reject(
                PlannerRejectCode.INSUFFICIENT_SIZE,
                f"final units={units} below configured minimum units={configured_min}",
                now_ns,
            )

        total_notional = per_unit_notional * units
        if total_notional < self._config.min_total_notional_cents:
            return self._reject(
                PlannerRejectCode.NOTIONAL_OUT_OF_BOUNDS,
                (
                    f"total_notional={total_notional} below min_total_notional="
                    f"{self._config.min_total_notional_cents}"
                ),
                now_ns,
            )

        return units

    def _compute_marketable_limit(
        self,
        leg: OpportunityLeg,
        snapshot: PricingSnapshot,
        executable_price_ticks: int,
        now_ns: int,
    ) -> int | PlannerDecision:
        slip_bps = leg.max_slippage_bps if leg.max_slippage_bps is not None else self._config.default_max_slippage_bps

        if leg.side == Side.BUY:
            aggressive = _ceil_div(executable_price_ticks * (_BPS_SCALE + self._config.aggressiveness_bps), _BPS_SCALE)
            aggressive = _round_up_to_tick(aggressive, snapshot.tick_size_ticks)

            slippage_cap = (leg.reference_price_ticks * (_BPS_SCALE + slip_bps)) // _BPS_SCALE
            slippage_cap = _round_down_to_tick(max(slippage_cap, snapshot.tick_size_ticks), snapshot.tick_size_ticks)

            touch_cap = (executable_price_ticks * (_BPS_SCALE + self._config.max_cross_bps_from_touch)) // _BPS_SCALE
            touch_cap = _round_down_to_tick(max(touch_cap, snapshot.tick_size_ticks), snapshot.tick_size_ticks)

            limit_price = min(aggressive, slippage_cap, touch_cap)
            if limit_price < executable_price_ticks:
                return self._reject(
                    PlannerRejectCode.PRICE_PROTECTION,
                    (
                        f"buy protection too tight for leg={leg.leg_id}, "
                        f"limit={limit_price}, ask={executable_price_ticks}"
                    ),
                    now_ns,
                    leg.leg_id,
                )
            return limit_price

        aggressive = (executable_price_ticks * (_BPS_SCALE - self._config.aggressiveness_bps)) // _BPS_SCALE
        aggressive = _round_down_to_tick(aggressive, snapshot.tick_size_ticks)

        slippage_floor = _ceil_div(leg.reference_price_ticks * (_BPS_SCALE - slip_bps), _BPS_SCALE)
        slippage_floor = _round_up_to_tick(max(slippage_floor, snapshot.tick_size_ticks), snapshot.tick_size_ticks)

        touch_floor = _ceil_div(executable_price_ticks * (_BPS_SCALE - self._config.max_cross_bps_from_touch), _BPS_SCALE)
        touch_floor = _round_up_to_tick(max(touch_floor, snapshot.tick_size_ticks), snapshot.tick_size_ticks)

        limit_price = max(aggressive, slippage_floor, touch_floor)
        if limit_price > executable_price_ticks:
            return self._reject(
                PlannerRejectCode.PRICE_PROTECTION,
                (
                    f"sell protection too tight for leg={leg.leg_id}, "
                    f"limit={limit_price}, bid={executable_price_ticks}"
                ),
                now_ns,
                leg.leg_id,
            )
        return limit_price

    def _validate_opportunity(self, opportunity: Opportunity, now_ns: int) -> PlannerDecision | None:
        if not opportunity.opportunity_id:
            return self._reject(PlannerRejectCode.INVALID_OPPORTUNITY, "opportunity_id is required", now_ns)
        if not opportunity.legs:
            return self._reject(PlannerRejectCode.INVALID_OPPORTUNITY, "at least one leg is required", now_ns)
        if now_ns >= opportunity.expires_at_ns:
            return self._reject(PlannerRejectCode.OPPORTUNITY_EXPIRED, "opportunity already expired", now_ns)
        if opportunity.confidence < self._config.min_confidence:
            return self._reject(
                PlannerRejectCode.LOW_CONFIDENCE,
                (
                    f"opportunity confidence={opportunity.confidence} below "
                    f"minimum {self._config.min_confidence}"
                ),
                now_ns,
            )

        if (
            opportunity.target_package_units <= 0
            or opportunity.min_package_units <= 0
            or opportunity.max_package_units <= 0
        ):
            return self._reject(
                PlannerRejectCode.INVALID_OPPORTUNITY,
                "package unit bounds must be > 0",
                now_ns,
            )

        if opportunity.min_package_units > opportunity.max_package_units:
            return self._reject(
                PlannerRejectCode.INVALID_OPPORTUNITY,
                "opportunity min_package_units exceeds max_package_units",
                now_ns,
            )

        seen_leg_ids: set[str] = set()
        for leg in opportunity.legs:
            if leg.leg_id in seen_leg_ids:
                return self._reject(
                    PlannerRejectCode.INVALID_OPPORTUNITY,
                    f"duplicate leg_id={leg.leg_id}",
                    now_ns,
                    leg.leg_id,
                )
            seen_leg_ids.add(leg.leg_id)

            if leg.quantity_ratio <= 0:
                return self._reject(
                    PlannerRejectCode.INVALID_OPPORTUNITY,
                    f"quantity_ratio must be > 0 for leg_id={leg.leg_id}",
                    now_ns,
                    leg.leg_id,
                )
            if leg.reference_price_ticks <= 0:
                return self._reject(
                    PlannerRejectCode.INVALID_OPPORTUNITY,
                    f"reference_price_ticks must be > 0 for leg_id={leg.leg_id}",
                    now_ns,
                    leg.leg_id,
                )
            if not leg.token_id:
                return self._reject(
                    PlannerRejectCode.INVALID_OPPORTUNITY,
                    f"token_id must be non-empty for leg_id={leg.leg_id}",
                    now_ns,
                    leg.leg_id,
                )

        return None

    def _validate_snapshot(
        self,
        leg: OpportunityLeg,
        snapshot: PricingSnapshot,
        now_ns: int,
    ) -> PlannerDecision | None:
        if snapshot.market_id != leg.market_id:
            return self._reject(
                PlannerRejectCode.MISSING_SNAPSHOT,
                (
                    f"snapshot market mismatch for leg_id={leg.leg_id}, "
                    f"expected={leg.market_id}, got={snapshot.market_id}"
                ),
                now_ns,
                leg.leg_id,
            )

        if snapshot.is_uncertain:
            return self._reject(
                PlannerRejectCode.UNCERTAIN_SNAPSHOT,
                f"uncertain snapshot for leg_id={leg.leg_id}",
                now_ns,
                leg.leg_id,
            )

        age_ns = now_ns - snapshot.ts_ns
        if age_ns < 0:
            return self._reject(
                PlannerRejectCode.UNCERTAIN_SNAPSHOT,
                f"snapshot timestamp in the future for leg_id={leg.leg_id}",
                now_ns,
                leg.leg_id,
            )

        if age_ns > self._config.max_snapshot_age_ms * _NS_PER_MS:
            return self._reject(
                PlannerRejectCode.STALE_SNAPSHOT,
                (
                    f"snapshot stale for leg_id={leg.leg_id}, age_ms="
                    f"{age_ns // _NS_PER_MS}, max={self._config.max_snapshot_age_ms}"
                ),
                now_ns,
                leg.leg_id,
            )

        if snapshot.tick_size_ticks <= 0:
            return self._reject(
                PlannerRejectCode.INVALID_BOOK,
                f"tick_size_ticks must be > 0 for leg_id={leg.leg_id}",
                now_ns,
                leg.leg_id,
            )

        if (
            snapshot.best_bid_ticks <= 0
            or snapshot.best_ask_ticks <= 0
            or snapshot.best_bid_size < 0
            or snapshot.best_ask_size < 0
        ):
            return self._reject(
                PlannerRejectCode.INVALID_BOOK,
                f"invalid top-of-book values for leg_id={leg.leg_id}",
                now_ns,
                leg.leg_id,
            )

        if self._config.reject_crossed_book and snapshot.best_bid_ticks >= snapshot.best_ask_ticks:
            return self._reject(
                PlannerRejectCode.INVALID_BOOK,
                (
                    f"crossed or locked book for leg_id={leg.leg_id}, "
                    f"bid={snapshot.best_bid_ticks}, ask={snapshot.best_ask_ticks}"
                ),
                now_ns,
                leg.leg_id,
            )

        return None

    def _reject(
        self,
        code: PlannerRejectCode,
        reason: str,
        ts_ns: int,
        leg_id: str | None = None,
    ) -> PlannerDecision:
        return PlannerDecision(plan=None, rejection=PlannerRejection(code=code, reason=reason, ts_ns=ts_ns, leg_id=leg_id))

    @staticmethod
    def _validate_config(config: PlannerConfig) -> None:
        if config.aggressiveness_bps < 0:
            raise ValueError("aggressiveness_bps must be >= 0")
        if not 0 <= config.max_cross_bps_from_touch <= _BPS_SCALE:
            raise ValueError("max_cross_bps_from_touch must be between 0 and 10000")
        if not 0 <= config.default_max_slippage_bps <= _BPS_SCALE:
            raise ValueError("default_max_slippage_bps must be between 0 and 10000")
        if config.max_snapshot_age_ms <= 0:
            raise ValueError("max_snapshot_age_ms must be > 0")
        if not 0.0 <= config.min_confidence <= 1.0:
            raise ValueError("min_confidence must be in [0, 1]")
        if config.min_expected_edge_bps < 0:
            raise ValueError("min_expected_edge_bps must be >= 0")
        if config.max_total_notional_cents <= 0:
            raise ValueError("max_total_notional_cents must be > 0")
        if config.min_total_notional_cents <= 0:
            raise ValueError("min_total_notional_cents must be > 0")
        if config.min_total_notional_cents > config.max_total_notional_cents:
            raise ValueError("min_total_notional_cents cannot exceed max_total_notional_cents")
        if config.min_package_units <= 0:
            raise ValueError("min_package_units must be > 0")
        if config.max_package_units <= 0:
            raise ValueError("max_package_units must be > 0")
        if config.min_package_units > config.max_package_units:
            raise ValueError("min_package_units cannot exceed max_package_units")
        if config.max_shares_per_leg <= 0:
            raise ValueError("max_shares_per_leg must be > 0")
        if config.max_total_shares <= 0:
            raise ValueError("max_total_shares must be > 0")
        if config.default_leg_timeout_ms <= 0:
            raise ValueError("default_leg_timeout_ms must be > 0")
        if config.package_timeout_ms <= 0:
            raise ValueError("package_timeout_ms must be > 0")
        if config.plan_ttl_ms <= 0:
            raise ValueError("plan_ttl_ms must be > 0")


def _ceil_div(numerator: int, denominator: int) -> int:
    return -(-numerator // denominator)


def _round_up_to_tick(price_ticks: int, tick_size_ticks: int) -> int:
    return _ceil_div(price_ticks, tick_size_ticks) * tick_size_ticks


def _round_down_to_tick(price_ticks: int, tick_size_ticks: int) -> int:
    return (price_ticks // tick_size_ticks) * tick_size_ticks