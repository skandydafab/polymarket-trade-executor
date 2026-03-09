from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Mapping, Protocol, Sequence

from .planner import ExecutionPlan, PlannedLeg
from .state_machine import (
    LegTimeoutEvent,
    PackageExecution,
    PackageState,
    PackageStateMachine,
    Side,
    StateMachineEvent,
    VenueFillEvent,
    VenueRejectEvent,
)
from .venue import MetricsSink, NullLogger, NullMetrics, StructuredLogger


_NS_PER_MS = 1_000_000


class RiskReasonCode(str, Enum):
    MANUAL_KILL_SWITCH = "MANUAL_KILL_SWITCH"
    AUTO_KILL_SWITCH = "AUTO_KILL_SWITCH"
    MAX_NOTIONAL_PER_PACKAGE = "MAX_NOTIONAL_PER_PACKAGE"
    MAX_NOTIONAL_PER_MARKET = "MAX_NOTIONAL_PER_MARKET"
    MAX_TOTAL_OPEN_NOTIONAL = "MAX_TOTAL_OPEN_NOTIONAL"
    MAX_ACTIVE_PACKAGES = "MAX_ACTIVE_PACKAGES"
    MIN_EXPECTED_NET_PROFIT = "MIN_EXPECTED_NET_PROFIT"
    MIN_DISPLAYED_EXECUTABLE_SIZE = "MIN_DISPLAYED_EXECUTABLE_SIZE"
    STALE_LEG_DATA = "STALE_LEG_DATA"
    DUPLICATE_PACKAGE = "DUPLICATE_PACKAGE"
    CONFLICTING_ACTIVE_PACKAGE = "CONFLICTING_ACTIVE_PACKAGE"
    RELATION_COOLDOWN_ACTIVE = "RELATION_COOLDOWN_ACTIVE"
    MARKET_COOLDOWN_ACTIVE = "MARKET_COOLDOWN_ACTIVE"
    SECOND_LEG_TIMEOUT = "SECOND_LEG_TIMEOUT"
    MAX_PARTIAL_FILL_EXPOSURE = "MAX_PARTIAL_FILL_EXPOSURE"
    MAX_OPEN_DIRECTIONAL_IMBALANCE = "MAX_OPEN_DIRECTIONAL_IMBALANCE"
    PACKAGE_EXECUTION_DEADLINE = "PACKAGE_EXECUTION_DEADLINE"
    FAILURE_RATE_SPIKE = "FAILURE_RATE_SPIKE"
    DAILY_LOSS_CAP = "DAILY_LOSS_CAP"
    REPEATED_REJECT_THRESHOLD = "REPEATED_REJECT_THRESHOLD"
    REPEATED_VENUE_AMBIGUITY_THRESHOLD = "REPEATED_VENUE_AMBIGUITY_THRESHOLD"
    REPEATED_STALE_DATA_THRESHOLD = "REPEATED_STALE_DATA_THRESHOLD"
    INVALID_REQUEST = "INVALID_REQUEST"


@dataclass(frozen=True, slots=True)
class RiskDecision:
    allowed: bool
    code: RiskReasonCode | None
    reason: str
    ts_ns: int
    package_id: str | None = None
    relation_id: str | None = None
    should_abort_package: bool = False
    should_halt_trading: bool = False

    @staticmethod
    def allow(ts_ns: int, reason: str = "allowed") -> RiskDecision:
        return RiskDecision(
            allowed=True,
            code=None,
            reason=reason,
            ts_ns=ts_ns,
        )

    @staticmethod
    def deny(
        *,
        code: RiskReasonCode,
        reason: str,
        ts_ns: int,
        package_id: str | None = None,
        relation_id: str | None = None,
        should_abort_package: bool = False,
        should_halt_trading: bool = False,
    ) -> RiskDecision:
        return RiskDecision(
            allowed=False,
            code=code,
            reason=reason,
            ts_ns=ts_ns,
            package_id=package_id,
            relation_id=relation_id,
            should_abort_package=should_abort_package,
            should_halt_trading=should_halt_trading,
        )


@dataclass(frozen=True, slots=True)
class RiskManagerConfig:
    max_notional_per_package_cents: int = 2_000_000
    max_notional_per_market_cents: int = 3_000_000
    max_total_open_notional_cents: int = 8_000_000
    max_simultaneous_active_packages: int = 25
    min_expected_net_profit_cents: int = 1
    min_displayed_executable_size_shares: int = 1
    max_leg_snapshot_age_ms: int = 250
    block_conflicting_markets: bool = True
    relation_cooldown_ms: int = 10_000
    market_cooldown_ms: int = 5_000
    second_leg_timeout_ms: int = 1_500
    max_partial_fill_exposure_cents: int = 100_000
    max_open_directional_imbalance_shares: int = 10_000
    package_execution_deadline_ms: int = 4_000
    failure_rate_window_ms: int = 60_000
    failure_rate_halt_threshold: float = 0.70
    failure_rate_min_sample: int = 10
    enable_auto_kill_switch: bool = True
    daily_loss_cap_cents: int = 200_000
    repeated_reject_threshold: int = 25
    repeated_venue_ambiguity_threshold: int = 8
    repeated_stale_data_threshold: int = 25
    repeated_event_window_ms: int = 60_000


@dataclass(frozen=True, slots=True)
class PreTradeRiskRequest:
    package_id: str
    relation_id: str
    plan: ExecutionPlan


@dataclass(frozen=True, slots=True)
class KillSwitchState:
    manual_active: bool
    auto_active: bool
    manual_reason: str | None
    auto_reason: str | None
    activated_ts_ns: int | None


@dataclass(frozen=True, slots=True)
class ExposureSnapshot:
    total_open_notional_cents: int
    open_notional_by_market_cents: Mapping[str, int]
    open_directional_imbalance_by_market: Mapping[str, int]
    active_package_count: int


@dataclass(slots=True)
class _RollingCounter:
    window_ns: int
    timestamps: deque[int] = field(default_factory=deque)

    def add(self, ts_ns: int) -> None:
        self.timestamps.append(ts_ns)
        self._trim(ts_ns)

    def count(self, now_ns: int) -> int:
        self._trim(now_ns)
        return len(self.timestamps)

    def _trim(self, now_ns: int) -> None:
        floor = now_ns - self.window_ns
        while self.timestamps and self.timestamps[0] < floor:
            self.timestamps.popleft()


@dataclass(slots=True)
class _ActiveLegRiskState:
    leg_id: str
    market_id: str
    side: Side
    intended_qty: int
    filled_qty: int = 0


@dataclass(slots=True)
class _ActivePackageRiskState:
    package_id: str
    relation_id: str
    opportunity_id: str
    opened_ts_ns: int
    deadline_ts_ns: int
    market_notional_cents: dict[str, int]
    legs: dict[str, _ActiveLegRiskState]
    total_reserved_notional_cents: int
    buy_filled_notional_cents: int = 0
    sell_filled_notional_cents: int = 0
    first_fill_ts_ns: int | None = None
    market_directional_shares: dict[str, int] = field(default_factory=dict)
    second_leg_timeout_breached: bool = False
    deadline_breached: bool = False

    @property
    def partial_fill_exposure_cents(self) -> int:
        return abs(self.buy_filled_notional_cents - self.sell_filled_notional_cents)

    @property
    def realized_pnl_cents(self) -> int:
        return self.sell_filled_notional_cents - self.buy_filled_notional_cents

    def all_legs_filled(self) -> bool:
        return all(leg.filled_qty >= leg.intended_qty for leg in self.legs.values())


class PreTradeRiskValidator(Protocol):
    def validate_pre_trade(self, request: PreTradeRiskRequest, now_ns: int) -> RiskDecision: ...


class IntraTradeRiskMonitor(Protocol):
    def on_state_event(
        self,
        package: PackageExecution,
        event: StateMachineEvent,
        now_ns: int,
    ) -> tuple[RiskDecision, ...]: ...

    def evaluate_active_risk(self, now_ns: int) -> tuple[RiskDecision, ...]: ...


class InMemoryExposureTracker:
    """Small in-memory exposure tracker for active package reservations and fills."""

    __slots__ = (
        "active_packages",
        "total_open_notional_cents",
        "open_notional_by_market_cents",
        "open_directional_imbalance_by_market",
    )

    def __init__(self) -> None:
        self.active_packages: dict[str, _ActivePackageRiskState] = {}
        self.total_open_notional_cents = 0
        self.open_notional_by_market_cents: dict[str, int] = {}
        self.open_directional_imbalance_by_market: dict[str, int] = {}

    def reserve_package(self, package_state: _ActivePackageRiskState) -> None:
        if package_state.package_id in self.active_packages:
            raise ValueError(f"package already reserved: {package_state.package_id}")

        self.active_packages[package_state.package_id] = package_state
        self.total_open_notional_cents += package_state.total_reserved_notional_cents
        for market_id, notional in package_state.market_notional_cents.items():
            self.open_notional_by_market_cents[market_id] = (
                self.open_notional_by_market_cents.get(market_id, 0) + notional
            )

        for market_id, signed_shares in package_state.market_directional_shares.items():
            self.open_directional_imbalance_by_market[market_id] = (
                self.open_directional_imbalance_by_market.get(market_id, 0) + signed_shares
            )

    def release_package(self, package_id: str) -> _ActivePackageRiskState | None:
        package = self.active_packages.pop(package_id, None)
        if package is None:
            return None

        self.total_open_notional_cents = max(
            0,
            self.total_open_notional_cents - package.total_reserved_notional_cents,
        )

        for market_id, notional in package.market_notional_cents.items():
            current = self.open_notional_by_market_cents.get(market_id, 0) - notional
            if current <= 0:
                self.open_notional_by_market_cents.pop(market_id, None)
            else:
                self.open_notional_by_market_cents[market_id] = current

        for market_id, signed_shares in package.market_directional_shares.items():
            current = self.open_directional_imbalance_by_market.get(market_id, 0) - signed_shares
            if current == 0:
                self.open_directional_imbalance_by_market.pop(market_id, None)
            else:
                self.open_directional_imbalance_by_market[market_id] = current

        return package

    def apply_fill(
        self,
        *,
        package_id: str,
        leg_id: str,
        fill_qty: int,
        fill_price_ticks: int,
        fill_ts_ns: int,
    ) -> _ActivePackageRiskState | None:
        if fill_qty <= 0 or fill_price_ticks <= 0:
            return None

        package = self.active_packages.get(package_id)
        if package is None:
            return None

        leg = package.legs.get(leg_id)
        if leg is None:
            return None

        remaining = max(0, leg.intended_qty - leg.filled_qty)
        incremental = min(fill_qty, remaining) if remaining > 0 else 0
        if incremental <= 0:
            return package

        leg.filled_qty += incremental
        if package.first_fill_ts_ns is None:
            package.first_fill_ts_ns = fill_ts_ns

        fill_notional = incremental * fill_price_ticks
        if leg.side == Side.BUY:
            package.buy_filled_notional_cents += fill_notional
            signed_qty = incremental
        else:
            package.sell_filled_notional_cents += fill_notional
            signed_qty = -incremental

        package.market_directional_shares[leg.market_id] = package.market_directional_shares.get(leg.market_id, 0) + signed_qty
        self.open_directional_imbalance_by_market[leg.market_id] = (
            self.open_directional_imbalance_by_market.get(leg.market_id, 0) + signed_qty
        )
        return package

    def snapshot(self) -> ExposureSnapshot:
        return ExposureSnapshot(
            total_open_notional_cents=self.total_open_notional_cents,
            open_notional_by_market_cents=dict(self.open_notional_by_market_cents),
            open_directional_imbalance_by_market=dict(self.open_directional_imbalance_by_market),
            active_package_count=len(self.active_packages),
        )


class ExecutionRiskManager(PreTradeRiskValidator, IntraTradeRiskMonitor):
    """Deterministic execution safety layer for pre-trade and intra-trade controls."""

    __slots__ = (
        "_config",
        "_logger",
        "_metrics",
        "_exposure",
        "_manual_kill_active",
        "_manual_kill_reason",
        "_auto_kill_active",
        "_auto_kill_reason",
        "_kill_activated_ts_ns",
        "_active_relation_packages",
        "_active_market_packages",
        "_active_opportunity_to_package",
        "_relation_cooldowns",
        "_market_cooldowns",
        "_starts_counter",
        "_failures_counter",
        "_reject_counter",
        "_ambiguity_counter",
        "_stale_counter",
        "_daily_realized_pnl_cents",
    )

    def __init__(
        self,
        config: RiskManagerConfig,
        *,
        logger: StructuredLogger | None = None,
        metrics: MetricsSink | None = None,
    ) -> None:
        self._validate_config(config)
        self._config = config
        self._logger = logger or NullLogger()
        self._metrics = metrics or NullMetrics()
        self._exposure = InMemoryExposureTracker()

        self._manual_kill_active = False
        self._manual_kill_reason: str | None = None
        self._auto_kill_active = False
        self._auto_kill_reason: str | None = None
        self._kill_activated_ts_ns: int | None = None

        self._active_relation_packages: dict[str, set[str]] = {}
        self._active_market_packages: dict[str, set[str]] = {}
        self._active_opportunity_to_package: dict[str, str] = {}

        self._relation_cooldowns: dict[str, int] = {}
        self._market_cooldowns: dict[str, int] = {}

        self._starts_counter = _RollingCounter(config.failure_rate_window_ms * _NS_PER_MS)
        self._failures_counter = _RollingCounter(config.failure_rate_window_ms * _NS_PER_MS)
        repeated_window_ns = config.repeated_event_window_ms * _NS_PER_MS
        self._reject_counter = _RollingCounter(repeated_window_ns)
        self._ambiguity_counter = _RollingCounter(repeated_window_ns)
        self._stale_counter = _RollingCounter(repeated_window_ns)

        self._daily_realized_pnl_cents = 0

    @property
    def config(self) -> RiskManagerConfig:
        return self._config

    @property
    def exposure(self) -> InMemoryExposureTracker:
        return self._exposure

    @property
    def is_trading_halted(self) -> bool:
        return self._manual_kill_active or self._auto_kill_active

    def kill_switch_state(self) -> KillSwitchState:
        return KillSwitchState(
            manual_active=self._manual_kill_active,
            auto_active=self._auto_kill_active,
            manual_reason=self._manual_kill_reason,
            auto_reason=self._auto_kill_reason,
            activated_ts_ns=self._kill_activated_ts_ns,
        )

    def activate_manual_kill_switch(self, reason: str, now_ns: int) -> RiskDecision:
        self._manual_kill_active = True
        self._manual_kill_reason = reason or "manual kill switch"
        self._kill_activated_ts_ns = now_ns
        self._logger.warning("manual kill switch activated", reason=self._manual_kill_reason)
        self._metrics.increment("risk.kill_switch.manual")
        return RiskDecision.deny(
            code=RiskReasonCode.MANUAL_KILL_SWITCH,
            reason=self._manual_kill_reason,
            ts_ns=now_ns,
            should_halt_trading=True,
        )

    def clear_manual_kill_switch(self, now_ns: int) -> RiskDecision:
        self._manual_kill_active = False
        self._manual_kill_reason = None
        self._kill_activated_ts_ns = None if not self._auto_kill_active else self._kill_activated_ts_ns
        self._logger.info("manual kill switch cleared", ts_ns=now_ns)
        return RiskDecision.allow(now_ns, reason="manual kill switch cleared")

    def clear_auto_kill_switch(self, now_ns: int) -> RiskDecision:
        self._auto_kill_active = False
        self._auto_kill_reason = None
        self._kill_activated_ts_ns = None if not self._manual_kill_active else self._kill_activated_ts_ns
        self._logger.info("auto kill switch cleared", ts_ns=now_ns)
        return RiskDecision.allow(now_ns, reason="auto kill switch cleared")

    def validate_pre_trade(self, request: PreTradeRiskRequest, now_ns: int) -> RiskDecision:
        base_reject = self._trading_block_reject(now_ns)
        if base_reject is not None:
            return base_reject

        invalid = self._validate_request(request, now_ns)
        if invalid is not None:
            return invalid

        relation_expiry = self._relation_cooldowns.get(request.relation_id, 0)
        if relation_expiry > now_ns:
            return RiskDecision.deny(
                code=RiskReasonCode.RELATION_COOLDOWN_ACTIVE,
                reason=f"relation {request.relation_id} in cooldown until {relation_expiry}",
                ts_ns=now_ns,
                package_id=request.package_id,
                relation_id=request.relation_id,
            )

        market_ids = _market_ids_from_plan(request.plan)
        for market_id in market_ids:
            market_expiry = self._market_cooldowns.get(market_id, 0)
            if market_expiry > now_ns:
                return RiskDecision.deny(
                    code=RiskReasonCode.MARKET_COOLDOWN_ACTIVE,
                    reason=f"market {market_id} in cooldown until {market_expiry}",
                    ts_ns=now_ns,
                    package_id=request.package_id,
                    relation_id=request.relation_id,
                )

        if request.package_id in self._exposure.active_packages:
            return RiskDecision.deny(
                code=RiskReasonCode.DUPLICATE_PACKAGE,
                reason=f"package already active: {request.package_id}",
                ts_ns=now_ns,
                package_id=request.package_id,
                relation_id=request.relation_id,
            )

        if request.plan.opportunity_id in self._active_opportunity_to_package:
            return RiskDecision.deny(
                code=RiskReasonCode.DUPLICATE_PACKAGE,
                reason=f"opportunity already active: {request.plan.opportunity_id}",
                ts_ns=now_ns,
                package_id=request.package_id,
                relation_id=request.relation_id,
            )

        if self._active_relation_packages.get(request.relation_id):
            return RiskDecision.deny(
                code=RiskReasonCode.CONFLICTING_ACTIVE_PACKAGE,
                reason=f"active package already exists for relation={request.relation_id}",
                ts_ns=now_ns,
                package_id=request.package_id,
                relation_id=request.relation_id,
            )

        if self._config.block_conflicting_markets:
            for market_id in market_ids:
                if self._active_market_packages.get(market_id):
                    return RiskDecision.deny(
                        code=RiskReasonCode.CONFLICTING_ACTIVE_PACKAGE,
                        reason=f"market conflict on market_id={market_id}",
                        ts_ns=now_ns,
                        package_id=request.package_id,
                        relation_id=request.relation_id,
                    )

        if len(self._exposure.active_packages) >= self._config.max_simultaneous_active_packages:
            return RiskDecision.deny(
                code=RiskReasonCode.MAX_ACTIVE_PACKAGES,
                reason=(
                    f"active package cap reached: {len(self._exposure.active_packages)} >= "
                    f"{self._config.max_simultaneous_active_packages}"
                ),
                ts_ns=now_ns,
                package_id=request.package_id,
                relation_id=request.relation_id,
            )

        if request.plan.expected_net_profit_cents < self._config.min_expected_net_profit_cents:
            return RiskDecision.deny(
                code=RiskReasonCode.MIN_EXPECTED_NET_PROFIT,
                reason=(
                    f"expected net profit {request.plan.expected_net_profit_cents} below minimum "
                    f"{self._config.min_expected_net_profit_cents}"
                ),
                ts_ns=now_ns,
                package_id=request.package_id,
                relation_id=request.relation_id,
            )

        if request.plan.total_notional_cents > self._config.max_notional_per_package_cents:
            return RiskDecision.deny(
                code=RiskReasonCode.MAX_NOTIONAL_PER_PACKAGE,
                reason=(
                    f"package notional {request.plan.total_notional_cents} exceeds cap "
                    f"{self._config.max_notional_per_package_cents}"
                ),
                ts_ns=now_ns,
                package_id=request.package_id,
                relation_id=request.relation_id,
            )

        projected_total_open_notional = self._exposure.total_open_notional_cents + request.plan.total_notional_cents
        if projected_total_open_notional > self._config.max_total_open_notional_cents:
            return RiskDecision.deny(
                code=RiskReasonCode.MAX_TOTAL_OPEN_NOTIONAL,
                reason=(
                    f"total open notional projection {projected_total_open_notional} exceeds cap "
                    f"{self._config.max_total_open_notional_cents}"
                ),
                ts_ns=now_ns,
                package_id=request.package_id,
                relation_id=request.relation_id,
            )

        per_market_notional = _market_notional_from_plan(request.plan)
        for market_id, plan_market_notional in per_market_notional.items():
            projected_market_notional = (
                self._exposure.open_notional_by_market_cents.get(market_id, 0) + plan_market_notional
            )
            if projected_market_notional > self._config.max_notional_per_market_cents:
                return RiskDecision.deny(
                    code=RiskReasonCode.MAX_NOTIONAL_PER_MARKET,
                    reason=(
                        f"market {market_id} projected notional {projected_market_notional} "
                        f"exceeds cap {self._config.max_notional_per_market_cents}"
                    ),
                    ts_ns=now_ns,
                    package_id=request.package_id,
                    relation_id=request.relation_id,
                )

        stale_decision = self._check_leg_freshness_and_displayed_size(request, now_ns)
        if stale_decision is not None:
            return stale_decision

        return RiskDecision.allow(now_ns)

    def register_package(self, request: PreTradeRiskRequest, now_ns: int) -> RiskDecision:
        decision = self.validate_pre_trade(request, now_ns)
        if not decision.allowed:
            self._log_decision("pre_trade_reject", decision)
            return decision

        package_state = _build_active_package_state(
            request=request,
            now_ns=now_ns,
            package_execution_deadline_ms=self._config.package_execution_deadline_ms,
        )
        self._exposure.reserve_package(package_state)
        self._active_opportunity_to_package[request.plan.opportunity_id] = request.package_id

        relation_set = self._active_relation_packages.setdefault(request.relation_id, set())
        relation_set.add(request.package_id)

        for market_id in package_state.market_notional_cents:
            market_set = self._active_market_packages.setdefault(market_id, set())
            market_set.add(request.package_id)

        self._starts_counter.add(now_ns)
        self._metrics.increment("risk.package.registered")
        registered = RiskDecision.allow(now_ns, reason="package registered")
        self._log_decision("package_registered", registered)
        return registered

    def rebuild_from_recovered_state(
        self,
        *,
        active_packages: Mapping[str, PackageExecution],
        relation_by_package: Mapping[str, str],
        now_ns: int,
    ) -> tuple[RiskDecision, ...]:
        self._exposure = InMemoryExposureTracker()
        self._active_relation_packages.clear()
        self._active_market_packages.clear()
        self._active_opportunity_to_package.clear()

        for package_id, package in active_packages.items():
            if package.state in {
                PackageState.COMPLETED,
                PackageState.ABORTED_UNWOUND,
                PackageState.FAILED,
                PackageState.HALTED,
            }:
                continue

            relation_id = relation_by_package.get(package_id, "recovered")
            rebuilt_state = _build_active_package_state_from_package_execution(
                package=package,
                relation_id=relation_id,
                package_execution_deadline_ms=self._config.package_execution_deadline_ms,
            )
            self._exposure.reserve_package(rebuilt_state)
            self._active_opportunity_to_package[rebuilt_state.opportunity_id] = rebuilt_state.package_id

            relation_set = self._active_relation_packages.setdefault(rebuilt_state.relation_id, set())
            relation_set.add(rebuilt_state.package_id)

            for market_id in rebuilt_state.market_notional_cents:
                market_set = self._active_market_packages.setdefault(market_id, set())
                market_set.add(rebuilt_state.package_id)

        self._metrics.increment("risk.recovery.rehydrated")
        self._metrics.observe("risk.recovery.active_packages", float(len(self._exposure.active_packages)))

        decisions = self.evaluate_active_risk(now_ns)
        for decision in decisions:
            self._log_decision("recovered_active_risk", decision)
        return decisions

    def close_package(
        self,
        *,
        package_id: str,
        now_ns: int,
        success: bool,
        realized_pnl_cents: int | None = None,
    ) -> tuple[RiskDecision, ...]:
        package = self._exposure.release_package(package_id)
        if package is None:
            return tuple()

        self._active_opportunity_to_package.pop(package.opportunity_id, None)
        relation_set = self._active_relation_packages.get(package.relation_id)
        if relation_set is not None:
            relation_set.discard(package_id)
            if not relation_set:
                self._active_relation_packages.pop(package.relation_id, None)

        for market_id in package.market_notional_cents:
            market_set = self._active_market_packages.get(market_id)
            if market_set is not None:
                market_set.discard(package_id)
                if not market_set:
                    self._active_market_packages.pop(market_id, None)

        decisions: list[RiskDecision] = []
        pnl = package.realized_pnl_cents if realized_pnl_cents is None else realized_pnl_cents
        self._daily_realized_pnl_cents += pnl

        if not success:
            self._failures_counter.add(now_ns)
            self._relation_cooldowns[package.relation_id] = now_ns + self._config.relation_cooldown_ms * _NS_PER_MS
            for market_id in package.market_notional_cents:
                self._market_cooldowns[market_id] = now_ns + self._config.market_cooldown_ms * _NS_PER_MS

            failure_rate_decision = self._maybe_trigger_failure_rate_halt(now_ns)
            if failure_rate_decision is not None:
                decisions.append(failure_rate_decision)

        if -self._daily_realized_pnl_cents >= self._config.daily_loss_cap_cents:
            decision = self._trigger_auto_kill(
                code=RiskReasonCode.DAILY_LOSS_CAP,
                reason=(
                    f"daily loss {-self._daily_realized_pnl_cents} reached cap "
                    f"{self._config.daily_loss_cap_cents}"
                ),
                now_ns=now_ns,
            )
            decisions.append(decision)

        if decisions:
            for item in decisions:
                self._log_decision("package_close_risk", item)

        return tuple(decisions)

    def on_state_event(
        self,
        package: PackageExecution,
        event: StateMachineEvent,
        now_ns: int,
    ) -> tuple[RiskDecision, ...]:
        decisions: list[RiskDecision] = []

        if isinstance(event, VenueFillEvent):
            active = self._exposure.apply_fill(
                package_id=event.package_id,
                leg_id=event.leg_id,
                fill_qty=event.fill_qty,
                fill_price_ticks=event.fill_price_ticks,
                fill_ts_ns=event.ts_ns,
            )
            if active is not None:
                if active.partial_fill_exposure_cents > self._config.max_partial_fill_exposure_cents:
                    decisions.append(
                        RiskDecision.deny(
                            code=RiskReasonCode.MAX_PARTIAL_FILL_EXPOSURE,
                            reason=(
                                f"partial-fill exposure {active.partial_fill_exposure_cents} exceeds cap "
                                f"{self._config.max_partial_fill_exposure_cents}"
                            ),
                            ts_ns=now_ns,
                            package_id=active.package_id,
                            relation_id=active.relation_id,
                            should_abort_package=True,
                        )
                    )

                leg_state = active.legs.get(event.leg_id)
                if leg_state is not None:
                    market_imbalance = abs(
                        self._exposure.open_directional_imbalance_by_market.get(leg_state.market_id, 0)
                    )
                    if market_imbalance > self._config.max_open_directional_imbalance_shares:
                        decisions.append(
                            RiskDecision.deny(
                                code=RiskReasonCode.MAX_OPEN_DIRECTIONAL_IMBALANCE,
                                reason=(
                                    f"directional imbalance {market_imbalance} for market {leg_state.market_id} "
                                    f"exceeds cap {self._config.max_open_directional_imbalance_shares}"
                                ),
                                ts_ns=now_ns,
                                package_id=active.package_id,
                                relation_id=active.relation_id,
                                should_abort_package=True,
                            )
                        )

        if isinstance(event, LegTimeoutEvent):
            active_timeout = self._exposure.active_packages.get(event.package_id)
            if active_timeout is not None and active_timeout.first_fill_ts_ns is not None and not active_timeout.all_legs_filled():
                decisions.append(
                    RiskDecision.deny(
                        code=RiskReasonCode.SECOND_LEG_TIMEOUT,
                        reason=(
                            f"leg timeout after first fill for package={event.package_id}; "
                            "unwind/abort required"
                        ),
                        ts_ns=now_ns,
                        package_id=event.package_id,
                        relation_id=active_timeout.relation_id,
                        should_abort_package=True,
                    )
                )

        if isinstance(event, VenueRejectEvent):
            self._reject_counter.add(now_ns)
            if self._reject_counter.count(now_ns) >= self._config.repeated_reject_threshold:
                decisions.append(
                    self._trigger_auto_kill(
                        code=RiskReasonCode.REPEATED_REJECT_THRESHOLD,
                        reason=(
                            f"reject count {self._reject_counter.count(now_ns)} reached threshold "
                            f"{self._config.repeated_reject_threshold}"
                        ),
                        now_ns=now_ns,
                    )
                )

        if package.state in {
            PackageState.COMPLETED,
            PackageState.ABORTED_UNWOUND,
            PackageState.FAILED,
            PackageState.HALTED,
        }:
            success = package.state == PackageState.COMPLETED
            decisions.extend(self.close_package(package_id=package.package_id, now_ns=now_ns, success=success))

        if decisions:
            for decision in decisions:
                self._log_decision("intra_trade_risk", decision)
        return tuple(decisions)

    def evaluate_active_risk(self, now_ns: int) -> tuple[RiskDecision, ...]:
        decisions: list[RiskDecision] = []

        for package in self._exposure.active_packages.values():
            if not package.deadline_breached and now_ns > package.deadline_ts_ns:
                package.deadline_breached = True
                decisions.append(
                    RiskDecision.deny(
                        code=RiskReasonCode.PACKAGE_EXECUTION_DEADLINE,
                        reason=(
                            f"package {package.package_id} exceeded execution deadline at "
                            f"{package.deadline_ts_ns}"
                        ),
                        ts_ns=now_ns,
                        package_id=package.package_id,
                        relation_id=package.relation_id,
                        should_abort_package=True,
                    )
                )

            if (
                package.first_fill_ts_ns is not None
                and not package.all_legs_filled()
                and not package.second_leg_timeout_breached
            ):
                timeout_ns = self._config.second_leg_timeout_ms * _NS_PER_MS
                if now_ns - package.first_fill_ts_ns > timeout_ns:
                    package.second_leg_timeout_breached = True
                    decisions.append(
                        RiskDecision.deny(
                            code=RiskReasonCode.SECOND_LEG_TIMEOUT,
                            reason=(
                                f"package {package.package_id} exceeded second-leg timeout "
                                f"{self._config.second_leg_timeout_ms}ms"
                            ),
                            ts_ns=now_ns,
                            package_id=package.package_id,
                            relation_id=package.relation_id,
                            should_abort_package=True,
                        )
                    )

        if decisions:
            for decision in decisions:
                self._log_decision("active_risk_scan", decision)

        return tuple(decisions)

    def record_venue_ambiguity(
        self,
        *,
        now_ns: int,
        detail: str,
        package_id: str | None = None,
    ) -> RiskDecision | None:
        self._ambiguity_counter.add(now_ns)
        current = self._ambiguity_counter.count(now_ns)
        self._logger.warning(
            "venue ambiguity recorded",
            count=current,
            threshold=self._config.repeated_venue_ambiguity_threshold,
            package_id=package_id,
            detail=detail,
        )
        self._metrics.increment("risk.venue_ambiguity")

        if current >= self._config.repeated_venue_ambiguity_threshold:
            decision = self._trigger_auto_kill(
                code=RiskReasonCode.REPEATED_VENUE_AMBIGUITY_THRESHOLD,
                reason=(
                    f"venue ambiguities {current} reached threshold "
                    f"{self._config.repeated_venue_ambiguity_threshold}: {detail}"
                ),
                now_ns=now_ns,
            )
            self._log_decision("ambiguity_threshold", decision)
            return decision
        return None

    def _check_leg_freshness_and_displayed_size(
        self,
        request: PreTradeRiskRequest,
        now_ns: int,
    ) -> RiskDecision | None:
        for leg in request.plan.legs:
            snapshot_age_ns = now_ns - leg.snapshot_ts_ns
            max_age_ns = self._config.max_leg_snapshot_age_ms * _NS_PER_MS
            if snapshot_age_ns < 0 or snapshot_age_ns > max_age_ns:
                self._stale_counter.add(now_ns)
                stale_count = self._stale_counter.count(now_ns)
                if stale_count >= self._config.repeated_stale_data_threshold:
                    return self._trigger_auto_kill(
                        code=RiskReasonCode.REPEATED_STALE_DATA_THRESHOLD,
                        reason=(
                            f"stale-data rejects {stale_count} reached threshold "
                            f"{self._config.repeated_stale_data_threshold}"
                        ),
                        now_ns=now_ns,
                    )

                return RiskDecision.deny(
                    code=RiskReasonCode.STALE_LEG_DATA,
                    reason=(
                        f"leg {leg.leg_id} snapshot age {snapshot_age_ns // _NS_PER_MS}ms exceeds "
                        f"limit {self._config.max_leg_snapshot_age_ms}ms"
                    ),
                    ts_ns=now_ns,
                    package_id=request.package_id,
                    relation_id=request.relation_id,
                )

            leg_units_ratio = max(1, leg.quantity // max(request.plan.package_units, 1))
            displayed_executable_shares = leg.available_units_at_plan * leg_units_ratio
            if displayed_executable_shares < self._config.min_displayed_executable_size_shares:
                return RiskDecision.deny(
                    code=RiskReasonCode.MIN_DISPLAYED_EXECUTABLE_SIZE,
                    reason=(
                        f"leg {leg.leg_id} displayed executable shares {displayed_executable_shares} "
                        f"below minimum {self._config.min_displayed_executable_size_shares}"
                    ),
                    ts_ns=now_ns,
                    package_id=request.package_id,
                    relation_id=request.relation_id,
                )
        return None

    def _maybe_trigger_failure_rate_halt(self, now_ns: int) -> RiskDecision | None:
        starts = self._starts_counter.count(now_ns)
        failures = self._failures_counter.count(now_ns)
        if starts < self._config.failure_rate_min_sample:
            return None
        failure_rate = failures / starts
        if failure_rate < self._config.failure_rate_halt_threshold:
            return None

        return self._trigger_auto_kill(
            code=RiskReasonCode.FAILURE_RATE_SPIKE,
            reason=(
                f"failure rate {failure_rate:.3f} with failures={failures}, starts={starts} "
                f"exceeds threshold {self._config.failure_rate_halt_threshold:.3f}"
            ),
            now_ns=now_ns,
        )

    def _trading_block_reject(self, now_ns: int) -> RiskDecision | None:
        if self._manual_kill_active:
            return RiskDecision.deny(
                code=RiskReasonCode.MANUAL_KILL_SWITCH,
                reason=self._manual_kill_reason or "manual kill switch active",
                ts_ns=now_ns,
                should_halt_trading=True,
            )
        if self._auto_kill_active:
            return RiskDecision.deny(
                code=RiskReasonCode.AUTO_KILL_SWITCH,
                reason=self._auto_kill_reason or "auto kill switch active",
                ts_ns=now_ns,
                should_halt_trading=True,
            )
        return None

    def _trigger_auto_kill(self, *, code: RiskReasonCode, reason: str, now_ns: int) -> RiskDecision:
        if not self._config.enable_auto_kill_switch:
            return RiskDecision.deny(
                code=code,
                reason=reason,
                ts_ns=now_ns,
            )

        self._auto_kill_active = True
        self._auto_kill_reason = reason
        self._kill_activated_ts_ns = now_ns
        self._metrics.increment("risk.kill_switch.auto", tags={"code": code.value})
        self._logger.error("auto kill switch activated", code=code.value, reason=reason)
        return RiskDecision.deny(
            code=code,
            reason=reason,
            ts_ns=now_ns,
            should_halt_trading=True,
        )

    def _log_decision(self, event_name: str, decision: RiskDecision) -> None:
        fields = {
            "allowed": decision.allowed,
            "code": decision.code.value if decision.code else None,
            "reason": decision.reason,
            "package_id": decision.package_id,
            "relation_id": decision.relation_id,
            "should_abort_package": decision.should_abort_package,
            "should_halt_trading": decision.should_halt_trading,
        }
        if decision.allowed:
            self._logger.debug(event_name, **fields)
        else:
            self._logger.warning(event_name, **fields)
            if decision.code is not None:
                self._metrics.increment("risk.decision.reject", tags={"code": decision.code.value})

    @staticmethod
    def _validate_request(request: PreTradeRiskRequest, now_ns: int) -> RiskDecision | None:
        if not request.package_id:
            return RiskDecision.deny(
                code=RiskReasonCode.INVALID_REQUEST,
                reason="package_id must be non-empty",
                ts_ns=now_ns,
            )
        if not request.relation_id:
            return RiskDecision.deny(
                code=RiskReasonCode.INVALID_REQUEST,
                reason="relation_id must be non-empty",
                ts_ns=now_ns,
                package_id=request.package_id,
            )
        if not request.plan.legs:
            return RiskDecision.deny(
                code=RiskReasonCode.INVALID_REQUEST,
                reason="plan must have at least one leg",
                ts_ns=now_ns,
                package_id=request.package_id,
                relation_id=request.relation_id,
            )
        return None

    @staticmethod
    def _validate_config(config: RiskManagerConfig) -> None:
        integer_positive_fields: Sequence[tuple[str, int]] = (
            ("max_notional_per_package_cents", config.max_notional_per_package_cents),
            ("max_notional_per_market_cents", config.max_notional_per_market_cents),
            ("max_total_open_notional_cents", config.max_total_open_notional_cents),
            ("max_simultaneous_active_packages", config.max_simultaneous_active_packages),
            ("min_displayed_executable_size_shares", config.min_displayed_executable_size_shares),
            ("max_leg_snapshot_age_ms", config.max_leg_snapshot_age_ms),
            ("relation_cooldown_ms", config.relation_cooldown_ms),
            ("market_cooldown_ms", config.market_cooldown_ms),
            ("second_leg_timeout_ms", config.second_leg_timeout_ms),
            ("max_partial_fill_exposure_cents", config.max_partial_fill_exposure_cents),
            ("max_open_directional_imbalance_shares", config.max_open_directional_imbalance_shares),
            ("package_execution_deadline_ms", config.package_execution_deadline_ms),
            ("failure_rate_window_ms", config.failure_rate_window_ms),
            ("failure_rate_min_sample", config.failure_rate_min_sample),
            ("daily_loss_cap_cents", config.daily_loss_cap_cents),
            ("repeated_reject_threshold", config.repeated_reject_threshold),
            ("repeated_venue_ambiguity_threshold", config.repeated_venue_ambiguity_threshold),
            ("repeated_stale_data_threshold", config.repeated_stale_data_threshold),
            ("repeated_event_window_ms", config.repeated_event_window_ms),
        )
        for field_name, value in integer_positive_fields:
            if value <= 0:
                raise ValueError(f"{field_name} must be > 0")

        if config.min_expected_net_profit_cents < 0:
            raise ValueError("min_expected_net_profit_cents must be >= 0")
        if not 0.0 <= config.failure_rate_halt_threshold <= 1.0:
            raise ValueError("failure_rate_halt_threshold must be in [0, 1]")


def apply_state_transition_with_risk(
    *,
    risk_manager: ExecutionRiskManager,
    package: PackageExecution,
    event: StateMachineEvent,
    now_ns: int,
) -> tuple[PackageExecution, tuple[RiskDecision, ...]]:
    """Example integration helper for executor state machine + risk layer.

    Integration pattern:
    1) state-machine transition determines canonical package state
    2) risk manager ingests resulting state/event and emits risk decisions
    """

    next_package = PackageStateMachine.transition(package, event)
    risk_decisions = risk_manager.on_state_event(next_package, event, now_ns)
    return next_package, risk_decisions


def _market_notional_from_plan(plan: ExecutionPlan) -> dict[str, int]:
    market_notional: dict[str, int] = {}
    for leg in plan.legs:
        leg_notional = leg.quantity * leg.limit_price_ticks
        market_notional[leg.market_id] = market_notional.get(leg.market_id, 0) + leg_notional
    return market_notional


def _market_ids_from_plan(plan: ExecutionPlan) -> tuple[str, ...]:
    return tuple({leg.market_id for leg in plan.legs})


def _build_active_package_state(
    *,
    request: PreTradeRiskRequest,
    now_ns: int,
    package_execution_deadline_ms: int,
) -> _ActivePackageRiskState:
    market_notional = _market_notional_from_plan(request.plan)
    leg_states: dict[str, _ActiveLegRiskState] = {}
    for leg in request.plan.legs:
        leg_states[leg.leg_id] = _ActiveLegRiskState(
            leg_id=leg.leg_id,
            market_id=leg.market_id,
            side=leg.side,
            intended_qty=leg.quantity,
        )

    plan_deadline_ns = now_ns + min(package_execution_deadline_ms, request.plan.package_timeout_ms) * _NS_PER_MS
    deadline_ts_ns = min(plan_deadline_ns, request.plan.expires_at_ns)
    return _ActivePackageRiskState(
        package_id=request.package_id,
        relation_id=request.relation_id,
        opportunity_id=request.plan.opportunity_id,
        opened_ts_ns=now_ns,
        deadline_ts_ns=deadline_ts_ns,
        market_notional_cents=market_notional,
        legs=leg_states,
        total_reserved_notional_cents=request.plan.total_notional_cents,
    )


def _build_active_package_state_from_package_execution(
    *,
    package: PackageExecution,
    relation_id: str,
    package_execution_deadline_ms: int,
) -> _ActivePackageRiskState:
    intended_legs = [leg for leg in package.legs if leg.leg_id in package.intended_leg_ids]
    if not intended_legs:
        raise ValueError(f"cannot rebuild recovered package without intended legs: {package.package_id}")

    market_notional: dict[str, int] = {}
    leg_states: dict[str, _ActiveLegRiskState] = {}
    buy_filled_notional_cents = 0
    sell_filled_notional_cents = 0
    first_fill_ts_ns: int | None = None
    market_directional_shares: dict[str, int] = {}

    for leg in intended_legs:
        market_notional[leg.market_id] = market_notional.get(leg.market_id, 0) + (leg.intended_qty * leg.limit_price_ticks)

        filled_qty = max(0, min(leg.filled_qty, leg.intended_qty))
        leg_states[leg.leg_id] = _ActiveLegRiskState(
            leg_id=leg.leg_id,
            market_id=leg.market_id,
            side=leg.side,
            intended_qty=leg.intended_qty,
            filled_qty=filled_qty,
        )

        if filled_qty <= 0:
            continue

        fill_price_ticks = leg.avg_fill_price_ticks or leg.last_fill_price_ticks or leg.limit_price_ticks
        fill_notional = filled_qty * fill_price_ticks
        if leg.side == Side.BUY:
            buy_filled_notional_cents += fill_notional
            signed_qty = filled_qty
        else:
            sell_filled_notional_cents += fill_notional
            signed_qty = -filled_qty
        market_directional_shares[leg.market_id] = market_directional_shares.get(leg.market_id, 0) + signed_qty

        first_fill_candidate = leg.ack_ts_ns or leg.submit_ts_ns or package.updated_ts_ns
        if first_fill_ts_ns is None or first_fill_candidate < first_fill_ts_ns:
            first_fill_ts_ns = first_fill_candidate

    deadline_ts_ns = package.created_ts_ns + package_execution_deadline_ms * _NS_PER_MS
    return _ActivePackageRiskState(
        package_id=package.package_id,
        relation_id=relation_id,
        opportunity_id=package.opportunity_id,
        opened_ts_ns=package.created_ts_ns,
        deadline_ts_ns=deadline_ts_ns,
        market_notional_cents=market_notional,
        legs=leg_states,
        total_reserved_notional_cents=sum(market_notional.values()),
        buy_filled_notional_cents=buy_filled_notional_cents,
        sell_filled_notional_cents=sell_filled_notional_cents,
        first_fill_ts_ns=first_fill_ts_ns,
        market_directional_shares=market_directional_shares,
    )


def build_execution_plan(
    *,
    plan_id: str,
    opportunity_id: str,
    created_ts_ns: int,
    expires_at_ns: int,
    package_timeout_ms: int,
    package_units: int,
    total_shares: int,
    total_notional_cents: int,
    expected_net_profit_cents: int,
    legs: tuple[PlannedLeg, ...],
) -> ExecutionPlan:
    """Convenience helper useful for tests and fixtures."""

    expected_gross_profit_cents = expected_net_profit_cents
    expected_fee_cents = 0
    expected_net_edge_bps = (
        (expected_net_profit_cents * 10_000) // total_notional_cents if total_notional_cents > 0 else 0
    )
    from .state_machine import TimeInForce

    return ExecutionPlan(
        plan_id=plan_id,
        opportunity_id=opportunity_id,
        created_ts_ns=created_ts_ns,
        expires_at_ns=expires_at_ns,
        package_timeout_ms=package_timeout_ms,
        package_units=package_units,
        tif=TimeInForce.IOC,
        total_shares=total_shares,
        total_notional_cents=total_notional_cents,
        expected_gross_profit_cents=expected_gross_profit_cents,
        expected_fee_cents=expected_fee_cents,
        expected_net_profit_cents=expected_net_profit_cents,
        expected_net_edge_bps=expected_net_edge_bps,
        legs=legs,
    )