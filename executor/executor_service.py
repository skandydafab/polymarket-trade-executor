from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from time import perf_counter
from typing import Callable, Mapping, Protocol

from .journal import ExecutorJournal, RecoveryCoordinator, RecoveryResult
from .planner import ExecutionPlan, ExecutionPlanner, Opportunity, PlannedLeg, PlannerRejectCode, PricingSnapshot
from .risk import ExecutionRiskManager, PreTradeRiskRequest, RiskDecision, RiskReasonCode
from .state_machine import (
    CancelRequestedEvent,
    EventValidationError,
    LegExecution,
    LegTimeoutEvent,
    OrderIntent,
    OrderState,
    PackageAbortEvent,
    PackageExecution,
    PackageState,
    PackageStateMachine,
    PackageUnwindRequiredEvent,
    StateMachineEvent,
    StateTransitionError,
    VenueCancelEvent,
    VenueFillEvent,
    VenueOrderAck,
    VenueRejectEvent,
    create_package_execution,
    leg_by_id,
)
from .venue import (
    ClientOrderIdFactory,
    ClientOrderIdFactoryConfig,
    MetricsSink,
    NormalizedOrderEvent,
    NullLogger,
    NullMetrics,
    StructuredLogger,
    SubmitOrderStatus,
    VenueAdapter,
    VenueOrderIntent,
)


_NS_PER_MS = 1_000_000


class ControlActionType(str, Enum):
    CANCEL_ORDER = "CANCEL_ORDER"
    REQUIRE_UNWIND = "REQUIRE_UNWIND"
    ABORT_PACKAGE = "ABORT_PACKAGE"


class UnwindHandler(Protocol):
    def on_unwind_required(self, *, package: PackageExecution, reason: str, now_ns: int) -> None: ...


@dataclass(frozen=True, slots=True)
class ExecutorServiceConfig:
    service_name: str = "executor"
    client_order_id_prefix: str = "exec"
    client_order_id_max_length: int = 96
    default_leg_timeout_ms: int = 800
    poll_venue_updates_on_tick: bool = True
    max_control_actions_per_tick: int = 128
    max_control_queue_size: int = 10_000
    auto_cancel_on_timeout: bool = True
    auto_cancel_on_abort: bool = True
    halt_after_recovery_with_active_packages: bool = True
    journal_errors_halt_trading: bool = False


@dataclass(frozen=True, slots=True)
class SubmittedLegInfo:
    leg_id: str
    client_order_id: str
    quantity: int
    limit_price_ticks: int


@dataclass(frozen=True, slots=True)
class OpportunityExecutionResult:
    accepted: bool
    package_id: str | None
    reason: str
    planner_reject_code: PlannerRejectCode | None = None
    risk_reject_code: RiskReasonCode | None = None
    submitted_legs: tuple[SubmittedLegInfo, ...] = tuple()


@dataclass(frozen=True, slots=True)
class ExecutorRuntimeSnapshot:
    is_running: bool
    is_halted: bool
    active_package_ids: tuple[str, ...]
    terminal_package_ids: tuple[str, ...]
    open_order_client_ids: tuple[str, ...]


@dataclass(slots=True)
class _PackageRuntime:
    package_id: str
    relation_id: str
    opportunity_id: str
    leg_timeout_ms_by_leg_id: dict[str, int]
    submission_attempts_by_leg_id: dict[str, int] = field(default_factory=dict)


@dataclass(slots=True)
class _OpenOrderRef:
    client_order_id: str
    package_id: str
    leg_id: str
    submitted_ts_ns: int
    last_update_ts_ns: int
    timeout_ms: int
    timeout_fired: bool = False


@dataclass(frozen=True, slots=True)
class _ControlAction:
    action_type: ControlActionType
    package_id: str
    reason: str
    leg_id: str | None = None
    client_order_id: str | None = None


class ExecutorService:
    """Top-level execution orchestrator.

    Hot-path caution:
    - this service is intentionally single-writer; call methods from one event loop
      or protect with an external mailbox/actor.
    - journaling is wrapped via _safe_journal_* helpers so persistence failures do
      not directly crash the trading loop.
    - control actions are queued to keep fast-path state transitions small and
      deterministic.
    """

    __slots__ = (
        "_planner",
        "_risk_manager",
        "_venue_adapter",
        "_journal",
        "_recovery_coordinator",
        "_logger",
        "_metrics",
        "_config",
        "_client_order_id_factory",
        "_unwind_handler",
        "_running",
        "_package_sequence",
        "_active_packages",
        "_terminal_packages",
        "_runtime_by_package",
        "_open_orders",
        "_open_orders_by_package",
        "_control_actions",
        "_pending_abort_packages",
        "_pending_unwind_packages",
    )

    def __init__(
        self,
        *,
        planner: ExecutionPlanner,
        risk_manager: ExecutionRiskManager,
        venue_adapter: VenueAdapter,
        journal: ExecutorJournal,
        recovery_coordinator: RecoveryCoordinator,
        logger: StructuredLogger | None = None,
        metrics: MetricsSink | None = None,
        config: ExecutorServiceConfig | None = None,
        unwind_handler: UnwindHandler | None = None,
    ) -> None:
        self._planner = planner
        self._risk_manager = risk_manager
        self._venue_adapter = venue_adapter
        self._journal = journal
        self._recovery_coordinator = recovery_coordinator
        self._logger = logger or NullLogger()
        self._metrics = metrics or NullMetrics()
        self._config = config or ExecutorServiceConfig()
        self._client_order_id_factory = ClientOrderIdFactory(
            ClientOrderIdFactoryConfig(
                prefix=self._config.client_order_id_prefix,
                max_length=self._config.client_order_id_max_length,
            )
        )
        self._unwind_handler = unwind_handler

        self._running = False
        self._package_sequence = 0

        self._active_packages: dict[str, PackageExecution] = {}
        self._terminal_packages: dict[str, PackageExecution] = {}
        self._runtime_by_package: dict[str, _PackageRuntime] = {}
        self._open_orders: dict[str, _OpenOrderRef] = {}
        self._open_orders_by_package: dict[str, set[str]] = {}

        self._control_actions: deque[_ControlAction] = deque()
        self._pending_abort_packages: set[str] = set()
        self._pending_unwind_packages: set[str] = set()

    @property
    def is_running(self) -> bool:
        return self._running

    @property
    def is_halted(self) -> bool:
        return self._risk_manager.is_trading_halted

    def start(self) -> None:
        self._running = True
        self._metrics.increment("executor.lifecycle.start")
        self._logger.info("executor started")

    def stop(self) -> None:
        self._running = False
        self._metrics.increment("executor.lifecycle.stop")
        self._logger.info("executor stopped")

    def activate_manual_halt(self, *, reason: str, now_ns: int) -> RiskDecision:
        decision = self._risk_manager.activate_manual_kill_switch(reason=reason, now_ns=now_ns)
        self._safe_journal(
            "record_kill_switch_activation_manual",
            lambda: self._journal.record_kill_switch_activation(reason=reason, ts_ns=now_ns, automatic=False),
        )
        self._metrics.increment("executor.halt.manual")
        return decision

    def clear_manual_halt(self, *, now_ns: int) -> RiskDecision:
        self._metrics.increment("executor.halt.manual_clear")
        return self._risk_manager.clear_manual_kill_switch(now_ns=now_ns)

    def snapshot(self) -> ExecutorRuntimeSnapshot:
        return ExecutorRuntimeSnapshot(
            is_running=self._running,
            is_halted=self.is_halted,
            active_package_ids=tuple(sorted(self._active_packages)),
            terminal_package_ids=tuple(sorted(self._terminal_packages)),
            open_order_client_ids=tuple(sorted(self._open_orders)),
        )

    def get_package(self, package_id: str) -> PackageExecution | None:
        package = self._active_packages.get(package_id)
        if package is not None:
            return package
        return self._terminal_packages.get(package_id)

    def recover(
        self,
        *,
        now_ns: int,
        start_partition: str | None = None,
        end_partition: str | None = None,
    ) -> RecoveryResult:
        result = self._recovery_coordinator.recover(
            venue_adapter=self._venue_adapter,
            start_partition=start_partition,
            end_partition=end_partition,
        )

        self._active_packages = dict(result.reconstructed_state.active_packages)
        self._terminal_packages = dict(result.reconstructed_state.terminal_packages)
        self._runtime_by_package.clear()
        self._open_orders.clear()
        self._open_orders_by_package.clear()

        for package_id, package in self._active_packages.items():
            relation_id = result.reconstructed_state.relation_by_package.get(package_id, "recovered")
            timeout_by_leg = {
                leg.leg_id: self._config.default_leg_timeout_ms for leg in package.legs
            }
            self._runtime_by_package[package_id] = _PackageRuntime(
                package_id=package_id,
                relation_id=relation_id,
                opportunity_id=package.opportunity_id,
                leg_timeout_ms_by_leg_id=timeout_by_leg,
            )
            for leg in package.legs:
                if leg.client_order_id and leg.state in {
                    OrderState.PENDING_ACK,
                    OrderState.WORKING,
                    OrderState.PARTIALLY_FILLED,
                    OrderState.CANCEL_REQUESTED,
                }:
                    submitted_ts_ns = leg.submit_ts_ns if leg.submit_ts_ns is not None else now_ns
                    self._open_orders[leg.client_order_id] = _OpenOrderRef(
                        client_order_id=leg.client_order_id,
                        package_id=package_id,
                        leg_id=leg.leg_id,
                        submitted_ts_ns=submitted_ts_ns,
                        last_update_ts_ns=package.updated_ts_ns,
                        timeout_ms=self._config.default_leg_timeout_ms,
                    )
                    package_open = self._open_orders_by_package.setdefault(package_id, set())
                    package_open.add(leg.client_order_id)

        rehydration_decisions = self._risk_manager.rebuild_from_recovered_state(
            active_packages=self._active_packages,
            relation_by_package=result.reconstructed_state.relation_by_package,
            now_ns=now_ns,
        )
        self._handle_risk_decisions(rehydration_decisions, now_ns)

        ambiguous_recovery_actions = [
            action
            for action in result.actions
            if action.action_type in {"UNKNOWN_OPEN_ORDER", "MISSING_EXPECTED_ORDER"}
        ]
        if ambiguous_recovery_actions and not self.is_halted:
            self.activate_manual_halt(
                reason=(
                    "recovery found ambiguous open-order state; "
                    f"actions={len(ambiguous_recovery_actions)}"
                ),
                now_ns=now_ns,
            )

        if result.reconstructed_state.kill_switch_active:
            reason = result.reconstructed_state.kill_switch_reason or "recovered kill switch active"
            self._risk_manager.activate_manual_kill_switch(reason=reason, now_ns=now_ns)

        if self._config.halt_after_recovery_with_active_packages and self._active_packages:
            self._risk_manager.activate_manual_kill_switch(
                reason="recovery found active packages; operator confirmation required",
                now_ns=now_ns,
            )
            self._safe_journal(
                "record_kill_switch_activation_recovery",
                lambda: self._journal.record_kill_switch_activation(
                    reason="recovery found active packages",
                    ts_ns=now_ns,
                    automatic=False,
                ),
            )

        self._metrics.increment("executor.recovery.runs")
        self._metrics.observe("executor.recovery.active_packages", float(len(self._active_packages)))
        self._logger.info(
            "executor recovery completed",
            records_loaded=result.records_loaded,
            active_packages=len(self._active_packages),
            open_orders=len(self._open_orders),
        )
        return result

    def on_opportunity(
        self,
        *,
        relation_id: str,
        opportunity: Opportunity,
        snapshots: Mapping[str, PricingSnapshot],
        now_ns: int,
    ) -> OpportunityExecutionResult:
        started = perf_counter()
        if not self._running:
            return OpportunityExecutionResult(
                accepted=False,
                package_id=None,
                reason="executor not running",
            )

        if self.is_halted:
            self._safe_journal(
                "record_opportunity_rejected_halted",
                lambda: self._journal.record_opportunity_rejected(
                    opportunity_id=opportunity.opportunity_id,
                    relation_id=relation_id,
                    reason="trading halted",
                    ts_ns=now_ns,
                ),
            )
            return OpportunityExecutionResult(
                accepted=False,
                package_id=None,
                reason="trading halted",
            )

        plan_decision = self._planner.plan(opportunity, snapshots, now_ns)
        if plan_decision.plan is None:
            rejection = plan_decision.rejection
            reason = rejection.reason if rejection is not None else "planner rejection"
            self._safe_journal(
                "record_opportunity_rejected_planner",
                lambda: self._journal.record_opportunity_rejected(
                    opportunity_id=opportunity.opportunity_id,
                    relation_id=relation_id,
                    reason=reason,
                    ts_ns=now_ns,
                ),
            )
            self._metrics.increment(
                "executor.opportunity.rejected.planner",
                tags={"code": rejection.code.value if rejection is not None else "UNKNOWN"},
            )
            return OpportunityExecutionResult(
                accepted=False,
                package_id=None,
                reason=reason,
                planner_reject_code=rejection.code if rejection is not None else None,
            )

        plan = plan_decision.plan
        package_id = self._next_package_id(opportunity_id=opportunity.opportunity_id, now_ns=now_ns)
        pretrade = self._risk_manager.register_package(
            PreTradeRiskRequest(package_id=package_id, relation_id=relation_id, plan=plan),
            now_ns,
        )
        if not pretrade.allowed:
            self._safe_journal(
                "record_opportunity_rejected_risk",
                lambda: self._journal.record_opportunity_rejected(
                    opportunity_id=opportunity.opportunity_id,
                    relation_id=relation_id,
                    reason=pretrade.reason,
                    ts_ns=now_ns,
                ),
            )
            self._handle_risk_decisions((pretrade,), now_ns)
            self._metrics.increment(
                "executor.opportunity.rejected.risk",
                tags={"code": pretrade.code.value if pretrade.code is not None else "UNKNOWN"},
            )
            return OpportunityExecutionResult(
                accepted=False,
                package_id=None,
                reason=pretrade.reason,
                risk_reject_code=pretrade.code,
            )

        package = self._package_from_plan(package_id=package_id, plan=plan, now_ns=now_ns)
        self._active_packages[package_id] = package
        self._runtime_by_package[package_id] = _PackageRuntime(
            package_id=package_id,
            relation_id=relation_id,
            opportunity_id=plan.opportunity_id,
            leg_timeout_ms_by_leg_id={leg.leg_id: leg.timeout_ms for leg in plan.legs},
        )

        self._safe_journal(
            "record_opportunity_accepted",
            lambda: self._journal.record_opportunity_accepted(
                package_id=package_id,
                opportunity_id=opportunity.opportunity_id,
                relation_id=relation_id,
                ts_ns=now_ns,
                metadata={"expected_edge_bps": opportunity.expected_edge_bps},
            ),
        )
        self._safe_journal(
            "record_execution_plan_created",
            lambda: self._journal.record_execution_plan_created(
                package_id=package_id,
                relation_id=relation_id,
                plan=plan,
                ts_ns=now_ns,
            ),
        )

        submissions: list[SubmittedLegInfo] = []
        for planned_leg in sorted(plan.legs, key=lambda item: item.submission_rank):
            submitted = self._submit_planned_leg(
                package_id=package_id,
                planned_leg=planned_leg,
                now_ns=now_ns,
            )
            if submitted is not None:
                submissions.append(submitted)
            if package_id not in self._active_packages:
                break

        elapsed_ms = (perf_counter() - started) * 1000.0
        self._metrics.observe("executor.on_opportunity.latency_ms", elapsed_ms)
        self._metrics.increment("executor.opportunity.accepted")
        self._logger.info(
            "opportunity accepted",
            package_id=package_id,
            relation_id=relation_id,
            legs_submitted=len(submissions),
            latency_ms=elapsed_ms,
        )
        return OpportunityExecutionResult(
            accepted=True,
            package_id=package_id,
            reason="accepted",
            submitted_legs=tuple(submissions),
        )

    def on_venue_event(self, event: NormalizedOrderEvent, *, now_ns: int) -> None:
        package = self._active_packages.get(event.package_id)
        if package is None:
            if isinstance(event, VenueFillEvent):
                scope = "terminal" if event.package_id in self._terminal_packages else "unknown"
                self._metrics.increment("executor.venue_event.late_fill", tags={"scope": scope})
                self._safe_journal(
                    "record_order_fill_late",
                    lambda: self._journal.record_order_fill(event, relation_id=None),
                )

                if not self.is_halted:
                    self.activate_manual_halt(
                        reason=f"late fill for non-active package package_id={event.package_id} scope={scope}",
                        now_ns=now_ns,
                    )

                self._logger.error(
                    "late fill received for non-active package",
                    package_id=event.package_id,
                    scope=scope,
                    leg_id=event.leg_id,
                    client_order_id=event.client_order_id,
                    fill_qty=event.fill_qty,
                )
                return

            self._metrics.increment("executor.venue_event.unknown_package")
            self._logger.debug(
                "venue event ignored for unknown package",
                package_id=event.package_id,
                event_type=type(event).__name__,
            )
            return

        runtime = self._runtime_by_package.get(event.package_id)
        relation_id = runtime.relation_id if runtime is not None else "unknown"
        self._apply_state_event(
            package_id=event.package_id,
            relation_id=relation_id,
            event=event,
            now_ns=now_ns,
        )

    def on_timer_tick(self, *, now_ns: int) -> None:
        if not self._running:
            return

        tick_started = perf_counter()
        self._drain_control_actions(now_ns=now_ns)

        if self._config.poll_venue_updates_on_tick:
            updates = self._venue_adapter.poll_or_process_order_updates()
            for update_event in updates:
                self.on_venue_event(update_event, now_ns=now_ns)

        self._check_open_order_timeouts(now_ns=now_ns)

        risk_decisions = self._risk_manager.evaluate_active_risk(now_ns)
        self._handle_risk_decisions(risk_decisions, now_ns)

        self._drain_control_actions(now_ns=now_ns)
        self._metrics.observe("executor.on_timer_tick.latency_ms", (perf_counter() - tick_started) * 1000.0)

    def _submit_planned_leg(
        self,
        *,
        package_id: str,
        planned_leg: PlannedLeg,
        now_ns: int,
    ) -> SubmittedLegInfo | None:
        package = self._active_packages.get(package_id)
        runtime = self._runtime_by_package.get(package_id)
        if package is None or runtime is None:
            return None

        attempt = runtime.submission_attempts_by_leg_id.get(planned_leg.leg_id, 0) + 1
        runtime.submission_attempts_by_leg_id[planned_leg.leg_id] = attempt
        client_order_id = self._build_client_order_id(
            package_id=package_id,
            leg_id=planned_leg.leg_id,
            attempt=attempt,
            now_ns=now_ns,
        )

        intent_event = OrderIntent(
            package_id=package_id,
            leg_id=planned_leg.leg_id,
            client_order_id=client_order_id,
            qty=planned_leg.quantity,
            limit_price_ticks=planned_leg.limit_price_ticks,
            tif=planned_leg.tif,
            ts_ns=now_ns,
        )
        self._apply_state_event(
            package_id=package_id,
            relation_id=runtime.relation_id,
            event=intent_event,
            now_ns=now_ns,
        )

        if package_id not in self._active_packages:
            return None

        # Hot-path caution: submit is synchronous; keep request shaping minimal.
        venue_intent = VenueOrderIntent(
            package_id=package_id,
            leg_id=planned_leg.leg_id,
            market_id=planned_leg.market_id,
            token_id=planned_leg.token_id,
            side=planned_leg.side,
            quantity=planned_leg.quantity,
            limit_price_ticks=planned_leg.limit_price_ticks,
            tif=planned_leg.tif,
            ts_ns=now_ns,
            expires_at_ns=None,
            client_order_id=client_order_id,
        )

        try:
            submit_result = self._venue_adapter.submit_order(venue_intent)
        except Exception as exc:
            self._metrics.increment("executor.submit.exception")
            decision = self._risk_manager.record_venue_ambiguity(
                now_ns=now_ns,
                detail=f"submit exception: {exc}",
                package_id=package_id,
            )
            if decision is not None:
                self._handle_risk_decisions((decision,), now_ns)
            self._enqueue_control(
                _ControlAction(
                    action_type=ControlActionType.REQUIRE_UNWIND,
                    package_id=package_id,
                    reason=f"submit exception for leg={planned_leg.leg_id}",
                    leg_id=planned_leg.leg_id,
                )
            )
            return SubmittedLegInfo(
                leg_id=planned_leg.leg_id,
                client_order_id=client_order_id,
                quantity=planned_leg.quantity,
                limit_price_ticks=planned_leg.limit_price_ticks,
            )

        if submit_result.status in {SubmitOrderStatus.AMBIGUOUS, SubmitOrderStatus.FAILED_RETRYABLE}:
            decision = self._risk_manager.record_venue_ambiguity(
                now_ns=now_ns,
                detail=f"submit status={submit_result.status.value}",
                package_id=package_id,
            )
            if decision is not None:
                self._handle_risk_decisions((decision,), now_ns)
            self._enqueue_control(
                _ControlAction(
                    action_type=ControlActionType.REQUIRE_UNWIND,
                    package_id=package_id,
                    reason=f"ambiguous submit for leg={planned_leg.leg_id}",
                    leg_id=planned_leg.leg_id,
                )
            )

        if submit_result.client_order_id != client_order_id:
            open_ref = self._open_orders.pop(client_order_id, None)
            if open_ref is not None:
                self._remove_open_order_index(package_id=package_id, client_order_id=client_order_id)
                open_ref.client_order_id = submit_result.client_order_id
                self._open_orders[submit_result.client_order_id] = open_ref
                package_open = self._open_orders_by_package.setdefault(package_id, set())
                package_open.add(submit_result.client_order_id)
            client_order_id = submit_result.client_order_id

        for event in submit_result.events:
            self.on_venue_event(event, now_ns=now_ns)

        return SubmittedLegInfo(
            leg_id=planned_leg.leg_id,
            client_order_id=client_order_id,
            quantity=planned_leg.quantity,
            limit_price_ticks=planned_leg.limit_price_ticks,
        )

    def _apply_state_event(
        self,
        *,
        package_id: str,
        relation_id: str,
        event: StateMachineEvent,
        now_ns: int,
    ) -> None:
        package = self._active_packages.get(package_id)
        if package is None:
            return

        try:
            next_package = PackageStateMachine.transition(package, event)
        except (StateTransitionError, EventValidationError) as exc:
            self._metrics.increment("executor.state_transition.error", tags={"event": type(event).__name__})
            self._logger.error(
                "state transition error",
                package_id=package_id,
                event_type=type(event).__name__,
                error=str(exc),
            )
            self._enqueue_control(
                _ControlAction(
                    action_type=ControlActionType.ABORT_PACKAGE,
                    package_id=package_id,
                    reason=f"state transition error: {exc}",
                )
            )
            return

        self._active_packages[package_id] = next_package
        self._journal_event(relation_id=relation_id, event=event)
        self._sync_open_order_tracking(next_package=next_package, event=event)

        decisions = self._risk_manager.on_state_event(next_package, event, now_ns)
        self._handle_risk_decisions(decisions, now_ns)

        if next_package.state == PackageState.UNWIND_REQUIRED:
            self._enqueue_control(
                _ControlAction(
                    action_type=ControlActionType.REQUIRE_UNWIND,
                    package_id=package_id,
                    reason="package entered UNWIND_REQUIRED",
                )
            )

        if next_package.state in {
            PackageState.COMPLETED,
            PackageState.ABORTED_UNWOUND,
            PackageState.FAILED,
            PackageState.HALTED,
        }:
            self._finalize_terminal_package(next_package)

    def _sync_open_order_tracking(self, *, next_package: PackageExecution, event: StateMachineEvent) -> None:
        if not hasattr(event, "leg_id"):
            return

        leg_id = getattr(event, "leg_id", None)
        if not isinstance(leg_id, str):
            return

        try:
            leg = leg_by_id(next_package, leg_id)
        except EventValidationError:
            return

        if leg.client_order_id is None:
            return

        open_states = {
            OrderState.PENDING_ACK,
            OrderState.WORKING,
            OrderState.PARTIALLY_FILLED,
            OrderState.CANCEL_REQUESTED,
        }
        if leg.state in open_states:
            timeout_ms = self._config.default_leg_timeout_ms
            runtime = self._runtime_by_package.get(next_package.package_id)
            if runtime is not None:
                timeout_ms = runtime.leg_timeout_ms_by_leg_id.get(leg.leg_id, timeout_ms)

            existing = self._open_orders.get(leg.client_order_id)
            if existing is None:
                submitted_ts_ns = leg.submit_ts_ns if leg.submit_ts_ns is not None else next_package.updated_ts_ns
                self._open_orders[leg.client_order_id] = _OpenOrderRef(
                    client_order_id=leg.client_order_id,
                    package_id=next_package.package_id,
                    leg_id=leg.leg_id,
                    submitted_ts_ns=submitted_ts_ns,
                    last_update_ts_ns=next_package.updated_ts_ns,
                    timeout_ms=timeout_ms,
                )
            else:
                existing.last_update_ts_ns = next_package.updated_ts_ns

            package_open = self._open_orders_by_package.setdefault(next_package.package_id, set())
            package_open.add(leg.client_order_id)
            return

        self._open_orders.pop(leg.client_order_id, None)
        self._remove_open_order_index(package_id=next_package.package_id, client_order_id=leg.client_order_id)

    def _remove_open_order_index(self, *, package_id: str, client_order_id: str) -> None:
        package_open = self._open_orders_by_package.get(package_id)
        if package_open is None:
            return
        package_open.discard(client_order_id)
        if not package_open:
            self._open_orders_by_package.pop(package_id, None)

    def _check_open_order_timeouts(self, *, now_ns: int) -> None:
        for client_order_id, open_ref in tuple(self._open_orders.items()):
            if open_ref.timeout_fired:
                continue
            timeout_ns = open_ref.timeout_ms * _NS_PER_MS
            if now_ns - open_ref.submitted_ts_ns <= timeout_ns:
                continue

            open_ref.timeout_fired = True
            timeout_event = LegTimeoutEvent(
                package_id=open_ref.package_id,
                leg_id=open_ref.leg_id,
                reason=f"leg timeout after {open_ref.timeout_ms}ms",
                ts_ns=now_ns,
            )

            runtime = self._runtime_by_package.get(open_ref.package_id)
            relation_id = runtime.relation_id if runtime is not None else "unknown"
            self._apply_state_event(
                package_id=open_ref.package_id,
                relation_id=relation_id,
                event=timeout_event,
                now_ns=now_ns,
            )

            if self._config.auto_cancel_on_timeout:
                self._enqueue_control(
                    _ControlAction(
                        action_type=ControlActionType.CANCEL_ORDER,
                        package_id=open_ref.package_id,
                        reason="timeout cancel",
                        leg_id=open_ref.leg_id,
                        client_order_id=client_order_id,
                    )
                )

    def _handle_risk_decisions(self, decisions: tuple[RiskDecision, ...], now_ns: int) -> None:
        for decision in decisions:
            if decision.allowed:
                continue

            if decision.should_abort_package and decision.package_id:
                self._enqueue_control(
                    _ControlAction(
                        action_type=ControlActionType.ABORT_PACKAGE,
                        package_id=decision.package_id,
                        reason=decision.reason,
                    )
                )

            if decision.should_halt_trading:
                self._safe_journal(
                    "record_kill_switch_activation_auto",
                    lambda: self._journal.record_kill_switch_activation(
                        reason=decision.reason,
                        ts_ns=now_ns,
                        automatic=True,
                    ),
                )

            if decision.code is not None:
                self._metrics.increment("executor.risk.rejected", tags={"code": decision.code.value})

    def _drain_control_actions(self, *, now_ns: int) -> None:
        processed = 0
        while self._control_actions and processed < self._config.max_control_actions_per_tick:
            action = self._control_actions.popleft()
            processed += 1

            if action.action_type == ControlActionType.ABORT_PACKAGE:
                self._pending_abort_packages.discard(action.package_id)
                self._execute_abort_package(action=action, now_ns=now_ns)
                continue

            if action.action_type == ControlActionType.REQUIRE_UNWIND:
                self._pending_unwind_packages.discard(action.package_id)
                self._execute_require_unwind(action=action, now_ns=now_ns)
                continue

            if action.action_type == ControlActionType.CANCEL_ORDER:
                self._execute_cancel_order(action=action, now_ns=now_ns)

    def _execute_abort_package(self, *, action: _ControlAction, now_ns: int) -> None:
        package = self._active_packages.get(action.package_id)
        runtime = self._runtime_by_package.get(action.package_id)
        if package is None or runtime is None:
            return

        if self._config.auto_cancel_on_abort:
            for client_order_id in tuple(self._open_orders_by_package.get(action.package_id, set())):
                open_ref = self._open_orders.get(client_order_id)
                if open_ref is None:
                    continue
                self._execute_cancel_order(
                    _ControlAction(
                        action_type=ControlActionType.CANCEL_ORDER,
                        package_id=action.package_id,
                        reason="abort cancel",
                        leg_id=open_ref.leg_id,
                        client_order_id=client_order_id,
                    ),
                    now_ns=now_ns,
                )

                if action.package_id not in self._active_packages:
                    return

        abort_event = PackageAbortEvent(
            package_id=action.package_id,
            reason=action.reason,
            ts_ns=now_ns,
        )
        self._apply_state_event(
            package_id=action.package_id,
            relation_id=runtime.relation_id,
            event=abort_event,
            now_ns=now_ns,
        )

    def _execute_require_unwind(self, *, action: _ControlAction, now_ns: int) -> None:
        package = self._active_packages.get(action.package_id)
        runtime = self._runtime_by_package.get(action.package_id)
        if package is None or runtime is None:
            return

        event = PackageUnwindRequiredEvent(
            package_id=action.package_id,
            reason=action.reason,
            ts_ns=now_ns,
        )
        self._apply_state_event(
            package_id=action.package_id,
            relation_id=runtime.relation_id,
            event=event,
            now_ns=now_ns,
        )

        latest = self._active_packages.get(action.package_id)
        if latest is None:
            return

        if self._unwind_handler is not None:
            # Control plane hand-off: unwind strategy stays outside venue adapter.
            self._unwind_handler.on_unwind_required(package=latest, reason=action.reason, now_ns=now_ns)
            return

        self._enqueue_control(
            _ControlAction(
                action_type=ControlActionType.ABORT_PACKAGE,
                package_id=action.package_id,
                reason="no unwind handler configured",
            )
        )

    def _execute_cancel_order(self, *, action: _ControlAction, now_ns: int) -> None:
        if action.client_order_id is None:
            return

        package = self._active_packages.get(action.package_id)
        runtime = self._runtime_by_package.get(action.package_id)
        if package is None or runtime is None:
            return

        open_ref = self._open_orders.get(action.client_order_id)
        if open_ref is None:
            return

        cancel_request = CancelRequestedEvent(
            package_id=action.package_id,
            leg_id=open_ref.leg_id,
            reason=action.reason,
            ts_ns=now_ns,
        )
        self._apply_state_event(
            package_id=action.package_id,
            relation_id=runtime.relation_id,
            event=cancel_request,
            now_ns=now_ns,
        )

        try:
            result = self._venue_adapter.cancel_order(
                client_order_id=action.client_order_id,
                reason=action.reason,
                now_ns=now_ns,
            )
        except Exception as exc:
            decision = self._risk_manager.record_venue_ambiguity(
                now_ns=now_ns,
                detail=f"cancel exception: {exc}",
                package_id=action.package_id,
            )
            if decision is not None:
                self._handle_risk_decisions((decision,), now_ns)
            return

        for event in result.events:
            self.on_venue_event(event, now_ns=now_ns)

    def _enqueue_control(self, action: _ControlAction) -> None:
        if action.action_type == ControlActionType.ABORT_PACKAGE:
            if action.package_id in self._pending_abort_packages:
                return
            self._pending_abort_packages.add(action.package_id)

        if action.action_type == ControlActionType.REQUIRE_UNWIND:
            if action.package_id in self._pending_unwind_packages:
                return
            self._pending_unwind_packages.add(action.package_id)

        if len(self._control_actions) >= self._config.max_control_queue_size:
            self._metrics.increment("executor.control_queue.full")
            now_ns = 0
            decision = self._risk_manager.activate_manual_kill_switch(
                reason="executor control queue full",
                now_ns=now_ns,
            )
            self._handle_risk_decisions((decision,), now_ns)
            return

        self._control_actions.append(action)

    def _finalize_terminal_package(self, package: PackageExecution) -> None:
        package_id = package.package_id
        runtime = self._runtime_by_package.pop(package_id, None)
        relation_id = runtime.relation_id if runtime is not None else None

        self._active_packages.pop(package_id, None)
        self._terminal_packages[package_id] = package

        for client_order_id in tuple(self._open_orders_by_package.get(package_id, set())):
            self._open_orders.pop(client_order_id, None)
        self._open_orders_by_package.pop(package_id, None)

        if package.state == PackageState.COMPLETED:
            self._safe_journal(
                "record_package_completion",
                lambda: self._journal.record_package_completion(
                    package_id=package_id,
                    opportunity_id=package.opportunity_id,
                    relation_id=relation_id,
                    ts_ns=package.updated_ts_ns,
                ),
            )
            self._metrics.increment("executor.package.completed")
            return

        failure_message = package.failure_reason.message if package.failure_reason is not None else package.state.value
        self._safe_journal(
            "record_package_failure",
            lambda: self._journal.record_package_failure(
                package_id=package_id,
                opportunity_id=package.opportunity_id,
                relation_id=relation_id,
                reason=failure_message,
                ts_ns=package.updated_ts_ns,
            ),
        )
        self._metrics.increment("executor.package.failed", tags={"state": package.state.value})

    def _journal_event(self, *, relation_id: str, event: StateMachineEvent) -> None:
        if isinstance(event, OrderIntent):
            self._safe_journal("record_order_submitted", lambda: self._journal.record_order_submitted(event, relation_id=relation_id))
            return
        if isinstance(event, VenueOrderAck):
            self._safe_journal("record_order_ack", lambda: self._journal.record_order_ack(event, relation_id=relation_id))
            return
        if isinstance(event, VenueFillEvent):
            self._safe_journal("record_order_fill", lambda: self._journal.record_order_fill(event, relation_id=relation_id))
            return
        if isinstance(event, CancelRequestedEvent):
            self._safe_journal("record_cancel_request", lambda: self._journal.record_cancel_request(event, relation_id=relation_id))
            return
        if isinstance(event, VenueCancelEvent):
            self._safe_journal("record_cancel_confirm", lambda: self._journal.record_cancel_confirm(event, relation_id=relation_id))
            return
        if isinstance(event, VenueRejectEvent):
            self._safe_journal("record_order_reject", lambda: self._journal.record_order_reject(event, relation_id=relation_id))
            return
        if isinstance(event, LegTimeoutEvent):
            self._safe_journal("record_timeout", lambda: self._journal.record_timeout(event, relation_id=relation_id))

    def _safe_journal(self, operation: str, callback: Callable[[], None]) -> None:
        try:
            callback()
        except Exception as exc:
            self._metrics.increment("executor.journal.error", tags={"operation": operation})
            self._logger.error(
                "journal operation failed",
                operation=operation,
                error=str(exc),
            )
            if self._config.journal_errors_halt_trading:
                decision = self._risk_manager.activate_manual_kill_switch(
                    reason=f"journal failure in {operation}",
                    now_ns=0,
                )
                self._handle_risk_decisions((decision,), 0)

    def _next_package_id(self, *, opportunity_id: str, now_ns: int) -> str:
        self._package_sequence += 1
        return f"{self._config.service_name}:{opportunity_id}:{self._package_sequence}:{now_ns & 0xFFFFFFFF:x}"

    def _build_client_order_id(self, *, package_id: str, leg_id: str, attempt: int, now_ns: int) -> str:
        return self._client_order_id_factory.build(
            package_id=package_id,
            leg_id=leg_id,
            attempt=attempt,
            now_ns=now_ns,
        )

    @staticmethod
    def _package_from_plan(*, package_id: str, plan: ExecutionPlan, now_ns: int) -> PackageExecution:
        legs = tuple(
            LegExecution(
                leg_id=leg.leg_id,
                market_id=leg.market_id,
                side=leg.side,
                intended_qty=leg.quantity,
                limit_price_ticks=leg.limit_price_ticks,
            )
            for leg in plan.legs
        )
        return create_package_execution(
            package_id=package_id,
            opportunity_id=plan.opportunity_id,
            created_ts_ns=now_ns,
            legs=legs,
        )
