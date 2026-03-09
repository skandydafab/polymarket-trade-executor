from __future__ import annotations

from dataclasses import dataclass, replace
from enum import Enum
from typing import Final, Sequence, TypeAlias


class StateTransitionError(ValueError):
    """Raised when a transition violates the explicit state transition table."""


class EventValidationError(ValueError):
    """Raised when an inbound event is structurally invalid for current state."""


class Side(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class TimeInForce(str, Enum):
    IOC = "IOC"
    GTC = "GTC"


class OrderState(str, Enum):
    """Per-leg venue order lifecycle state.

    One leg maps to one venue order at a time. A retry/resubmission is modeled as a
    transition back to PENDING_ACK with a new client_order_id.
    """

    NEW = "NEW"
    PENDING_ACK = "PENDING_ACK"
    WORKING = "WORKING"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCEL_REQUESTED = "CANCEL_REQUESTED"
    CANCELED = "CANCELED"
    REJECTED = "REJECTED"
    TIMED_OUT = "TIMED_OUT"
    HALTED = "HALTED"


class PackageState(str, Enum):
    """High-level package lifecycle state for a multi-leg execution."""

    NEW = "NEW"
    EXECUTING = "EXECUTING"
    UNWIND_REQUIRED = "UNWIND_REQUIRED"
    UNWINDING = "UNWINDING"
    COMPLETED = "COMPLETED"
    ABORTED_UNWOUND = "ABORTED_UNWOUND"
    FAILED = "FAILED"
    HALTED = "HALTED"


class FailureCode(str, Enum):
    LEG_REJECTED = "LEG_REJECTED"
    LEG_TIMEOUT = "LEG_TIMEOUT"
    UNWIND_REQUIRED = "UNWIND_REQUIRED"
    UNWIND_CONFIRMED = "UNWIND_CONFIRMED"
    MANUAL_ABORT = "MANUAL_ABORT"
    EMERGENCY_HALT = "EMERGENCY_HALT"


@dataclass(frozen=True, slots=True)
class PackageFailureReason:
    code: FailureCode
    message: str
    ts_ns: int
    leg_id: str | None = None


@dataclass(frozen=True, slots=True)
class LegExecution:
    """Immutable leg runtime state owned by the state machine.

    The state machine is the only location where this object can evolve. All updates
    return a *new* LegExecution via dataclasses.replace.
    """

    leg_id: str
    market_id: str
    side: Side
    intended_qty: int
    limit_price_ticks: int
    state: OrderState = OrderState.NEW
    client_order_id: str | None = None
    venue_order_id: str | None = None
    filled_qty: int = 0
    canceled_qty: int = 0
    avg_fill_price_ticks: int | None = None
    last_fill_price_ticks: int | None = None
    rejection_reason: str | None = None
    timeout_count: int = 0
    submit_ts_ns: int | None = None
    ack_ts_ns: int | None = None
    done_ts_ns: int | None = None
    is_unwind_leg: bool = False


@dataclass(frozen=True, slots=True)
class PackageExecution:
    """Immutable package execution aggregate.

    - intended_leg_ids define the originally planned strategy legs.
    - unwind legs can be added later with is_unwind_leg=True.
    """

    package_id: str
    opportunity_id: str
    created_ts_ns: int
    updated_ts_ns: int
    state: PackageState
    intended_leg_ids: tuple[str, ...]
    legs: tuple[LegExecution, ...]
    failure_reason: PackageFailureReason | None = None


@dataclass(frozen=True, slots=True)
class OrderIntent:
    """Intent to submit a leg order.

    For expected legs, leg_id references an existing leg. For explicit unwind, the
    event may introduce a synthetic unwind leg with is_unwind=True.
    """

    package_id: str
    leg_id: str
    client_order_id: str
    qty: int
    limit_price_ticks: int
    tif: TimeInForce
    ts_ns: int
    side: Side | None = None
    market_id: str | None = None
    is_unwind: bool = False


@dataclass(frozen=True, slots=True)
class VenueOrderAck:
    package_id: str
    leg_id: str
    client_order_id: str
    venue_order_id: str
    ts_ns: int


@dataclass(frozen=True, slots=True)
class VenueFillEvent:
    package_id: str
    leg_id: str
    client_order_id: str
    fill_qty: int
    fill_price_ticks: int
    ts_ns: int
    cumulative_qty: int | None = None


@dataclass(frozen=True, slots=True)
class VenueCancelEvent:
    package_id: str
    leg_id: str
    client_order_id: str
    canceled_qty: int
    reason: str
    ts_ns: int


@dataclass(frozen=True, slots=True)
class VenueRejectEvent:
    package_id: str
    leg_id: str
    client_order_id: str
    reason: str
    ts_ns: int


@dataclass(frozen=True, slots=True)
class CancelRequestedEvent:
    package_id: str
    leg_id: str
    reason: str
    ts_ns: int


@dataclass(frozen=True, slots=True)
class LegTimeoutEvent:
    package_id: str
    leg_id: str
    reason: str
    ts_ns: int


@dataclass(frozen=True, slots=True)
class PackageUnwindRequiredEvent:
    package_id: str
    reason: str
    ts_ns: int


@dataclass(frozen=True, slots=True)
class PackageUnwoundEvent:
    package_id: str
    reason: str
    ts_ns: int


@dataclass(frozen=True, slots=True)
class PackageAbortEvent:
    package_id: str
    reason: str
    ts_ns: int


@dataclass(frozen=True, slots=True)
class EmergencyHaltEvent:
    package_id: str
    reason: str
    ts_ns: int


StateMachineEvent: TypeAlias = (
    OrderIntent
    | VenueOrderAck
    | VenueFillEvent
    | VenueCancelEvent
    | VenueRejectEvent
    | CancelRequestedEvent
    | LegTimeoutEvent
    | PackageUnwindRequiredEvent
    | PackageUnwoundEvent
    | PackageAbortEvent
    | EmergencyHaltEvent
)


# Explicit transition tables keep behavior auditable and easy to review.
ORDER_TRANSITIONS: Final[dict[OrderState, frozenset[OrderState]]] = {
    OrderState.NEW: frozenset({OrderState.PENDING_ACK, OrderState.HALTED}),
    OrderState.PENDING_ACK: frozenset(
        {
            OrderState.WORKING,
            OrderState.PARTIALLY_FILLED,
            OrderState.FILLED,
            OrderState.CANCEL_REQUESTED,
            OrderState.CANCELED,
            OrderState.REJECTED,
            OrderState.TIMED_OUT,
            OrderState.HALTED,
        }
    ),
    OrderState.WORKING: frozenset(
        {
            OrderState.PARTIALLY_FILLED,
            OrderState.FILLED,
            OrderState.CANCEL_REQUESTED,
            OrderState.CANCELED,
            OrderState.REJECTED,
            OrderState.TIMED_OUT,
            OrderState.HALTED,
        }
    ),
    OrderState.PARTIALLY_FILLED: frozenset(
        {
            OrderState.PARTIALLY_FILLED,
            OrderState.FILLED,
            OrderState.CANCEL_REQUESTED,
            OrderState.CANCELED,
            OrderState.REJECTED,
            OrderState.TIMED_OUT,
            OrderState.HALTED,
        }
    ),
    OrderState.CANCEL_REQUESTED: frozenset(
        {
            OrderState.PARTIALLY_FILLED,
            OrderState.FILLED,
            OrderState.CANCELED,
            OrderState.REJECTED,
            OrderState.TIMED_OUT,
            OrderState.HALTED,
        }
    ),
    OrderState.CANCELED: frozenset({OrderState.PENDING_ACK}),
    OrderState.REJECTED: frozenset({OrderState.PENDING_ACK}),
    OrderState.TIMED_OUT: frozenset({OrderState.PENDING_ACK, OrderState.CANCELED, OrderState.HALTED}),
    OrderState.FILLED: frozenset(),
    OrderState.HALTED: frozenset(),
}

PACKAGE_TRANSITIONS: Final[dict[PackageState, frozenset[PackageState]]] = {
    PackageState.NEW: frozenset({PackageState.EXECUTING, PackageState.FAILED, PackageState.HALTED}),
    PackageState.EXECUTING: frozenset(
        {
            PackageState.UNWIND_REQUIRED,
            PackageState.COMPLETED,
            PackageState.FAILED,
            PackageState.HALTED,
        }
    ),
    PackageState.UNWIND_REQUIRED: frozenset(
        {
            PackageState.UNWINDING,
            PackageState.ABORTED_UNWOUND,
            PackageState.FAILED,
            PackageState.HALTED,
        }
    ),
    PackageState.UNWINDING: frozenset(
        {
            PackageState.UNWIND_REQUIRED,
            PackageState.ABORTED_UNWOUND,
            PackageState.FAILED,
            PackageState.HALTED,
        }
    ),
    PackageState.COMPLETED: frozenset(),
    PackageState.ABORTED_UNWOUND: frozenset(),
    PackageState.FAILED: frozenset(),
    PackageState.HALTED: frozenset(),
}

TERMINAL_ORDER_STATES: Final[frozenset[OrderState]] = frozenset(
    {OrderState.FILLED, OrderState.CANCELED, OrderState.REJECTED, OrderState.TIMED_OUT, OrderState.HALTED}
)
LIVE_ORDER_STATES: Final[frozenset[OrderState]] = frozenset(
    {OrderState.PENDING_ACK, OrderState.WORKING, OrderState.PARTIALLY_FILLED, OrderState.CANCEL_REQUESTED}
)
TERMINAL_PACKAGE_STATES: Final[frozenset[PackageState]] = frozenset(
    {PackageState.COMPLETED, PackageState.ABORTED_UNWOUND, PackageState.FAILED, PackageState.HALTED}
)


def create_package_execution(
    package_id: str,
    opportunity_id: str,
    created_ts_ns: int,
    legs: Sequence[LegExecution],
) -> PackageExecution:
    """Create an immutable package aggregate in NEW state.

    Guarded to reject malformed initial state early so replay and live paths use the
    same deterministic constraints.
    """

    if not package_id:
        raise EventValidationError("package_id must be non-empty")
    if not opportunity_id:
        raise EventValidationError("opportunity_id must be non-empty")
    if not legs:
        raise EventValidationError("at least one intended leg is required")

    seen_leg_ids: set[str] = set()
    normalized_legs: list[LegExecution] = []
    for leg in legs:
        if leg.leg_id in seen_leg_ids:
            raise EventValidationError(f"duplicate leg_id={leg.leg_id}")
        if leg.intended_qty <= 0:
            raise EventValidationError(f"leg {leg.leg_id} intended_qty must be > 0")
        if leg.limit_price_ticks <= 0:
            raise EventValidationError(f"leg {leg.leg_id} limit_price_ticks must be > 0")
        if leg.state != OrderState.NEW:
            raise EventValidationError(f"leg {leg.leg_id} must start in NEW state")
        seen_leg_ids.add(leg.leg_id)
        normalized_legs.append(leg)

    intended_leg_ids = tuple(leg.leg_id for leg in normalized_legs)
    return PackageExecution(
        package_id=package_id,
        opportunity_id=opportunity_id,
        created_ts_ns=created_ts_ns,
        updated_ts_ns=created_ts_ns,
        state=PackageState.NEW,
        intended_leg_ids=intended_leg_ids,
        legs=tuple(normalized_legs),
        failure_reason=None,
    )


def leg_by_id(package: PackageExecution, leg_id: str) -> LegExecution:
    for leg in package.legs:
        if leg.leg_id == leg_id:
            return leg
    raise EventValidationError(f"unknown leg_id={leg_id}")


def apply_event(package: PackageExecution, event: StateMachineEvent) -> PackageExecution:
    """Pure reducer entry point.

    Deterministic contract:
    - input package + input event -> output package
    - no side effects
    - no hidden mutation
    """

    _validate_package_match(package, event.package_id)

    if package.state in TERMINAL_PACKAGE_STATES and not isinstance(event, EmergencyHaltEvent):
        raise StateTransitionError(f"package is terminal: state={package.state}")

    if isinstance(event, OrderIntent):
        return _on_order_intent(package, event)
    if isinstance(event, VenueOrderAck):
        return _on_order_ack(package, event)
    if isinstance(event, VenueFillEvent):
        return _on_fill(package, event)
    if isinstance(event, VenueCancelEvent):
        return _on_cancel_confirmed(package, event)
    if isinstance(event, VenueRejectEvent):
        return _on_reject(package, event)
    if isinstance(event, CancelRequestedEvent):
        return _on_cancel_requested(package, event)
    if isinstance(event, LegTimeoutEvent):
        return _on_timeout(package, event)
    if isinstance(event, PackageUnwindRequiredEvent):
        return _on_unwind_required(package, event)
    if isinstance(event, PackageUnwoundEvent):
        return _on_unwound(package, event)
    if isinstance(event, PackageAbortEvent):
        return _on_abort(package, event)
    if isinstance(event, EmergencyHaltEvent):
        return _on_emergency_halt(package, event)

    raise EventValidationError(f"unsupported event type={type(event)!r}")


class PackageStateMachine:
    """Stateless wrapper to make the reducer integration explicit at call sites."""

    @staticmethod
    def transition(package: PackageExecution, event: StateMachineEvent) -> PackageExecution:
        return apply_event(package, event)


def _on_order_intent(package: PackageExecution, event: OrderIntent) -> PackageExecution:
    if event.qty <= 0:
        raise EventValidationError("order intent qty must be > 0")
    if event.limit_price_ticks <= 0:
        raise EventValidationError("order intent limit_price_ticks must be > 0")

    try:
        leg = leg_by_id(package, event.leg_id)
    except EventValidationError:
        if not event.is_unwind:
            raise
        if event.side is None or event.market_id is None:
            raise EventValidationError("unwind intent for unknown leg requires side and market_id")

        synthetic_unwind_leg = LegExecution(
            leg_id=event.leg_id,
            market_id=event.market_id,
            side=event.side,
            intended_qty=event.qty,
            limit_price_ticks=event.limit_price_ticks,
            state=OrderState.PENDING_ACK,
            client_order_id=event.client_order_id,
            submit_ts_ns=event.ts_ns,
            is_unwind_leg=True,
        )
        package = _append_leg(package, synthetic_unwind_leg, event.ts_ns)
        if package.state == PackageState.UNWIND_REQUIRED:
            package = _move_package_state(package, PackageState.UNWINDING, event.ts_ns)
        return _recompute_package_state(package, event.ts_ns)

    # Idempotent replay handling for duplicate intent.
    if leg.client_order_id == event.client_order_id and leg.state in LIVE_ORDER_STATES:
        return package

    if not leg.is_unwind_leg and event.qty != leg.intended_qty:
        raise EventValidationError(
            f"intended leg qty mismatch for {event.leg_id}: expected {leg.intended_qty}, got {event.qty}"
        )

    _assert_order_transition(leg.state, OrderState.PENDING_ACK, leg.leg_id)
    next_leg = replace(
        leg,
        intended_qty=event.qty if leg.is_unwind_leg else leg.intended_qty,
        limit_price_ticks=event.limit_price_ticks,
        state=OrderState.PENDING_ACK,
        client_order_id=event.client_order_id,
        venue_order_id=None,
        rejection_reason=None,
        submit_ts_ns=event.ts_ns,
        done_ts_ns=None,
    )
    package = _replace_leg(package, next_leg, event.ts_ns)
    if package.state == PackageState.UNWIND_REQUIRED and (event.is_unwind or leg.is_unwind_leg):
        package = _move_package_state(package, PackageState.UNWINDING, event.ts_ns)
    return _recompute_package_state(package, event.ts_ns)


def _on_order_ack(package: PackageExecution, event: VenueOrderAck) -> PackageExecution:
    leg = leg_by_id(package, event.leg_id)
    _assert_client_order_match(leg, event.client_order_id)

    if leg.state in {OrderState.WORKING, OrderState.PARTIALLY_FILLED, OrderState.FILLED, OrderState.CANCEL_REQUESTED, OrderState.CANCELED}:
        # Out-of-order ack after fills/cancel can happen on real venues. Keep state,
        # only enrich venue_order_id/ack_ts for auditability.
        next_leg = replace(leg, venue_order_id=event.venue_order_id, ack_ts_ns=event.ts_ns)
        package = _replace_leg(package, next_leg, event.ts_ns)
        return _recompute_package_state(package, event.ts_ns)

    _assert_order_transition(leg.state, OrderState.WORKING, leg.leg_id)
    next_leg = replace(
        leg,
        state=OrderState.WORKING,
        venue_order_id=event.venue_order_id,
        ack_ts_ns=event.ts_ns,
    )
    package = _replace_leg(package, next_leg, event.ts_ns)
    return _recompute_package_state(package, event.ts_ns)


def _on_fill(package: PackageExecution, event: VenueFillEvent) -> PackageExecution:
    leg = leg_by_id(package, event.leg_id)
    _assert_client_order_match(leg, event.client_order_id)

    if event.fill_qty <= 0:
        raise EventValidationError("fill_qty must be > 0")
    if event.fill_price_ticks <= 0:
        raise EventValidationError("fill_price_ticks must be > 0")

    new_cumulative_qty = event.cumulative_qty if event.cumulative_qty is not None else leg.filled_qty + event.fill_qty
    if new_cumulative_qty < leg.filled_qty:
        raise EventValidationError(
            f"non-monotonic cumulative qty for leg {leg.leg_id}: {new_cumulative_qty} < {leg.filled_qty}"
        )

    if not leg.is_unwind_leg and new_cumulative_qty > leg.intended_qty:
        raise EventValidationError(
            f"overfill intended leg {leg.leg_id}: cumulative={new_cumulative_qty} intended={leg.intended_qty}"
        )

    incremental_qty = new_cumulative_qty - leg.filled_qty
    if incremental_qty == 0:
        return package

    if leg.state in {OrderState.CANCELED, OrderState.REJECTED, OrderState.TIMED_OUT}:
        previous_notional = (leg.avg_fill_price_ticks or 0) * leg.filled_qty
        fill_notional = incremental_qty * event.fill_price_ticks
        new_avg = (previous_notional + fill_notional) // new_cumulative_qty

        next_leg = replace(
            leg,
            filled_qty=new_cumulative_qty,
            last_fill_price_ticks=event.fill_price_ticks,
            avg_fill_price_ticks=new_avg,
        )
        package = _replace_leg(package, next_leg, event.ts_ns)
        package = _set_failure_reason(
            package,
            PackageFailureReason(
                code=FailureCode.UNWIND_REQUIRED,
                message=f"late fill on terminal leg {leg.leg_id} in state={leg.state}",
                ts_ns=event.ts_ns,
                leg_id=leg.leg_id,
            ),
            event.ts_ns,
        )
        if package.state in {PackageState.UNWIND_REQUIRED, PackageState.UNWINDING}:
            return package
        return _move_package_state(package, PackageState.UNWIND_REQUIRED, event.ts_ns)

    if leg.state == OrderState.HALTED:
        raise StateTransitionError(f"late fill on terminal leg {leg.leg_id} in state={leg.state}")

    if leg.state == OrderState.NEW:
        raise StateTransitionError(f"fill before submit for leg={leg.leg_id}")

    is_full = new_cumulative_qty >= leg.intended_qty
    target_state = OrderState.FILLED if is_full else OrderState.PARTIALLY_FILLED
    _assert_order_transition(leg.state, target_state, leg.leg_id)

    previous_notional = (leg.avg_fill_price_ticks or 0) * leg.filled_qty
    fill_notional = incremental_qty * event.fill_price_ticks
    new_avg = (previous_notional + fill_notional) // new_cumulative_qty

    next_leg = replace(
        leg,
        state=target_state,
        filled_qty=new_cumulative_qty,
        last_fill_price_ticks=event.fill_price_ticks,
        avg_fill_price_ticks=new_avg,
        done_ts_ns=event.ts_ns if target_state == OrderState.FILLED else leg.done_ts_ns,
    )
    package = _replace_leg(package, next_leg, event.ts_ns)
    return _recompute_package_state(package, event.ts_ns)


def _on_reject(package: PackageExecution, event: VenueRejectEvent) -> PackageExecution:
    leg = leg_by_id(package, event.leg_id)
    _assert_client_order_match(leg, event.client_order_id)

    if leg.state == OrderState.REJECTED:
        return package

    _assert_order_transition(leg.state, OrderState.REJECTED, leg.leg_id)
    next_leg = replace(
        leg,
        state=OrderState.REJECTED,
        rejection_reason=event.reason,
        done_ts_ns=event.ts_ns,
    )
    package = _replace_leg(package, next_leg, event.ts_ns)
    package = _set_failure_reason(
        package,
        PackageFailureReason(
            code=FailureCode.LEG_REJECTED,
            message=event.reason,
            ts_ns=event.ts_ns,
            leg_id=event.leg_id,
        ),
        event.ts_ns,
    )
    return _recompute_package_state(package, event.ts_ns)


def _on_cancel_requested(package: PackageExecution, event: CancelRequestedEvent) -> PackageExecution:
    leg = leg_by_id(package, event.leg_id)

    if leg.state == OrderState.CANCEL_REQUESTED:
        return package
    if leg.state in {OrderState.CANCELED, OrderState.REJECTED, OrderState.TIMED_OUT}:
        return package
    if leg.state in {OrderState.FILLED, OrderState.HALTED, OrderState.NEW}:
        raise StateTransitionError(f"cannot request cancel for leg {leg.leg_id} in state={leg.state}")

    _assert_order_transition(leg.state, OrderState.CANCEL_REQUESTED, leg.leg_id)
    next_leg = replace(leg, state=OrderState.CANCEL_REQUESTED)
    package = _replace_leg(package, next_leg, event.ts_ns)
    return _recompute_package_state(package, event.ts_ns)


def _on_cancel_confirmed(package: PackageExecution, event: VenueCancelEvent) -> PackageExecution:
    leg = leg_by_id(package, event.leg_id)
    _assert_client_order_match(leg, event.client_order_id)

    if event.canceled_qty < 0:
        raise EventValidationError("canceled_qty must be >= 0")

    if leg.state == OrderState.CANCELED:
        return package

    _assert_order_transition(leg.state, OrderState.CANCELED, leg.leg_id)
    next_leg = replace(
        leg,
        state=OrderState.CANCELED,
        canceled_qty=max(leg.canceled_qty, event.canceled_qty),
        done_ts_ns=event.ts_ns,
    )
    package = _replace_leg(package, next_leg, event.ts_ns)
    return _recompute_package_state(package, event.ts_ns)


def _on_timeout(package: PackageExecution, event: LegTimeoutEvent) -> PackageExecution:
    leg = leg_by_id(package, event.leg_id)

    if leg.state == OrderState.TIMED_OUT:
        return package
    if leg.state in {OrderState.CANCELED, OrderState.REJECTED, OrderState.FILLED, OrderState.HALTED}:
        return package

    _assert_order_transition(leg.state, OrderState.TIMED_OUT, leg.leg_id)
    next_leg = replace(
        leg,
        state=OrderState.TIMED_OUT,
        timeout_count=leg.timeout_count + 1,
        done_ts_ns=event.ts_ns,
    )
    package = _replace_leg(package, next_leg, event.ts_ns)
    package = _set_failure_reason(
        package,
        PackageFailureReason(
            code=FailureCode.LEG_TIMEOUT,
            message=event.reason,
            ts_ns=event.ts_ns,
            leg_id=event.leg_id,
        ),
        event.ts_ns,
    )
    return _recompute_package_state(package, event.ts_ns)


def _on_unwind_required(package: PackageExecution, event: PackageUnwindRequiredEvent) -> PackageExecution:
    if package.state in {PackageState.UNWIND_REQUIRED, PackageState.UNWINDING}:
        return package
    package = _set_failure_reason(
        package,
        PackageFailureReason(
            code=FailureCode.UNWIND_REQUIRED,
            message=event.reason,
            ts_ns=event.ts_ns,
        ),
        event.ts_ns,
    )
    return _move_package_state(package, PackageState.UNWIND_REQUIRED, event.ts_ns)


def _on_unwound(package: PackageExecution, event: PackageUnwoundEvent) -> PackageExecution:
    if package.state not in {PackageState.UNWIND_REQUIRED, PackageState.UNWINDING}:
        raise StateTransitionError(f"cannot mark unwound from state={package.state}")
    package = _set_failure_reason(
        package,
        PackageFailureReason(
            code=FailureCode.UNWIND_CONFIRMED,
            message=event.reason,
            ts_ns=event.ts_ns,
        ),
        event.ts_ns,
    )
    return _move_package_state(package, PackageState.ABORTED_UNWOUND, event.ts_ns)


def _on_abort(package: PackageExecution, event: PackageAbortEvent) -> PackageExecution:
    package = _set_failure_reason(
        package,
        PackageFailureReason(
            code=FailureCode.MANUAL_ABORT,
            message=event.reason,
            ts_ns=event.ts_ns,
        ),
        event.ts_ns,
    )
    if package.state == PackageState.FAILED:
        return package
    return _move_package_state(package, PackageState.FAILED, event.ts_ns)


def _on_emergency_halt(package: PackageExecution, event: EmergencyHaltEvent) -> PackageExecution:
    # Emergency halt is intentionally global and strongest-priority. It freezes all
    # non-terminal legs to HALTED and transitions package to HALTED.
    halted_legs: list[LegExecution] = []
    for leg in package.legs:
        if leg.state in TERMINAL_ORDER_STATES:
            halted_legs.append(leg)
            continue
        _assert_order_transition(leg.state, OrderState.HALTED, leg.leg_id)
        halted_legs.append(replace(leg, state=OrderState.HALTED, done_ts_ns=event.ts_ns))

    package = replace(package, legs=tuple(halted_legs), updated_ts_ns=event.ts_ns)
    package = _set_failure_reason(
        package,
        PackageFailureReason(
            code=FailureCode.EMERGENCY_HALT,
            message=event.reason,
            ts_ns=event.ts_ns,
        ),
        event.ts_ns,
    )
    if package.state == PackageState.HALTED:
        return package
    return _move_package_state(package, PackageState.HALTED, event.ts_ns)


def _recompute_package_state(package: PackageExecution, ts_ns: int) -> PackageExecution:
    """Derive package state from leg states.

    Explicit asymmetry handling:
    - one leg filled, another pending -> EXECUTING
    - one leg rejected while others live -> UNWIND_REQUIRED
    - one leg timed out after another fills -> UNWIND_REQUIRED

    This keeps package-level lifecycle deterministic and replay-safe.
    """

    if package.state in TERMINAL_PACKAGE_STATES:
        return package
    if package.state == PackageState.HALTED:
        return package

    intended_legs = [leg for leg in package.legs if leg.leg_id in package.intended_leg_ids]
    if not intended_legs:
        raise EventValidationError("package has no intended legs")

    all_filled = all(leg.state == OrderState.FILLED for leg in intended_legs)
    any_failure = any(leg.state in {OrderState.REJECTED, OrderState.TIMED_OUT} for leg in intended_legs)
    any_live = any(leg.state in LIVE_ORDER_STATES for leg in intended_legs)
    any_filled_qty = any(leg.filled_qty > 0 for leg in intended_legs)
    any_started = any(leg.state != OrderState.NEW for leg in intended_legs)

    if all_filled:
        return _move_package_state(package, PackageState.COMPLETED, ts_ns)

    if any_failure:
        # Core asymmetry rule:
        # If a leg failed and another leg has exposure OR is still live, we require
        # unwind logic to resolve residual risk deterministically.
        if any_filled_qty or any_live:
            if package.state == PackageState.UNWINDING:
                return package
            return _move_package_state(package, PackageState.UNWIND_REQUIRED, ts_ns)
        return _move_package_state(package, PackageState.FAILED, ts_ns)

    if any_started:
        if package.state in {PackageState.UNWIND_REQUIRED, PackageState.UNWINDING}:
            return package
        return _move_package_state(package, PackageState.EXECUTING, ts_ns)

    return package


def _append_leg(package: PackageExecution, leg: LegExecution, ts_ns: int) -> PackageExecution:
    if any(existing.leg_id == leg.leg_id for existing in package.legs):
        raise EventValidationError(f"duplicate leg_id={leg.leg_id} in package")
    return replace(package, legs=package.legs + (leg,), updated_ts_ns=ts_ns)


def _replace_leg(package: PackageExecution, updated_leg: LegExecution, ts_ns: int) -> PackageExecution:
    legs = list(package.legs)
    for index, leg in enumerate(legs):
        if leg.leg_id == updated_leg.leg_id:
            legs[index] = updated_leg
            return replace(package, legs=tuple(legs), updated_ts_ns=ts_ns)
    raise EventValidationError(f"unknown leg_id={updated_leg.leg_id}")


def _move_package_state(package: PackageExecution, target: PackageState, ts_ns: int) -> PackageExecution:
    if package.state == target:
        return package
    allowed_targets = PACKAGE_TRANSITIONS[package.state]
    if target not in allowed_targets:
        raise StateTransitionError(
            f"invalid package transition package_id={package.package_id} {package.state} -> {target}"
        )
    return replace(package, state=target, updated_ts_ns=ts_ns)


def _set_failure_reason(
    package: PackageExecution,
    reason: PackageFailureReason,
    ts_ns: int,
) -> PackageExecution:
    if package.failure_reason == reason:
        return package
    return replace(package, failure_reason=reason, updated_ts_ns=ts_ns)


def _assert_order_transition(current: OrderState, target: OrderState, leg_id: str) -> None:
    if current == target:
        return
    allowed_targets = ORDER_TRANSITIONS[current]
    if target not in allowed_targets:
        raise StateTransitionError(f"invalid leg transition leg_id={leg_id} {current} -> {target}")


def _assert_client_order_match(leg: LegExecution, client_order_id: str) -> None:
    if leg.client_order_id is None:
        raise EventValidationError(f"leg {leg.leg_id} has no client_order_id yet")
    if leg.client_order_id != client_order_id:
        raise EventValidationError(
            f"client_order_id mismatch for leg={leg.leg_id}: expected={leg.client_order_id} got={client_order_id}"
        )


def _validate_package_match(package: PackageExecution, event_package_id: str) -> None:
    if package.package_id != event_package_id:
        raise EventValidationError(
            f"event package_id={event_package_id} does not match package={package.package_id}"
        )