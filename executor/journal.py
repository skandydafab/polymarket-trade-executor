from __future__ import annotations

from collections import deque
from collections.abc import Iterable, Iterator
from dataclasses import asdict, dataclass, field, is_dataclass, replace
from datetime import datetime, timezone
from enum import Enum
from itertools import count
import json
from pathlib import Path
import queue
import threading
from typing import Any, Mapping, Protocol, Sequence
from uuid import uuid4

from .planner import ExecutionPlan, PlannedLeg
from .state_machine import (
    CancelRequestedEvent,
    LegExecution,
    LegTimeoutEvent,
    OrderIntent,
    OrderState,
    PackageAbortEvent,
    PackageExecution,
    PackageState,
    PackageStateMachine,
    Side,
    StateMachineEvent,
    TimeInForce,
    VenueCancelEvent,
    VenueFillEvent,
    VenueOrderAck,
    VenueRejectEvent,
    create_package_execution,
)
from .venue import NormalizedOrderEvent, ReconciliationResult, VenueAdapter


class JournalEventType(str, Enum):
    OPPORTUNITY_ACCEPTED = "OPPORTUNITY_ACCEPTED"
    OPPORTUNITY_REJECTED = "OPPORTUNITY_REJECTED"
    EXECUTION_PLAN_CREATED = "EXECUTION_PLAN_CREATED"
    ORDER_SUBMITTED = "ORDER_SUBMITTED"
    ORDER_ACK = "ORDER_ACK"
    ORDER_FILL = "ORDER_FILL"
    ORDER_CANCEL_REQUESTED = "ORDER_CANCEL_REQUESTED"
    ORDER_CANCEL_CONFIRMED = "ORDER_CANCEL_CONFIRMED"
    ORDER_REJECTED = "ORDER_REJECTED"
    ORDER_TIMEOUT = "ORDER_TIMEOUT"
    PACKAGE_COMPLETED = "PACKAGE_COMPLETED"
    PACKAGE_FAILED = "PACKAGE_FAILED"
    KILL_SWITCH_ACTIVATED = "KILL_SWITCH_ACTIVATED"


@dataclass(frozen=True, slots=True)
class JournalRecord:
    event_id: str
    ts_ns: int
    event_type: JournalEventType
    package_id: str | None
    opportunity_id: str | None
    relation_id: str | None
    payload: Mapping[str, Any]
    source: str = "executor"
    schema_version: int = 1


@dataclass(frozen=True, slots=True)
class JournalWriterConfig:
    background: bool = True
    queue_max_size: int = 20_000
    enqueue_timeout_ms: int = 5
    flush_every_n_events: int = 50


@dataclass(frozen=True, slots=True)
class RecoveryAction:
    action_type: str
    message: str
    package_id: str | None = None
    client_order_id: str | None = None


@dataclass(slots=True)
class ReconstructedExecutorState:
    active_packages: dict[str, PackageExecution] = field(default_factory=dict)
    terminal_packages: dict[str, PackageExecution] = field(default_factory=dict)
    accepted_opportunities: set[str] = field(default_factory=set)
    rejected_opportunities: set[str] = field(default_factory=set)
    relation_by_package: dict[str, str] = field(default_factory=dict)
    kill_switch_active: bool = False
    kill_switch_reason: str | None = None
    replay_errors: list[str] = field(default_factory=list)
    orphan_records: list[JournalRecord] = field(default_factory=list)

    def expected_open_client_order_ids(self) -> set[str]:
        expected: set[str] = set()
        open_states = {
            OrderState.PENDING_ACK,
            OrderState.WORKING,
            OrderState.PARTIALLY_FILLED,
            OrderState.CANCEL_REQUESTED,
        }
        for package in self.active_packages.values():
            for leg in package.legs:
                if leg.client_order_id and leg.state in open_states:
                    expected.add(leg.client_order_id)
        return expected

    def find_package_by_client_order_id(self, client_order_id: str) -> str | None:
        for package in self.active_packages.values():
            for leg in package.legs:
                if leg.client_order_id == client_order_id:
                    return package.package_id
        for package in self.terminal_packages.values():
            for leg in package.legs:
                if leg.client_order_id == client_order_id:
                    return package.package_id
        return None


@dataclass(frozen=True, slots=True)
class RecoveryResult:
    reconstructed_state: ReconstructedExecutorState
    records_loaded: int
    reconciliation: ReconciliationResult
    actions: tuple[RecoveryAction, ...]


class JournalStorage(Protocol):
    def append_line(self, partition_key: str, line: str) -> None: ...

    def iter_lines(
        self,
        *,
        start_partition: str | None = None,
        end_partition: str | None = None,
    ) -> Iterator[str]: ...

    def list_partitions(self) -> Sequence[str]: ...

    def flush(self) -> None: ...

    def close(self) -> None: ...


class JournalEventSerializer:
    def dumps(self, record: JournalRecord) -> str:
        payload = {
            "schema_version": record.schema_version,
            "event_id": record.event_id,
            "ts_ns": record.ts_ns,
            "event_type": record.event_type.value,
            "package_id": record.package_id,
            "opportunity_id": record.opportunity_id,
            "relation_id": record.relation_id,
            "source": record.source,
            "payload": _to_json_primitive(record.payload),
        }
        return json.dumps(payload, sort_keys=True, separators=(",", ":"))

    def loads(self, line: str) -> JournalRecord:
        data = json.loads(line)
        if not isinstance(data, dict):
            raise ValueError("journal line must decode to object")

        event_type = JournalEventType(str(data["event_type"]))
        payload = data.get("payload", {})
        if not isinstance(payload, dict):
            raise ValueError("journal payload must be object")

        return JournalRecord(
            event_id=str(data.get("event_id") or uuid4().hex),
            ts_ns=int(data["ts_ns"]),
            event_type=event_type,
            package_id=_as_optional_str(data.get("package_id")),
            opportunity_id=_as_optional_str(data.get("opportunity_id")),
            relation_id=_as_optional_str(data.get("relation_id")),
            payload=payload,
            source=str(data.get("source") or "executor"),
            schema_version=int(data.get("schema_version", 1)),
        )


class JSONLFileJournalStorage(JournalStorage):
    """Append-only JSONL storage with day partitioning.

    Path layout:
      {root}/{YYYY-MM-DD}/events.jsonl
    """

    __slots__ = ("_root", "_lock", "_handles")

    def __init__(self, root_directory: str | Path) -> None:
        self._root = Path(root_directory)
        self._root.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._handles: dict[str, Any] = {}

    def append_line(self, partition_key: str, line: str) -> None:
        with self._lock:
            handle = self._handles.get(partition_key)
            if handle is None:
                partition_dir = self._root / partition_key
                partition_dir.mkdir(parents=True, exist_ok=True)
                path = partition_dir / "events.jsonl"
                handle = path.open("a", encoding="utf-8", buffering=1024 * 128)
                self._handles[partition_key] = handle

            handle.write(line)
            handle.write("\n")

    def iter_lines(
        self,
        *,
        start_partition: str | None = None,
        end_partition: str | None = None,
    ) -> Iterator[str]:
        partitions = sorted(self.list_partitions())
        for partition in partitions:
            if start_partition is not None and partition < start_partition:
                continue
            if end_partition is not None and partition > end_partition:
                continue

            path = self._root / partition / "events.jsonl"
            if not path.exists():
                continue
            with path.open("r", encoding="utf-8") as handle:
                for line in handle:
                    stripped = line.strip()
                    if stripped:
                        yield stripped

    def list_partitions(self) -> Sequence[str]:
        if not self._root.exists():
            return tuple()
        partitions: list[str] = []
        for child in self._root.iterdir():
            if child.is_dir():
                partitions.append(child.name)
        partitions.sort()
        return tuple(partitions)

    def flush(self) -> None:
        with self._lock:
            for handle in self._handles.values():
                handle.flush()

    def close(self) -> None:
        with self._lock:
            for handle in self._handles.values():
                try:
                    handle.flush()
                finally:
                    handle.close()
            self._handles.clear()


class InMemoryJournalStorage(JournalStorage):
    __slots__ = ("_lines",)

    def __init__(self) -> None:
        self._lines: dict[str, list[str]] = {}

    def append_line(self, partition_key: str, line: str) -> None:
        bucket = self._lines.setdefault(partition_key, [])
        bucket.append(line)

    def iter_lines(
        self,
        *,
        start_partition: str | None = None,
        end_partition: str | None = None,
    ) -> Iterator[str]:
        for partition in sorted(self._lines):
            if start_partition is not None and partition < start_partition:
                continue
            if end_partition is not None and partition > end_partition:
                continue
            for line in self._lines[partition]:
                yield line

    def list_partitions(self) -> Sequence[str]:
        return tuple(sorted(self._lines))

    def flush(self) -> None:
        return None

    def close(self) -> None:
        return None


class JournalWriter:
    """Buffered append-only writer.

    Uses background queue by default to keep hot path lightweight.
    """

    __slots__ = (
        "_storage",
        "_serializer",
        "_config",
        "_closed",
        "_lock",
        "_queue",
        "_worker",
        "_stop_sentinel",
        "_writes_since_flush",
    )

    def __init__(
        self,
        storage: JournalStorage,
        *,
        serializer: JournalEventSerializer | None = None,
        config: JournalWriterConfig | None = None,
    ) -> None:
        self._storage = storage
        self._serializer = serializer or JournalEventSerializer()
        self._config = config or JournalWriterConfig()
        self._closed = False
        self._lock = threading.Lock()
        self._queue: queue.Queue[JournalRecord | object] | None = None
        self._worker: threading.Thread | None = None
        self._stop_sentinel = object()
        self._writes_since_flush = 0

        if self._config.background:
            self._queue = queue.Queue(maxsize=self._config.queue_max_size)
            self._worker = threading.Thread(target=self._worker_loop, name="journal-writer", daemon=True)
            self._worker.start()

    def append(self, record: JournalRecord) -> None:
        if self._closed:
            raise RuntimeError("journal writer is closed")

        if self._queue is None:
            self._append_sync(record)
            return

        timeout_sec = max(0.001, self._config.enqueue_timeout_ms / 1000.0)
        try:
            self._queue.put(record, timeout=timeout_sec)
        except queue.Full:
            # Backpressure is preferred over data loss. This blocks until queue space
            # is available, preserving event order and auditability.
            self._queue.put(record)

    def flush(self) -> None:
        if self._queue is not None:
            self._queue.join()
        self._storage.flush()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True

        if self._queue is not None and self._worker is not None:
            self._queue.put(self._stop_sentinel)
            self._queue.join()
            self._worker.join(timeout=2.0)

        self._storage.flush()
        self._storage.close()

    def _worker_loop(self) -> None:
        assert self._queue is not None
        while True:
            item = self._queue.get()
            try:
                if item is self._stop_sentinel:
                    self._storage.flush()
                    return
                assert isinstance(item, JournalRecord)
                self._append_sync(item)
            finally:
                self._queue.task_done()

    def _append_sync(self, record: JournalRecord) -> None:
        line = self._serializer.dumps(record)
        partition_key = partition_key_from_ts_ns(record.ts_ns)
        with self._lock:
            self._storage.append_line(partition_key, line)
            self._writes_since_flush += 1
            if self._writes_since_flush >= self._config.flush_every_n_events:
                self._storage.flush()
                self._writes_since_flush = 0


class ExecutorJournal:
    """Convenience wrapper for journaling canonical executor events."""

    __slots__ = ("_writer", "_seq")

    def __init__(self, writer: JournalWriter) -> None:
        self._writer = writer
        self._seq = count(1)

    def record_opportunity_accepted(
        self,
        *,
        package_id: str,
        opportunity_id: str,
        relation_id: str,
        ts_ns: int,
        metadata: Mapping[str, Any] | None = None,
    ) -> JournalRecord:
        record = self._build_record(
            event_type=JournalEventType.OPPORTUNITY_ACCEPTED,
            ts_ns=ts_ns,
            package_id=package_id,
            opportunity_id=opportunity_id,
            relation_id=relation_id,
            payload={"metadata": dict(metadata or {})},
        )
        self._writer.append(record)
        return record

    def record_opportunity_rejected(
        self,
        *,
        opportunity_id: str,
        relation_id: str,
        reason: str,
        ts_ns: int,
    ) -> JournalRecord:
        record = self._build_record(
            event_type=JournalEventType.OPPORTUNITY_REJECTED,
            ts_ns=ts_ns,
            package_id=None,
            opportunity_id=opportunity_id,
            relation_id=relation_id,
            payload={"reason": reason},
        )
        self._writer.append(record)
        return record

    def record_execution_plan_created(
        self,
        *,
        package_id: str,
        relation_id: str,
        plan: ExecutionPlan,
        ts_ns: int,
    ) -> JournalRecord:
        record = self._build_record(
            event_type=JournalEventType.EXECUTION_PLAN_CREATED,
            ts_ns=ts_ns,
            package_id=package_id,
            opportunity_id=plan.opportunity_id,
            relation_id=relation_id,
            payload={"plan": serialize_execution_plan(plan)},
        )
        self._writer.append(record)
        return record

    def record_order_submitted(
        self,
        event: OrderIntent,
        *,
        relation_id: str | None = None,
    ) -> JournalRecord:
        record = self._build_record(
            event_type=JournalEventType.ORDER_SUBMITTED,
            ts_ns=event.ts_ns,
            package_id=event.package_id,
            opportunity_id=None,
            relation_id=relation_id,
            payload={"event": serialize_order_intent(event)},
        )
        self._writer.append(record)
        return record

    def record_order_ack(self, event: VenueOrderAck, *, relation_id: str | None = None) -> JournalRecord:
        record = self._build_record(
            event_type=JournalEventType.ORDER_ACK,
            ts_ns=event.ts_ns,
            package_id=event.package_id,
            opportunity_id=None,
            relation_id=relation_id,
            payload={"event": serialize_venue_order_ack(event)},
        )
        self._writer.append(record)
        return record

    def record_order_fill(self, event: VenueFillEvent, *, relation_id: str | None = None) -> JournalRecord:
        record = self._build_record(
            event_type=JournalEventType.ORDER_FILL,
            ts_ns=event.ts_ns,
            package_id=event.package_id,
            opportunity_id=None,
            relation_id=relation_id,
            payload={"event": serialize_venue_fill_event(event)},
        )
        self._writer.append(record)
        return record

    def record_cancel_request(
        self,
        event: CancelRequestedEvent,
        *,
        relation_id: str | None = None,
    ) -> JournalRecord:
        record = self._build_record(
            event_type=JournalEventType.ORDER_CANCEL_REQUESTED,
            ts_ns=event.ts_ns,
            package_id=event.package_id,
            opportunity_id=None,
            relation_id=relation_id,
            payload={"event": serialize_cancel_requested_event(event)},
        )
        self._writer.append(record)
        return record

    def record_cancel_confirm(
        self,
        event: VenueCancelEvent,
        *,
        relation_id: str | None = None,
    ) -> JournalRecord:
        record = self._build_record(
            event_type=JournalEventType.ORDER_CANCEL_CONFIRMED,
            ts_ns=event.ts_ns,
            package_id=event.package_id,
            opportunity_id=None,
            relation_id=relation_id,
            payload={"event": serialize_venue_cancel_event(event)},
        )
        self._writer.append(record)
        return record

    def record_order_reject(
        self,
        event: VenueRejectEvent,
        *,
        relation_id: str | None = None,
    ) -> JournalRecord:
        record = self._build_record(
            event_type=JournalEventType.ORDER_REJECTED,
            ts_ns=event.ts_ns,
            package_id=event.package_id,
            opportunity_id=None,
            relation_id=relation_id,
            payload={"event": serialize_venue_reject_event(event)},
        )
        self._writer.append(record)
        return record

    def record_timeout(
        self,
        event: LegTimeoutEvent,
        *,
        relation_id: str | None = None,
    ) -> JournalRecord:
        record = self._build_record(
            event_type=JournalEventType.ORDER_TIMEOUT,
            ts_ns=event.ts_ns,
            package_id=event.package_id,
            opportunity_id=None,
            relation_id=relation_id,
            payload={"event": serialize_leg_timeout_event(event)},
        )
        self._writer.append(record)
        return record

    def record_package_completion(
        self,
        *,
        package_id: str,
        opportunity_id: str,
        relation_id: str | None,
        ts_ns: int,
        metadata: Mapping[str, Any] | None = None,
    ) -> JournalRecord:
        record = self._build_record(
            event_type=JournalEventType.PACKAGE_COMPLETED,
            ts_ns=ts_ns,
            package_id=package_id,
            opportunity_id=opportunity_id,
            relation_id=relation_id,
            payload={"metadata": dict(metadata or {})},
        )
        self._writer.append(record)
        return record

    def record_package_failure(
        self,
        *,
        package_id: str,
        opportunity_id: str | None,
        relation_id: str | None,
        reason: str,
        ts_ns: int,
    ) -> JournalRecord:
        record = self._build_record(
            event_type=JournalEventType.PACKAGE_FAILED,
            ts_ns=ts_ns,
            package_id=package_id,
            opportunity_id=opportunity_id,
            relation_id=relation_id,
            payload={"reason": reason},
        )
        self._writer.append(record)
        return record

    def record_kill_switch_activation(
        self,
        *,
        reason: str,
        ts_ns: int,
        automatic: bool,
    ) -> JournalRecord:
        record = self._build_record(
            event_type=JournalEventType.KILL_SWITCH_ACTIVATED,
            ts_ns=ts_ns,
            package_id=None,
            opportunity_id=None,
            relation_id=None,
            payload={"reason": reason, "automatic": automatic},
        )
        self._writer.append(record)
        return record

    def _build_record(
        self,
        *,
        event_type: JournalEventType,
        ts_ns: int,
        package_id: str | None,
        opportunity_id: str | None,
        relation_id: str | None,
        payload: Mapping[str, Any],
    ) -> JournalRecord:
        sequence = next(self._seq)
        event_id = f"{ts_ns}-{sequence}-{event_type.value}"
        return JournalRecord(
            event_id=event_id,
            ts_ns=ts_ns,
            event_type=event_type,
            package_id=package_id,
            opportunity_id=opportunity_id,
            relation_id=relation_id,
            payload=payload,
        )


class JournalReplayLoader:
    __slots__ = ("_storage", "_serializer")

    def __init__(self, storage: JournalStorage, serializer: JournalEventSerializer | None = None) -> None:
        self._storage = storage
        self._serializer = serializer or JournalEventSerializer()

    def iter_records(
        self,
        *,
        start_partition: str | None = None,
        end_partition: str | None = None,
        event_types: set[JournalEventType] | None = None,
    ) -> Iterator[JournalRecord]:
        for line in self._storage.iter_lines(start_partition=start_partition, end_partition=end_partition):
            record = self._serializer.loads(line)
            if event_types is not None and record.event_type not in event_types:
                continue
            yield record

    def load_records(
        self,
        *,
        start_partition: str | None = None,
        end_partition: str | None = None,
        event_types: set[JournalEventType] | None = None,
    ) -> list[JournalRecord]:
        return list(
            self.iter_records(
                start_partition=start_partition,
                end_partition=end_partition,
                event_types=event_types,
            )
        )


class JournalStateReconstructor:
    __slots__ = ()

    def reconstruct(self, records: Iterable[JournalRecord]) -> ReconstructedExecutorState:
        state = ReconstructedExecutorState()
        for record in records:
            self.apply_record(state, record)
        return state

    def apply_record(self, state: ReconstructedExecutorState, record: JournalRecord) -> None:
        if record.event_type == JournalEventType.OPPORTUNITY_ACCEPTED:
            if record.opportunity_id:
                state.accepted_opportunities.add(record.opportunity_id)
            if record.package_id and record.relation_id:
                state.relation_by_package[record.package_id] = record.relation_id
            return

        if record.event_type == JournalEventType.OPPORTUNITY_REJECTED:
            if record.opportunity_id:
                state.rejected_opportunities.add(record.opportunity_id)
            return

        if record.event_type == JournalEventType.KILL_SWITCH_ACTIVATED:
            state.kill_switch_active = True
            state.kill_switch_reason = _as_optional_str(record.payload.get("reason"))
            return

        if record.event_type == JournalEventType.EXECUTION_PLAN_CREATED:
            self._apply_plan_created(state, record)
            return

        if record.event_type == JournalEventType.PACKAGE_COMPLETED:
            self._force_terminal_state(state, record.package_id, PackageState.COMPLETED)
            return

        if record.event_type == JournalEventType.PACKAGE_FAILED:
            package_id = record.package_id
            if package_id is None:
                state.orphan_records.append(record)
                return
            package = state.active_packages.get(package_id)
            if package is None:
                state.orphan_records.append(record)
                return
            reason = _as_optional_str(record.payload.get("reason")) or "package failed"
            try:
                updated = PackageStateMachine.transition(
                    package,
                    PackageAbortEvent(package_id=package_id, reason=reason, ts_ns=record.ts_ns),
                )
            except Exception as exc:
                state.replay_errors.append(f"failed to apply package failure {record.event_id}: {exc}")
                return
            self._update_package_bucket(state, updated)
            return

        state_event = self._event_from_record(record)
        if state_event is None:
            return
        self._apply_state_event(state, state_event, record)

    def apply_normalized_event(
        self,
        state: ReconstructedExecutorState,
        *,
        event: NormalizedOrderEvent,
    ) -> None:
        pseudo_record = JournalRecord(
            event_id=f"reconcile-{uuid4().hex}",
            ts_ns=getattr(event, "ts_ns", 0),
            event_type=self._event_type_for_normalized(event),
            package_id=event.package_id,
            opportunity_id=None,
            relation_id=state.relation_by_package.get(event.package_id),
            payload={},
        )
        self._apply_state_event(state, event, pseudo_record)

    def _apply_plan_created(self, state: ReconstructedExecutorState, record: JournalRecord) -> None:
        package_id = record.package_id
        if package_id is None:
            state.orphan_records.append(record)
            return

        raw_plan = record.payload.get("plan")
        if not isinstance(raw_plan, dict):
            state.replay_errors.append(f"plan payload missing for record {record.event_id}")
            return

        try:
            plan = deserialize_execution_plan(raw_plan)
        except Exception as exc:
            state.replay_errors.append(f"plan decode failed for record {record.event_id}: {exc}")
            return

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

        package = create_package_execution(
            package_id=package_id,
            opportunity_id=plan.opportunity_id,
            created_ts_ns=record.ts_ns,
            legs=legs,
        )
        state.active_packages[package_id] = package
        if record.relation_id:
            state.relation_by_package[package_id] = record.relation_id

    def _force_terminal_state(
        self,
        state: ReconstructedExecutorState,
        package_id: str | None,
        package_state: PackageState,
    ) -> None:
        if package_id is None:
            return
        package = state.active_packages.pop(package_id, None)
        if package is None:
            package = state.terminal_packages.get(package_id)
            if package is None:
                return
        state.terminal_packages[package_id] = replace(package, state=package_state)

    def _event_from_record(self, record: JournalRecord) -> StateMachineEvent | None:
        event_payload = record.payload.get("event")
        if not isinstance(event_payload, dict):
            return None

        if record.event_type == JournalEventType.ORDER_SUBMITTED:
            return deserialize_order_intent(event_payload)
        if record.event_type == JournalEventType.ORDER_ACK:
            return deserialize_venue_order_ack(event_payload)
        if record.event_type == JournalEventType.ORDER_FILL:
            return deserialize_venue_fill_event(event_payload)
        if record.event_type == JournalEventType.ORDER_CANCEL_REQUESTED:
            return deserialize_cancel_requested_event(event_payload)
        if record.event_type == JournalEventType.ORDER_CANCEL_CONFIRMED:
            return deserialize_venue_cancel_event(event_payload)
        if record.event_type == JournalEventType.ORDER_REJECTED:
            return deserialize_venue_reject_event(event_payload)
        if record.event_type == JournalEventType.ORDER_TIMEOUT:
            return deserialize_leg_timeout_event(event_payload)
        return None

    def _apply_state_event(
        self,
        state: ReconstructedExecutorState,
        state_event: StateMachineEvent,
        record: JournalRecord,
    ) -> None:
        package_id = getattr(state_event, "package_id", None)
        if not isinstance(package_id, str):
            state.orphan_records.append(record)
            return

        package = state.active_packages.get(package_id)
        if package is None:
            state.orphan_records.append(record)
            return

        try:
            updated = PackageStateMachine.transition(package, state_event)
        except Exception as exc:
            state.replay_errors.append(f"failed to apply {record.event_type.value} {record.event_id}: {exc}")
            return
        self._update_package_bucket(state, updated)

    def _update_package_bucket(self, state: ReconstructedExecutorState, package: PackageExecution) -> None:
        terminal_states = {
            PackageState.COMPLETED,
            PackageState.ABORTED_UNWOUND,
            PackageState.FAILED,
            PackageState.HALTED,
        }
        if package.state in terminal_states:
            state.active_packages.pop(package.package_id, None)
            state.terminal_packages[package.package_id] = package
            return
        state.active_packages[package.package_id] = package
        state.terminal_packages.pop(package.package_id, None)

    @staticmethod
    def _event_type_for_normalized(event: NormalizedOrderEvent) -> JournalEventType:
        if isinstance(event, VenueOrderAck):
            return JournalEventType.ORDER_ACK
        if isinstance(event, VenueFillEvent):
            return JournalEventType.ORDER_FILL
        if isinstance(event, VenueCancelEvent):
            return JournalEventType.ORDER_CANCEL_CONFIRMED
        if isinstance(event, VenueRejectEvent):
            return JournalEventType.ORDER_REJECTED
        raise ValueError(f"unsupported normalized event: {type(event)!r}")


class RecoveryCoordinator:
    """Restart recovery coordinator.

    Flow:
    1) replay persisted journal to reconstruct in-memory state
    2) derive expected open client order ids from reconstructed active packages
    3) reconcile with venue open orders
    4) apply generated venue reconciliation events
    5) return recovery actions for ambiguous/missing state
    """

    __slots__ = ("_loader", "_reconstructor")

    def __init__(
        self,
        loader: JournalReplayLoader,
        reconstructor: JournalStateReconstructor | None = None,
    ) -> None:
        self._loader = loader
        self._reconstructor = reconstructor or JournalStateReconstructor()

    def recover(
        self,
        *,
        venue_adapter: VenueAdapter,
        start_partition: str | None = None,
        end_partition: str | None = None,
        additional_expected_open_client_order_ids: set[str] | None = None,
    ) -> RecoveryResult:
        records = self._loader.load_records(start_partition=start_partition, end_partition=end_partition)
        state = self._reconstructor.reconstruct(records)

        expected_open_ids = state.expected_open_client_order_ids()
        if additional_expected_open_client_order_ids:
            expected_open_ids.update(additional_expected_open_client_order_ids)

        reconciliation = venue_adapter.reconcile_open_orders(expected_open_client_order_ids=expected_open_ids)
        actions: list[RecoveryAction] = []

        for client_order_id in reconciliation.unknown_open_client_order_ids:
            actions.append(
                RecoveryAction(
                    action_type="UNKNOWN_OPEN_ORDER",
                    message="venue has open order not present in reconstructed state",
                    package_id=state.find_package_by_client_order_id(client_order_id),
                    client_order_id=client_order_id,
                )
            )

        for client_order_id in reconciliation.missing_expected_client_order_ids:
            actions.append(
                RecoveryAction(
                    action_type="MISSING_EXPECTED_ORDER",
                    message="reconstructed open order not found at venue; treat as ambiguous until resolved",
                    package_id=state.find_package_by_client_order_id(client_order_id),
                    client_order_id=client_order_id,
                )
            )

        for event in reconciliation.generated_events:
            self._reconstructor.apply_normalized_event(state, event=event)
            actions.append(
                RecoveryAction(
                    action_type="RECONCILED_EVENT_APPLIED",
                    message="applied reconciliation-generated order event",
                    package_id=event.package_id,
                    client_order_id=getattr(event, "client_order_id", None),
                )
            )

        return RecoveryResult(
            reconstructed_state=state,
            records_loaded=len(records),
            reconciliation=reconciliation,
            actions=tuple(actions),
        )


def partition_key_from_ts_ns(ts_ns: int) -> str:
    ts_seconds = ts_ns / 1_000_000_000
    return datetime.fromtimestamp(ts_seconds, tz=timezone.utc).strftime("%Y-%m-%d")


def serialize_execution_plan(plan: ExecutionPlan) -> dict[str, Any]:
    return {
        "plan_id": plan.plan_id,
        "opportunity_id": plan.opportunity_id,
        "created_ts_ns": plan.created_ts_ns,
        "expires_at_ns": plan.expires_at_ns,
        "package_timeout_ms": plan.package_timeout_ms,
        "package_units": plan.package_units,
        "tif": plan.tif.value,
        "total_shares": plan.total_shares,
        "total_notional_cents": plan.total_notional_cents,
        "expected_gross_profit_cents": plan.expected_gross_profit_cents,
        "expected_fee_cents": plan.expected_fee_cents,
        "expected_net_profit_cents": plan.expected_net_profit_cents,
        "expected_net_edge_bps": plan.expected_net_edge_bps,
        "legs": [serialize_planned_leg(leg) for leg in plan.legs],
    }


def deserialize_execution_plan(data: Mapping[str, Any]) -> ExecutionPlan:
    return ExecutionPlan(
        plan_id=str(data["plan_id"]),
        opportunity_id=str(data["opportunity_id"]),
        created_ts_ns=int(data["created_ts_ns"]),
        expires_at_ns=int(data["expires_at_ns"]),
        package_timeout_ms=int(data["package_timeout_ms"]),
        package_units=int(data["package_units"]),
        tif=TimeInForce(str(data["tif"])),
        total_shares=int(data["total_shares"]),
        total_notional_cents=int(data["total_notional_cents"]),
        expected_gross_profit_cents=int(data["expected_gross_profit_cents"]),
        expected_fee_cents=int(data["expected_fee_cents"]),
        expected_net_profit_cents=int(data["expected_net_profit_cents"]),
        expected_net_edge_bps=int(data["expected_net_edge_bps"]),
        legs=tuple(deserialize_planned_leg(item) for item in _as_mapping_list(data.get("legs"))),
    )


def serialize_planned_leg(leg: PlannedLeg) -> dict[str, Any]:
    return {
        "leg_id": leg.leg_id,
        "market_id": leg.market_id,
        "token_id": leg.token_id,
        "side": leg.side.value,
        "submission_rank": leg.submission_rank,
        "quantity": leg.quantity,
        "executable_price_ticks": leg.executable_price_ticks,
        "limit_price_ticks": leg.limit_price_ticks,
        "tick_size_ticks": leg.tick_size_ticks,
        "timeout_ms": leg.timeout_ms,
        "tif": leg.tif.value,
        "snapshot_ts_ns": leg.snapshot_ts_ns,
        "available_units_at_plan": leg.available_units_at_plan,
    }


def deserialize_planned_leg(data: Mapping[str, Any]) -> PlannedLeg:
    market_id = str(data["market_id"])
    token_id_raw = _as_optional_str(data.get("token_id"))
    token_id = token_id_raw if token_id_raw else market_id
    return PlannedLeg(
        leg_id=str(data["leg_id"]),
        market_id=market_id,
        token_id=token_id,
        side=Side(str(data["side"])),
        submission_rank=int(data["submission_rank"]),
        quantity=int(data["quantity"]),
        executable_price_ticks=int(data["executable_price_ticks"]),
        limit_price_ticks=int(data["limit_price_ticks"]),
        tick_size_ticks=int(data["tick_size_ticks"]),
        timeout_ms=int(data["timeout_ms"]),
        tif=TimeInForce(str(data["tif"])),
        snapshot_ts_ns=int(data["snapshot_ts_ns"]),
        available_units_at_plan=int(data["available_units_at_plan"]),
    )


def serialize_order_intent(event: OrderIntent) -> dict[str, Any]:
    return {
        "package_id": event.package_id,
        "leg_id": event.leg_id,
        "client_order_id": event.client_order_id,
        "qty": event.qty,
        "limit_price_ticks": event.limit_price_ticks,
        "tif": event.tif.value,
        "ts_ns": event.ts_ns,
        "side": event.side.value if event.side is not None else None,
        "market_id": event.market_id,
        "is_unwind": event.is_unwind,
    }


def deserialize_order_intent(data: Mapping[str, Any]) -> OrderIntent:
    side_value = _as_optional_str(data.get("side"))
    return OrderIntent(
        package_id=str(data["package_id"]),
        leg_id=str(data["leg_id"]),
        client_order_id=str(data["client_order_id"]),
        qty=int(data["qty"]),
        limit_price_ticks=int(data["limit_price_ticks"]),
        tif=TimeInForce(str(data["tif"])),
        ts_ns=int(data["ts_ns"]),
        side=Side(side_value) if side_value is not None else None,
        market_id=_as_optional_str(data.get("market_id")),
        is_unwind=bool(data.get("is_unwind", False)),
    )


def serialize_venue_order_ack(event: VenueOrderAck) -> dict[str, Any]:
    return asdict(event)


def deserialize_venue_order_ack(data: Mapping[str, Any]) -> VenueOrderAck:
    return VenueOrderAck(
        package_id=str(data["package_id"]),
        leg_id=str(data["leg_id"]),
        client_order_id=str(data["client_order_id"]),
        venue_order_id=str(data["venue_order_id"]),
        ts_ns=int(data["ts_ns"]),
    )


def serialize_venue_fill_event(event: VenueFillEvent) -> dict[str, Any]:
    return asdict(event)


def deserialize_venue_fill_event(data: Mapping[str, Any]) -> VenueFillEvent:
    cumulative = data.get("cumulative_qty")
    return VenueFillEvent(
        package_id=str(data["package_id"]),
        leg_id=str(data["leg_id"]),
        client_order_id=str(data["client_order_id"]),
        fill_qty=int(data["fill_qty"]),
        fill_price_ticks=int(data["fill_price_ticks"]),
        ts_ns=int(data["ts_ns"]),
        cumulative_qty=int(cumulative) if cumulative is not None else None,
    )


def serialize_cancel_requested_event(event: CancelRequestedEvent) -> dict[str, Any]:
    return asdict(event)


def deserialize_cancel_requested_event(data: Mapping[str, Any]) -> CancelRequestedEvent:
    return CancelRequestedEvent(
        package_id=str(data["package_id"]),
        leg_id=str(data["leg_id"]),
        reason=str(data["reason"]),
        ts_ns=int(data["ts_ns"]),
    )


def serialize_venue_cancel_event(event: VenueCancelEvent) -> dict[str, Any]:
    return asdict(event)


def deserialize_venue_cancel_event(data: Mapping[str, Any]) -> VenueCancelEvent:
    return VenueCancelEvent(
        package_id=str(data["package_id"]),
        leg_id=str(data["leg_id"]),
        client_order_id=str(data["client_order_id"]),
        canceled_qty=int(data["canceled_qty"]),
        reason=str(data["reason"]),
        ts_ns=int(data["ts_ns"]),
    )


def serialize_venue_reject_event(event: VenueRejectEvent) -> dict[str, Any]:
    return asdict(event)


def deserialize_venue_reject_event(data: Mapping[str, Any]) -> VenueRejectEvent:
    return VenueRejectEvent(
        package_id=str(data["package_id"]),
        leg_id=str(data["leg_id"]),
        client_order_id=str(data["client_order_id"]),
        reason=str(data["reason"]),
        ts_ns=int(data["ts_ns"]),
    )


def serialize_leg_timeout_event(event: LegTimeoutEvent) -> dict[str, Any]:
    return asdict(event)


def deserialize_leg_timeout_event(data: Mapping[str, Any]) -> LegTimeoutEvent:
    return LegTimeoutEvent(
        package_id=str(data["package_id"]),
        leg_id=str(data["leg_id"]),
        reason=str(data["reason"]),
        ts_ns=int(data["ts_ns"]),
    )


def _to_json_primitive(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value
    if is_dataclass(value):
        return _to_json_primitive(asdict(value))
    if isinstance(value, dict):
        return {str(k): _to_json_primitive(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, deque, set)):
        return [_to_json_primitive(v) for v in value]
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def _as_optional_str(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return str(value)


def _as_mapping_list(value: object) -> list[Mapping[str, Any]]:
    if not isinstance(value, list):
        return []
    output: list[Mapping[str, Any]] = []
    for item in value:
        if isinstance(item, dict):
            output.append(item)
    return output