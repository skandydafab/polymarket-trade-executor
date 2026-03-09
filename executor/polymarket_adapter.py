from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from hashlib import blake2b
from time import perf_counter
from typing import Mapping, Protocol, Sequence

from .state_machine import VenueCancelEvent, VenueFillEvent, VenueOrderAck, VenueRejectEvent
from .venue import (
    CancelOrderResult,
    CancelOrderStatus,
    ClientOrderIdFactory,
    ClientOrderIdFactoryConfig,
    MetricsSink,
    NormalizedOrderEvent,
    NullLogger,
    NullMetrics,
    ReconciliationResult,
    StructuredLogger,
    SubmitOrderResult,
    SubmitOrderStatus,
    VenueAdapter,
    VenueOrderIntent,
)


class VenueTransportError(RuntimeError):
    """Raised for network/transport-level failures reaching the venue."""


class VenueTimeoutError(TimeoutError):
    """Raised when a venue request times out without deterministic outcome."""


class _OrderLifecycle(str, Enum):
    NEW = "NEW"
    SUBMITTING = "SUBMITTING"
    ACKED = "ACKED"
    PARTIAL = "PARTIAL"
    FILLED = "FILLED"
    CANCELED = "CANCELED"
    REJECTED = "REJECTED"
    AMBIGUOUS = "AMBIGUOUS"


@dataclass(frozen=True, slots=True)
class PolymarketAdapterConfig:
    api_url: str
    private_key: str | None = None
    api_key: str | None = None
    api_secret: str | None = None
    passphrase: str | None = None
    submit_timeout_ms: int = 500
    cancel_timeout_ms: int = 500
    poll_timeout_ms: int = 700
    poll_batch_limit: int = 200
    client_order_id_prefix: str = "pmx"
    client_order_id_max_length: int = 96


@dataclass(frozen=True, slots=True)
class PolymarketOrderRequest:
    market_id: str
    token_id: str
    side: str
    size: str
    price: str
    tif: str
    client_order_id: str
    expiration_ts: int | None


@dataclass(frozen=True, slots=True)
class PolymarketSubmitResponse:
    accepted: bool
    order_id: str | None
    status: str
    reason: str | None
    raw: Mapping[str, object]


@dataclass(frozen=True, slots=True)
class PolymarketOrderUpdate:
    client_order_id: str
    venue_order_id: str | None
    status: str
    ts_ns: int
    sequence: int | None
    event_id: str | None
    fill_delta: int | None = None
    cumulative_fill: int | None = None
    fill_price_ticks: int | None = None
    canceled_qty: int | None = None
    reason: str | None = None


@dataclass(slots=True)
class _OrderRecord:
    package_id: str
    leg_id: str
    market_id: str
    token_id: str
    quantity: int
    client_order_id: str
    venue_order_id: str | None
    submit_fingerprint: str
    submit_attempts: int
    cancel_attempts: int
    lifecycle: _OrderLifecycle
    last_sequence: int
    last_ts_ns: int
    cumulative_fill_qty: int
    ambiguous_submit: bool
    ambiguous_cancel: bool


@dataclass(frozen=True, slots=True)
class _NormalizedEnvelope:
    event_key: str
    sequence: int | None
    ts_ns: int
    client_order_id: str
    event: NormalizedOrderEvent


class PolymarketCLOBClient(Protocol):
    """Minimal client interface required by adapter.

    Concrete implementation can wrap polymarket-clob-client or custom transport.
    """

    def create_signed_order(self, request: PolymarketOrderRequest) -> Mapping[str, object]: ...

    def submit_order(self, signed_order: Mapping[str, object], timeout_ms: int) -> Mapping[str, object]: ...

    def cancel_order(
        self,
        *,
        client_order_id: str | None = None,
        venue_order_id: str | None = None,
        timeout_ms: int,
    ) -> Mapping[str, object]: ...

    def get_order_updates(
        self,
        *,
        since_sequence: int | None,
        limit: int,
        timeout_ms: int,
    ) -> Sequence[Mapping[str, object]]: ...

    def get_open_orders(self, *, timeout_ms: int) -> Sequence[Mapping[str, object]]: ...


class PolymarketVenueAdapter(VenueAdapter):
    """Production-oriented Polymarket adapter skeleton.

    Hot path expectations:
    - submit_order: synchronous and latency-sensitive; minimal CPU work.
    - cancel_order: synchronous but not strategy-critical; still bounded timeout.
    - poll_or_process_order_updates: can run on a dedicated I/O loop/thread.
    - reconcile_open_orders: deferred to startup/recovery and periodic audit jobs.

    Safety principles:
    - Network errors on submit/cancel are ambiguous until confirmed by updates.
    - Duplicate and out-of-order updates are deduplicated before emission.
    - Internal order lifecycle is monotonic and never regresses.
    """

    __slots__ = (
        "_client",
        "_config",
        "_logger",
        "_metrics",
        "_id_factory",
        "_orders_by_client_id",
        "_client_by_venue_id",
        "_processed_event_keys",
        "_update_cursor",
        "_attempts_by_leg",
    )

    def __init__(
        self,
        client: PolymarketCLOBClient,
        config: PolymarketAdapterConfig,
        *,
        logger: StructuredLogger | None = None,
        metrics: MetricsSink | None = None,
    ) -> None:
        self._validate_config(config)
        self._client = client
        self._config = config
        self._logger = logger or NullLogger()
        self._metrics = metrics or NullMetrics()
        self._id_factory = ClientOrderIdFactory(
            ClientOrderIdFactoryConfig(
                prefix=config.client_order_id_prefix,
                max_length=config.client_order_id_max_length,
            )
        )
        self._orders_by_client_id: dict[str, _OrderRecord] = {}
        self._client_by_venue_id: dict[str, str] = {}
        self._processed_event_keys: set[str] = set()
        self._update_cursor: int | None = None
        self._attempts_by_leg: dict[tuple[str, str], int] = {}

    def submit_order(self, intent: VenueOrderIntent) -> SubmitOrderResult:
        started = perf_counter()
        self._validate_intent(intent)
        leg_key = (intent.package_id, intent.leg_id)
        next_attempt = self._attempts_by_leg.get(leg_key, 0) + 1

        client_order_id = intent.client_order_id or self._id_factory.build(
            package_id=intent.package_id,
            leg_id=intent.leg_id,
            attempt=next_attempt,
            now_ns=intent.ts_ns,
        )

        fingerprint = self._fingerprint_intent(intent, client_order_id)
        existing = self._orders_by_client_id.get(client_order_id)
        if existing is not None:
            if existing.submit_fingerprint != fingerprint:
                raise ValueError(
                    f"client_order_id collision with different payload: {client_order_id}"
                )

            status = SubmitOrderStatus.AMBIGUOUS if existing.ambiguous_submit else SubmitOrderStatus.ALREADY_SUBMITTED
            self._metrics.increment("venue.submit.idempotent_hit", tags={"status": status.value})
            return SubmitOrderResult(
                status=status,
                client_order_id=client_order_id,
                venue_order_id=existing.venue_order_id,
                events=tuple(),
                ambiguous=existing.ambiguous_submit,
                message="duplicate submit prevented by adapter idempotency",
            )

        self._attempts_by_leg[leg_key] = next_attempt
        record = _OrderRecord(
            package_id=intent.package_id,
            leg_id=intent.leg_id,
            market_id=intent.market_id,
            token_id=intent.token_id,
            quantity=intent.quantity,
            client_order_id=client_order_id,
            venue_order_id=None,
            submit_fingerprint=fingerprint,
            submit_attempts=1,
            cancel_attempts=0,
            lifecycle=_OrderLifecycle.SUBMITTING,
            last_sequence=-1,
            last_ts_ns=intent.ts_ns,
            cumulative_fill_qty=0,
            ambiguous_submit=False,
            ambiguous_cancel=False,
        )
        self._orders_by_client_id[client_order_id] = record

        request = self._build_request(intent, client_order_id)
        try:
            signed = self._client.create_signed_order(request)
            raw_response = self._client.submit_order(signed, timeout_ms=self._config.submit_timeout_ms)
        except TimeoutError as exc:
            record.ambiguous_submit = True
            record.lifecycle = _OrderLifecycle.AMBIGUOUS
            self._metrics.increment("venue.submit.timeout")
            self._logger.warning(
                "submit timeout produced ambiguous outcome",
                client_order_id=client_order_id,
                package_id=intent.package_id,
                leg_id=intent.leg_id,
            )
            raise VenueTimeoutError(
                f"submit timeout for client_order_id={client_order_id}; outcome ambiguous"
            ) from exc
        except OSError as exc:
            record.ambiguous_submit = True
            record.lifecycle = _OrderLifecycle.AMBIGUOUS
            self._metrics.increment("venue.submit.network_error")
            self._logger.warning(
                "submit network error produced ambiguous outcome",
                client_order_id=client_order_id,
                package_id=intent.package_id,
                leg_id=intent.leg_id,
                error=str(exc),
            )
            raise VenueTransportError(
                f"submit network error for client_order_id={client_order_id}; outcome ambiguous"
            ) from exc

        response = self._normalize_submit_response(raw_response)
        latency_ms = (perf_counter() - started) * 1000.0
        self._metrics.observe("venue.submit.latency_ms", latency_ms)

        if response.accepted:
            ack = VenueOrderAck(
                package_id=intent.package_id,
                leg_id=intent.leg_id,
                client_order_id=client_order_id,
                venue_order_id=response.order_id or "",
                ts_ns=intent.ts_ns,
            )
            record.venue_order_id = response.order_id
            record.lifecycle = _OrderLifecycle.ACKED
            record.ambiguous_submit = False
            if response.order_id:
                self._client_by_venue_id[response.order_id] = client_order_id
            self._logger.info(
                "submit acknowledged",
                client_order_id=client_order_id,
                venue_order_id=response.order_id,
                package_id=intent.package_id,
                leg_id=intent.leg_id,
            )
            self._metrics.increment("venue.submit.ack")
            return SubmitOrderResult(
                status=SubmitOrderStatus.ACKNOWLEDGED,
                client_order_id=client_order_id,
                venue_order_id=response.order_id,
                events=(ack,),
                ambiguous=False,
                message=response.status,
            )

        reject = VenueRejectEvent(
            package_id=intent.package_id,
            leg_id=intent.leg_id,
            client_order_id=client_order_id,
            reason=response.reason or response.status,
            ts_ns=intent.ts_ns,
        )
        record.lifecycle = _OrderLifecycle.REJECTED
        record.ambiguous_submit = False
        self._logger.warning(
            "submit rejected",
            client_order_id=client_order_id,
            package_id=intent.package_id,
            leg_id=intent.leg_id,
            reason=response.reason,
        )
        self._metrics.increment("venue.submit.reject")
        return SubmitOrderResult(
            status=SubmitOrderStatus.REJECTED,
            client_order_id=client_order_id,
            venue_order_id=response.order_id,
            events=(reject,),
            ambiguous=False,
            message=response.reason,
        )

    def cancel_order(
        self,
        *,
        client_order_id: str | None = None,
        venue_order_id: str | None = None,
        reason: str = "",
        now_ns: int | None = None,
    ) -> CancelOrderResult:
        if not client_order_id and not venue_order_id:
            raise ValueError("cancel requires client_order_id or venue_order_id")

        resolved_client_id = client_order_id or self._client_by_venue_id.get(venue_order_id or "")
        record = self._orders_by_client_id.get(resolved_client_id) if resolved_client_id else None

        if record and record.lifecycle in {_OrderLifecycle.CANCELED, _OrderLifecycle.FILLED, _OrderLifecycle.REJECTED}:
            status = CancelOrderStatus.ALREADY_CANCELED if record.lifecycle == _OrderLifecycle.CANCELED else CancelOrderStatus.NOT_FOUND
            return CancelOrderResult(
                status=status,
                client_order_id=record.client_order_id,
                venue_order_id=record.venue_order_id,
                events=tuple(),
                ambiguous=False,
                message="cancel is idempotent on terminal order",
            )

        cancel_ts_ns = now_ns if now_ns is not None else 0
        try:
            raw_response = self._client.cancel_order(
                client_order_id=resolved_client_id,
                venue_order_id=venue_order_id,
                timeout_ms=self._config.cancel_timeout_ms,
            )
        except TimeoutError as exc:
            if record:
                record.ambiguous_cancel = True
            self._metrics.increment("venue.cancel.timeout")
            raise VenueTimeoutError(
                f"cancel timeout for client_order_id={resolved_client_id}; outcome ambiguous"
            ) from exc
        except OSError as exc:
            if record:
                record.ambiguous_cancel = True
            self._metrics.increment("venue.cancel.network_error")
            raise VenueTransportError(
                f"cancel network error for client_order_id={resolved_client_id}; outcome ambiguous"
            ) from exc

        status_text = str(raw_response.get("status", "")).upper()
        if status_text in {"NOT_FOUND", "UNKNOWN_ORDER"}:
            self._metrics.increment("venue.cancel.not_found")
            return CancelOrderResult(
                status=CancelOrderStatus.NOT_FOUND,
                client_order_id=resolved_client_id,
                venue_order_id=venue_order_id,
                events=tuple(),
                ambiguous=False,
                message=status_text,
            )

        if record is None:
            self._metrics.increment("venue.cancel.unknown_client_id")
            return CancelOrderResult(
                status=CancelOrderStatus.CANCELED,
                client_order_id=resolved_client_id,
                venue_order_id=venue_order_id,
                events=tuple(),
                ambiguous=False,
                message="cancel acknowledged for externally-tracked order",
            )

        canceled_qty = _as_int(raw_response.get("canceled_size"), default=0)
        cancel_event = VenueCancelEvent(
            package_id=record.package_id,
            leg_id=record.leg_id,
            client_order_id=record.client_order_id,
            canceled_qty=canceled_qty,
            reason=reason or status_text or "venue_cancel_ack",
            ts_ns=cancel_ts_ns,
        )
        record.lifecycle = _OrderLifecycle.CANCELED
        record.ambiguous_cancel = False
        self._metrics.increment("venue.cancel.ack")
        return CancelOrderResult(
            status=CancelOrderStatus.CANCELED,
            client_order_id=record.client_order_id,
            venue_order_id=record.venue_order_id,
            events=(cancel_event,),
            ambiguous=False,
            message=status_text or "CANCELED",
        )

    def poll_or_process_order_updates(
        self,
        raw_updates: Sequence[Mapping[str, object]] | None = None,
    ) -> tuple[NormalizedOrderEvent, ...]:
        """Normalize raw venue updates and emit deduplicated internal events.

        Duplicate ack/fill/cancel/reject updates are dropped by event-key dedupe.
        Out-of-order updates are ignored when sequence regresses for a known order.
        """

        updates = raw_updates
        if updates is None:
            updates = self._client.get_order_updates(
                since_sequence=self._update_cursor,
                limit=self._config.poll_batch_limit,
                timeout_ms=self._config.poll_timeout_ms,
            )

        emitted: list[NormalizedOrderEvent] = []
        for raw in updates:
            normalized = self._normalize_order_update(raw)
            if normalized is None:
                continue

            if normalized.event_key in self._processed_event_keys:
                self._metrics.increment("venue.update.duplicate")
                continue

            record = self._orders_by_client_id.get(normalized.client_order_id)
            if record and normalized.sequence is not None and normalized.sequence <= record.last_sequence:
                self._metrics.increment("venue.update.out_of_order")
                self._logger.debug(
                    "dropping out-of-order update",
                    client_order_id=normalized.client_order_id,
                    update_sequence=normalized.sequence,
                    last_sequence=record.last_sequence,
                )
                continue

            self._processed_event_keys.add(normalized.event_key)
            emitted.append(normalized.event)
            self._apply_event_to_record(normalized.event)

            if record:
                if normalized.sequence is not None:
                    record.last_sequence = max(record.last_sequence, normalized.sequence)
                record.last_ts_ns = max(record.last_ts_ns, normalized.ts_ns)

            if normalized.sequence is not None:
                self._update_cursor = normalized.sequence if self._update_cursor is None else max(
                    self._update_cursor,
                    normalized.sequence,
                )

        return tuple(emitted)

    def reconcile_open_orders(self, expected_open_client_order_ids: set[str]) -> ReconciliationResult:
        """Reconcile venue open orders with executor expected-open set.

        Startup/recovery pattern:
        1) load persisted order correlation/journal
        2) call reconcile_open_orders(expected_open)
        3) process generated ack events and then normal update polling
        """

        raw_open = self._client.get_open_orders(timeout_ms=self._config.poll_timeout_ms)
        venue_open_ids: set[str] = set()
        generated: list[NormalizedOrderEvent] = []

        for item in raw_open:
            client_order_id = _as_str(item.get("client_order_id") or item.get("clientOrderId"))
            if not client_order_id:
                continue
            venue_open_ids.add(client_order_id)

            venue_order_id = _as_str(item.get("order_id") or item.get("id"))
            record = self._orders_by_client_id.get(client_order_id)
            if record is None:
                parsed = self._id_factory.parse(client_order_id)
                if parsed and parsed.package_id and parsed.leg_id:
                    # Best-effort bootstrap for recovery if this process was restarted
                    # and journal replay has not yet hydrated in-memory state.
                    record = _OrderRecord(
                        package_id=parsed.package_id,
                        leg_id=parsed.leg_id,
                        market_id="",
                        token_id="",
                        quantity=0,
                        client_order_id=client_order_id,
                        venue_order_id=venue_order_id,
                        submit_fingerprint="",
                        submit_attempts=max(parsed.attempt, 1),
                        cancel_attempts=0,
                        lifecycle=_OrderLifecycle.ACKED,
                        last_sequence=-1,
                        last_ts_ns=0,
                        cumulative_fill_qty=0,
                        ambiguous_submit=False,
                        ambiguous_cancel=False,
                    )
                    self._orders_by_client_id[client_order_id] = record
                else:
                    continue

            if venue_order_id:
                record.venue_order_id = venue_order_id
                self._client_by_venue_id[venue_order_id] = client_order_id

            if record.lifecycle in {_OrderLifecycle.NEW, _OrderLifecycle.SUBMITTING, _OrderLifecycle.AMBIGUOUS}:
                ack = VenueOrderAck(
                    package_id=record.package_id,
                    leg_id=record.leg_id,
                    client_order_id=record.client_order_id,
                    venue_order_id=record.venue_order_id or "",
                    ts_ns=0,
                )
                generated.append(ack)
                record.lifecycle = _OrderLifecycle.ACKED

        unknown_open = tuple(sorted(venue_open_ids - expected_open_client_order_ids))
        missing_expected = tuple(sorted(expected_open_client_order_ids - venue_open_ids))

        self._logger.info(
            "reconciliation complete",
            venue_open_count=len(venue_open_ids),
            unknown_open_count=len(unknown_open),
            missing_expected_count=len(missing_expected),
        )
        self._metrics.increment("venue.reconcile.runs")
        return ReconciliationResult(
            venue_open_client_order_ids=tuple(sorted(venue_open_ids)),
            unknown_open_client_order_ids=unknown_open,
            missing_expected_client_order_ids=missing_expected,
            generated_events=tuple(generated),
        )

    def _build_request(self, intent: VenueOrderIntent, client_order_id: str) -> PolymarketOrderRequest:
        return PolymarketOrderRequest(
            market_id=intent.market_id,
            token_id=intent.token_id,
            side=intent.side.value,
            size=str(intent.quantity),
            price=str(intent.limit_price_ticks),
            tif=intent.tif.value,
            client_order_id=client_order_id,
            expiration_ts=(intent.expires_at_ns // 1_000_000_000) if intent.expires_at_ns else None,
        )

    def _normalize_submit_response(self, raw: Mapping[str, object]) -> PolymarketSubmitResponse:
        status = _as_str(raw.get("status") or raw.get("state") or "UNKNOWN").upper()
        order_id = _as_str(raw.get("order_id") or raw.get("id"))
        reason = _as_str(raw.get("reason") or raw.get("error") or raw.get("message"))

        accepted_states = {"ACCEPTED", "OPEN", "LIVE", "PLACED", "SUCCESS", "OK"}
        rejected_states = {"REJECTED", "FAILED", "ERROR", "INVALID"}

        if status in accepted_states or (order_id and status not in rejected_states):
            return PolymarketSubmitResponse(
                accepted=True,
                order_id=order_id,
                status=status,
                reason=reason,
                raw=raw,
            )

        return PolymarketSubmitResponse(
            accepted=False,
            order_id=order_id,
            status=status,
            reason=reason,
            raw=raw,
        )

    def _normalize_order_update(self, raw: Mapping[str, object]) -> _NormalizedEnvelope | None:
        update = _parse_raw_update(raw)
        if not update.client_order_id:
            self._metrics.increment("venue.update.missing_client_order_id")
            return None

        record = self._orders_by_client_id.get(update.client_order_id)
        if record is None:
            parsed = self._id_factory.parse(update.client_order_id)
            if parsed and parsed.package_id and parsed.leg_id:
                record = _OrderRecord(
                    package_id=parsed.package_id,
                    leg_id=parsed.leg_id,
                    market_id="",
                    token_id="",
                    quantity=0,
                    client_order_id=update.client_order_id,
                    venue_order_id=update.venue_order_id,
                    submit_fingerprint="",
                    submit_attempts=max(parsed.attempt, 1),
                    cancel_attempts=0,
                    lifecycle=_OrderLifecycle.NEW,
                    last_sequence=-1,
                    last_ts_ns=0,
                    cumulative_fill_qty=0,
                    ambiguous_submit=False,
                    ambiguous_cancel=False,
                )
                self._orders_by_client_id[update.client_order_id] = record
            else:
                self._logger.warning(
                    "dropping update for unknown client_order_id",
                    client_order_id=update.client_order_id,
                )
                self._metrics.increment("venue.update.unknown_order")
                return None

        if update.venue_order_id:
            record.venue_order_id = update.venue_order_id
            self._client_by_venue_id[update.venue_order_id] = update.client_order_id

        # Fill updates have priority because they represent realized exposure.
        if update.fill_delta is not None and update.fill_delta > 0:
            fill_event = VenueFillEvent(
                package_id=record.package_id,
                leg_id=record.leg_id,
                client_order_id=record.client_order_id,
                fill_qty=update.fill_delta,
                fill_price_ticks=update.fill_price_ticks or 0,
                ts_ns=update.ts_ns,
                cumulative_qty=update.cumulative_fill,
            )
            key = self._event_key(update, suffix="fill")
            return _NormalizedEnvelope(
                event_key=key,
                sequence=update.sequence,
                ts_ns=update.ts_ns,
                client_order_id=record.client_order_id,
                event=fill_event,
            )

        status = update.status.upper()
        if status in {"OPEN", "LIVE", "PLACED", "ACCEPTED", "ACK"}:
            ack = VenueOrderAck(
                package_id=record.package_id,
                leg_id=record.leg_id,
                client_order_id=record.client_order_id,
                venue_order_id=record.venue_order_id or "",
                ts_ns=update.ts_ns,
            )
            return _NormalizedEnvelope(
                event_key=self._event_key(update, suffix="ack"),
                sequence=update.sequence,
                ts_ns=update.ts_ns,
                client_order_id=record.client_order_id,
                event=ack,
            )

        if status in {"REJECTED", "FAILED", "ERROR", "INVALID"}:
            reject = VenueRejectEvent(
                package_id=record.package_id,
                leg_id=record.leg_id,
                client_order_id=record.client_order_id,
                reason=update.reason or status,
                ts_ns=update.ts_ns,
            )
            return _NormalizedEnvelope(
                event_key=self._event_key(update, suffix="reject"),
                sequence=update.sequence,
                ts_ns=update.ts_ns,
                client_order_id=record.client_order_id,
                event=reject,
            )

        if status in {"CANCELED", "CANCELLED", "DONE"}:
            cancel = VenueCancelEvent(
                package_id=record.package_id,
                leg_id=record.leg_id,
                client_order_id=record.client_order_id,
                canceled_qty=update.canceled_qty or 0,
                reason=update.reason or status,
                ts_ns=update.ts_ns,
            )
            return _NormalizedEnvelope(
                event_key=self._event_key(update, suffix="cancel"),
                sequence=update.sequence,
                ts_ns=update.ts_ns,
                client_order_id=record.client_order_id,
                event=cancel,
            )

        self._metrics.increment("venue.update.ignored", tags={"status": status})
        return None

    def _apply_event_to_record(self, event: NormalizedOrderEvent) -> None:
        if isinstance(event, VenueOrderAck):
            record = self._orders_by_client_id.get(event.client_order_id)
            if record:
                record.lifecycle = _OrderLifecycle.ACKED
                record.ambiguous_submit = False
                if event.venue_order_id:
                    record.venue_order_id = event.venue_order_id
                    self._client_by_venue_id[event.venue_order_id] = record.client_order_id
            return

        if isinstance(event, VenueFillEvent):
            record = self._orders_by_client_id.get(event.client_order_id)
            if record:
                next_qty = event.cumulative_qty if event.cumulative_qty is not None else record.cumulative_fill_qty + event.fill_qty
                if next_qty < record.cumulative_fill_qty:
                    return
                record.cumulative_fill_qty = next_qty
                if record.quantity > 0 and record.cumulative_fill_qty >= record.quantity:
                    record.lifecycle = _OrderLifecycle.FILLED
                else:
                    record.lifecycle = _OrderLifecycle.PARTIAL
            return

        if isinstance(event, VenueCancelEvent):
            record = self._orders_by_client_id.get(event.client_order_id)
            if record:
                record.lifecycle = _OrderLifecycle.CANCELED
                record.ambiguous_cancel = False
            return

        if isinstance(event, VenueRejectEvent):
            record = self._orders_by_client_id.get(event.client_order_id)
            if record:
                record.lifecycle = _OrderLifecycle.REJECTED
                record.ambiguous_submit = False

    def _event_key(self, update: PolymarketOrderUpdate, suffix: str) -> str:
        if update.event_id:
            return f"{update.event_id}:{suffix}"
        seq_part = str(update.sequence) if update.sequence is not None else "ns"
        key_material = f"{update.client_order_id}|{seq_part}|{update.ts_ns}|{suffix}|{update.status}|{update.fill_delta}|{update.cumulative_fill}"
        digest = blake2b(key_material.encode("utf-8"), digest_size=12).hexdigest()
        return f"{update.client_order_id}:{digest}:{suffix}"

    @staticmethod
    def _fingerprint_intent(intent: VenueOrderIntent, client_order_id: str) -> str:
        payload = (
            f"{intent.package_id}|{intent.leg_id}|{intent.market_id}|{intent.token_id}|"
            f"{intent.side.value}|{intent.quantity}|{intent.limit_price_ticks}|{intent.tif.value}|"
            f"{intent.expires_at_ns}|{client_order_id}"
        )
        return blake2b(payload.encode("utf-8"), digest_size=16).hexdigest()

    @staticmethod
    def _validate_intent(intent: VenueOrderIntent) -> None:
        if not intent.package_id:
            raise ValueError("intent package_id is required")
        if not intent.leg_id:
            raise ValueError("intent leg_id is required")
        if not intent.market_id:
            raise ValueError("intent market_id is required")
        if not intent.token_id:
            raise ValueError("intent token_id is required")
        if intent.quantity <= 0:
            raise ValueError("intent quantity must be > 0")
        if intent.limit_price_ticks <= 0:
            raise ValueError("intent limit_price_ticks must be > 0")

    @staticmethod
    def _validate_config(config: PolymarketAdapterConfig) -> None:
        if not config.api_url:
            raise ValueError("api_url is required")
        if config.submit_timeout_ms <= 0:
            raise ValueError("submit_timeout_ms must be > 0")
        if config.cancel_timeout_ms <= 0:
            raise ValueError("cancel_timeout_ms must be > 0")
        if config.poll_timeout_ms <= 0:
            raise ValueError("poll_timeout_ms must be > 0")
        if config.poll_batch_limit <= 0:
            raise ValueError("poll_batch_limit must be > 0")
        if not config.client_order_id_prefix:
            raise ValueError("client_order_id_prefix is required")
        if config.client_order_id_max_length < 32:
            raise ValueError("client_order_id_max_length must be >= 32")


def _parse_raw_update(raw: Mapping[str, object]) -> PolymarketOrderUpdate:
    return PolymarketOrderUpdate(
        client_order_id=_as_str(raw.get("client_order_id") or raw.get("clientOrderId")) or "",
        venue_order_id=_as_str(raw.get("order_id") or raw.get("id")),
        status=_as_str(raw.get("status") or raw.get("state") or "UNKNOWN") or "UNKNOWN",
        ts_ns=_as_ts_ns(raw),
        sequence=_as_int(raw.get("sequence") or raw.get("seq"), default=None),
        event_id=_as_str(raw.get("event_id") or raw.get("eventId")),
        fill_delta=_as_int(raw.get("fill_delta") or raw.get("last_fill_qty"), default=None),
        cumulative_fill=_as_int(raw.get("cumulative_fill") or raw.get("filled_size"), default=None),
        fill_price_ticks=_as_int(raw.get("fill_price") or raw.get("last_fill_price"), default=None),
        canceled_qty=_as_int(raw.get("canceled_size"), default=None),
        reason=_as_str(raw.get("reason") or raw.get("error") or raw.get("message")),
    )


def _as_ts_ns(raw: Mapping[str, object]) -> int:
    ns = _as_int(raw.get("ts_ns"), default=None)
    if ns is not None:
        return ns
    ms = _as_int(raw.get("timestamp_ms") or raw.get("ts_ms") or raw.get("timestamp"), default=0)
    return ms * 1_000_000


def _as_str(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return str(value)


def _as_int(value: object, default: int | None) -> int | None:
    if value is None:
        return default
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return default
        try:
            if "." in text:
                return int(float(text))
            return int(text)
        except ValueError:
            return default
    return default