from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Sequence

from .state_machine import VenueCancelEvent, VenueFillEvent, VenueOrderAck, VenueRejectEvent
from .venue import (
    CancelOrderResult,
    CancelOrderStatus,
    ClientOrderIdFactory,
    ClientOrderIdFactoryConfig,
    NormalizedOrderEvent,
    ReconciliationResult,
    SubmitOrderResult,
    SubmitOrderStatus,
    VenueAdapter,
    VenueOrderIntent,
)


@dataclass(slots=True)
class _FakeOrderRecord:
    package_id: str
    leg_id: str
    client_order_id: str
    venue_order_id: str
    filled_qty: int
    canceled: bool
    rejected: bool


class FakeVenueAdapter(VenueAdapter):
    """Small deterministic fake adapter for unit/integration tests.

    Behavior:
    - submit_order returns immediate ack and stores order
    - cancel_order returns immediate cancel confirmation when known
    - poll_or_process_order_updates returns queued events (optionally seeded via
      enqueue_* helpers)
    """

    __slots__ = ("_id_factory", "_order_seq", "_orders", "_queued_updates")

    def __init__(self, client_order_prefix: str = "fake") -> None:
        self._id_factory = ClientOrderIdFactory(
            ClientOrderIdFactoryConfig(prefix=client_order_prefix, max_length=96)
        )
        self._order_seq = 0
        self._orders: dict[str, _FakeOrderRecord] = {}
        self._queued_updates: list[NormalizedOrderEvent] = []

    def submit_order(self, intent: VenueOrderIntent) -> SubmitOrderResult:
        self._order_seq += 1
        client_order_id = intent.client_order_id or self._id_factory.build(
            package_id=intent.package_id,
            leg_id=intent.leg_id,
            attempt=1,
            now_ns=intent.ts_ns,
        )

        existing = self._orders.get(client_order_id)
        if existing is not None:
            return SubmitOrderResult(
                status=SubmitOrderStatus.ALREADY_SUBMITTED,
                client_order_id=client_order_id,
                venue_order_id=existing.venue_order_id,
                events=tuple(),
                ambiguous=False,
                message="duplicate submit idempotent hit",
            )

        venue_order_id = f"fake-ord-{self._order_seq}"
        self._orders[client_order_id] = _FakeOrderRecord(
            package_id=intent.package_id,
            leg_id=intent.leg_id,
            client_order_id=client_order_id,
            venue_order_id=venue_order_id,
            filled_qty=0,
            canceled=False,
            rejected=False,
        )
        ack = VenueOrderAck(
            package_id=intent.package_id,
            leg_id=intent.leg_id,
            client_order_id=client_order_id,
            venue_order_id=venue_order_id,
            ts_ns=intent.ts_ns,
        )
        return SubmitOrderResult(
            status=SubmitOrderStatus.ACKNOWLEDGED,
            client_order_id=client_order_id,
            venue_order_id=venue_order_id,
            events=(ack,),
            ambiguous=False,
            message="fake immediate ack",
        )

    def cancel_order(
        self,
        *,
        client_order_id: str | None = None,
        venue_order_id: str | None = None,
        reason: str = "",
        now_ns: int | None = None,
    ) -> CancelOrderResult:
        record = None
        if client_order_id is not None:
            record = self._orders.get(client_order_id)
        elif venue_order_id is not None:
            for item in self._orders.values():
                if item.venue_order_id == venue_order_id:
                    record = item
                    break

        if record is None:
            return CancelOrderResult(
                status=CancelOrderStatus.NOT_FOUND,
                client_order_id=client_order_id,
                venue_order_id=venue_order_id,
                events=tuple(),
                ambiguous=False,
                message="fake order not found",
            )

        if record.canceled:
            return CancelOrderResult(
                status=CancelOrderStatus.ALREADY_CANCELED,
                client_order_id=record.client_order_id,
                venue_order_id=record.venue_order_id,
                events=tuple(),
                ambiguous=False,
                message="already canceled",
            )

        record.canceled = True
        event = VenueCancelEvent(
            package_id=record.package_id,
            leg_id=record.leg_id,
            client_order_id=record.client_order_id,
            canceled_qty=0,
            reason=reason or "fake cancel",
            ts_ns=now_ns or 0,
        )
        return CancelOrderResult(
            status=CancelOrderStatus.CANCELED,
            client_order_id=record.client_order_id,
            venue_order_id=record.venue_order_id,
            events=(event,),
            ambiguous=False,
            message="fake cancel ack",
        )

    def poll_or_process_order_updates(
        self,
        raw_updates: Sequence[Mapping[str, object]] | None = None,
    ) -> tuple[NormalizedOrderEvent, ...]:
        _ = raw_updates
        queued = tuple(self._queued_updates)
        self._queued_updates.clear()
        return queued

    def reconcile_open_orders(self, expected_open_client_order_ids: set[str]) -> ReconciliationResult:
        open_ids = tuple(
            sorted(
                record.client_order_id
                for record in self._orders.values()
                if not record.canceled and not record.rejected
            )
        )
        open_set = set(open_ids)
        return ReconciliationResult(
            venue_open_client_order_ids=open_ids,
            unknown_open_client_order_ids=tuple(sorted(open_set - expected_open_client_order_ids)),
            missing_expected_client_order_ids=tuple(sorted(expected_open_client_order_ids - open_set)),
            generated_events=tuple(),
        )

    def enqueue_fill(
        self,
        *,
        client_order_id: str,
        fill_qty: int,
        fill_price_ticks: int,
        ts_ns: int,
    ) -> None:
        record = self._orders[client_order_id]
        record.filled_qty += fill_qty
        self._queued_updates.append(
            VenueFillEvent(
                package_id=record.package_id,
                leg_id=record.leg_id,
                client_order_id=record.client_order_id,
                fill_qty=fill_qty,
                fill_price_ticks=fill_price_ticks,
                ts_ns=ts_ns,
                cumulative_qty=record.filled_qty,
            )
        )

    def enqueue_reject(self, *, client_order_id: str, reason: str, ts_ns: int) -> None:
        record = self._orders[client_order_id]
        record.rejected = True
        self._queued_updates.append(
            VenueRejectEvent(
                package_id=record.package_id,
                leg_id=record.leg_id,
                client_order_id=record.client_order_id,
                reason=reason,
                ts_ns=ts_ns,
            )
        )