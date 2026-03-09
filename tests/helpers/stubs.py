from __future__ import annotations

from dataclasses import dataclass, field
from typing import Mapping, Sequence

from executor.polymarket_adapter import PolymarketOrderRequest


@dataclass(slots=True)
class StubPolymarketClient:
    submit_script: list[Mapping[str, object] | Exception] = field(default_factory=list)
    cancel_script: list[Mapping[str, object] | Exception] = field(default_factory=list)
    updates: list[Mapping[str, object]] = field(default_factory=list)
    open_orders: list[Mapping[str, object]] = field(default_factory=list)
    signed_requests: list[PolymarketOrderRequest] = field(default_factory=list)
    submit_calls: int = 0
    cancel_calls: int = 0

    def create_signed_order(self, request: PolymarketOrderRequest) -> Mapping[str, object]:
        self.signed_requests.append(request)
        return {"signed": True, "client_order_id": request.client_order_id}

    def submit_order(self, signed_order: Mapping[str, object], timeout_ms: int) -> Mapping[str, object]:
        _ = (signed_order, timeout_ms)
        self.submit_calls += 1
        if self.submit_script:
            step = self.submit_script.pop(0)
            if isinstance(step, Exception):
                raise step
            return step
        return {"status": "ACCEPTED", "order_id": f"vo-{self.submit_calls}"}

    def cancel_order(
        self,
        *,
        client_order_id: str | None = None,
        venue_order_id: str | None = None,
        timeout_ms: int,
    ) -> Mapping[str, object]:
        _ = (client_order_id, venue_order_id, timeout_ms)
        self.cancel_calls += 1
        if self.cancel_script:
            step = self.cancel_script.pop(0)
            if isinstance(step, Exception):
                raise step
            return step
        return {"status": "CANCELED", "canceled_size": 0}

    def get_order_updates(
        self,
        *,
        since_sequence: int | None,
        limit: int,
        timeout_ms: int,
    ) -> Sequence[Mapping[str, object]]:
        _ = (since_sequence, limit, timeout_ms)
        items = list(self.updates)
        self.updates.clear()
        return items

    def get_open_orders(self, *, timeout_ms: int) -> Sequence[Mapping[str, object]]:
        _ = timeout_ms
        return list(self.open_orders)
