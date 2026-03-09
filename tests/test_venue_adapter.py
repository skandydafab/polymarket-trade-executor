from __future__ import annotations

import unittest
from collections.abc import Mapping, Sequence

from executor.fake_venue_adapter import FakeVenueAdapter
from executor.polymarket_adapter import (
    PolymarketAdapterConfig,
    PolymarketOrderRequest,
    PolymarketVenueAdapter,
    VenueTimeoutError,
)
from executor.state_machine import Side, TimeInForce, VenueFillEvent, VenueOrderAck
from executor.venue import CancelOrderStatus, SubmitOrderStatus, VenueOrderIntent


class _StubPolymarketClient:
    def __init__(
        self,
        *,
        submit_script: Sequence[Mapping[str, object] | Exception] | None = None,
        cancel_script: Sequence[Mapping[str, object] | Exception] | None = None,
    ) -> None:
        self.submit_script = list(submit_script or [])
        self.cancel_script = list(cancel_script or [])
        self.submit_calls = 0
        self.cancel_calls = 0
        self.signed_requests: list[PolymarketOrderRequest] = []
        self.updates: list[Mapping[str, object]] = []
        self.open_orders: list[Mapping[str, object]] = []

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
        updates = list(self.updates)
        self.updates.clear()
        return updates

    def get_open_orders(self, *, timeout_ms: int) -> Sequence[Mapping[str, object]]:
        _ = timeout_ms
        return list(self.open_orders)


class VenueAdapterTests(unittest.TestCase):
    def _config(self) -> PolymarketAdapterConfig:
        return PolymarketAdapterConfig(api_url="https://clob.polymarket.test")

    def _intent(self, client_order_id: str | None = None) -> VenueOrderIntent:
        return VenueOrderIntent(
            package_id="pkg-1",
            leg_id="leg-1",
            market_id="market-a",
            token_id="token-a",
            side=Side.BUY,
            quantity=10,
            limit_price_ticks=100,
            tif=TimeInForce.IOC,
            ts_ns=1_000_000_000,
            client_order_id=client_order_id,
        )

    def test_submit_ack_then_duplicate_submit_is_idempotent(self) -> None:
        client = _StubPolymarketClient(submit_script=[{"status": "ACCEPTED", "order_id": "vo-1"}])
        adapter = PolymarketVenueAdapter(client=client, config=self._config())
        intent = self._intent(client_order_id="cid-1")

        first = adapter.submit_order(intent)
        second = adapter.submit_order(intent)

        self.assertEqual(first.status, SubmitOrderStatus.ACKNOWLEDGED)
        self.assertEqual(client.submit_calls, 1)
        self.assertEqual(second.status, SubmitOrderStatus.ALREADY_SUBMITTED)
        self.assertFalse(second.ambiguous)
        self.assertEqual(len(first.events), 1)
        self.assertIsInstance(first.events[0], VenueOrderAck)

    def test_submit_timeout_marks_ambiguous_until_reconciliation(self) -> None:
        client = _StubPolymarketClient(submit_script=[TimeoutError("submit timeout")])
        adapter = PolymarketVenueAdapter(client=client, config=self._config())
        intent = self._intent(client_order_id="cid-timeout")

        with self.assertRaises(VenueTimeoutError):
            adapter.submit_order(intent)

        duplicate = adapter.submit_order(intent)
        self.assertEqual(client.submit_calls, 1)
        self.assertEqual(duplicate.status, SubmitOrderStatus.AMBIGUOUS)
        self.assertTrue(duplicate.ambiguous)

    def test_poll_updates_deduplicates_and_ignores_out_of_order(self) -> None:
        client = _StubPolymarketClient(submit_script=[{"status": "ACCEPTED", "order_id": "vo-2"}])
        adapter = PolymarketVenueAdapter(client=client, config=self._config())
        intent = self._intent(client_order_id="cid-upd")
        adapter.submit_order(intent)

        client.updates.extend(
            [
                {
                    "event_id": "evt-2",
                    "sequence": 2,
                    "client_order_id": "cid-upd",
                    "status": "OPEN",
                    "fill_delta": 3,
                    "cumulative_fill": 3,
                    "fill_price": 101,
                    "ts_ns": 100,
                },
                {
                    "event_id": "evt-2",
                    "sequence": 2,
                    "client_order_id": "cid-upd",
                    "status": "OPEN",
                    "fill_delta": 3,
                    "cumulative_fill": 3,
                    "fill_price": 101,
                    "ts_ns": 101,
                },
                {
                    "event_id": "evt-1",
                    "sequence": 1,
                    "client_order_id": "cid-upd",
                    "status": "CANCELED",
                    "canceled_size": 7,
                    "ts_ns": 90,
                },
            ]
        )

        updates = adapter.poll_or_process_order_updates()
        self.assertEqual(len(updates), 1)
        self.assertIsInstance(updates[0], VenueFillEvent)

    def test_fake_adapter_submit_fill_cancel_flow(self) -> None:
        adapter = FakeVenueAdapter()
        intent = self._intent(client_order_id="fake-1")

        submit_result = adapter.submit_order(intent)
        self.assertEqual(submit_result.status, SubmitOrderStatus.ACKNOWLEDGED)
        client_order_id = submit_result.client_order_id

        adapter.enqueue_fill(
            client_order_id=client_order_id,
            fill_qty=4,
            fill_price_ticks=99,
            ts_ns=1_111,
        )
        updates = adapter.poll_or_process_order_updates()
        self.assertEqual(len(updates), 1)
        self.assertIsInstance(updates[0], VenueFillEvent)

        cancel_result = adapter.cancel_order(client_order_id=client_order_id, reason="test", now_ns=1_222)
        self.assertEqual(cancel_result.status, CancelOrderStatus.CANCELED)


if __name__ == "__main__":
    unittest.main()