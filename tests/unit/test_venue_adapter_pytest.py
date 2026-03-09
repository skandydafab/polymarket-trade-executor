from __future__ import annotations

import pytest

from executor.polymarket_adapter import PolymarketAdapterConfig, PolymarketVenueAdapter, VenueTimeoutError
from executor.state_machine import Side, TimeInForce, VenueFillEvent
from executor.venue import SubmitOrderStatus, VenueOrderIntent
from tests.helpers.stubs import StubPolymarketClient


pytestmark = pytest.mark.unit


def _intent(*, client_order_id: str) -> VenueOrderIntent:
    return VenueOrderIntent(
        package_id="pkg-venue",
        leg_id="leg-a",
        market_id="mkt-a",
        token_id="token-a",
        side=Side.BUY,
        quantity=10,
        limit_price_ticks=100,
        tif=TimeInForce.IOC,
        ts_ns=1_700_000_000_000_000_000,
        client_order_id=client_order_id,
    )


def test_duplicate_and_out_of_order_venue_events_are_ignored() -> None:
    client = StubPolymarketClient(submit_script=[{"status": "ACCEPTED", "order_id": "vo-1"}])
    adapter = PolymarketVenueAdapter(client=client, config=PolymarketAdapterConfig(api_url="https://unit.test"))
    _ = adapter.submit_order(_intent(client_order_id="cid-upd"))

    updates = (
        {
            "event_id": "e1",
            "sequence": 2,
            "client_order_id": "cid-upd",
            "status": "OPEN",
            "fill_delta": 3,
            "cumulative_fill": 3,
            "fill_price": 101,
            "ts_ns": 100,
        },
        {
            "event_id": "e1",
            "sequence": 2,
            "client_order_id": "cid-upd",
            "status": "OPEN",
            "fill_delta": 3,
            "cumulative_fill": 3,
            "fill_price": 101,
            "ts_ns": 101,
        },
        {
            "event_id": "e0",
            "sequence": 1,
            "client_order_id": "cid-upd",
            "status": "CANCELED",
            "canceled_size": 7,
            "ts_ns": 90,
        },
    )

    normalized = adapter.poll_or_process_order_updates(updates)

    assert len(normalized) == 1
    assert isinstance(normalized[0], VenueFillEvent)


def test_ambiguous_submit_outcome_on_timeout() -> None:
    client = StubPolymarketClient(submit_script=[TimeoutError("submit timeout")])
    adapter = PolymarketVenueAdapter(client=client, config=PolymarketAdapterConfig(api_url="https://unit.test"))
    intent = _intent(client_order_id="cid-amb")

    with pytest.raises(VenueTimeoutError):
        _ = adapter.submit_order(intent)

    second = adapter.submit_order(intent)

    assert client.submit_calls == 1
    assert second.status == SubmitOrderStatus.AMBIGUOUS
    assert second.ambiguous is True
