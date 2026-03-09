from __future__ import annotations

from dataclasses import dataclass, field
from typing import Mapping

import pytest

from executor.polymarket_adapter import PolymarketOrderRequest
from executor.polymarket_clob_client import PolymarketCLOBHttpClient, PolymarketCLOBHttpClientConfig


pytestmark = pytest.mark.unit


@dataclass(slots=True)
class _StubTransport:
    steps: list[tuple[int, str] | Exception] = field(default_factory=list)
    calls: list[dict[str, object]] = field(default_factory=list)

    def request(
        self,
        *,
        method: str,
        url: str,
        timeout_ms: int,
        headers: Mapping[str, str],
        params: Mapping[str, object] | None = None,
        json_body: Mapping[str, object] | None = None,
    ) -> tuple[int, str]:
        self.calls.append(
            {
                "method": method,
                "url": url,
                "timeout_ms": timeout_ms,
                "headers": dict(headers),
                "params": dict(params or {}),
                "json_body": dict(json_body or {}),
            }
        )
        if self.steps:
            step = self.steps.pop(0)
            if isinstance(step, Exception):
                raise step
            return step
        return 200, "{}"


def _request(*, client_order_id: str = "cid-1") -> PolymarketOrderRequest:
    return PolymarketOrderRequest(
        market_id="mkt-a",
        token_id="token-a",
        side="BUY",
        size="10",
        price="100",
        tif="IOC",
        client_order_id=client_order_id,
        expiration_ts=1234567890,
    )


def test_create_signed_order_uses_custom_signer() -> None:
    transport = _StubTransport()
    config = PolymarketCLOBHttpClientConfig(api_url="https://clob.polymarket.test", private_key="pk")

    def signer(unsigned: dict[str, object], private_key: str | None) -> Mapping[str, object]:
        return {
            "signed": True,
            "client_order_id": unsigned["client_order_id"],
            "private_key": private_key,
        }

    client = PolymarketCLOBHttpClient(config, transport=transport, signer=signer)

    signed = client.create_signed_order(_request(client_order_id="cid-signed"))

    assert signed["signed"] is True
    assert signed["client_order_id"] == "cid-signed"
    assert signed["private_key"] == "pk"


def test_submit_and_cancel_shape_requests() -> None:
    transport = _StubTransport(
        steps=[
            (200, '{"status":"ACCEPTED","order_id":"vo-1"}'),
            (200, '{"status":"CANCELED","canceled_size":4}'),
        ]
    )
    config = PolymarketCLOBHttpClientConfig(
        api_url="https://clob.polymarket.test",
        api_key="key",
        api_secret="secret",
        passphrase="pass",
    )
    client = PolymarketCLOBHttpClient(config, transport=transport)

    submit = client.submit_order({"order": {"client_order_id": "cid-1"}}, timeout_ms=750)
    cancel = client.cancel_order(client_order_id="cid-1", timeout_ms=850)

    assert submit["status"] == "ACCEPTED"
    assert submit["order_id"] == "vo-1"
    assert submit["http_status"] == 200

    assert cancel["status"] == "CANCELED"
    assert cancel["canceled_size"] == 4
    assert cancel["http_status"] == 200

    assert len(transport.calls) == 2
    assert transport.calls[0]["method"] == "POST"
    assert str(transport.calls[0]["url"]).endswith("/order")
    assert transport.calls[0]["headers"]["X-API-KEY"] == "key"
    assert transport.calls[1]["method"] == "POST"
    assert str(transport.calls[1]["url"]).endswith("/cancel")


def test_get_order_updates_retries_and_normalizes_payload() -> None:
    transport = _StubTransport(
        steps=[
            TimeoutError("temporary timeout"),
            (200, '{"data":{"updates":[{"client_order_id":"cid-1","status":"OPEN","sequence":7}]}}'),
        ]
    )
    config = PolymarketCLOBHttpClientConfig(
        api_url="https://clob.polymarket.test",
        updates_max_retries=2,
        updates_retry_base_ms=0,
    )
    client = PolymarketCLOBHttpClient(config, transport=transport)

    updates = client.get_order_updates(since_sequence=5, limit=50, timeout_ms=500)

    assert len(updates) == 1
    assert updates[0]["client_order_id"] == "cid-1"
    assert updates[0]["status"] == "OPEN"
    assert len(transport.calls) == 2


def test_get_open_orders_http_error_raises() -> None:
    transport = _StubTransport(steps=[(401, '{"error":"unauthorized"}')])
    config = PolymarketCLOBHttpClientConfig(api_url="https://clob.polymarket.test")
    client = PolymarketCLOBHttpClient(config, transport=transport)

    with pytest.raises(OSError):
        _ = client.get_open_orders(timeout_ms=400)
