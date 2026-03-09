from __future__ import annotations

from dataclasses import dataclass

import pytest

from executor.polymarket_sdk_signer import (
    PyClobClientOrderSigner,
    PyClobClientOrderSignerConfig,
    _PyClobSdkBindings,
)


pytestmark = pytest.mark.unit


@dataclass(slots=True)
class _FakeOrderArgs:
    token_id: str
    side: str
    size: str
    price: str
    client_order_id: str


class _FakeClientWithCreateOrder:
    init_calls = 0

    def __init__(self, host: str, key: str, chain_id: int) -> None:
        self.host = host
        self.key = key
        self.chain_id = chain_id
        _FakeClientWithCreateOrder.init_calls += 1

    def create_order(self, order_args: _FakeOrderArgs) -> dict[str, object]:
        return {
            "signed": True,
            "client_order_id": order_args.client_order_id,
            "token_id": order_args.token_id,
        }


class _FakeClientWithKwargSigner:
    def __init__(self, host: str, key: str) -> None:
        self.host = host
        self.key = key

    def sign_order(
        self,
        *,
        token_id: str,
        side: str,
        size: str,
        price: str,
        client_order_id: str,
    ) -> dict[str, object]:
        return {
            "signed": True,
            "client_order_id": client_order_id,
            "token_id": token_id,
            "side": side,
            "size": size,
            "price": price,
        }


def _unsigned(client_order_id: str) -> dict[str, object]:
    return {
        "market_id": "mkt-a",
        "token_id": "token-a",
        "side": "BUY",
        "size": "10",
        "price": "100",
        "tif": "IOC",
        "client_order_id": client_order_id,
        "expiration_ts": 123,
    }


def test_sdk_signer_requires_private_key() -> None:
    bindings = _PyClobSdkBindings(
        clob_client_cls=_FakeClientWithCreateOrder,
        order_args_cls=_FakeOrderArgs,
    )
    signer = PyClobClientOrderSigner(
        PyClobClientOrderSignerConfig(api_url="https://clob.polymarket.test"),
        bindings=bindings,
    )

    with pytest.raises(ValueError):
        _ = signer(_unsigned("cid-no-key"), None)


def test_sdk_signer_uses_order_args_and_reuses_client() -> None:
    _FakeClientWithCreateOrder.init_calls = 0
    bindings = _PyClobSdkBindings(
        clob_client_cls=_FakeClientWithCreateOrder,
        order_args_cls=_FakeOrderArgs,
    )
    signer = PyClobClientOrderSigner(
        PyClobClientOrderSignerConfig(api_url="https://clob.polymarket.test", chain_id=137),
        bindings=bindings,
    )

    first = signer(_unsigned("cid-1"), "pk-1")
    second = signer(_unsigned("cid-2"), "pk-1")

    assert first["signed"] is True
    assert first["client_order_id"] == "cid-1"
    assert second["client_order_id"] == "cid-2"
    assert _FakeClientWithCreateOrder.init_calls == 1


def test_sdk_signer_supports_kwarg_style_methods() -> None:
    bindings = _PyClobSdkBindings(
        clob_client_cls=_FakeClientWithKwargSigner,
        order_args_cls=None,
    )
    signer = PyClobClientOrderSigner(
        PyClobClientOrderSignerConfig(api_url="https://clob.polymarket.test"),
        bindings=bindings,
    )

    signed = signer(_unsigned("cid-kw"), "pk-kw")

    assert signed["signed"] is True
    assert signed["client_order_id"] == "cid-kw"
    assert signed["token_id"] == "token-a"
