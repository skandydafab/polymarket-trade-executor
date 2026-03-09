from __future__ import annotations

from dataclasses import asdict, dataclass, is_dataclass
import inspect
from typing import Any, Callable, Mapping


OrderSigner = Callable[[dict[str, object], str | None], Mapping[str, object]]


@dataclass(frozen=True, slots=True)
class PyClobClientOrderSignerConfig:
    api_url: str
    chain_id: int = 137
    signature_type: int | None = None
    funder: str | None = None
    maker: str | None = None


@dataclass(frozen=True, slots=True)
class _PyClobSdkBindings:
    clob_client_cls: type[Any]
    order_args_cls: type[Any] | None


class PyClobClientOrderSigner:
    __slots__ = ("_config", "_bindings", "_clients_by_private_key")

    def __init__(
        self,
        config: PyClobClientOrderSignerConfig,
        *,
        bindings: _PyClobSdkBindings | None = None,
    ) -> None:
        if not config.api_url:
            raise ValueError("api_url must be non-empty")
        if config.chain_id <= 0:
            raise ValueError("chain_id must be > 0")
        self._config = config
        self._bindings = bindings or _load_py_clob_sdk_bindings()
        self._clients_by_private_key: dict[str, Any] = {}

    def __call__(self, unsigned: dict[str, object], private_key: str | None) -> Mapping[str, object]:
        if not private_key:
            raise ValueError("private_key is required for SDK signing")

        client = self._client_for_private_key(private_key)
        order_input = _build_order_input(unsigned, self._bindings.order_args_cls)
        signed = _invoke_sign(client, order_input)
        signed_mapping = _to_mapping(signed)
        if not signed_mapping:
            raise RuntimeError("SDK signer returned an empty or unsupported payload")
        return signed_mapping

    def _client_for_private_key(self, private_key: str) -> Any:
        existing = self._clients_by_private_key.get(private_key)
        if existing is not None:
            return existing

        kwargs = {
            "host": self._config.api_url,
            "key": private_key,
            "chain_id": self._config.chain_id,
            "signature_type": self._config.signature_type,
            "funder": self._config.funder,
            "maker": self._config.maker,
        }
        filtered_kwargs = _filter_kwargs_for_callable(self._bindings.clob_client_cls, kwargs)
        client = self._bindings.clob_client_cls(**filtered_kwargs)
        self._clients_by_private_key[private_key] = client
        return client


def build_py_clob_client_signer(config: PyClobClientOrderSignerConfig) -> OrderSigner:
    return PyClobClientOrderSigner(config)


def _load_py_clob_sdk_bindings() -> _PyClobSdkBindings:
    try:
        from py_clob_client.client import ClobClient
    except ImportError as exc:
        raise RuntimeError(
            "py-clob-client is required for SDK signing. Install with: pip install py-clob-client"
        ) from exc

    order_args_cls: type[Any] | None = None
    try:
        from py_clob_client.clob_types import OrderArgs

        order_args_cls = OrderArgs
    except Exception:
        order_args_cls = None

    return _PyClobSdkBindings(clob_client_cls=ClobClient, order_args_cls=order_args_cls)


def _build_order_input(unsigned: Mapping[str, object], order_args_cls: type[Any] | None) -> object:
    payload: dict[str, object] = {
        "market_id": unsigned.get("market_id"),
        "token_id": unsigned.get("token_id"),
        "side": unsigned.get("side"),
        "size": unsigned.get("size"),
        "price": unsigned.get("price"),
        "tif": unsigned.get("tif"),
        "time_in_force": unsigned.get("tif"),
        "client_order_id": unsigned.get("client_order_id"),
        "expiration_ts": unsigned.get("expiration_ts"),
        "expiration": unsigned.get("expiration_ts"),
    }
    payload = {key: value for key, value in payload.items() if value is not None}

    if order_args_cls is None:
        return payload

    try:
        return order_args_cls(**payload)
    except Exception:
        filtered_payload = _filter_kwargs_for_callable(order_args_cls, payload)
        return order_args_cls(**filtered_payload)


def _invoke_sign(client: Any, order_input: object) -> object:
    method_names = ("create_order", "sign_order", "create_signed_order")
    last_exception: Exception | None = None
    order_mapping = _to_mapping(order_input)

    for method_name in method_names:
        method = getattr(client, method_name, None)
        if not callable(method):
            continue

        try:
            result = method(order_input)
            if result is not None:
                return result
        except TypeError as exc:
            last_exception = exc

        if order_mapping:
            try:
                filtered_kwargs = _filter_kwargs_for_callable(method, order_mapping)
                if filtered_kwargs:
                    result = method(**filtered_kwargs)
                    if result is not None:
                        return result
            except TypeError as exc:
                last_exception = exc

    if last_exception is not None:
        raise RuntimeError("SDK signer could not invoke a compatible signing method") from last_exception
    raise RuntimeError("SDK client does not expose a supported signing method")


def _filter_kwargs_for_callable(callable_obj: object, values: Mapping[str, object]) -> dict[str, object]:
    try:
        signature = inspect.signature(callable_obj)
    except (TypeError, ValueError):
        return {key: value for key, value in values.items() if value is not None}

    accepts_var_kw = any(
        parameter.kind is inspect.Parameter.VAR_KEYWORD
        for parameter in signature.parameters.values()
    )
    if accepts_var_kw:
        return {key: value for key, value in values.items() if value is not None}

    accepted = {
        name
        for name, parameter in signature.parameters.items()
        if parameter.kind in (inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY)
    }
    accepted.discard("self")
    return {
        key: value
        for key, value in values.items()
        if key in accepted and value is not None
    }


def _to_mapping(value: object) -> dict[str, object]:
    if isinstance(value, Mapping):
        return dict(value)
    if is_dataclass(value):
        return asdict(value)

    dict_method = getattr(value, "dict", None)
    if callable(dict_method):
        try:
            mapped = dict_method()
            if isinstance(mapped, Mapping):
                return dict(mapped)
        except Exception:
            return {}

    model_dump_method = getattr(value, "model_dump", None)
    if callable(model_dump_method):
        try:
            mapped = model_dump_method()
            if isinstance(mapped, Mapping):
                return dict(mapped)
        except Exception:
            return {}

    value_dict = getattr(value, "__dict__", None)
    if isinstance(value_dict, Mapping):
        return {
            str(key): value
            for key, value in value_dict.items()
            if not str(key).startswith("_")
        }

    return {}
