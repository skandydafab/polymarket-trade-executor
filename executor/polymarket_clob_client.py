from __future__ import annotations

from dataclasses import dataclass
import json
import random
import socket
import time
from typing import Any, Callable, Mapping, Protocol, Sequence
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .polymarket_adapter import PolymarketCLOBClient, PolymarketOrderRequest


OrderSigner = Callable[[dict[str, object], str | None], Mapping[str, object]]


@dataclass(frozen=True, slots=True)
class PolymarketCLOBHttpClientConfig:
    api_url: str
    private_key: str | None = None
    api_key: str | None = None
    api_secret: str | None = None
    passphrase: str | None = None
    request_timeout_ms: int = 1_500
    updates_max_retries: int = 3
    updates_retry_base_ms: int = 100
    submit_path: str = "/order"
    cancel_path: str = "/cancel"
    updates_path: str = "/orders/updates"
    open_orders_path: str = "/orders/open"
    user_agent: str = "polyexecutor/1.0"


class SyncHttpTransport(Protocol):
    def request(
        self,
        *,
        method: str,
        url: str,
        timeout_ms: int,
        headers: Mapping[str, str],
        params: Mapping[str, object] | None = None,
        json_body: Mapping[str, object] | None = None,
    ) -> tuple[int, str]: ...


class UrllibHttpTransport:
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
        final_url = _append_query(url, params)
        payload_bytes: bytes | None = None
        if json_body is not None:
            payload_bytes = json.dumps(dict(json_body), separators=(",", ":")).encode("utf-8")

        request = Request(final_url, data=payload_bytes, method=method.upper())
        for name, value in headers.items():
            request.add_header(name, value)

        timeout_seconds = max(0.1, timeout_ms / 1000.0)
        try:
            with urlopen(request, timeout=timeout_seconds) as response:
                body = response.read().decode("utf-8", errors="replace")
                status = int(getattr(response, "status", 200))
                return status, body
        except HTTPError as exc:
            body_bytes = exc.read() if hasattr(exc, "read") else b""
            body = body_bytes.decode("utf-8", errors="replace") if isinstance(body_bytes, (bytes, bytearray)) else str(body_bytes)
            return int(exc.code), body
        except socket.timeout as exc:
            raise TimeoutError("http request timed out") from exc
        except URLError as exc:
            reason = getattr(exc, "reason", None)
            if isinstance(reason, (TimeoutError, socket.timeout)):
                raise TimeoutError("http request timed out") from exc
            text = str(reason or exc)
            if "timed out" in text.lower():
                raise TimeoutError("http request timed out") from exc
            raise OSError(text) from exc
        except OSError:
            raise
        except Exception as exc:
            raise OSError(str(exc)) from exc


class PolymarketCLOBHttpClient(PolymarketCLOBClient):
    __slots__ = ("_config", "_transport", "_signer")

    def __init__(
        self,
        config: PolymarketCLOBHttpClientConfig,
        *,
        transport: SyncHttpTransport | None = None,
        signer: OrderSigner | None = None,
    ) -> None:
        self._validate_config(config)
        self._config = config
        self._transport = transport or UrllibHttpTransport()
        self._signer = signer

    def create_signed_order(self, request: PolymarketOrderRequest) -> Mapping[str, object]:
        unsigned: dict[str, object] = {
            "market_id": request.market_id,
            "token_id": request.token_id,
            "side": request.side,
            "size": request.size,
            "price": request.price,
            "tif": request.tif,
            "client_order_id": request.client_order_id,
        }
        if request.expiration_ts is not None:
            unsigned["expiration_ts"] = int(request.expiration_ts)

        if self._signer is None:
            return {"order": unsigned}

        signed = self._signer(unsigned, self._config.private_key)
        return _as_mapping(signed, fallback={"order": unsigned})

    def submit_order(self, signed_order: Mapping[str, object], timeout_ms: int) -> Mapping[str, object]:
        status, payload = self._request_json(
            method="POST",
            path=self._config.submit_path,
            timeout_ms=timeout_ms,
            json_body=_as_mapping(signed_order, fallback={}),
        )
        response = _as_mapping(payload, fallback={"raw": payload})
        if "status" not in response and "state" not in response:
            response["status"] = f"HTTP_{status}"
        response.setdefault("http_status", status)
        return response

    def cancel_order(
        self,
        *,
        client_order_id: str | None = None,
        venue_order_id: str | None = None,
        timeout_ms: int,
    ) -> Mapping[str, object]:
        body: dict[str, object] = {}
        if client_order_id is not None:
            body["client_order_id"] = client_order_id
        if venue_order_id is not None:
            body["order_id"] = venue_order_id

        status, payload = self._request_json(
            method="POST",
            path=self._config.cancel_path,
            timeout_ms=timeout_ms,
            json_body=body,
        )
        response = _as_mapping(payload, fallback={"raw": payload})
        if "status" not in response and "state" not in response:
            response["status"] = f"HTTP_{status}"
        response.setdefault("http_status", status)
        return response

    def get_order_updates(
        self,
        *,
        since_sequence: int | None,
        limit: int,
        timeout_ms: int,
    ) -> Sequence[Mapping[str, object]]:
        params: dict[str, object] = {"limit": max(1, int(limit))}
        if since_sequence is not None:
            params["since_sequence"] = int(since_sequence)

        attempts = max(1, self._config.updates_max_retries)
        for attempt in range(1, attempts + 1):
            try:
                status, payload = self._request_json(
                    method="GET",
                    path=self._config.updates_path,
                    timeout_ms=timeout_ms,
                    params=params,
                    json_body=None,
                )
                if status >= 400:
                    return tuple()
                return tuple(_extract_items(payload))
            except (TimeoutError, OSError):
                if attempt >= attempts:
                    return tuple()
                delay_ms = self._config.updates_retry_base_ms * (2 ** (attempt - 1))
                jitter_ms = random.uniform(0.0, self._config.updates_retry_base_ms)
                time.sleep((delay_ms + jitter_ms) / 1000.0)
        return tuple()

    def get_open_orders(self, *, timeout_ms: int) -> Sequence[Mapping[str, object]]:
        status, payload = self._request_json(
            method="GET",
            path=self._config.open_orders_path,
            timeout_ms=timeout_ms,
            params=None,
            json_body=None,
        )
        if status >= 400:
            raise OSError(f"open orders request failed with http_status={status}")
        return tuple(_extract_items(payload))

    def _request_json(
        self,
        *,
        method: str,
        path: str,
        timeout_ms: int,
        params: Mapping[str, object] | None = None,
        json_body: Mapping[str, object] | None = None,
    ) -> tuple[int, object]:
        status, body = self._transport.request(
            method=method,
            url=_join_url(self._config.api_url, path),
            timeout_ms=max(1, int(timeout_ms)),
            headers=self._headers(),
            params=params,
            json_body=json_body,
        )
        return status, _decode_json(body)

    def _headers(self) -> dict[str, str]:
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": self._config.user_agent,
        }
        if self._config.api_key:
            headers["X-API-KEY"] = self._config.api_key
        if self._config.api_secret:
            headers["X-API-SECRET"] = self._config.api_secret
        if self._config.passphrase:
            headers["X-API-PASSPHRASE"] = self._config.passphrase
        return headers

    @staticmethod
    def _validate_config(config: PolymarketCLOBHttpClientConfig) -> None:
        if not config.api_url:
            raise ValueError("api_url must be non-empty")
        if config.request_timeout_ms <= 0:
            raise ValueError("request_timeout_ms must be > 0")
        if config.updates_max_retries <= 0:
            raise ValueError("updates_max_retries must be > 0")
        if config.updates_retry_base_ms < 0:
            raise ValueError("updates_retry_base_ms must be >= 0")


def _decode_json(body: str) -> object:
    text = body.strip()
    if not text:
        return {}
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return {"raw_body": body}


def _extract_items(payload: object) -> list[Mapping[str, object]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, Mapping)]

    if isinstance(payload, Mapping):
        for key in ("updates", "events", "items", "orders", "open_orders", "openOrders"):
            candidate = payload.get(key)
            if isinstance(candidate, list):
                extracted = [item for item in candidate if isinstance(item, Mapping)]
                if extracted:
                    return extracted

        for key in ("data", "payload", "result"):
            nested = payload.get(key)
            extracted = _extract_items(nested)
            if extracted:
                return extracted

        if any(key in payload for key in ("client_order_id", "clientOrderId", "order_id", "id", "status", "state")):
            return [dict(payload)]

    return []


def _join_url(base_url: str, path: str) -> str:
    base = base_url.rstrip("/")
    suffix = path if path.startswith("/") else f"/{path}"
    return f"{base}{suffix}"


def _append_query(url: str, params: Mapping[str, object] | None) -> str:
    if not params:
        return url
    query = urlencode({key: value for key, value in params.items() if value is not None})
    if not query:
        return url
    joiner = "&" if "?" in url else "?"
    return f"{url}{joiner}{query}"


def _as_mapping(value: object, fallback: dict[str, object]) -> dict[str, object]:
    if isinstance(value, Mapping):
        return dict(value)
    return dict(fallback)
