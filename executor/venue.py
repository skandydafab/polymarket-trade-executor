from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from hashlib import blake2b
from typing import Mapping, Protocol, Sequence, TypeAlias

from .state_machine import Side, TimeInForce, VenueCancelEvent, VenueFillEvent, VenueOrderAck, VenueRejectEvent


NormalizedOrderEvent: TypeAlias = VenueOrderAck | VenueFillEvent | VenueCancelEvent | VenueRejectEvent


class SubmitOrderStatus(str, Enum):
    ACKNOWLEDGED = "ACKNOWLEDGED"
    REJECTED = "REJECTED"
    ALREADY_SUBMITTED = "ALREADY_SUBMITTED"
    AMBIGUOUS = "AMBIGUOUS"
    FAILED_RETRYABLE = "FAILED_RETRYABLE"


class CancelOrderStatus(str, Enum):
    CANCELED = "CANCELED"
    ALREADY_CANCELED = "ALREADY_CANCELED"
    NOT_FOUND = "NOT_FOUND"
    AMBIGUOUS = "AMBIGUOUS"
    FAILED_RETRYABLE = "FAILED_RETRYABLE"


@dataclass(frozen=True, slots=True)
class VenueOrderIntent:
    """Internal order intent passed from execution layer to venue adapter.

    This is strategy-agnostic and deliberately limited to order placement fields.
    """

    package_id: str
    leg_id: str
    market_id: str
    token_id: str
    side: Side
    quantity: int
    limit_price_ticks: int
    tif: TimeInForce
    ts_ns: int
    expires_at_ns: int | None = None
    client_order_id: str | None = None


@dataclass(frozen=True, slots=True)
class SubmitOrderResult:
    status: SubmitOrderStatus
    client_order_id: str
    venue_order_id: str | None
    events: tuple[NormalizedOrderEvent, ...]
    ambiguous: bool
    message: str | None = None


@dataclass(frozen=True, slots=True)
class CancelOrderResult:
    status: CancelOrderStatus
    client_order_id: str | None
    venue_order_id: str | None
    events: tuple[NormalizedOrderEvent, ...]
    ambiguous: bool
    message: str | None = None


@dataclass(frozen=True, slots=True)
class ReconciliationResult:
    venue_open_client_order_ids: tuple[str, ...]
    unknown_open_client_order_ids: tuple[str, ...]
    missing_expected_client_order_ids: tuple[str, ...]
    generated_events: tuple[NormalizedOrderEvent, ...]


@dataclass(frozen=True, slots=True)
class ClientOrderIdFactoryConfig:
    prefix: str = "pmx"
    max_length: int = 96


@dataclass(frozen=True, slots=True)
class ParsedClientOrderId:
    package_id: str | None
    leg_id: str | None
    attempt: int
    nonce: str


class ClientOrderIdFactory:
    """Deterministic client order id factory.

    Generation scheme (preferred, fully parseable):
      {prefix}:{package_id}:{leg_id}:{attempt}:{nonce}

    Fallback (when length limit would be exceeded):
      {prefix}:h:{pkg_hash}:{leg_hash}:{attempt}:{nonce}

    For recovery, the recommended approach is to persist order correlation in the
    journal at submit-intent time. Hash fallback is still unique but lossy.
    """

    __slots__ = ("_config",)

    def __init__(self, config: ClientOrderIdFactoryConfig) -> None:
        if not config.prefix:
            raise ValueError("client order id prefix must be non-empty")
        if config.max_length < 32:
            raise ValueError("client order id max_length is too small")
        self._config = config

    def build(self, package_id: str, leg_id: str, attempt: int, now_ns: int) -> str:
        if attempt < 1:
            raise ValueError("attempt must be >= 1")
        base_package = _sanitize_component(package_id)
        base_leg = _sanitize_component(leg_id)
        nonce = f"{now_ns & 0xFFFFFFFF:08x}"

        full = f"{self._config.prefix}:{base_package}:{base_leg}:{attempt}:{nonce}"
        if len(full) <= self._config.max_length:
            return full

        pkg_hash = blake2b(base_package.encode("utf-8"), digest_size=8).hexdigest()
        leg_hash = blake2b(base_leg.encode("utf-8"), digest_size=8).hexdigest()
        fallback = f"{self._config.prefix}:h:{pkg_hash}:{leg_hash}:{attempt}:{nonce}"
        if len(fallback) > self._config.max_length:
            raise ValueError("cannot build client order id within max_length")
        return fallback

    def parse(self, client_order_id: str) -> ParsedClientOrderId | None:
        parts = client_order_id.split(":")
        if len(parts) != 5 and len(parts) != 6:
            return None

        if len(parts) == 5:
            prefix, package_id, leg_id, attempt_text, nonce = parts
            if prefix != self._config.prefix:
                return None
            try:
                attempt = int(attempt_text)
            except ValueError:
                return None
            return ParsedClientOrderId(package_id=package_id, leg_id=leg_id, attempt=attempt, nonce=nonce)

        prefix, mode, package_hash, leg_hash, attempt_text, nonce = parts
        if prefix != self._config.prefix or mode != "h":
            return None
        _ = (package_hash, leg_hash)
        try:
            attempt = int(attempt_text)
        except ValueError:
            return None
        return ParsedClientOrderId(package_id=None, leg_id=None, attempt=attempt, nonce=nonce)


class StructuredLogger(Protocol):
    def debug(self, message: str, **fields: object) -> None: ...

    def info(self, message: str, **fields: object) -> None: ...

    def warning(self, message: str, **fields: object) -> None: ...

    def error(self, message: str, **fields: object) -> None: ...


class MetricsSink(Protocol):
    def increment(self, metric: str, value: int = 1, tags: Mapping[str, str] | None = None) -> None: ...

    def observe(self, metric: str, value: float, tags: Mapping[str, str] | None = None) -> None: ...


class NullLogger:
    def debug(self, message: str, **fields: object) -> None:
        _ = (message, fields)

    def info(self, message: str, **fields: object) -> None:
        _ = (message, fields)

    def warning(self, message: str, **fields: object) -> None:
        _ = (message, fields)

    def error(self, message: str, **fields: object) -> None:
        _ = (message, fields)


class NullMetrics:
    def increment(self, metric: str, value: int = 1, tags: Mapping[str, str] | None = None) -> None:
        _ = (metric, value, tags)

    def observe(self, metric: str, value: float, tags: Mapping[str, str] | None = None) -> None:
        _ = (metric, value, tags)


class VenueAdapter(Protocol):
    def submit_order(self, intent: VenueOrderIntent) -> SubmitOrderResult: ...

    def cancel_order(
        self,
        *,
        client_order_id: str | None = None,
        venue_order_id: str | None = None,
        reason: str = "",
        now_ns: int | None = None,
    ) -> CancelOrderResult: ...

    def poll_or_process_order_updates(
        self,
        raw_updates: Sequence[Mapping[str, object]] | None = None,
    ) -> tuple[NormalizedOrderEvent, ...]: ...

    def reconcile_open_orders(self, expected_open_client_order_ids: set[str]) -> ReconciliationResult: ...


def _sanitize_component(value: str) -> str:
    cleaned = "".join(ch if ch.isalnum() or ch in "-_" else "_" for ch in value)
    return cleaned or "na"