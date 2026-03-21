from __future__ import annotations

from dataclasses import dataclass
import json
import threading
import time
from typing import Iterable, Mapping
from urllib.parse import urlencode
from urllib.request import Request, urlopen

try:
    import websocket  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    websocket = None


@dataclass(slots=True)
class OrderBookTop:
    best_bid_ticks: int
    best_bid_size: int
    best_ask_ticks: int
    best_ask_size: int
    tick_size_ticks: int
    ts_ms: int


@dataclass(slots=True)
class _BookState:
    bids: dict[int, float]
    asks: dict[int, float]
    best_bid_ticks: int | None
    best_ask_ticks: int | None
    best_bid_size: int
    best_ask_size: int
    tick_size_ticks: int
    last_update_ms: int
    has_full_book: bool


class OrderBookStore:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._books: dict[str, _BookState] = {}

    def update_snapshot(
        self,
        *,
        token_id: str,
        bids: Iterable[tuple[int, float]],
        asks: Iterable[tuple[int, float]],
        tick_size_ticks: int,
        ts_ms: int,
    ) -> None:
        bid_map = {price: size for price, size in bids if size > 0}
        ask_map = {price: size for price, size in asks if size > 0}
        best_bid, best_bid_size = _best_level(bid_map, is_bid=True)
        best_ask, best_ask_size = _best_level(ask_map, is_bid=False)
        with self._lock:
            self._books[token_id] = _BookState(
                bids=bid_map,
                asks=ask_map,
                best_bid_ticks=best_bid,
                best_ask_ticks=best_ask,
                best_bid_size=best_bid_size,
                best_ask_size=best_ask_size,
                tick_size_ticks=max(1, tick_size_ticks),
                last_update_ms=ts_ms,
                has_full_book=True,
            )

    def apply_price_change(
        self,
        *,
        token_id: str,
        side: str,
        price_ticks: int,
        size: float,
        ts_ms: int,
    ) -> None:
        with self._lock:
            book = self._books.get(token_id)
            if book is None:
                book = _BookState(
                    bids={},
                    asks={},
                    best_bid_ticks=None,
                    best_ask_ticks=None,
                    best_bid_size=0,
                    best_ask_size=0,
                    tick_size_ticks=1,
                    last_update_ms=ts_ms,
                    has_full_book=False,
                )
                self._books[token_id] = book

            levels = book.bids if side.upper() == "BUY" else book.asks
            if size <= 0:
                levels.pop(price_ticks, None)
            else:
                levels[price_ticks] = size

            book.best_bid_ticks, book.best_bid_size = _best_level(book.bids, is_bid=True)
            book.best_ask_ticks, book.best_ask_size = _best_level(book.asks, is_bid=False)
            book.last_update_ms = ts_ms

    def update_best_bid_ask(
        self,
        *,
        token_id: str,
        best_bid_ticks: int | None,
        best_ask_ticks: int | None,
        ts_ms: int,
    ) -> None:
        with self._lock:
            book = self._books.get(token_id)
            if book is None:
                book = _BookState(
                    bids={},
                    asks={},
                    best_bid_ticks=None,
                    best_ask_ticks=None,
                    best_bid_size=0,
                    best_ask_size=0,
                    tick_size_ticks=1,
                    last_update_ms=ts_ms,
                    has_full_book=False,
                )
                self._books[token_id] = book

            if best_bid_ticks is not None:
                book.best_bid_ticks = best_bid_ticks
                if book.best_bid_size <= 0:
                    book.best_bid_size = 1
            if best_ask_ticks is not None:
                book.best_ask_ticks = best_ask_ticks
                if book.best_ask_size <= 0:
                    book.best_ask_size = 1
            book.last_update_ms = ts_ms

    def update_tick_size(self, *, token_id: str, tick_size_ticks: int, ts_ms: int) -> None:
        with self._lock:
            book = self._books.get(token_id)
            if book is None:
                book = _BookState(
                    bids={},
                    asks={},
                    best_bid_ticks=None,
                    best_ask_ticks=None,
                    best_bid_size=0,
                    best_ask_size=0,
                    tick_size_ticks=max(1, tick_size_ticks),
                    last_update_ms=ts_ms,
                    has_full_book=False,
                )
                self._books[token_id] = book
            else:
                book.tick_size_ticks = max(1, tick_size_ticks)
                book.last_update_ms = ts_ms

    def get_top_of_book(self, *, token_id: str, stale_ms: int, now_ms: int | None = None) -> OrderBookTop | None:
        current_ms = now_ms if now_ms is not None else int(time.time() * 1000)
        with self._lock:
            book = self._books.get(token_id)
            if book is None:
                return None
            if stale_ms > 0 and current_ms - book.last_update_ms > stale_ms:
                return None
            if book.best_bid_ticks is None or book.best_ask_ticks is None:
                return None
            return OrderBookTop(
                best_bid_ticks=book.best_bid_ticks,
                best_bid_size=max(1, book.best_bid_size),
                best_ask_ticks=book.best_ask_ticks,
                best_ask_size=max(1, book.best_ask_size),
                tick_size_ticks=max(1, book.tick_size_ticks),
                ts_ms=book.last_update_ms,
            )


class OrderBookWsClient:
    def __init__(
        self,
        *,
        ws_url: str,
        store: OrderBookStore,
        custom_feature_enabled: bool = True,
    ) -> None:
        self._ws_url = ws_url
        self._store = store
        self._custom_feature_enabled = custom_feature_enabled
        self._thread: threading.Thread | None = None
        self._ws_app: object | None = None
        self._stop_event = threading.Event()
        self._lock = threading.Lock()
        self._subscribed: set[str] = set()
        self._pending_subscribe: set[str] = set()

    def start(self) -> None:
        if websocket is None:
            raise RuntimeError("websocket-client is not installed")
        if self._thread is not None:
            return
        self._thread = threading.Thread(target=self._run, name="orderbook-ws", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        ws_app = self._ws_app
        if ws_app is not None and hasattr(ws_app, "close"):
            ws_app.close()
        thread = self._thread
        if thread is not None:
            thread.join(timeout=2.0)
        self._thread = None

    def ensure_subscribed(
        self,
        *,
        token_ids: Iterable[str],
        api_url: str,
        timeout_ms: int,
        max_concurrency: int,
    ) -> None:
        new_tokens = set(token_ids)
        if not new_tokens:
            return
        with self._lock:
            pending = new_tokens - self._subscribed
            if not pending:
                return
            self._pending_subscribe.update(pending)
            self._subscribed.update(pending)

        _seed_orderbooks(
            store=self._store,
            token_ids=pending,
            api_url=api_url,
            timeout_ms=timeout_ms,
            max_concurrency=max_concurrency,
        )
        self._send_subscribe(pending)

    def _run(self) -> None:
        assert websocket is not None
        self._ws_app = websocket.WebSocketApp(
            self._ws_url,
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close,
        )
        while not self._stop_event.is_set():
            self._ws_app.run_forever(ping_interval=20, ping_timeout=10)
            if not self._stop_event.is_set():
                time.sleep(1.0)

    def _send_subscribe(self, token_ids: Iterable[str]) -> None:
        ws_app = self._ws_app
        if ws_app is None or not hasattr(ws_app, "send"):
            return
        payload = {
            "assets_ids": list(token_ids),
            "operation": "subscribe",
        }
        try:
            ws_app.send(json.dumps(payload))
        except Exception:
            return

    def _on_open(self, ws_app) -> None:  # type: ignore[no-untyped-def]
        with self._lock:
            pending = set(self._pending_subscribe)
            self._pending_subscribe.clear()
            subscribed = set(self._subscribed)
        if subscribed:
            payload = {
                "type": "market",
                "assets_ids": list(subscribed),
                "custom_feature_enabled": self._custom_feature_enabled,
            }
            try:
                ws_app.send(json.dumps(payload))
            except Exception:
                return
        if pending:
            self._send_subscribe(pending)

    def _on_close(self, ws_app, status_code, message) -> None:  # type: ignore[no-untyped-def]
        _ = (ws_app, status_code, message)

    def _on_error(self, ws_app, error) -> None:  # type: ignore[no-untyped-def]
        _ = (ws_app, error)

    def _on_message(self, ws_app, message: str) -> None:  # type: ignore[no-untyped-def]
        _ = ws_app
        try:
            payload = json.loads(message)
        except Exception:
            return

        event_type = payload.get("event_type")
        ts_ms = _parse_timestamp_ms(payload.get("timestamp"))

        if event_type == "book":
            token_id = str(payload.get("asset_id") or "")
            bids_raw = payload.get("bids")
            asks_raw = payload.get("asks")
            if not token_id or not isinstance(bids_raw, list) or not isinstance(asks_raw, list):
                return
            bids = _parse_levels(bids_raw)
            asks = _parse_levels(asks_raw)
            tick_size = _to_probability(payload.get("tick_size")) or 0.01
            tick_size_ticks = _probability_to_ticks(tick_size)
            self._store.update_snapshot(
                token_id=token_id,
                bids=bids,
                asks=asks,
                tick_size_ticks=tick_size_ticks,
                ts_ms=ts_ms,
            )
            return

        if event_type == "price_change":
            changes = payload.get("price_changes")
            if not isinstance(changes, list):
                return
            for change in changes:
                if not isinstance(change, Mapping):
                    continue
                token_id = str(change.get("asset_id") or "")
                side = str(change.get("side") or "")
                price = _to_probability(change.get("price"))
                size = _to_float(change.get("size"))
                if not token_id or not side or price is None or size is None:
                    continue
                self._store.apply_price_change(
                    token_id=token_id,
                    side=side,
                    price_ticks=_probability_to_ticks(price),
                    size=size,
                    ts_ms=ts_ms,
                )
            return

        if event_type == "best_bid_ask":
            token_id = str(payload.get("asset_id") or "")
            if not token_id:
                return
            best_bid = _to_probability(payload.get("best_bid"))
            best_ask = _to_probability(payload.get("best_ask"))
            self._store.update_best_bid_ask(
                token_id=token_id,
                best_bid_ticks=_probability_to_ticks(best_bid) if best_bid is not None else None,
                best_ask_ticks=_probability_to_ticks(best_ask) if best_ask is not None else None,
                ts_ms=ts_ms,
            )
            return

        if event_type == "tick_size_change":
            token_id = str(payload.get("asset_id") or "")
            tick_size = _to_probability(payload.get("new_tick_size"))
            if not token_id or tick_size is None:
                return
            self._store.update_tick_size(
                token_id=token_id,
                tick_size_ticks=_probability_to_ticks(tick_size),
                ts_ms=ts_ms,
            )


def _parse_levels(entries: list[object]) -> list[tuple[int, float]]:
    levels: list[tuple[int, float]] = []
    for entry in entries:
        if not isinstance(entry, Mapping):
            continue
        price = _to_probability(entry.get("price"))
        size = _to_float(entry.get("size"))
        if price is None or size is None:
            continue
        if size <= 0:
            continue
        levels.append((_probability_to_ticks(price), size))
    return levels


def _seed_orderbooks(
    *,
    store: OrderBookStore,
    token_ids: Iterable[str],
    api_url: str,
    timeout_ms: int,
    max_concurrency: int,
) -> None:
    token_list = list(token_ids)
    if not token_list:
        return

    worker_count = max(1, min(max_concurrency, len(token_list)))
    if worker_count == 1:
        for token_id in token_list:
            snapshot = _fetch_orderbook_snapshot(token_id=token_id, api_url=api_url, timeout_ms=timeout_ms)
            if snapshot is None:
                continue
            store.update_snapshot(
                token_id=token_id,
                bids=snapshot.bids,
                asks=snapshot.asks,
                tick_size_ticks=snapshot.tick_size_ticks,
                ts_ms=snapshot.ts_ms,
            )
        return

    from concurrent.futures import ThreadPoolExecutor, as_completed

    with ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix="book-snapshot") as pool:
        future_to_token = {
            pool.submit(_fetch_orderbook_snapshot, token_id=token_id, api_url=api_url, timeout_ms=timeout_ms): token_id
            for token_id in token_list
        }
        for future in as_completed(future_to_token):
            token_id = future_to_token[future]
            try:
                snapshot = future.result()
            except Exception:
                snapshot = None
            if snapshot is None:
                continue
            store.update_snapshot(
                token_id=token_id,
                bids=snapshot.bids,
                asks=snapshot.asks,
                tick_size_ticks=snapshot.tick_size_ticks,
                ts_ms=snapshot.ts_ms,
            )


@dataclass(slots=True)
class _Snapshot:
    bids: list[tuple[int, float]]
    asks: list[tuple[int, float]]
    tick_size_ticks: int
    ts_ms: int


def _fetch_orderbook_snapshot(*, token_id: str, api_url: str, timeout_ms: int) -> _Snapshot | None:
    url = f"{api_url.rstrip('/')}/book?{urlencode({'token_id': token_id})}"
    request = Request(url, method="GET", headers={"Accept": "application/json", "User-Agent": "polyexecutor/1.0"})
    try:
        with urlopen(request, timeout=max(0.1, timeout_ms / 1000.0)) as response:
            body = response.read().decode("utf-8", errors="replace")
    except Exception:
        return None

    try:
        payload = json.loads(body)
    except Exception:
        return None

    if not isinstance(payload, Mapping):
        return None
    bids_raw = payload.get("bids")
    asks_raw = payload.get("asks")
    if not isinstance(bids_raw, list) or not isinstance(asks_raw, list):
        return None

    tick_size = _to_probability(payload.get("tick_size")) or 0.01
    tick_size_ticks = _probability_to_ticks(tick_size)
    ts_ms = _parse_timestamp_ms(payload.get("timestamp"))

    return _Snapshot(
        bids=_parse_levels(bids_raw),
        asks=_parse_levels(asks_raw),
        tick_size_ticks=tick_size_ticks,
        ts_ms=ts_ms,
    )


def _best_level(levels: Mapping[int, float], *, is_bid: bool) -> tuple[int | None, int]:
    if not levels:
        return None, 0
    key = max(levels) if is_bid else min(levels)
    size = levels.get(key, 0.0)
    return key, max(1, int(size))


def _parse_timestamp_ms(value: object) -> int:
    if value is None:
        return int(time.time() * 1000)
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return int(time.time() * 1000)
        try:
            return int(float(text))
        except ValueError:
            return int(time.time() * 1000)
    return int(time.time() * 1000)


def _to_probability(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        numeric = float(value)
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            numeric = float(text)
        except ValueError:
            return None
    else:
        return None

    if numeric < 0:
        return None
    if numeric <= 1:
        return numeric
    if numeric <= 100:
        return numeric / 100.0
    return None


def _to_float(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return float(text)
        except ValueError:
            return None
    return None


def _probability_to_ticks(probability: float) -> int:
    return max(1, min(99, int(round(probability * 100))))
