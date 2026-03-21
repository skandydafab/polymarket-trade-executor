from __future__ import annotations

import pytest

from executor.orderbook_ws import OrderBookStore


pytestmark = pytest.mark.unit


def test_orderbook_store_snapshot_and_top_of_book() -> None:
    store = OrderBookStore()
    store.update_snapshot(
        token_id="t1",
        bids=[(40, 2.0), (42, 1.0)],
        asks=[(50, 3.0), (48, 1.5)],
        tick_size_ticks=1,
        ts_ms=1000,
    )

    top = store.get_top_of_book(token_id="t1", stale_ms=5000, now_ms=1001)
    assert top is not None
    assert top.best_bid_ticks == 42
    assert top.best_ask_ticks == 48
    assert top.best_bid_size == 1
    assert top.best_ask_size == 1


def test_orderbook_store_price_change_adds_and_removes_levels() -> None:
    store = OrderBookStore()
    store.apply_price_change(
        token_id="t2",
        side="BUY",
        price_ticks=55,
        size=10.0,
        ts_ms=2000,
    )
    store.apply_price_change(
        token_id="t2",
        side="SELL",
        price_ticks=60,
        size=5.0,
        ts_ms=2001,
    )

    top = store.get_top_of_book(token_id="t2", stale_ms=5000, now_ms=2002)
    assert top is not None
    assert top.best_bid_ticks == 55
    assert top.best_ask_ticks == 60

    store.apply_price_change(
        token_id="t2",
        side="BUY",
        price_ticks=55,
        size=0.0,
        ts_ms=2003,
    )
    top = store.get_top_of_book(token_id="t2", stale_ms=5000, now_ms=2004)
    assert top is None


def test_orderbook_store_best_bid_ask_updates_without_full_book() -> None:
    store = OrderBookStore()
    store.update_best_bid_ask(
        token_id="t3",
        best_bid_ticks=33,
        best_ask_ticks=35,
        ts_ms=3000,
    )

    top = store.get_top_of_book(token_id="t3", stale_ms=5000, now_ms=3001)
    assert top is not None
    assert top.best_bid_ticks == 33
    assert top.best_ask_ticks == 35


def test_orderbook_store_stale_returns_none() -> None:
    store = OrderBookStore()
    store.update_snapshot(
        token_id="t4",
        bids=[(10, 1.0)],
        asks=[(20, 1.0)],
        tick_size_ticks=1,
        ts_ms=1000,
    )

    assert store.get_top_of_book(token_id="t4", stale_ms=500, now_ms=2000) is None
