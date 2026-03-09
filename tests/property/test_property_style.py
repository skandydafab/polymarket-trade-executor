from __future__ import annotations

import random

import pytest

from executor.planner import ExecutionPlanner
from executor.state_machine import (
    OrderIntent,
    OrderState,
    PackageState,
    PackageStateMachine,
    TimeInForce,
    VenueFillEvent,
    VenueOrderAck,
    leg_by_id,
)
from tests.helpers.factories import make_default_planner_config, make_opportunity, make_package_execution, make_snapshots


pytestmark = pytest.mark.property_style


def test_planner_determinism_under_seeded_inputs(now_ns: int) -> None:
    rng = random.Random(20260309)
    planner = ExecutionPlanner(make_default_planner_config())

    for index in range(120):
        buy_ask = rng.randint(90, 110)
        buy_bid = buy_ask - rng.randint(1, 3)
        sell_bid = buy_ask + rng.randint(1, 8)
        sell_ask = sell_bid + rng.randint(1, 3)
        buy_size = rng.randint(10, 200)
        sell_size = rng.randint(10, 200)

        opportunity = make_opportunity(
            now_ns=now_ns,
            opportunity_id=f"opp-prop-{index}",
            target_package_units=rng.randint(2, 40),
            min_package_units=1,
            max_package_units=50,
            buy_reference_price_ticks=buy_ask,
            sell_reference_price_ticks=sell_bid,
            expected_edge_bps=20,
        )
        snapshots = make_snapshots(
            now_ns=now_ns,
            buy_best_bid_ticks=buy_bid,
            buy_best_ask_ticks=buy_ask,
            buy_best_ask_size=buy_size,
            buy_best_bid_size=buy_size,
            sell_best_bid_ticks=sell_bid,
            sell_best_ask_ticks=sell_ask,
            sell_best_bid_size=sell_size,
            sell_best_ask_size=sell_size,
            snapshot_age_ms=10,
        )

        first = planner.plan(opportunity, snapshots, now_ns)
        second = planner.plan(opportunity, snapshots, now_ns)

        assert first == second
        if first.plan is not None:
            assert first.plan.package_units > 0
            assert first.plan.expires_at_ns > now_ns
            assert first.plan.total_notional_cents > 0


def test_state_machine_fill_invariants_under_seeded_chunks(now_ns: int) -> None:
    rng = random.Random(1337)

    for trial in range(40):
        quantity = rng.randint(2, 20)
        package = make_package_execution(
            package_id=f"pkg-prop-{trial}",
            opportunity_id=f"opp-prop-{trial}",
            now_ns=now_ns + trial,
            quantity=quantity,
        )

        for leg_id, client_order_id, price in (
            ("leg-buy", f"cid-buy-{trial}", 100),
            ("leg-sell", f"cid-sell-{trial}", 106),
        ):
            package = PackageStateMachine.transition(
                package,
                OrderIntent(
                    package_id=package.package_id,
                    leg_id=leg_id,
                    client_order_id=client_order_id,
                    qty=quantity,
                    limit_price_ticks=price,
                    tif=TimeInForce.IOC,
                    ts_ns=now_ns + trial,
                ),
            )
            package = PackageStateMachine.transition(
                package,
                VenueOrderAck(
                    package_id=package.package_id,
                    leg_id=leg_id,
                    client_order_id=client_order_id,
                    venue_order_id=f"vo-{client_order_id}",
                    ts_ns=now_ns + trial + 1,
                ),
            )

        for leg_id, client_order_id, price in (
            ("leg-buy", f"cid-buy-{trial}", 100),
            ("leg-sell", f"cid-sell-{trial}", 106),
        ):
            cumulative = 0
            while cumulative < quantity:
                delta = min(rng.randint(1, 4), quantity - cumulative)
                cumulative += delta
                package = PackageStateMachine.transition(
                    package,
                    VenueFillEvent(
                        package_id=package.package_id,
                        leg_id=leg_id,
                        client_order_id=client_order_id,
                        fill_qty=delta,
                        fill_price_ticks=price,
                        ts_ns=now_ns + trial + cumulative,
                        cumulative_qty=cumulative,
                    ),
                )
                leg_state = leg_by_id(package, leg_id)
                assert 0 <= leg_state.filled_qty <= leg_state.intended_qty
                assert leg_state.state in {OrderState.PARTIALLY_FILLED, OrderState.FILLED}

        assert package.state == PackageState.COMPLETED
