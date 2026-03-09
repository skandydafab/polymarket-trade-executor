from __future__ import annotations

from time import perf_counter

import pytest

from executor.planner import ExecutionPlanner
from executor.state_machine import OrderIntent, PackageStateMachine, TimeInForce, VenueFillEvent, VenueOrderAck
from tests.helpers.factories import make_default_planner_config, make_opportunity, make_package_execution, make_snapshots


pytestmark = pytest.mark.performance


def test_planner_hotpath_latency_regression(now_ns: int) -> None:
    planner = ExecutionPlanner(make_default_planner_config())
    opportunity = make_opportunity(now_ns=now_ns)
    snapshots = make_snapshots(now_ns=now_ns)

    iterations = 3_000
    start = perf_counter()
    for _ in range(iterations):
        decision = planner.plan(opportunity, snapshots, now_ns)
        assert decision.plan is not None
    elapsed = perf_counter() - start

    assert elapsed < 4.0, f"planner hotpath regression: {elapsed:.3f}s for {iterations} iterations"


def test_state_machine_hotpath_latency_regression(now_ns: int) -> None:
    iterations = 2_000
    start = perf_counter()
    for index in range(iterations):
        package = make_package_execution(
            package_id=f"pkg-perf-{index}",
            opportunity_id=f"opp-perf-{index}",
            now_ns=now_ns + index,
            quantity=5,
        )
        package = PackageStateMachine.transition(
            package,
            OrderIntent(
                package_id=package.package_id,
                leg_id="leg-buy",
                client_order_id=f"cid-{index}",
                qty=5,
                limit_price_ticks=100,
                tif=TimeInForce.IOC,
                ts_ns=now_ns + index + 1,
            ),
        )
        package = PackageStateMachine.transition(
            package,
            VenueOrderAck(
                package_id=package.package_id,
                leg_id="leg-buy",
                client_order_id=f"cid-{index}",
                venue_order_id=f"vo-{index}",
                ts_ns=now_ns + index + 2,
            ),
        )
        package = PackageStateMachine.transition(
            package,
            VenueFillEvent(
                package_id=package.package_id,
                leg_id="leg-buy",
                client_order_id=f"cid-{index}",
                fill_qty=5,
                fill_price_ticks=100,
                cumulative_qty=5,
                ts_ns=now_ns + index + 3,
            ),
        )
        assert package is not None

    elapsed = perf_counter() - start
    assert elapsed < 5.0, f"state machine hotpath regression: {elapsed:.3f}s for {iterations} loops"
