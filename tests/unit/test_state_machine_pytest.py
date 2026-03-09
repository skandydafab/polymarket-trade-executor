from __future__ import annotations

import pytest

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
from tests.helpers.factories import make_package_execution


pytestmark = pytest.mark.unit


def test_valid_package_lifecycle_completes(now_ns: int) -> None:
    package = make_package_execution(package_id="pkg-1", opportunity_id="opp-1", now_ns=now_ns)

    package = PackageStateMachine.transition(
        package,
        OrderIntent(
            package_id="pkg-1",
            leg_id="leg-buy",
            client_order_id="cid-buy-1",
            qty=10,
            limit_price_ticks=100,
            tif=TimeInForce.IOC,
            ts_ns=now_ns + 1,
        ),
    )
    package = PackageStateMachine.transition(
        package,
        VenueOrderAck(
            package_id="pkg-1",
            leg_id="leg-buy",
            client_order_id="cid-buy-1",
            venue_order_id="vo-buy-1",
            ts_ns=now_ns + 2,
        ),
    )
    package = PackageStateMachine.transition(
        package,
        VenueFillEvent(
            package_id="pkg-1",
            leg_id="leg-buy",
            client_order_id="cid-buy-1",
            fill_qty=10,
            fill_price_ticks=100,
            cumulative_qty=10,
            ts_ns=now_ns + 3,
        ),
    )

    package = PackageStateMachine.transition(
        package,
        OrderIntent(
            package_id="pkg-1",
            leg_id="leg-sell",
            client_order_id="cid-sell-1",
            qty=10,
            limit_price_ticks=106,
            tif=TimeInForce.IOC,
            ts_ns=now_ns + 4,
        ),
    )
    package = PackageStateMachine.transition(
        package,
        VenueOrderAck(
            package_id="pkg-1",
            leg_id="leg-sell",
            client_order_id="cid-sell-1",
            venue_order_id="vo-sell-1",
            ts_ns=now_ns + 5,
        ),
    )
    package = PackageStateMachine.transition(
        package,
        VenueFillEvent(
            package_id="pkg-1",
            leg_id="leg-sell",
            client_order_id="cid-sell-1",
            fill_qty=10,
            fill_price_ticks=106,
            cumulative_qty=10,
            ts_ns=now_ns + 6,
        ),
    )

    assert package.state == PackageState.COMPLETED
    assert leg_by_id(package, "leg-buy").state == OrderState.FILLED
    assert leg_by_id(package, "leg-sell").state == OrderState.FILLED


def test_partial_fill_asymmetry_one_leg_filled_other_pending(now_ns: int) -> None:
    package = make_package_execution(package_id="pkg-2", opportunity_id="opp-2", now_ns=now_ns)

    package = PackageStateMachine.transition(
        package,
        OrderIntent(
            package_id="pkg-2",
            leg_id="leg-buy",
            client_order_id="cid-buy-2",
            qty=10,
            limit_price_ticks=100,
            tif=TimeInForce.IOC,
            ts_ns=now_ns + 1,
        ),
    )
    package = PackageStateMachine.transition(
        package,
        VenueOrderAck(
            package_id="pkg-2",
            leg_id="leg-buy",
            client_order_id="cid-buy-2",
            venue_order_id="vo-buy-2",
            ts_ns=now_ns + 2,
        ),
    )
    package = PackageStateMachine.transition(
        package,
        VenueFillEvent(
            package_id="pkg-2",
            leg_id="leg-buy",
            client_order_id="cid-buy-2",
            fill_qty=10,
            fill_price_ticks=101,
            cumulative_qty=10,
            ts_ns=now_ns + 3,
        ),
    )

    package = PackageStateMachine.transition(
        package,
        OrderIntent(
            package_id="pkg-2",
            leg_id="leg-sell",
            client_order_id="cid-sell-2",
            qty=10,
            limit_price_ticks=106,
            tif=TimeInForce.IOC,
            ts_ns=now_ns + 4,
        ),
    )

    assert package.state == PackageState.EXECUTING
    assert leg_by_id(package, "leg-buy").state == OrderState.FILLED
    assert leg_by_id(package, "leg-sell").state == OrderState.PENDING_ACK
