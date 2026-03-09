from __future__ import annotations

import unittest

from executor.state_machine import (
    CancelRequestedEvent,
    EmergencyHaltEvent,
    EventValidationError,
    FailureCode,
    LegExecution,
    LegTimeoutEvent,
    OrderIntent,
    OrderState,
    PackageState,
    PackageStateMachine,
    Side,
    StateTransitionError,
    TimeInForce,
    VenueCancelEvent,
    VenueFillEvent,
    VenueOrderAck,
    VenueRejectEvent,
    create_package_execution,
    leg_by_id,
)


class PackageStateMachineTests(unittest.TestCase):
    def _new_two_leg_package(self) -> tuple[str, object]:
        package_id = "pkg-1"
        package = create_package_execution(
            package_id=package_id,
            opportunity_id="opp-1",
            created_ts_ns=1,
            legs=(
                LegExecution(
                    leg_id="leg-a",
                    market_id="mkt-a",
                    side=Side.BUY,
                    intended_qty=10,
                    limit_price_ticks=1200,
                ),
                LegExecution(
                    leg_id="leg-b",
                    market_id="mkt-b",
                    side=Side.SELL,
                    intended_qty=10,
                    limit_price_ticks=1300,
                ),
            ),
        )
        return package_id, package

    def test_happy_path_completes_when_all_intended_legs_fill(self) -> None:
        package_id, package = self._new_two_leg_package()

        package = PackageStateMachine.transition(
            package,
            OrderIntent(
                package_id=package_id,
                leg_id="leg-a",
                client_order_id="coid-a-1",
                qty=10,
                limit_price_ticks=1200,
                tif=TimeInForce.IOC,
                ts_ns=2,
            ),
        )
        package = PackageStateMachine.transition(
            package,
            VenueOrderAck(
                package_id=package_id,
                leg_id="leg-a",
                client_order_id="coid-a-1",
                venue_order_id="vo-a-1",
                ts_ns=3,
            ),
        )
        package = PackageStateMachine.transition(
            package,
            VenueFillEvent(
                package_id=package_id,
                leg_id="leg-a",
                client_order_id="coid-a-1",
                fill_qty=10,
                fill_price_ticks=1199,
                cumulative_qty=10,
                ts_ns=4,
            ),
        )

        package = PackageStateMachine.transition(
            package,
            OrderIntent(
                package_id=package_id,
                leg_id="leg-b",
                client_order_id="coid-b-1",
                qty=10,
                limit_price_ticks=1300,
                tif=TimeInForce.IOC,
                ts_ns=5,
            ),
        )
        package = PackageStateMachine.transition(
            package,
            VenueOrderAck(
                package_id=package_id,
                leg_id="leg-b",
                client_order_id="coid-b-1",
                venue_order_id="vo-b-1",
                ts_ns=6,
            ),
        )
        package = PackageStateMachine.transition(
            package,
            VenueFillEvent(
                package_id=package_id,
                leg_id="leg-b",
                client_order_id="coid-b-1",
                fill_qty=10,
                fill_price_ticks=1301,
                cumulative_qty=10,
                ts_ns=7,
            ),
        )

        self.assertEqual(package.state, PackageState.COMPLETED)
        self.assertEqual(leg_by_id(package, "leg-a").state, OrderState.FILLED)
        self.assertEqual(leg_by_id(package, "leg-b").state, OrderState.FILLED)

    def test_partial_fill_asymmetry_one_leg_filled_other_pending(self) -> None:
        package_id, package = self._new_two_leg_package()

        package = PackageStateMachine.transition(
            package,
            OrderIntent(
                package_id=package_id,
                leg_id="leg-a",
                client_order_id="coid-a-1",
                qty=10,
                limit_price_ticks=1200,
                tif=TimeInForce.IOC,
                ts_ns=2,
            ),
        )
        package = PackageStateMachine.transition(
            package,
            VenueOrderAck(
                package_id=package_id,
                leg_id="leg-a",
                client_order_id="coid-a-1",
                venue_order_id="vo-a-1",
                ts_ns=3,
            ),
        )
        package = PackageStateMachine.transition(
            package,
            VenueFillEvent(
                package_id=package_id,
                leg_id="leg-a",
                client_order_id="coid-a-1",
                fill_qty=10,
                fill_price_ticks=1201,
                cumulative_qty=10,
                ts_ns=4,
            ),
        )

        package = PackageStateMachine.transition(
            package,
            OrderIntent(
                package_id=package_id,
                leg_id="leg-b",
                client_order_id="coid-b-1",
                qty=10,
                limit_price_ticks=1300,
                tif=TimeInForce.IOC,
                ts_ns=5,
            ),
        )

        self.assertEqual(leg_by_id(package, "leg-a").state, OrderState.FILLED)
        self.assertEqual(leg_by_id(package, "leg-b").state, OrderState.PENDING_ACK)
        self.assertEqual(package.state, PackageState.EXECUTING)

    def test_reject_with_other_live_leg_marks_unwind_required(self) -> None:
        package_id, package = self._new_two_leg_package()

        package = PackageStateMachine.transition(
            package,
            OrderIntent(
                package_id=package_id,
                leg_id="leg-a",
                client_order_id="coid-a-1",
                qty=10,
                limit_price_ticks=1200,
                tif=TimeInForce.IOC,
                ts_ns=2,
            ),
        )
        package = PackageStateMachine.transition(
            package,
            VenueOrderAck(
                package_id=package_id,
                leg_id="leg-a",
                client_order_id="coid-a-1",
                venue_order_id="vo-a-1",
                ts_ns=3,
            ),
        )
        package = PackageStateMachine.transition(
            package,
            OrderIntent(
                package_id=package_id,
                leg_id="leg-b",
                client_order_id="coid-b-1",
                qty=10,
                limit_price_ticks=1300,
                tif=TimeInForce.IOC,
                ts_ns=4,
            ),
        )
        package = PackageStateMachine.transition(
            package,
            VenueRejectEvent(
                package_id=package_id,
                leg_id="leg-b",
                client_order_id="coid-b-1",
                reason="price outside collar",
                ts_ns=5,
            ),
        )

        self.assertEqual(package.state, PackageState.UNWIND_REQUIRED)
        self.assertIsNotNone(package.failure_reason)
        self.assertEqual(package.failure_reason.code, FailureCode.LEG_REJECTED)

    def test_timeout_after_other_leg_filled_marks_unwind_required(self) -> None:
        package_id, package = self._new_two_leg_package()

        package = PackageStateMachine.transition(
            package,
            OrderIntent(
                package_id=package_id,
                leg_id="leg-a",
                client_order_id="coid-a-1",
                qty=10,
                limit_price_ticks=1200,
                tif=TimeInForce.IOC,
                ts_ns=2,
            ),
        )
        package = PackageStateMachine.transition(
            package,
            VenueOrderAck(
                package_id=package_id,
                leg_id="leg-a",
                client_order_id="coid-a-1",
                venue_order_id="vo-a-1",
                ts_ns=3,
            ),
        )
        package = PackageStateMachine.transition(
            package,
            VenueFillEvent(
                package_id=package_id,
                leg_id="leg-a",
                client_order_id="coid-a-1",
                fill_qty=10,
                fill_price_ticks=1201,
                cumulative_qty=10,
                ts_ns=4,
            ),
        )

        package = PackageStateMachine.transition(
            package,
            OrderIntent(
                package_id=package_id,
                leg_id="leg-b",
                client_order_id="coid-b-1",
                qty=10,
                limit_price_ticks=1300,
                tif=TimeInForce.IOC,
                ts_ns=5,
            ),
        )
        package = PackageStateMachine.transition(
            package,
            VenueOrderAck(
                package_id=package_id,
                leg_id="leg-b",
                client_order_id="coid-b-1",
                venue_order_id="vo-b-1",
                ts_ns=6,
            ),
        )
        package = PackageStateMachine.transition(
            package,
            LegTimeoutEvent(
                package_id=package_id,
                leg_id="leg-b",
                reason="hedge timeout",
                ts_ns=7,
            ),
        )

        self.assertEqual(package.state, PackageState.UNWIND_REQUIRED)
        self.assertEqual(package.failure_reason.code, FailureCode.LEG_TIMEOUT)

    def test_late_fill_after_timeout_keeps_leg_exposure_and_forces_unwind_required(self) -> None:
        package_id, package = self._new_two_leg_package()

        package = PackageStateMachine.transition(
            package,
            OrderIntent(
                package_id=package_id,
                leg_id="leg-a",
                client_order_id="coid-a-1",
                qty=10,
                limit_price_ticks=1200,
                tif=TimeInForce.IOC,
                ts_ns=2,
            ),
        )
        package = PackageStateMachine.transition(
            package,
            VenueOrderAck(
                package_id=package_id,
                leg_id="leg-a",
                client_order_id="coid-a-1",
                venue_order_id="vo-a-1",
                ts_ns=3,
            ),
        )
        package = PackageStateMachine.transition(
            package,
            VenueFillEvent(
                package_id=package_id,
                leg_id="leg-a",
                client_order_id="coid-a-1",
                fill_qty=10,
                fill_price_ticks=1201,
                cumulative_qty=10,
                ts_ns=4,
            ),
        )
        package = PackageStateMachine.transition(
            package,
            OrderIntent(
                package_id=package_id,
                leg_id="leg-b",
                client_order_id="coid-b-1",
                qty=10,
                limit_price_ticks=1300,
                tif=TimeInForce.IOC,
                ts_ns=5,
            ),
        )
        package = PackageStateMachine.transition(
            package,
            VenueOrderAck(
                package_id=package_id,
                leg_id="leg-b",
                client_order_id="coid-b-1",
                venue_order_id="vo-b-1",
                ts_ns=6,
            ),
        )
        package = PackageStateMachine.transition(
            package,
            LegTimeoutEvent(
                package_id=package_id,
                leg_id="leg-b",
                reason="hedge timeout",
                ts_ns=7,
            ),
        )

        package = PackageStateMachine.transition(
            package,
            VenueFillEvent(
                package_id=package_id,
                leg_id="leg-b",
                client_order_id="coid-b-1",
                fill_qty=3,
                fill_price_ticks=1300,
                cumulative_qty=3,
                ts_ns=8,
            ),
        )

        leg_b = leg_by_id(package, "leg-b")
        self.assertEqual(leg_b.state, OrderState.TIMED_OUT)
        self.assertEqual(leg_b.filled_qty, 3)
        self.assertEqual(package.state, PackageState.UNWIND_REQUIRED)
        self.assertEqual(package.failure_reason.code, FailureCode.UNWIND_REQUIRED)

    def test_late_fill_after_cancel_confirm_forces_unwind_required(self) -> None:
        package_id, package = self._new_two_leg_package()

        package = PackageStateMachine.transition(
            package,
            OrderIntent(
                package_id=package_id,
                leg_id="leg-a",
                client_order_id="coid-a-1",
                qty=10,
                limit_price_ticks=1200,
                tif=TimeInForce.IOC,
                ts_ns=2,
            ),
        )
        package = PackageStateMachine.transition(
            package,
            VenueOrderAck(
                package_id=package_id,
                leg_id="leg-a",
                client_order_id="coid-a-1",
                venue_order_id="vo-a-1",
                ts_ns=3,
            ),
        )
        package = PackageStateMachine.transition(
            package,
            VenueFillEvent(
                package_id=package_id,
                leg_id="leg-a",
                client_order_id="coid-a-1",
                fill_qty=10,
                fill_price_ticks=1201,
                cumulative_qty=10,
                ts_ns=4,
            ),
        )
        package = PackageStateMachine.transition(
            package,
            OrderIntent(
                package_id=package_id,
                leg_id="leg-b",
                client_order_id="coid-b-1",
                qty=10,
                limit_price_ticks=1300,
                tif=TimeInForce.IOC,
                ts_ns=5,
            ),
        )
        package = PackageStateMachine.transition(
            package,
            VenueOrderAck(
                package_id=package_id,
                leg_id="leg-b",
                client_order_id="coid-b-1",
                venue_order_id="vo-b-1",
                ts_ns=6,
            ),
        )
        package = PackageStateMachine.transition(
            package,
            CancelRequestedEvent(
                package_id=package_id,
                leg_id="leg-b",
                reason="cancel hedge leg",
                ts_ns=7,
            ),
        )
        package = PackageStateMachine.transition(
            package,
            VenueCancelEvent(
                package_id=package_id,
                leg_id="leg-b",
                client_order_id="coid-b-1",
                canceled_qty=10,
                reason="venue canceled",
                ts_ns=8,
            ),
        )

        package = PackageStateMachine.transition(
            package,
            VenueFillEvent(
                package_id=package_id,
                leg_id="leg-b",
                client_order_id="coid-b-1",
                fill_qty=2,
                fill_price_ticks=1300,
                cumulative_qty=2,
                ts_ns=9,
            ),
        )

        leg_b = leg_by_id(package, "leg-b")
        self.assertEqual(leg_b.state, OrderState.CANCELED)
        self.assertEqual(leg_b.filled_qty, 2)
        self.assertEqual(package.state, PackageState.UNWIND_REQUIRED)
        self.assertEqual(package.failure_reason.code, FailureCode.UNWIND_REQUIRED)

    def test_emergency_halt_moves_package_and_live_legs_to_halted(self) -> None:
        package_id, package = self._new_two_leg_package()

        package = PackageStateMachine.transition(
            package,
            OrderIntent(
                package_id=package_id,
                leg_id="leg-a",
                client_order_id="coid-a-1",
                qty=10,
                limit_price_ticks=1200,
                tif=TimeInForce.IOC,
                ts_ns=2,
            ),
        )
        package = PackageStateMachine.transition(
            package,
            VenueOrderAck(
                package_id=package_id,
                leg_id="leg-a",
                client_order_id="coid-a-1",
                venue_order_id="vo-a-1",
                ts_ns=3,
            ),
        )
        package = PackageStateMachine.transition(
            package,
            OrderIntent(
                package_id=package_id,
                leg_id="leg-b",
                client_order_id="coid-b-1",
                qty=10,
                limit_price_ticks=1300,
                tif=TimeInForce.IOC,
                ts_ns=4,
            ),
        )

        package = PackageStateMachine.transition(
            package,
            EmergencyHaltEvent(
                package_id=package_id,
                reason="global kill switch",
                ts_ns=5,
            ),
        )

        self.assertEqual(package.state, PackageState.HALTED)
        self.assertEqual(package.failure_reason.code, FailureCode.EMERGENCY_HALT)
        self.assertEqual(leg_by_id(package, "leg-a").state, OrderState.HALTED)
        self.assertEqual(leg_by_id(package, "leg-b").state, OrderState.HALTED)

    def test_invalid_transition_ack_before_submit_raises(self) -> None:
        package_id, package = self._new_two_leg_package()

        with self.assertRaises((StateTransitionError, EventValidationError)):
            PackageStateMachine.transition(
                package,
                VenueOrderAck(
                    package_id=package_id,
                    leg_id="leg-a",
                    client_order_id="coid-a-1",
                    venue_order_id="vo-a-1",
                    ts_ns=2,
                ),
            )

    def test_cancel_requested_then_reject_keeps_unwind_required(self) -> None:
        package_id, package = self._new_two_leg_package()

        package = PackageStateMachine.transition(
            package,
            OrderIntent(
                package_id=package_id,
                leg_id="leg-a",
                client_order_id="coid-a-1",
                qty=10,
                limit_price_ticks=1200,
                tif=TimeInForce.IOC,
                ts_ns=2,
            ),
        )
        package = PackageStateMachine.transition(
            package,
            VenueOrderAck(
                package_id=package_id,
                leg_id="leg-a",
                client_order_id="coid-a-1",
                venue_order_id="vo-a-1",
                ts_ns=3,
            ),
        )
        package = PackageStateMachine.transition(
            package,
            CancelRequestedEvent(
                package_id=package_id,
                leg_id="leg-a",
                reason="operator requested cancel",
                ts_ns=4,
            ),
        )
        package = PackageStateMachine.transition(
            package,
            OrderIntent(
                package_id=package_id,
                leg_id="leg-b",
                client_order_id="coid-b-1",
                qty=10,
                limit_price_ticks=1300,
                tif=TimeInForce.IOC,
                ts_ns=5,
            ),
        )
        package = PackageStateMachine.transition(
            package,
            VenueRejectEvent(
                package_id=package_id,
                leg_id="leg-b",
                client_order_id="coid-b-1",
                reason="venue reject",
                ts_ns=6,
            ),
        )

        self.assertEqual(package.state, PackageState.UNWIND_REQUIRED)


if __name__ == "__main__":
    unittest.main()