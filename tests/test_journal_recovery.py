from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

from executor.fake_venue_adapter import FakeVenueAdapter
from executor.journal import (
    ExecutorJournal,
    InMemoryJournalStorage,
    JSONLFileJournalStorage,
    JournalEventSerializer,
    JournalEventType,
    JournalRecord,
    JournalReplayLoader,
    JournalStateReconstructor,
    JournalWriter,
    JournalWriterConfig,
    RecoveryCoordinator,
    partition_key_from_ts_ns,
    serialize_order_intent,
)
from executor.planner import ExecutionPlan, PlannedLeg
from executor.state_machine import (
    OrderIntent,
    OrderState,
    PackageState,
    Side,
    TimeInForce,
    VenueFillEvent,
    VenueOrderAck,
)


class JournalRecoveryTests(unittest.TestCase):
    def _plan(self, now_ns: int, opportunity_id: str = "opp-1") -> ExecutionPlan:
        legs = (
            PlannedLeg(
                leg_id="leg-a",
                market_id="mkt-a",
                token_id="token-a",
                side=Side.BUY,
                submission_rank=1,
                quantity=5,
                executable_price_ticks=100,
                limit_price_ticks=100,
                tick_size_ticks=1,
                timeout_ms=500,
                tif=TimeInForce.IOC,
                snapshot_ts_ns=now_ns - 1_000_000,
                available_units_at_plan=100,
            ),
            PlannedLeg(
                leg_id="leg-b",
                market_id="mkt-b",
                token_id="token-b",
                side=Side.SELL,
                submission_rank=2,
                quantity=5,
                executable_price_ticks=105,
                limit_price_ticks=105,
                tick_size_ticks=1,
                timeout_ms=500,
                tif=TimeInForce.IOC,
                snapshot_ts_ns=now_ns - 1_000_000,
                available_units_at_plan=100,
            ),
        )
        total_notional = 5 * 100 + 5 * 105
        net_profit = 25
        return ExecutionPlan(
            plan_id=f"plan-{opportunity_id}",
            opportunity_id=opportunity_id,
            created_ts_ns=now_ns,
            expires_at_ns=now_ns + 5_000_000_000,
            package_timeout_ms=1_500,
            package_units=5,
            tif=TimeInForce.IOC,
            total_shares=10,
            total_notional_cents=total_notional,
            expected_gross_profit_cents=net_profit,
            expected_fee_cents=0,
            expected_net_profit_cents=net_profit,
            expected_net_edge_bps=(net_profit * 10_000) // total_notional,
            legs=legs,
        )

    def test_event_roundtrip(self) -> None:
        serializer = JournalEventSerializer()
        intent = OrderIntent(
            package_id="pkg-1",
            leg_id="leg-a",
            client_order_id="cid-1",
            qty=5,
            limit_price_ticks=100,
            tif=TimeInForce.IOC,
            ts_ns=1_000_000,
            side=Side.BUY,
            market_id="mkt-a",
        )
        record = JournalRecord(
            event_id="evt-1",
            ts_ns=1_000_000,
            event_type=JournalEventType.ORDER_SUBMITTED,
            package_id="pkg-1",
            opportunity_id="opp-1",
            relation_id="rel-1",
            payload={"event": serialize_order_intent(intent)},
        )

        encoded = serializer.dumps(record)
        decoded = serializer.loads(encoded)

        self.assertEqual(decoded.event_id, record.event_id)
        self.assertEqual(decoded.event_type, record.event_type)
        self.assertEqual(decoded.package_id, record.package_id)
        self.assertEqual(decoded.payload, record.payload)

    def test_replay_reconstruction_to_terminal_completion(self) -> None:
        now_ns = 1_000_000_000_000
        storage = InMemoryJournalStorage()
        writer = JournalWriter(storage, config=JournalWriterConfig(background=False, flush_every_n_events=1))
        journal = ExecutorJournal(writer)
        plan = self._plan(now_ns)

        journal.record_opportunity_accepted(
            package_id="pkg-1",
            opportunity_id=plan.opportunity_id,
            relation_id="rel-1",
            ts_ns=now_ns,
        )
        journal.record_execution_plan_created(
            package_id="pkg-1",
            relation_id="rel-1",
            plan=plan,
            ts_ns=now_ns + 1,
        )
        journal.record_order_submitted(
            OrderIntent(
                package_id="pkg-1",
                leg_id="leg-a",
                client_order_id="cid-a-1",
                qty=5,
                limit_price_ticks=100,
                tif=TimeInForce.IOC,
                ts_ns=now_ns + 2,
            )
        )
        journal.record_order_ack(
            VenueOrderAck(
                package_id="pkg-1",
                leg_id="leg-a",
                client_order_id="cid-a-1",
                venue_order_id="vo-a-1",
                ts_ns=now_ns + 3,
            )
        )
        journal.record_order_fill(
            VenueFillEvent(
                package_id="pkg-1",
                leg_id="leg-a",
                client_order_id="cid-a-1",
                fill_qty=5,
                fill_price_ticks=100,
                ts_ns=now_ns + 4,
                cumulative_qty=5,
            )
        )
        journal.record_order_submitted(
            OrderIntent(
                package_id="pkg-1",
                leg_id="leg-b",
                client_order_id="cid-b-1",
                qty=5,
                limit_price_ticks=105,
                tif=TimeInForce.IOC,
                ts_ns=now_ns + 5,
            )
        )
        journal.record_order_ack(
            VenueOrderAck(
                package_id="pkg-1",
                leg_id="leg-b",
                client_order_id="cid-b-1",
                venue_order_id="vo-b-1",
                ts_ns=now_ns + 6,
            )
        )
        journal.record_order_fill(
            VenueFillEvent(
                package_id="pkg-1",
                leg_id="leg-b",
                client_order_id="cid-b-1",
                fill_qty=5,
                fill_price_ticks=105,
                ts_ns=now_ns + 7,
                cumulative_qty=5,
            )
        )
        writer.close()

        records = JournalReplayLoader(storage).load_records()
        reconstructed = JournalStateReconstructor().reconstruct(records)

        self.assertEqual(len(records), 8)
        self.assertIn("pkg-1", reconstructed.terminal_packages)
        self.assertEqual(reconstructed.terminal_packages["pkg-1"].state, PackageState.COMPLETED)
        self.assertEqual(reconstructed.expected_open_client_order_ids(), set())

    def test_restart_loads_partially_completed_package_and_reconciles(self) -> None:
        now_ns = 1_500_000_000_000
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "journal"
            storage = JSONLFileJournalStorage(root)
            writer = JournalWriter(storage, config=JournalWriterConfig(background=False, flush_every_n_events=1))
            journal = ExecutorJournal(writer)
            plan = self._plan(now_ns, opportunity_id="opp-restart")

            journal.record_opportunity_accepted(
                package_id="pkg-r",
                opportunity_id=plan.opportunity_id,
                relation_id="rel-r",
                ts_ns=now_ns,
            )
            journal.record_execution_plan_created(
                package_id="pkg-r",
                relation_id="rel-r",
                plan=plan,
                ts_ns=now_ns + 1,
            )
            journal.record_order_submitted(
                OrderIntent(
                    package_id="pkg-r",
                    leg_id="leg-a",
                    client_order_id="cid-ra-1",
                    qty=5,
                    limit_price_ticks=100,
                    tif=TimeInForce.IOC,
                    ts_ns=now_ns + 2,
                )
            )
            journal.record_order_ack(
                VenueOrderAck(
                    package_id="pkg-r",
                    leg_id="leg-a",
                    client_order_id="cid-ra-1",
                    venue_order_id="vo-ra-1",
                    ts_ns=now_ns + 3,
                )
            )
            journal.record_order_fill(
                VenueFillEvent(
                    package_id="pkg-r",
                    leg_id="leg-a",
                    client_order_id="cid-ra-1",
                    fill_qty=5,
                    fill_price_ticks=100,
                    ts_ns=now_ns + 4,
                    cumulative_qty=5,
                )
            )
            journal.record_order_submitted(
                OrderIntent(
                    package_id="pkg-r",
                    leg_id="leg-b",
                    client_order_id="cid-rb-1",
                    qty=5,
                    limit_price_ticks=105,
                    tif=TimeInForce.IOC,
                    ts_ns=now_ns + 5,
                )
            )
            writer.close()

            read_storage = JSONLFileJournalStorage(root)
            loader = JournalReplayLoader(read_storage)
            recovery = RecoveryCoordinator(loader=loader).recover(venue_adapter=FakeVenueAdapter())

            self.assertEqual(partition_key_from_ts_ns(now_ns), next(iter(read_storage.list_partitions())))
            self.assertIn("pkg-r", recovery.reconstructed_state.active_packages)
            package = recovery.reconstructed_state.active_packages["pkg-r"]
            leg_states = {leg.leg_id: leg.state for leg in package.legs}
            self.assertEqual(leg_states["leg-a"], OrderState.FILLED)
            self.assertEqual(leg_states["leg-b"], OrderState.PENDING_ACK)
            self.assertIn("cid-rb-1", recovery.reconciliation.missing_expected_client_order_ids)


if __name__ == "__main__":
    unittest.main()