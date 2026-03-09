from __future__ import annotations

import pytest

from executor.fake_venue_adapter import FakeVenueAdapter
from executor.journal import (
    ExecutorJournal,
    InMemoryJournalStorage,
    JournalReplayLoader,
    JournalStateReconstructor,
    JournalWriter,
    JournalWriterConfig,
    RecoveryCoordinator,
)
from executor.state_machine import OrderIntent, PackageState, Side, TimeInForce, VenueFillEvent, VenueOrderAck
from executor.venue import VenueOrderIntent
from tests.helpers.factories import make_opportunity, make_plan, make_snapshots


pytestmark = pytest.mark.replay


def _build_partial_lifecycle_journal(*, now_ns: int) -> InMemoryJournalStorage:
    storage = InMemoryJournalStorage()
    writer = JournalWriter(storage, config=JournalWriterConfig(background=False, flush_every_n_events=1))
    journal = ExecutorJournal(writer)

    opportunity = make_opportunity(now_ns=now_ns, opportunity_id="opp-replay")
    plan = make_plan(now_ns=now_ns, opportunity=opportunity, snapshots=make_snapshots(now_ns=now_ns))

    journal.record_opportunity_accepted(
        package_id="pkg-replay",
        opportunity_id=plan.opportunity_id,
        relation_id="rel-replay",
        ts_ns=now_ns,
    )
    journal.record_execution_plan_created(
        package_id="pkg-replay",
        relation_id="rel-replay",
        plan=plan,
        ts_ns=now_ns + 1,
    )

    journal.record_order_submitted(
        OrderIntent(
            package_id="pkg-replay",
            leg_id="leg-buy",
            client_order_id="cid-buy",
            qty=10,
            limit_price_ticks=100,
            tif=TimeInForce.IOC,
            ts_ns=now_ns + 2,
        ),
        relation_id="rel-replay",
    )
    journal.record_order_ack(
        VenueOrderAck(
            package_id="pkg-replay",
            leg_id="leg-buy",
            client_order_id="cid-buy",
            venue_order_id="vo-buy",
            ts_ns=now_ns + 3,
        ),
        relation_id="rel-replay",
    )
    journal.record_order_fill(
        VenueFillEvent(
            package_id="pkg-replay",
            leg_id="leg-buy",
            client_order_id="cid-buy",
            fill_qty=10,
            fill_price_ticks=100,
            cumulative_qty=10,
            ts_ns=now_ns + 3,
        ),
        relation_id="rel-replay",
    )
    journal.record_order_submitted(
        OrderIntent(
            package_id="pkg-replay",
            leg_id="leg-sell",
            client_order_id="cid-sell",
            qty=10,
            limit_price_ticks=106,
            tif=TimeInForce.IOC,
            ts_ns=now_ns + 4,
        ),
        relation_id="rel-replay",
    )
    writer.close()
    return storage


def test_replay_is_deterministic_for_same_record_stream(now_ns: int) -> None:
    storage = _build_partial_lifecycle_journal(now_ns=now_ns)
    records = JournalReplayLoader(storage).load_records()

    reconstructor = JournalStateReconstructor()
    state_a = reconstructor.reconstruct(records)
    state_b = reconstructor.reconstruct(records)

    assert state_a.active_packages == state_b.active_packages
    assert state_a.terminal_packages == state_b.terminal_packages
    assert state_a.replay_errors == state_b.replay_errors
    assert state_a.orphan_records == state_b.orphan_records


def test_restart_recovery_with_open_orders(now_ns: int) -> None:
    storage = _build_partial_lifecycle_journal(now_ns=now_ns)
    venue = FakeVenueAdapter()
    _ = venue.submit_order(
        VenueOrderIntent(
            package_id="pkg-replay",
            leg_id="leg-sell",
            market_id="mkt-b",
            token_id="token-mkt-b",
            side=Side.SELL,
            quantity=10,
            limit_price_ticks=106,
            tif=TimeInForce.IOC,
            ts_ns=now_ns + 5,
            client_order_id="cid-sell",
        )
    )

    recovery = RecoveryCoordinator(loader=JournalReplayLoader(storage)).recover(venue_adapter=venue)

    assert recovery.records_loaded > 0
    assert "pkg-replay" in recovery.reconstructed_state.active_packages
    active_package = recovery.reconstructed_state.active_packages["pkg-replay"]
    assert active_package.state in {PackageState.EXECUTING, PackageState.NEW}
    assert recovery.reconciliation.missing_expected_client_order_ids == tuple()
