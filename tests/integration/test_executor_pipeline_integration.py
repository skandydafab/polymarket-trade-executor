from __future__ import annotations

from dataclasses import replace

import pytest

from executor.fake_venue_adapter import FakeVenueAdapter
from executor.journal import (
    ExecutorJournal,
    InMemoryJournalStorage,
    JournalReplayLoader,
    JournalStateReconstructor,
    JournalWriter,
    JournalWriterConfig,
)
from executor.risk import ExecutionRiskManager
from executor.state_machine import (
    LegExecution,
    OrderIntent,
    PackageState,
    PackageStateMachine,
    create_package_execution,
)
from executor.venue import VenueOrderIntent
from tests.helpers.factories import (
    make_default_risk_config,
    make_opportunity,
    make_plan,
    make_pretrade_request,
    make_snapshots,
)


pytestmark = pytest.mark.integration


def _package_from_plan(*, package_id: str, plan_id: str, now_ns: int, plan_legs: tuple[object, ...]):
    legs = tuple(
        LegExecution(
            leg_id=leg.leg_id,
            market_id=leg.market_id,
            side=leg.side,
            intended_qty=leg.quantity,
            limit_price_ticks=leg.limit_price_ticks,
        )
        for leg in plan_legs
    )
    return create_package_execution(
        package_id=package_id,
        opportunity_id=plan_id,
        created_ts_ns=now_ns,
        legs=legs,
    )


def test_valid_package_lifecycle_integration(now_ns: int) -> None:
    opportunity = make_opportunity(now_ns=now_ns)
    snapshots = make_snapshots(now_ns=now_ns)
    plan = make_plan(now_ns=now_ns, opportunity=opportunity, snapshots=snapshots)

    risk = ExecutionRiskManager(make_default_risk_config())
    register = risk.register_package(
        make_pretrade_request(package_id="pkg-int-1", relation_id="rel-int", plan=plan),
        now_ns,
    )
    assert register.allowed

    package = _package_from_plan(
        package_id="pkg-int-1",
        plan_id=plan.opportunity_id,
        now_ns=now_ns,
        plan_legs=plan.legs,
    )
    package = replace(package, state=PackageState.NEW, updated_ts_ns=now_ns)

    adapter = FakeVenueAdapter()
    storage = InMemoryJournalStorage()
    writer = JournalWriter(storage, config=JournalWriterConfig(background=False, flush_every_n_events=1))
    journal = ExecutorJournal(writer)

    journal.record_opportunity_accepted(
        package_id="pkg-int-1",
        opportunity_id=plan.opportunity_id,
        relation_id="rel-int",
        ts_ns=now_ns,
    )
    journal.record_execution_plan_created(
        package_id="pkg-int-1",
        relation_id="rel-int",
        plan=plan,
        ts_ns=now_ns + 1,
    )

    ts_cursor = now_ns + 10
    for leg in sorted(plan.legs, key=lambda item: item.submission_rank):
        client_order_id = f"cid-{leg.leg_id}"
        venue_intent = VenueOrderIntent(
            package_id="pkg-int-1",
            leg_id=leg.leg_id,
            market_id=leg.market_id,
            token_id=leg.token_id,
            side=leg.side,
            quantity=leg.quantity,
            limit_price_ticks=leg.limit_price_ticks,
            tif=leg.tif,
            ts_ns=ts_cursor,
            client_order_id=client_order_id,
        )
        submit_result = adapter.submit_order(venue_intent)

        submit_event = OrderIntent(
            package_id="pkg-int-1",
            leg_id=leg.leg_id,
            client_order_id=client_order_id,
            qty=leg.quantity,
            limit_price_ticks=leg.limit_price_ticks,
            tif=leg.tif,
            ts_ns=ts_cursor,
        )
        package = PackageStateMachine.transition(package, submit_event)
        journal.record_order_submitted(submit_event, relation_id="rel-int")
        ts_cursor += 1

        for ack_event in submit_result.events:
            package = PackageStateMachine.transition(package, ack_event)
            journal.record_order_ack(ack_event, relation_id="rel-int")
            _ = risk.on_state_event(package, ack_event, ts_cursor)
            ts_cursor += 1

        adapter.enqueue_fill(
            client_order_id=client_order_id,
            fill_qty=leg.quantity,
            fill_price_ticks=leg.limit_price_ticks,
            ts_ns=ts_cursor,
        )
        updates = adapter.poll_or_process_order_updates()
        for update_event in updates:
            package = PackageStateMachine.transition(package, update_event)
            journal.record_order_fill(update_event, relation_id="rel-int")
            _ = risk.on_state_event(package, update_event, ts_cursor)
            ts_cursor += 1

    assert package.state == PackageState.COMPLETED
    journal.record_package_completion(
        package_id="pkg-int-1",
        opportunity_id=plan.opportunity_id,
        relation_id="rel-int",
        ts_ns=ts_cursor,
    )
    writer.close()

    replay_records = JournalReplayLoader(storage).load_records()
    reconstructed = JournalStateReconstructor().reconstruct(replay_records)

    assert "pkg-int-1" in reconstructed.terminal_packages
    assert reconstructed.terminal_packages["pkg-int-1"].state == PackageState.COMPLETED
