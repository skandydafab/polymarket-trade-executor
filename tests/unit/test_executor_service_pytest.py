from __future__ import annotations

import pytest

from executor.executor_service import ExecutorService
from executor.fake_venue_adapter import FakeVenueAdapter
from executor.journal import (
    ExecutorJournal,
    InMemoryJournalStorage,
    JournalReplayLoader,
    JournalWriter,
    JournalWriterConfig,
    RecoveryCoordinator,
)
from executor.planner import ExecutionPlanner
from executor.risk import ExecutionRiskManager
from executor.state_machine import PackageState, VenueFillEvent
from tests.helpers.factories import (
    make_default_planner_config,
    make_default_risk_config,
    make_opportunity,
    make_snapshots,
)


pytestmark = pytest.mark.unit


def _build_service(*, storage: InMemoryJournalStorage, venue: FakeVenueAdapter) -> tuple[ExecutorService, JournalWriter]:
    planner = ExecutionPlanner(make_default_planner_config())
    risk = ExecutionRiskManager(make_default_risk_config())
    writer = JournalWriter(storage, config=JournalWriterConfig(background=False, flush_every_n_events=1))
    journal = ExecutorJournal(writer)
    recovery = RecoveryCoordinator(loader=JournalReplayLoader(storage))
    service = ExecutorService(
        planner=planner,
        risk_manager=risk,
        venue_adapter=venue,
        journal=journal,
        recovery_coordinator=recovery,
    )
    return service, writer


def test_executor_happy_path_to_completion(now_ns: int) -> None:
    storage = InMemoryJournalStorage()
    venue = FakeVenueAdapter()
    service, writer = _build_service(storage=storage, venue=venue)
    service.start()

    result = service.on_opportunity(
        relation_id="rel-exec-1",
        opportunity=make_opportunity(now_ns=now_ns, opportunity_id="opp-exec-1"),
        snapshots=make_snapshots(now_ns=now_ns),
        now_ns=now_ns,
    )

    assert result.accepted
    assert result.package_id is not None
    assert len(result.submitted_legs) == 2

    for leg in result.submitted_legs:
        venue.enqueue_fill(
            client_order_id=leg.client_order_id,
            fill_qty=leg.quantity,
            fill_price_ticks=leg.limit_price_ticks,
            ts_ns=now_ns + 1,
        )

    service.on_timer_tick(now_ns=now_ns + 2)
    package = service.get_package(result.package_id)
    assert package is not None
    assert package.state == PackageState.COMPLETED

    snap = service.snapshot()
    assert result.package_id in snap.terminal_package_ids
    assert not snap.open_order_client_ids

    service.stop()
    writer.close()


def test_manual_halt_blocks_new_opportunities(now_ns: int) -> None:
    storage = InMemoryJournalStorage()
    venue = FakeVenueAdapter()
    service, writer = _build_service(storage=storage, venue=venue)
    service.start()
    _ = service.activate_manual_halt(reason="operator pause", now_ns=now_ns)

    result = service.on_opportunity(
        relation_id="rel-exec-2",
        opportunity=make_opportunity(now_ns=now_ns, opportunity_id="opp-exec-2"),
        snapshots=make_snapshots(now_ns=now_ns),
        now_ns=now_ns,
    )

    assert not result.accepted
    assert result.reason == "trading halted"

    service.stop()
    writer.close()


def test_recover_rebuilds_active_and_open_maps(now_ns: int) -> None:
    storage = InMemoryJournalStorage()
    venue = FakeVenueAdapter()

    service_a, writer_a = _build_service(storage=storage, venue=venue)
    service_a.start()
    result = service_a.on_opportunity(
        relation_id="rel-exec-3",
        opportunity=make_opportunity(now_ns=now_ns, opportunity_id="opp-exec-3"),
        snapshots=make_snapshots(now_ns=now_ns),
        now_ns=now_ns,
    )
    assert result.accepted
    assert result.package_id is not None

    service_b, writer_b = _build_service(storage=storage, venue=venue)
    recovery_result = service_b.recover(now_ns=now_ns + 10)
    snap = service_b.snapshot()

    assert recovery_result.records_loaded > 0
    assert snap.active_package_ids
    assert snap.open_order_client_ids
    assert snap.is_halted

    risk_snapshot = service_b._risk_manager.exposure.snapshot()  # noqa: SLF001 - intentional white-box assertion
    assert risk_snapshot.active_package_count == len(snap.active_package_ids)
    assert risk_snapshot.total_open_notional_cents > 0

    writer_a.close()
    writer_b.close()


def test_late_fill_for_non_active_package_triggers_halt(now_ns: int) -> None:
    storage = InMemoryJournalStorage()
    venue = FakeVenueAdapter()
    service, writer = _build_service(storage=storage, venue=venue)
    service.start()

    service.on_venue_event(
        VenueFillEvent(
            package_id="pkg-missing",
            leg_id="leg-missing",
            client_order_id="cid-missing",
            fill_qty=1,
            fill_price_ticks=101,
            ts_ns=now_ns,
        ),
        now_ns=now_ns,
    )

    assert service.is_halted

    service.stop()
    writer.close()
