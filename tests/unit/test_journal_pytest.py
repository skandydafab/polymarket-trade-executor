from __future__ import annotations

import pytest

from executor.journal import (
    InMemoryJournalStorage,
    JournalEventSerializer,
    JournalEventType,
    JournalRecord,
    JournalReplayLoader,
    JournalStateReconstructor,
    JournalWriter,
    JournalWriterConfig,
)
from tests.helpers.factories import make_plan


pytestmark = pytest.mark.unit


def test_journal_record_roundtrip() -> None:
    serializer = JournalEventSerializer()
    record = JournalRecord(
        event_id="evt-1",
        ts_ns=1_000,
        event_type=JournalEventType.OPPORTUNITY_ACCEPTED,
        package_id="pkg-1",
        opportunity_id="opp-1",
        relation_id="rel-1",
        payload={"metadata": {"expected_edge_bps": 50}},
    )

    payload = serializer.dumps(record)
    parsed = serializer.loads(payload)

    assert parsed == record


def test_background_writer_flushes_and_replay_loader_reads(now_ns: int) -> None:
    storage = InMemoryJournalStorage()
    writer = JournalWriter(storage, config=JournalWriterConfig(background=True, flush_every_n_events=1))
    plan = make_plan(now_ns=now_ns)

    writer.append(
        JournalRecord(
            event_id="evt-plan",
            ts_ns=now_ns,
            event_type=JournalEventType.EXECUTION_PLAN_CREATED,
            package_id="pkg-x",
            opportunity_id=plan.opportunity_id,
            relation_id="rel-x",
            payload={
                "plan": {
                    "plan_id": plan.plan_id,
                    "opportunity_id": plan.opportunity_id,
                    "created_ts_ns": plan.created_ts_ns,
                    "expires_at_ns": plan.expires_at_ns,
                    "package_timeout_ms": plan.package_timeout_ms,
                    "package_units": plan.package_units,
                    "tif": plan.tif.value,
                    "total_shares": plan.total_shares,
                    "total_notional_cents": plan.total_notional_cents,
                    "expected_gross_profit_cents": plan.expected_gross_profit_cents,
                    "expected_fee_cents": plan.expected_fee_cents,
                    "expected_net_profit_cents": plan.expected_net_profit_cents,
                    "expected_net_edge_bps": plan.expected_net_edge_bps,
                    "legs": [
                        {
                            "leg_id": leg.leg_id,
                            "market_id": leg.market_id,
                            "token_id": leg.token_id,
                            "side": leg.side.value,
                            "submission_rank": leg.submission_rank,
                            "quantity": leg.quantity,
                            "executable_price_ticks": leg.executable_price_ticks,
                            "limit_price_ticks": leg.limit_price_ticks,
                            "tick_size_ticks": leg.tick_size_ticks,
                            "timeout_ms": leg.timeout_ms,
                            "tif": leg.tif.value,
                            "snapshot_ts_ns": leg.snapshot_ts_ns,
                            "available_units_at_plan": leg.available_units_at_plan,
                        }
                        for leg in plan.legs
                    ],
                }
            },
        )
    )
    writer.flush()
    writer.close()

    records = JournalReplayLoader(storage).load_records()
    state = JournalStateReconstructor().reconstruct(records)

    assert len(records) == 1
    assert "pkg-x" in state.active_packages
