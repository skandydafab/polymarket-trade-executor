from __future__ import annotations

from dataclasses import dataclass

from executor import (
    ExecutorJournal,
    ExecutorService,
    FakeVenueAdapter,
    InMemoryJournalStorage,
    JournalReplayLoader,
    JournalWriter,
    JournalWriterConfig,
    Opportunity,
    OpportunityLeg,
    PricingSnapshot,
    RecoveryCoordinator,
    RiskManagerConfig,
    Side,
)
from executor.planner import ExecutionPlanner, PlannerConfig
from executor.risk import ExecutionRiskManager


@dataclass(slots=True)
class PrintLogger:
    def debug(self, message: str, **fields: object) -> None:
        _ = (message, fields)

    def info(self, message: str, **fields: object) -> None:
        print("INFO", message, fields)

    def warning(self, message: str, **fields: object) -> None:
        print("WARN", message, fields)

    def error(self, message: str, **fields: object) -> None:
        print("ERROR", message, fields)


def main() -> None:
    now_ns = 1_700_000_000_000_000_000

    planner = ExecutionPlanner(PlannerConfig())
    risk = ExecutionRiskManager(RiskManagerConfig())
    venue = FakeVenueAdapter()

    storage = InMemoryJournalStorage()
    writer = JournalWriter(storage, config=JournalWriterConfig(background=False, flush_every_n_events=1))
    journal = ExecutorJournal(writer)
    recovery = RecoveryCoordinator(loader=JournalReplayLoader(storage))

    service = ExecutorService(
        planner=planner,
        risk_manager=risk,
        venue_adapter=venue,
        journal=journal,
        recovery_coordinator=recovery,
        logger=PrintLogger(),
    )

    service.start()

    opportunity = Opportunity(
        opportunity_id="opp-minimal-1",
        detected_ts_ns=now_ns - 1_000_000,
        expires_at_ns=now_ns + 5_000_000_000,
        expected_edge_bps=40,
        confidence=0.99,
        target_package_units=5,
        min_package_units=1,
        max_package_units=10,
        legs=(
            OpportunityLeg(
                leg_id="leg-buy",
                market_id="mkt-a",
                token_id="token-a",
                side=Side.BUY,
                quantity_ratio=1,
                reference_price_ticks=100,
            ),
            OpportunityLeg(
                leg_id="leg-sell",
                market_id="mkt-b",
                token_id="token-b",
                side=Side.SELL,
                quantity_ratio=1,
                reference_price_ticks=106,
            ),
        ),
    )

    snapshots = {
        "mkt-a": PricingSnapshot(
            market_id="mkt-a",
            ts_ns=now_ns,
            best_bid_ticks=99,
            best_bid_size=1_000,
            best_ask_ticks=100,
            best_ask_size=1_000,
            tick_size_ticks=1,
        ),
        "mkt-b": PricingSnapshot(
            market_id="mkt-b",
            ts_ns=now_ns,
            best_bid_ticks=106,
            best_bid_size=1_000,
            best_ask_ticks=107,
            best_ask_size=1_000,
            tick_size_ticks=1,
        ),
    }

    result = service.on_opportunity(
        relation_id="rel-minimal",
        opportunity=opportunity,
        snapshots=snapshots,
        now_ns=now_ns,
    )
    print("accepted", result.accepted, "package", result.package_id)

    # Simulate venue fills and drive the event loop tick.
    for submitted in result.submitted_legs:
        venue.enqueue_fill(
            client_order_id=submitted.client_order_id,
            fill_qty=submitted.quantity,
            fill_price_ticks=submitted.limit_price_ticks,
            ts_ns=now_ns + 10,
        )

    service.on_timer_tick(now_ns=now_ns + 11)
    print("snapshot", service.snapshot())

    service.stop()
    writer.close()


if __name__ == "__main__":
    main()
