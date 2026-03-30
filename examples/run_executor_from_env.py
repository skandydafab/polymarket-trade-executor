from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
import csv
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import sys
import time
from typing import Any, Mapping
from urllib.parse import urlencode
from urllib.request import Request, urlopen


_BPS_SCALE = 10_000


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from executor import (
    ExecutorJournal,
    ExecutorService,
    ExecutorServiceConfig,
    ExecutionRiskManager,
    FakeVenueAdapter,
    JournalReplayLoader,
    JournalWriter,
    JournalWriterConfig,
    JSONLFileJournalStorage,
    Opportunity,
    OpportunityLeg,
    PlannerConfig,
    PricingSnapshot,
    PolymarketAdapterConfig,
    PolymarketVenueAdapter,
    RecoveryCoordinator,
    RiskManagerConfig,
    Side,
)
from executor.planner import ExecutionPlanner
from executor.polymarket_clob_client import PolymarketCLOBHttpClient, PolymarketCLOBHttpClientConfig
from executor.polymarket_sdk_signer import PyClobClientOrderSigner, PyClobClientOrderSignerConfig
from executor.slug_structural_arb import (
    SlugMarket,
    SlugResolutionConfig,
    SlugSubscriptionPlan,
    StructuralArbCandidate,
    build_slug_pairs,
    build_structural_arb_candidates,
    fetch_active_markets_for_slugs,
    parse_slug_pairs,
)


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


@dataclass(frozen=True, slots=True)
class SubscriptionScope:
    market_ids: tuple[str, ...]
    token_ids: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class _OutcomeQuote:
    market_id: str
    token_id: str
    price_cents: int
    outcome: str


@dataclass(frozen=True, slots=True)
class _ObservedOpportunity:
    window_key: str
    relation_id: str
    observed_ts_ns: int
    payload: dict[str, object]
    opportunity: Opportunity
    snapshots: dict[str, PricingSnapshot]


@dataclass(frozen=True, slots=True)
class _BuildOpportunityResult:
    opportunity: Opportunity | None
    snapshots: dict[str, PricingSnapshot]
    theoretical_edge_bps: int | None
    reason_code: str | None = None
    reason: str | None = None
    context: dict[str, object] | None = None

    @property
    def success(self) -> bool:
        return self.opportunity is not None


@dataclass(frozen=True, slots=True)
class _ExecutabilityCheckResult:
    executable: bool
    reason_code: str | None
    reason: str | None
    leg_checks: tuple[dict[str, object], ...]
    executable_edge_bps: int | None


@dataclass(frozen=True, slots=True)
class _RejectedOpportunity:
    observed_ts_ns: int
    relation_id: str
    window_key: str
    candidate_market_ids: tuple[str, ...]
    payload: dict[str, object]


@dataclass(slots=True)
class _OpportunityWindowState:
    window_key: str
    relation_id: str
    first_seen_ns: int
    last_seen_ns: int
    ticks_seen: int
    peak_edge_bps: int
    peak_profit_cents: int
    peak_package_units: int
    peak_payload: dict[str, object]
    last_payload: dict[str, object]


class _ArbResearchLogger:
    __slots__ = (
        "_jsonl_handle",
        "_csv_handle",
        "_csv_writer",
        "_active_windows",
    )

    def __init__(self, *, jsonl_path: Path, csv_path: Path) -> None:
        jsonl_path.parent.mkdir(parents=True, exist_ok=True)
        csv_path.parent.mkdir(parents=True, exist_ok=True)

        self._jsonl_handle = jsonl_path.open("a", encoding="utf-8")
        write_header = not csv_path.exists() or csv_path.stat().st_size == 0
        self._csv_handle = csv_path.open("a", encoding="utf-8", newline="")
        self._csv_writer = csv.DictWriter(
            self._csv_handle,
            fieldnames=(
                "window_key",
                "relation_id",
                "asset",
                "slug_left",
                "slug_right",
                "boundary_relation",
                "strike_low_x100",
                "strike_high_x100",
                "first_seen_utc",
                "last_seen_utc",
                "duration_ms",
                "ticks_seen",
                "peak_edge_bps",
                "peak_profit_cents",
                "peak_package_units",
                "close_reason",
                "price_source",
            ),
        )
        if write_header:
            self._csv_writer.writeheader()
            self._csv_handle.flush()

        self._active_windows: dict[str, _OpportunityWindowState] = {}

    @property
    def active_count(self) -> int:
        return len(self._active_windows)

    def observe(
        self,
        observations: list[_ObservedOpportunity],
        *,
        observed_ts_ns: int,
    ) -> tuple[list[_ObservedOpportunity], int]:
        seen_keys: set[str] = set()
        newly_opened: list[_ObservedOpportunity] = []

        for item in observations:
            seen_keys.add(item.window_key)
            state = self._active_windows.get(item.window_key)
            if state is None:
                payload = dict(item.payload)
                peak_edge = int(payload.get("expected_edge_bps") or 0)
                peak_profit = int(payload.get("max_fillable_profit_cents") or 0)
                peak_units = int(payload.get("max_fillable_units") or 0)
                self._active_windows[item.window_key] = _OpportunityWindowState(
                    window_key=item.window_key,
                    relation_id=item.relation_id,
                    first_seen_ns=item.observed_ts_ns,
                    last_seen_ns=item.observed_ts_ns,
                    ticks_seen=1,
                    peak_edge_bps=peak_edge,
                    peak_profit_cents=peak_profit,
                    peak_package_units=peak_units,
                    peak_payload=payload,
                    last_payload=payload,
                )
                newly_opened.append(item)
                self._write_json_event("opened", payload)
                continue

            state.last_seen_ns = item.observed_ts_ns
            state.ticks_seen += 1
            state.last_payload = dict(item.payload)

            edge = int(item.payload.get("expected_edge_bps") or 0)
            profit = int(item.payload.get("max_fillable_profit_cents") or 0)
            if profit > state.peak_profit_cents or (profit == state.peak_profit_cents and edge > state.peak_edge_bps):
                state.peak_profit_cents = profit
                state.peak_edge_bps = edge
                state.peak_package_units = int(item.payload.get("max_fillable_units") or 0)
                state.peak_payload = dict(item.payload)

            self._write_json_event("update", state.last_payload)

        closed_count = 0
        for window_key in tuple(self._active_windows.keys()):
            if window_key in seen_keys:
                continue
            self._close_window(
                window_key,
                close_reason="price_changed_or_missing",
                closed_ts_ns=observed_ts_ns,
            )
            closed_count += 1

        return newly_opened, closed_count

    def close_all(self, *, close_reason: str, closed_ts_ns: int) -> None:
        for window_key in tuple(self._active_windows.keys()):
            self._close_window(window_key, close_reason=close_reason, closed_ts_ns=closed_ts_ns)

    def close(self) -> None:
        self._jsonl_handle.close()
        self._csv_handle.close()

    def _close_window(self, window_key: str, *, close_reason: str, closed_ts_ns: int) -> None:
        state = self._active_windows.pop(window_key, None)
        if state is None:
            return

        duration_ms = max(0, (state.last_seen_ns - state.first_seen_ns) // 1_000_000)
        peak_payload = dict(state.peak_payload)

        summary = {
            "window_key": state.window_key,
            "relation_id": state.relation_id,
            "asset": peak_payload.get("asset"),
            "slug_left": peak_payload.get("slug_left"),
            "slug_right": peak_payload.get("slug_right"),
            "boundary_relation": peak_payload.get("boundary_relation"),
            "strike_low_x100": peak_payload.get("strike_low_x100"),
            "strike_high_x100": peak_payload.get("strike_high_x100"),
            "first_seen_utc": _ns_to_utc_iso(state.first_seen_ns),
            "last_seen_utc": _ns_to_utc_iso(state.last_seen_ns),
            "duration_ms": duration_ms,
            "ticks_seen": state.ticks_seen,
            "peak_edge_bps": state.peak_edge_bps,
            "peak_profit_cents": state.peak_profit_cents,
            "peak_package_units": state.peak_package_units,
            "close_reason": close_reason,
            "price_source": peak_payload.get("price_source"),
            "closed_utc": _ns_to_utc_iso(closed_ts_ns),
        }

        self._write_json_event("closed", summary)
        self._csv_writer.writerow({key: summary.get(key) for key in self._csv_writer.fieldnames})
        self._csv_handle.flush()

    def _write_json_event(self, event_type: str, payload: Mapping[str, object]) -> None:
        record = {
            "event_type": event_type,
            **dict(payload),
        }
        self._jsonl_handle.write(json.dumps(record, separators=(",", ":"), ensure_ascii=True))
        self._jsonl_handle.write("\n")
        self._jsonl_handle.flush()


class _ArbRejectionLogger:
    __slots__ = ("_jsonl_handle",)

    def __init__(self, *, jsonl_path: Path) -> None:
        jsonl_path.parent.mkdir(parents=True, exist_ok=True)
        self._jsonl_handle = jsonl_path.open("a", encoding="utf-8")

    def write_batch(self, rejections: list[_RejectedOpportunity]) -> None:
        if not rejections:
            return
        for item in rejections:
            record = {
                "event_type": "rejected",
                "observed_ts_ns": item.observed_ts_ns,
                "observed_utc": _ns_to_utc_iso(item.observed_ts_ns),
                "relation_id": item.relation_id,
                "window_key": item.window_key,
                "candidate_market_ids": item.candidate_market_ids,
                **dict(item.payload),
            }
            self._jsonl_handle.write(json.dumps(record, separators=(",", ":"), ensure_ascii=True))
            self._jsonl_handle.write("\n")
        self._jsonl_handle.flush()

    def close(self) -> None:
        self._jsonl_handle.close()


def main() -> None:
    project_root = PROJECT_ROOT
    _load_dotenv_file(project_root / ".env")

    live_enabled = _env_bool("EXECUTOR_LIVE_TRADING_ENABLED", False)
    run_recovery_on_start = _env_bool("EXECUTOR_RUN_RECOVERY_ON_START", False)
    subscription_scope = _read_subscription_scope_from_env()
    slug_plan = _read_slug_subscription_plan_from_env()

    logger = PrintLogger()
    planner = ExecutionPlanner(PlannerConfig(default_max_slippage_bps=10_000))
    risk_manager = ExecutionRiskManager(RiskManagerConfig())

    adapter_config = _build_adapter_config()
    executor_config = _build_executor_config()
    venue_adapter = _build_venue_adapter(live_enabled=live_enabled, adapter_config=adapter_config)

    journal_storage = JSONLFileJournalStorage(_resolve_journal_dir(project_root))
    journal_writer = JournalWriter(journal_storage, config=JournalWriterConfig())
    journal = ExecutorJournal(journal_writer)
    recovery = RecoveryCoordinator(loader=JournalReplayLoader(journal_storage))

    service = ExecutorService(
        planner=planner,
        risk_manager=risk_manager,
        venue_adapter=venue_adapter,
        journal=journal,
        recovery_coordinator=recovery,
        logger=logger,
        config=executor_config,
    )

    now_ns = time.time_ns()
    try:
        service.start()
        print("mode", "live" if live_enabled else "paper")
        if subscription_scope.market_ids or subscription_scope.token_ids:
            print(
                "subscription_scope",
                {
                    "market_ids": subscription_scope.market_ids,
                    "token_ids": subscription_scope.token_ids,
                },
            )
        else:
            print("subscription_scope", "not configured")

        if slug_plan.slugs:
            _process_slug_based_structural_arb(
                service=service,
                planner=planner,
                slug_plan=slug_plan,
                project_root=project_root,
                now_ns=now_ns,
            )
        else:
            print("slug_subscription", "not configured")

        print("snapshot", service.snapshot())

        if run_recovery_on_start:
            result = service.recover(now_ns=now_ns)
            print("recovery_records_loaded", result.records_loaded)
            print("recovery_actions", len(result.actions))
            print("snapshot_after_recovery", service.snapshot())
    finally:
        service.stop()
        journal_writer.close()


def _build_adapter_config() -> PolymarketAdapterConfig:
    return PolymarketAdapterConfig(
        api_url=os.environ.get("POLYMARKET_API_URL", "https://clob.polymarket.com"),
        private_key=_env_optional("POLYMARKET_PRIVATE_KEY"),
        api_key=_env_optional("POLYMARKET_API_KEY"),
        api_secret=_env_optional("POLYMARKET_API_SECRET"),
        passphrase=_env_optional("POLYMARKET_PASSPHRASE"),
        submit_timeout_ms=_env_int("POLYMARKET_SUBMIT_TIMEOUT_MS", 500),
        cancel_timeout_ms=_env_int("POLYMARKET_CANCEL_TIMEOUT_MS", 500),
        poll_timeout_ms=_env_int("POLYMARKET_POLL_TIMEOUT_MS", 700),
        poll_batch_limit=_env_int("POLYMARKET_POLL_BATCH_LIMIT", 200),
        client_order_id_prefix=os.environ.get("POLYMARKET_CLIENT_ORDER_ID_PREFIX", "pmx"),
        client_order_id_max_length=_env_int("POLYMARKET_CLIENT_ORDER_ID_MAX_LENGTH", 96),
    )


def _build_executor_config() -> ExecutorServiceConfig:
    return ExecutorServiceConfig(
        service_name=os.environ.get("EXECUTOR_SERVICE_NAME", "executor"),
        client_order_id_prefix=os.environ.get("EXECUTOR_CLIENT_ORDER_ID_PREFIX", "exec"),
        client_order_id_max_length=_env_int("EXECUTOR_CLIENT_ORDER_ID_MAX_LENGTH", 96),
        default_leg_timeout_ms=_env_int("EXECUTOR_DEFAULT_LEG_TIMEOUT_MS", 800),
        poll_venue_updates_on_tick=_env_bool("EXECUTOR_POLL_VENUE_UPDATES_ON_TICK", True),
        max_control_actions_per_tick=_env_int("EXECUTOR_MAX_CONTROL_ACTIONS_PER_TICK", 128),
        max_control_queue_size=_env_int("EXECUTOR_MAX_CONTROL_QUEUE_SIZE", 10_000),
        auto_cancel_on_timeout=_env_bool("EXECUTOR_AUTO_CANCEL_ON_TIMEOUT", True),
        auto_cancel_on_abort=_env_bool("EXECUTOR_AUTO_CANCEL_ON_ABORT", True),
        halt_after_recovery_with_active_packages=_env_bool(
            "EXECUTOR_HALT_AFTER_RECOVERY_WITH_ACTIVE_PACKAGES",
            True,
        ),
        journal_errors_halt_trading=_env_bool("EXECUTOR_JOURNAL_ERRORS_HALT_TRADING", False),
    )


def _build_venue_adapter(*, live_enabled: bool, adapter_config: PolymarketAdapterConfig):
    if not live_enabled:
        return FakeVenueAdapter(client_order_prefix=adapter_config.client_order_id_prefix)

    clob_client_config = PolymarketCLOBHttpClientConfig(
        api_url=adapter_config.api_url,
        private_key=adapter_config.private_key,
        api_key=adapter_config.api_key,
        api_secret=adapter_config.api_secret,
        passphrase=adapter_config.passphrase,
        request_timeout_ms=_env_int("POLYMARKET_HTTP_REQUEST_TIMEOUT_MS", 1_500),
        updates_max_retries=_env_int("POLYMARKET_UPDATES_MAX_RETRIES", 3),
        updates_retry_base_ms=_env_int("POLYMARKET_UPDATES_RETRY_BASE_MS", 100),
        submit_path=os.environ.get("POLYMARKET_SUBMIT_PATH", "/order"),
        cancel_path=os.environ.get("POLYMARKET_CANCEL_PATH", "/cancel"),
        updates_path=os.environ.get("POLYMARKET_UPDATES_PATH", "/orders/updates"),
        open_orders_path=os.environ.get("POLYMARKET_OPEN_ORDERS_PATH", "/orders/open"),
    )

    signer = _build_optional_signer(adapter_config)
    client = PolymarketCLOBHttpClient(clob_client_config, signer=signer)
    return PolymarketVenueAdapter(client=client, config=adapter_config)


def _build_optional_signer(adapter_config: PolymarketAdapterConfig):
    use_sdk_signer = _env_bool("POLYMARKET_USE_SDK_SIGNER", True)
    if not use_sdk_signer:
        return None

    private_key = adapter_config.private_key
    if not private_key:
        return None

    signer_config = PyClobClientOrderSignerConfig(
        api_url=adapter_config.api_url,
        chain_id=_env_int("POLYMARKET_CHAIN_ID", 137),
        signature_type=_env_optional_int("POLYMARKET_SIGNATURE_TYPE"),
        funder=_env_optional("POLYMARKET_FUNDER"),
        maker=_env_optional("POLYMARKET_MAKER"),
    )
    return PyClobClientOrderSigner(signer_config)


def _read_subscription_scope_from_env() -> SubscriptionScope:
    return SubscriptionScope(
        market_ids=_env_csv("POLYMARKET_SUBSCRIBE_MARKET_IDS"),
        token_ids=_env_csv("POLYMARKET_SUBSCRIBE_TOKEN_IDS"),
    )


def _read_slug_subscription_plan_from_env() -> SlugSubscriptionPlan:
    return SlugSubscriptionPlan(
        slugs=_env_csv("POLYMARKET_SUBSCRIBE_SLUGS"),
        explicit_pairs=parse_slug_pairs(_env_optional("POLYMARKET_SUBSCRIBE_SLUG_PAIRS")),
        auto_pair=_env_bool("POLYMARKET_AUTO_PAIR_SLUGS", True),
        match_mode=_env_optional("POLYMARKET_STRUCT_ARB_MATCH_MODE") or "strict",
    )


def _process_slug_based_structural_arb(
    *,
    service: ExecutorService,
    planner: ExecutionPlanner,
    slug_plan: SlugSubscriptionPlan,
    project_root: Path,
    now_ns: int,
) -> None:
    resolution_config = SlugResolutionConfig(
        gamma_api_url=os.environ.get("POLYMARKET_GAMMA_API_URL", "https://gamma-api.polymarket.com"),
        timeout_ms=_env_int("POLYMARKET_GAMMA_TIMEOUT_MS", 5000),
        include_only_active_tradable=_env_bool("POLYMARKET_SUBSCRIBE_ONLY_ACTIVE_TRADABLE", True),
        max_concurrency=max(1, _env_int("POLYMARKET_GAMMA_MAX_CONCURRENCY", 8)),
    )

    slug_pairs = build_slug_pairs(slug_plan)
    if not slug_pairs:
        print("slug_pairs", "none")
        return

    min_similarity = _env_float("POLYMARKET_STRUCT_ARB_MIN_SIMILARITY", 0.40)
    max_candidates = _env_int("POLYMARKET_STRUCT_ARB_MAX_CANDIDATES", 200)
    min_edge_bps = _env_int("POLYMARKET_STRUCT_ARB_MIN_EDGE_BPS", 1)
    monitor_interval_ms = max(50, _env_int("POLYMARKET_STRUCT_ARB_MONITOR_INTERVAL_MS", 250))
    monitor_duration_seconds = _env_float("POLYMARKET_STRUCT_ARB_MONITOR_DURATION_SECONDS", 300.0)

    use_public_orderbook = _env_bool("POLYMARKET_STRUCT_ARB_USE_PUBLIC_ORDERBOOK", True)
    public_book_timeout_ms = _env_int("POLYMARKET_PUBLIC_BOOK_TIMEOUT_MS", 1200)
    public_book_max_concurrency = max(1, _env_int("POLYMARKET_PUBLIC_BOOK_MAX_CONCURRENCY", 8))
    public_book_api_url = os.environ.get("POLYMARKET_API_URL", "https://clob.polymarket.com")

    execution_mode = (os.environ.get("POLYMARKET_STRUCT_ARB_EXECUTION_MODE", "emit") or "emit").strip().lower()

    jsonl_log_path = _resolve_output_path(
        project_root,
        "POLYMARKET_STRUCT_ARB_LOG_JSONL_PATH",
        "./data/research/structural_arb_events.jsonl",
    )
    csv_log_path = _resolve_output_path(
        project_root,
        "POLYMARKET_STRUCT_ARB_LOG_CSV_PATH",
        "./data/research/structural_arb_windows.csv",
    )
    rejection_log_enabled = _env_bool("POLYMARKET_STRUCT_ARB_LOG_REJECTIONS", True)
    rejection_jsonl_path = (
        _resolve_output_path(
            project_root,
            "POLYMARKET_STRUCT_ARB_LOG_REJECTIONS_JSONL_PATH",
            "./data/research/structural_arb_rejections.jsonl",
        )
        if rejection_log_enabled
        else None
    )

    research_logger = _ArbResearchLogger(jsonl_path=jsonl_log_path, csv_path=csv_log_path)
    rejection_logger = (
        _ArbRejectionLogger(jsonl_path=rejection_jsonl_path)
        if rejection_jsonl_path is not None
        else None
    )

    print("slug_pairs", slug_pairs)
    print(
        "structural_arb_monitor",
        {
            "interval_ms": monitor_interval_ms,
            "duration_seconds": monitor_duration_seconds,
            "min_edge_bps": min_edge_bps,
            "execution_mode": execution_mode,
            "use_public_orderbook": use_public_orderbook,
            "public_book_max_concurrency": public_book_max_concurrency,
            "log_jsonl": str(jsonl_log_path),
            "log_csv": str(csv_log_path),
            "log_rejections": rejection_log_enabled,
            "log_rejections_jsonl": str(rejection_jsonl_path) if rejection_jsonl_path is not None else None,
        },
    )

    started_at = time.monotonic()
    poll_count = 0
    execute_accepted = 0
    execute_rejected = 0
    execute_skipped = 0
    cumulative_rejected_by_class: dict[str, int] = {}
    cumulative_rejected_by_reason: dict[str, int] = {}
    cumulative_executable_candidates = 0

    try:
        while True:
            poll_count += 1
            poll_ts_ns = time.time_ns()

            try:
                markets_by_slug = fetch_active_markets_for_slugs(slug_plan.slugs, resolution_config)
            except Exception as exc:
                print("slug_resolution_error", str(exc))
                research_logger.close_all(close_reason="data_fetch_error", closed_ts_ns=poll_ts_ns)
                if _should_stop_monitoring(started_at=started_at, duration_seconds=monitor_duration_seconds):
                    break
                time.sleep(monitor_interval_ms / 1000.0)
                continue

            market_counts = {slug: len(markets) for slug, markets in markets_by_slug.items()}
            candidates = build_structural_arb_candidates(
                slug_pairs,
                markets_by_slug,
                match_mode=slug_plan.match_mode,
                min_similarity=min_similarity,
            )
            candidates = candidates[:max_candidates]

            observations, rejections = _collect_observed_opportunities(
                candidates=candidates,
                planner=planner,
                poll_ts_ns=poll_ts_ns,
                min_edge_bps=min_edge_bps,
                use_public_orderbook=use_public_orderbook,
                public_book_api_url=public_book_api_url,
                public_book_timeout_ms=public_book_timeout_ms,
                public_book_max_concurrency=public_book_max_concurrency,
            )

            if rejection_logger is not None:
                rejection_logger.write_batch(rejections)

            poll_rejected_by_class, poll_rejected_by_reason = _summarize_rejections(rejections)
            poll_executable_candidates = len(observations) + poll_rejected_by_class.get("PRICE_PROTECTION", 0)
            cumulative_executable_candidates += poll_executable_candidates
            for key, value in poll_rejected_by_class.items():
                cumulative_rejected_by_class[key] = cumulative_rejected_by_class.get(key, 0) + value
            for key, value in poll_rejected_by_reason.items():
                cumulative_rejected_by_reason[key] = cumulative_rejected_by_reason.get(key, 0) + value

            opened, closed = research_logger.observe(observations, observed_ts_ns=poll_ts_ns)

            if execution_mode == "execute":
                accepted, rejected, skipped = _execute_opened_observations(
                    service=service,
                    observations=opened,
                    now_ns=poll_ts_ns,
                )
                execute_accepted += accepted
                execute_rejected += rejected
                execute_skipped += skipped

            print(
                "structural_arb_poll",
                {
                    "poll": poll_count,
                    "market_counts": market_counts,
                    "candidate_count": len(candidates),
                    "observed_count": len(observations),
                    "rejected": len(rejections),
                    "executable_candidates": poll_executable_candidates,
                    "rejected_by_class": poll_rejected_by_class,
                    "rejected_by_reason": poll_rejected_by_reason,
                    "opened": len(opened),
                    "closed": closed,
                    "active_windows": research_logger.active_count,
                    "cumulative_executable_candidates": cumulative_executable_candidates,
                    "cumulative_rejected_by_class": cumulative_rejected_by_class,
                },
            )

            if poll_count == 1 and candidates:
                preview = [
                    {
                        "left_slug": item.slug_left,
                        "right_slug": item.slug_right,
                        "left_market_id": item.left_market.market_id,
                        "right_market_id": item.right_market.market_id,
                        "similarity": round(item.similarity, 3),
                        "match_mode": item.match_mode,
                        "boundary_relation": item.boundary_relation,
                        "range_market_id": item.range_market.market_id if item.range_market is not None else None,
                        "lower_strike_market_id": (
                            item.lower_strike_market.market_id if item.lower_strike_market is not None else None
                        ),
                        "upper_strike_market_id": (
                            item.upper_strike_market.market_id if item.upper_strike_market is not None else None
                        ),
                        "strike_low": item.strike_low,
                        "strike_high": item.strike_high,
                    }
                    for item in candidates[:5]
                ]
                print("structural_arb_preview", preview)

            if _should_stop_monitoring(started_at=started_at, duration_seconds=monitor_duration_seconds):
                break

            time.sleep(monitor_interval_ms / 1000.0)
    except KeyboardInterrupt:
        print("structural_arb_monitor", "interrupted")
    finally:
        research_logger.close_all(close_reason="shutdown", closed_ts_ns=time.time_ns())
        research_logger.close()
        if rejection_logger is not None:
            rejection_logger.close()
        print(
            "structural_arb_logs",
            {
                "jsonl": str(jsonl_log_path),
                "csv": str(csv_log_path),
                "rejections_jsonl": str(rejection_jsonl_path) if rejection_jsonl_path is not None else None,
            },
        )
        if execution_mode == "execute":
            print(
                "structural_arb_execution",
                {
                    "mode": "execute",
                    "accepted": execute_accepted,
                    "rejected": execute_rejected,
                    "skipped": execute_skipped,
                },
            )
        else:
            print("structural_arb_execution", "emit")


def _collect_observed_opportunities(
    *,
    candidates: tuple[StructuralArbCandidate, ...],
    planner: ExecutionPlanner,
    poll_ts_ns: int,
    min_edge_bps: int,
    use_public_orderbook: bool,
    public_book_api_url: str,
    public_book_timeout_ms: int,
    public_book_max_concurrency: int,
) -> tuple[list[_ObservedOpportunity], list[_RejectedOpportunity]]:
    observations: list[_ObservedOpportunity] = []
    rejections: list[_RejectedOpportunity] = []
    orderbook_cache: dict[str, tuple[int, int, int, int] | None] = {}

    for index, candidate in enumerate(candidates, start=1):
        observed_ts_ns = poll_ts_ns + index
        relation_id = _build_relation_id(candidate)

        build_result = _candidate_to_opportunity(candidate, now_ns=observed_ts_ns)
        if not build_result.success:
            rejections.append(
                _build_rejected_opportunity(
                    candidate=candidate,
                    relation_id=relation_id,
                    observed_ts_ns=observed_ts_ns,
                    reason_code=build_result.reason_code or "BUILD_FAILED",
                    reason=build_result.reason or "candidate build failed",
                    diagnostic_class="BUILD_FAILURE",
                    theoretical_edge_bps=build_result.theoretical_edge_bps,
                    context=build_result.context,
                )
            )
            continue

        opportunity = build_result.opportunity
        if opportunity is None:
            rejections.append(
                _build_rejected_opportunity(
                    candidate=candidate,
                    relation_id=relation_id,
                    observed_ts_ns=observed_ts_ns,
                    reason_code="BUILD_FAILED",
                    reason="build_result had no opportunity",
                    diagnostic_class="BUILD_FAILURE",
                    theoretical_edge_bps=build_result.theoretical_edge_bps,
                    context=build_result.context,
                )
            )
            continue

        snapshots = dict(build_result.snapshots)

        snapshots, book_hits = _apply_public_orderbook_snapshots(
            opportunity=opportunity,
            fallback_snapshots=snapshots,
            now_ns=observed_ts_ns,
            use_public_orderbook=use_public_orderbook,
            public_book_api_url=public_book_api_url,
            public_book_timeout_ms=public_book_timeout_ms,
            public_book_max_concurrency=public_book_max_concurrency,
            orderbook_cache=orderbook_cache,
        )

        price_source = (
            "public_orderbook"
            if book_hits == len(opportunity.legs)
            else "mixed"
            if book_hits > 0
            else "gamma_fallback"
        )

        book_validation = _validate_top_of_book(opportunity=opportunity, snapshots=snapshots)
        if not book_validation.executable:
            rejections.append(
                _build_rejected_opportunity(
                    candidate=candidate,
                    relation_id=relation_id,
                    observed_ts_ns=observed_ts_ns,
                    reason_code=book_validation.reason_code or "INVALID_BOOK",
                    reason=book_validation.reason or "invalid top-of-book",
                    diagnostic_class="INVALID_BOOK",
                    theoretical_edge_bps=build_result.theoretical_edge_bps,
                    executable_edge_bps=book_validation.executable_edge_bps,
                    opportunity=opportunity,
                    price_source=price_source,
                    leg_checks=book_validation.leg_checks,
                    context=build_result.context,
                )
            )
            continue

        price_validation = _validate_price_protection(
            opportunity=opportunity,
            snapshots=snapshots,
            planner_config=planner.config,
        )
        if not price_validation.executable:
            rejections.append(
                _build_rejected_opportunity(
                    candidate=candidate,
                    relation_id=relation_id,
                    observed_ts_ns=observed_ts_ns,
                    reason_code=price_validation.reason_code or "PRICE_PROTECTION_FAIL",
                    reason=price_validation.reason or "price protection failed",
                    diagnostic_class="PRICE_PROTECTION",
                    theoretical_edge_bps=build_result.theoretical_edge_bps,
                    executable_edge_bps=price_validation.executable_edge_bps,
                    opportunity=opportunity,
                    price_source=price_source,
                    leg_checks=price_validation.leg_checks,
                    context=build_result.context,
                )
            )
            continue

        decision = planner.plan(opportunity, snapshots, now_ns=observed_ts_ns)
        if not decision.accepted:
            rejection = decision.rejection
            reason_code = (
                rejection.code.value
                if rejection is not None
                else "PLANNER_REJECTED"
            )
            reason = (
                rejection.reason
                if rejection is not None
                else "planner rejected without reason"
            )
            rejections.append(
                _build_rejected_opportunity(
                    candidate=candidate,
                    relation_id=relation_id,
                    observed_ts_ns=observed_ts_ns,
                    reason_code=reason_code,
                    reason=reason,
                    diagnostic_class=_diagnostic_class_from_reason_code(reason_code),
                    rejection_leg_id=rejection.leg_id if rejection is not None else None,
                    theoretical_edge_bps=build_result.theoretical_edge_bps,
                    executable_edge_bps=price_validation.executable_edge_bps,
                    opportunity=opportunity,
                    price_source=price_source,
                    leg_checks=price_validation.leg_checks,
                    context=build_result.context,
                )
            )
            continue

        plan = decision.plan
        if plan is None:
            rejections.append(
                _build_rejected_opportunity(
                    candidate=candidate,
                    relation_id=relation_id,
                    observed_ts_ns=observed_ts_ns,
                    reason_code="PLAN_NONE",
                    reason="planner accepted with no plan",
                    diagnostic_class="BUILD_FAILURE",
                    theoretical_edge_bps=build_result.theoretical_edge_bps,
                    executable_edge_bps=price_validation.executable_edge_bps,
                    opportunity=opportunity,
                    price_source=price_source,
                    leg_checks=price_validation.leg_checks,
                    context=build_result.context,
                )
            )
            continue
        if plan.expected_net_edge_bps < min_edge_bps:
            rejections.append(
                _build_rejected_opportunity(
                    candidate=candidate,
                    relation_id=relation_id,
                    observed_ts_ns=observed_ts_ns,
                    reason_code="EDGE_BELOW_MONITOR_MIN",
                    reason=(
                        f"net_edge_bps={plan.expected_net_edge_bps} below monitor_min={min_edge_bps}"
                    ),
                    diagnostic_class="PRICE_PROTECTION",
                    theoretical_edge_bps=build_result.theoretical_edge_bps,
                    executable_edge_bps=plan.expected_net_edge_bps,
                    opportunity=opportunity,
                    price_source=price_source,
                    leg_checks=price_validation.leg_checks,
                    context=build_result.context,
                )
            )
            continue

        window_key = _build_window_key(relation_id, opportunity)
        payload = _build_observation_payload(
            candidate=candidate,
            relation_id=relation_id,
            window_key=window_key,
            observed_ts_ns=observed_ts_ns,
            opportunity=opportunity,
            snapshots=snapshots,
            price_source=price_source,
            max_fillable_units=plan.package_units,
            max_fillable_profit_cents=plan.expected_net_profit_cents,
            theoretical_edge_bps=build_result.theoretical_edge_bps,
            executable_edge_bps=plan.expected_net_edge_bps,
            expected_gross_profit_cents=plan.expected_gross_profit_cents,
            expected_fee_cents=plan.expected_fee_cents,
            total_notional_cents=plan.total_notional_cents,
            leg_checks=price_validation.leg_checks,
        )

        observations.append(
            _ObservedOpportunity(
                window_key=window_key,
                relation_id=relation_id,
                observed_ts_ns=observed_ts_ns,
                payload=payload,
                opportunity=opportunity,
                snapshots=snapshots,
            )
        )

    return observations, rejections


def _validate_top_of_book(
    *,
    opportunity: Opportunity,
    snapshots: Mapping[str, PricingSnapshot],
) -> _ExecutabilityCheckResult:
    leg_checks: list[dict[str, object]] = []

    for leg in opportunity.legs:
        snapshot = snapshots.get(leg.market_id)
        if snapshot is None:
            leg_checks.append(
                {
                    "leg_id": leg.leg_id,
                    "market_id": leg.market_id,
                    "side": leg.side.value,
                    "token_id": leg.token_id,
                    "best_bid_ticks": None,
                    "best_ask_ticks": None,
                    "limit_price_ticks": None,
                }
            )
            return _ExecutabilityCheckResult(
                executable=False,
                reason_code="MISSING_BOOK",
                reason=f"missing snapshot for leg_id={leg.leg_id}",
                leg_checks=tuple(leg_checks),
                executable_edge_bps=None,
            )

        check = {
            "leg_id": leg.leg_id,
            "market_id": leg.market_id,
            "side": leg.side.value,
            "token_id": leg.token_id,
            "best_bid_ticks": snapshot.best_bid_ticks,
            "best_ask_ticks": snapshot.best_ask_ticks,
            "limit_price_ticks": None,
        }
        leg_checks.append(check)

        if snapshot.best_bid_ticks <= 0 or snapshot.best_ask_ticks <= 0:
            return _ExecutabilityCheckResult(
                executable=False,
                reason_code="NON_POSITIVE_BOOK",
                reason=(
                    f"non-positive top-of-book for leg_id={leg.leg_id}, "
                    f"bid={snapshot.best_bid_ticks}, ask={snapshot.best_ask_ticks}"
                ),
                leg_checks=tuple(leg_checks),
                executable_edge_bps=None,
            )

        if snapshot.best_bid_ticks >= snapshot.best_ask_ticks:
            return _ExecutabilityCheckResult(
                executable=False,
                reason_code="LOCKED_BOOK",
                reason=(
                    f"locked/crossed top-of-book for leg_id={leg.leg_id}, "
                    f"bid={snapshot.best_bid_ticks}, ask={snapshot.best_ask_ticks}"
                ),
                leg_checks=tuple(leg_checks),
                executable_edge_bps=None,
            )

    executable_edge_bps = _compute_executable_edge_bps(opportunity=opportunity, snapshots=snapshots)
    return _ExecutabilityCheckResult(
        executable=True,
        reason_code=None,
        reason=None,
        leg_checks=tuple(leg_checks),
        executable_edge_bps=executable_edge_bps,
    )


def _validate_price_protection(
    *,
    opportunity: Opportunity,
    snapshots: Mapping[str, PricingSnapshot],
    planner_config: PlannerConfig,
) -> _ExecutabilityCheckResult:
    leg_checks: list[dict[str, object]] = []

    for leg in opportunity.legs:
        snapshot = snapshots.get(leg.market_id)
        if snapshot is None:
            return _ExecutabilityCheckResult(
                executable=False,
                reason_code="MISSING_BOOK",
                reason=f"missing snapshot for leg_id={leg.leg_id}",
                leg_checks=tuple(leg_checks),
                executable_edge_bps=None,
            )

        executable_price_ticks = snapshot.best_ask_ticks if leg.side == Side.BUY else snapshot.best_bid_ticks
        limit_price = _compute_price_protection_limit(
            leg=leg,
            snapshot=snapshot,
            planner_config=planner_config,
        )

        check = {
            "leg_id": leg.leg_id,
            "market_id": leg.market_id,
            "side": leg.side.value,
            "token_id": leg.token_id,
            "best_bid_ticks": snapshot.best_bid_ticks,
            "best_ask_ticks": snapshot.best_ask_ticks,
            "limit_price_ticks": limit_price,
        }
        leg_checks.append(check)

        if leg.side == Side.BUY and snapshot.best_ask_ticks > limit_price:
            return _ExecutabilityCheckResult(
                executable=False,
                reason_code="PRICE_PROTECTION_FAIL",
                reason=(
                    f"buy protection failed for leg={leg.leg_id}, "
                    f"limit={limit_price}, ask={snapshot.best_ask_ticks}"
                ),
                leg_checks=tuple(leg_checks),
                executable_edge_bps=_compute_executable_edge_bps(opportunity=opportunity, snapshots=snapshots),
            )

        if leg.side == Side.SELL and snapshot.best_bid_ticks < limit_price:
            return _ExecutabilityCheckResult(
                executable=False,
                reason_code="PRICE_PROTECTION_FAIL",
                reason=(
                    f"sell protection failed for leg={leg.leg_id}, "
                    f"limit={limit_price}, bid={snapshot.best_bid_ticks}"
                ),
                leg_checks=tuple(leg_checks),
                executable_edge_bps=_compute_executable_edge_bps(opportunity=opportunity, snapshots=snapshots),
            )

    return _ExecutabilityCheckResult(
        executable=True,
        reason_code=None,
        reason=None,
        leg_checks=tuple(leg_checks),
        executable_edge_bps=_compute_executable_edge_bps(opportunity=opportunity, snapshots=snapshots),
    )


def _compute_price_protection_limit(
    *,
    leg: OpportunityLeg,
    snapshot: PricingSnapshot,
    planner_config: PlannerConfig,
) -> int:
    slip_bps = leg.max_slippage_bps if leg.max_slippage_bps is not None else planner_config.default_max_slippage_bps

    if leg.side == Side.BUY:
        executable_price_ticks = snapshot.best_ask_ticks
        aggressive = _ceil_div_int(executable_price_ticks * (_BPS_SCALE + planner_config.aggressiveness_bps), _BPS_SCALE)
        aggressive = _round_up_to_tick_local(aggressive, snapshot.tick_size_ticks)

        slippage_cap = (leg.reference_price_ticks * (_BPS_SCALE + slip_bps)) // _BPS_SCALE
        slippage_cap = _round_down_to_tick_local(max(slippage_cap, snapshot.tick_size_ticks), snapshot.tick_size_ticks)

        touch_cap = (executable_price_ticks * (_BPS_SCALE + planner_config.max_cross_bps_from_touch)) // _BPS_SCALE
        touch_cap = _round_down_to_tick_local(max(touch_cap, snapshot.tick_size_ticks), snapshot.tick_size_ticks)

        return min(aggressive, slippage_cap, touch_cap)

    executable_price_ticks = snapshot.best_bid_ticks
    aggressive = (executable_price_ticks * (_BPS_SCALE - planner_config.aggressiveness_bps)) // _BPS_SCALE
    aggressive = _round_down_to_tick_local(aggressive, snapshot.tick_size_ticks)

    slippage_floor = _ceil_div_int(leg.reference_price_ticks * (_BPS_SCALE - slip_bps), _BPS_SCALE)
    slippage_floor = _round_up_to_tick_local(max(slippage_floor, snapshot.tick_size_ticks), snapshot.tick_size_ticks)

    touch_floor = _ceil_div_int(executable_price_ticks * (_BPS_SCALE - planner_config.max_cross_bps_from_touch), _BPS_SCALE)
    touch_floor = _round_up_to_tick_local(max(touch_floor, snapshot.tick_size_ticks), snapshot.tick_size_ticks)

    return max(aggressive, slippage_floor, touch_floor)


def _compute_executable_edge_bps(
    *,
    opportunity: Opportunity,
    snapshots: Mapping[str, PricingSnapshot],
) -> int | None:
    total_notional = 0
    gross_edge = 0

    for leg in opportunity.legs:
        snapshot = snapshots.get(leg.market_id)
        if snapshot is None:
            return None
        executable_price_ticks = snapshot.best_ask_ticks if leg.side == Side.BUY else snapshot.best_bid_ticks
        quantity = leg.quantity_ratio
        notional = quantity * executable_price_ticks
        total_notional += notional
        if leg.side == Side.BUY:
            gross_edge -= notional
        else:
            gross_edge += notional

    if total_notional <= 0:
        return None
    return (gross_edge * _BPS_SCALE) // total_notional


def _ceil_div_int(numerator: int, denominator: int) -> int:
    return (numerator + denominator - 1) // denominator


def _round_down_to_tick_local(price: int, tick_size: int) -> int:
    if tick_size <= 0:
        return price
    return price - (price % tick_size)


def _round_up_to_tick_local(price: int, tick_size: int) -> int:
    if tick_size <= 0:
        return price
    remainder = price % tick_size
    if remainder == 0:
        return price
    return price + (tick_size - remainder)


def _diagnostic_class_from_reason_code(reason_code: str) -> str:
    upper_code = reason_code.strip().upper()
    if upper_code in {
        "LOCKED_BOOK",
        "MISSING_BOOK",
        "NON_POSITIVE_BOOK",
        "INVALID_BOOK",
        "MISSING_SNAPSHOT",
        "STALE_SNAPSHOT",
        "UNCERTAIN_SNAPSHOT",
    }:
        return "INVALID_BOOK"
    if upper_code in {
        "PRICE_PROTECTION_FAIL",
        "PRICE_PROTECTION",
        "EDGE_BELOW_MONITOR_MIN",
    }:
        return "PRICE_PROTECTION"
    if upper_code in {
        "BUILD_FAILED",
        "PLAN_NONE",
        "CONVERT_NONE",
        "CONVERT_MISSING_QUOTES",
        "CONVERT_INVALID_RELATION",
        "CONVERT_NO_VARIANT",
    }:
        return "BUILD_FAILURE"
    return "PRICE_PROTECTION"


def _summarize_rejections(
    rejections: list[_RejectedOpportunity],
) -> tuple[dict[str, int], dict[str, int]]:
    by_class: dict[str, int] = {}
    by_reason: dict[str, int] = {}
    for entry in rejections:
        payload = entry.payload
        diag_class = str(payload.get("diagnostic_class") or "UNKNOWN")
        reason_code = str(payload.get("reason_code") or payload.get("rejection_code") or "UNKNOWN")
        by_class[diag_class] = by_class.get(diag_class, 0) + 1
        by_reason[reason_code] = by_reason.get(reason_code, 0) + 1
    return by_class, by_reason


def _build_rejected_opportunity(
    *,
    candidate: StructuralArbCandidate,
    relation_id: str,
    observed_ts_ns: int,
    reason_code: str,
    reason: str,
    diagnostic_class: str,
    rejection_leg_id: str | None = None,
    theoretical_edge_bps: int | None = None,
    executable_edge_bps: int | None = None,
    opportunity: Opportunity | None = None,
    price_source: str | None = None,
    leg_checks: tuple[dict[str, object], ...] = tuple(),
    context: dict[str, object] | None = None,
) -> _RejectedOpportunity:
    candidate_market_ids = _candidate_market_ids(candidate)
    window_key = (
        _build_window_key(relation_id, opportunity)
        if opportunity is not None
        else f"{relation_id}|candidate:{'|'.join(candidate_market_ids)}"
    )

    payload: dict[str, object] = {
        "asset": _infer_asset_from_candidate(candidate),
        "slug_left": candidate.slug_left,
        "slug_right": candidate.slug_right,
        "match_mode": candidate.match_mode,
        "boundary_relation": candidate.boundary_relation,
        "strike_low_x100": candidate.strike_low,
        "strike_high_x100": candidate.strike_high,
        "strike_window": _format_strike_window(candidate),
        "reason_code": reason_code,
        "rejection_code": reason_code,
        "reason": reason,
        "rejection_reason": reason,
        "diagnostic_class": diagnostic_class,
        "theoretical_edge_bps": theoretical_edge_bps,
        "executable_edge_bps": executable_edge_bps,
        "price_source": price_source,
        "leg_checks": [dict(item) for item in leg_checks],
    }
    if rejection_leg_id is not None:
        payload["rejection_leg_id"] = rejection_leg_id
    if context is not None:
        payload["build_context"] = dict(context)
    if opportunity is not None:
        payload["opportunity_id"] = opportunity.opportunity_id
        payload["opportunity_theoretical_edge_bps"] = opportunity.expected_edge_bps
        payload["opportunity_leg_count"] = len(opportunity.legs)

    return _RejectedOpportunity(
        observed_ts_ns=observed_ts_ns,
        relation_id=relation_id,
        window_key=window_key,
        candidate_market_ids=candidate_market_ids,
        payload=payload,
    )


def _format_strike_window(candidate: StructuralArbCandidate) -> str:
    if candidate.strike_low is None or candidate.strike_high is None:
        return "unknown"
    return f"{candidate.strike_low}:{candidate.strike_high}"


def _candidate_market_ids(candidate: StructuralArbCandidate) -> tuple[str, ...]:
    market_ids: set[str] = {
        candidate.left_market.market_id,
        candidate.right_market.market_id,
    }
    if candidate.range_market is not None:
        market_ids.add(candidate.range_market.market_id)
    if candidate.lower_strike_market is not None:
        market_ids.add(candidate.lower_strike_market.market_id)
    if candidate.upper_strike_market is not None:
        market_ids.add(candidate.upper_strike_market.market_id)
    return tuple(sorted(market_ids))


def _execute_opened_observations(
    *,
    service: ExecutorService,
    observations: list[_ObservedOpportunity],
    now_ns: int,
) -> tuple[int, int, int]:
    accepted = 0
    rejected = 0
    skipped = 0

    for index, item in enumerate(observations, start=1):
        if item.opportunity.legs is None or not item.opportunity.legs:
            skipped += 1
            continue

        result = service.on_opportunity(
            relation_id=item.relation_id,
            opportunity=item.opportunity,
            snapshots=item.snapshots,
            now_ns=now_ns + index,
        )
        if result.accepted:
            accepted += 1
        else:
            rejected += 1

    return accepted, rejected, skipped


def _apply_public_orderbook_snapshots(
    *,
    opportunity: Opportunity,
    fallback_snapshots: dict[str, PricingSnapshot],
    now_ns: int,
    use_public_orderbook: bool,
    public_book_api_url: str,
    public_book_timeout_ms: int,
    public_book_max_concurrency: int,
    orderbook_cache: dict[str, tuple[int, int, int, int] | None],
) -> tuple[dict[str, PricingSnapshot], int]:
    if not use_public_orderbook:
        return dict(fallback_snapshots), 0

    token_ids = tuple({leg.token_id for leg in opportunity.legs})
    missing_token_ids = [token_id for token_id in token_ids if token_id not in orderbook_cache]

    if missing_token_ids:
        worker_count = max(1, min(public_book_max_concurrency, len(missing_token_ids)))
        if worker_count == 1:
            for token_id in missing_token_ids:
                orderbook_cache[token_id] = _fetch_public_top_of_book_uncached(
                    token_id=token_id,
                    api_url=public_book_api_url,
                    timeout_ms=public_book_timeout_ms,
                )
        else:
            with ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix="book-fetch") as pool:
                future_to_token_id = {
                    pool.submit(
                        _fetch_public_top_of_book_uncached,
                        token_id=token_id,
                        api_url=public_book_api_url,
                        timeout_ms=public_book_timeout_ms,
                    ): token_id
                    for token_id in missing_token_ids
                }
                for future in as_completed(future_to_token_id):
                    token_id = future_to_token_id[future]
                    try:
                        orderbook_cache[token_id] = future.result()
                    except Exception:
                        orderbook_cache[token_id] = None

    resolved: dict[str, PricingSnapshot] = {}
    hits = 0
    for leg in opportunity.legs:
        fallback = fallback_snapshots.get(leg.market_id)
        top_of_book = _fetch_public_top_of_book(
            token_id=leg.token_id,
            cache=orderbook_cache,
        )

        if top_of_book is None:
            if fallback is not None:
                resolved[leg.market_id] = fallback
            continue

        best_bid_ticks, best_bid_size, best_ask_ticks, best_ask_size = top_of_book
        resolved[leg.market_id] = PricingSnapshot(
            market_id=leg.market_id,
            ts_ns=now_ns,
            best_bid_ticks=best_bid_ticks,
            best_bid_size=best_bid_size,
            best_ask_ticks=best_ask_ticks,
            best_ask_size=best_ask_size,
            tick_size_ticks=1,
            is_uncertain=False,
        )
        hits += 1

    for market_id, snapshot in fallback_snapshots.items():
        resolved.setdefault(market_id, snapshot)

    return resolved, hits


def _fetch_public_top_of_book(
    *,
    token_id: str,
    cache: dict[str, tuple[int, int, int, int] | None],
) -> tuple[int, int, int, int] | None:
    return cache.get(token_id)


def _fetch_public_top_of_book_uncached(
    *,
    token_id: str,
    api_url: str,
    timeout_ms: int,
) -> tuple[int, int, int, int] | None:

    url = f"{api_url.rstrip('/')}/book?{urlencode({'token_id': token_id})}"
    request = Request(url, method="GET", headers={"Accept": "application/json", "User-Agent": "polyexecutor/1.0"})
    try:
        with urlopen(request, timeout=max(0.1, timeout_ms / 1000.0)) as response:
            body = response.read().decode("utf-8", errors="replace")
    except Exception:
        return None

    try:
        payload = json.loads(body)
    except Exception:
        return None

    return _parse_top_of_book_payload(payload)


def _parse_top_of_book_payload(payload: object) -> tuple[int, int, int, int] | None:
    if not isinstance(payload, Mapping):
        return None

    bids = payload.get("bids")
    asks = payload.get("asks")
    if not isinstance(bids, list) or not isinstance(asks, list):
        return None

    best_bid_price = None
    best_bid_size = 0.0
    for entry in bids:
        if not isinstance(entry, Mapping):
            continue
        price = _to_probability(entry.get("price"))
        size = _to_float(entry.get("size"))
        if price is None or size is None or size <= 0:
            continue
        if best_bid_price is None or price > best_bid_price:
            best_bid_price = price
            best_bid_size = size
        elif price == best_bid_price:
            best_bid_size += size

    best_ask_price = None
    best_ask_size = 0.0
    for entry in asks:
        if not isinstance(entry, Mapping):
            continue
        price = _to_probability(entry.get("price"))
        size = _to_float(entry.get("size"))
        if price is None or size is None or size <= 0:
            continue
        if best_ask_price is None or price < best_ask_price:
            best_ask_price = price
            best_ask_size = size
        elif price == best_ask_price:
            best_ask_size += size

    if best_bid_price is None or best_ask_price is None:
        return None

    best_bid_ticks = _probability_to_ticks(best_bid_price)
    best_ask_ticks = _probability_to_ticks(best_ask_price)
    best_bid_size_units = max(1, int(best_bid_size))
    best_ask_size_units = max(1, int(best_ask_size))
    return (best_bid_ticks, best_bid_size_units, best_ask_ticks, best_ask_size_units)


def _build_observation_payload(
    *,
    candidate: StructuralArbCandidate,
    relation_id: str,
    window_key: str,
    observed_ts_ns: int,
    opportunity: Opportunity,
    snapshots: Mapping[str, PricingSnapshot],
    price_source: str,
    max_fillable_units: int,
    max_fillable_profit_cents: int,
    theoretical_edge_bps: int | None,
    executable_edge_bps: int | None,
    expected_gross_profit_cents: int,
    expected_fee_cents: int,
    total_notional_cents: int,
    leg_checks: tuple[dict[str, object], ...] = tuple(),
) -> dict[str, object]:
    leg_rows: list[dict[str, object]] = []
    buy_notional = 0
    sell_notional = 0
    question_by_market = _candidate_question_map(candidate)

    for leg in opportunity.legs:
        quantity = max_fillable_units * leg.quantity_ratio
        leg_notional = quantity * leg.reference_price_ticks
        if leg.side == Side.BUY:
            buy_notional += leg_notional
        else:
            sell_notional += leg_notional

        snapshot = snapshots.get(leg.market_id)
        leg_rows.append(
            {
                "leg_id": leg.leg_id,
                "market_id": leg.market_id,
                "question": question_by_market.get(leg.market_id),
                "token_id": leg.token_id,
                "side": leg.side.value,
                "outcome": _extract_outcome_from_leg_id(leg.leg_id),
                "quantity": quantity,
                "reference_price_ticks": leg.reference_price_ticks,
                "best_bid_ticks": snapshot.best_bid_ticks if snapshot is not None else None,
                "best_ask_ticks": snapshot.best_ask_ticks if snapshot is not None else None,
                "best_bid_size": snapshot.best_bid_size if snapshot is not None else None,
                "best_ask_size": snapshot.best_ask_size if snapshot is not None else None,
            }
        )

    return {
        "window_key": window_key,
        "relation_id": relation_id,
        "observed_ts_ns": observed_ts_ns,
        "observed_utc": _ns_to_utc_iso(observed_ts_ns),
        "asset": _infer_asset_from_candidate(candidate),
        "slug_left": candidate.slug_left,
        "slug_right": candidate.slug_right,
        "match_mode": candidate.match_mode,
        "boundary_relation": candidate.boundary_relation,
        "strike_low_x100": candidate.strike_low,
        "strike_high_x100": candidate.strike_high,
        "strike_window": _format_strike_window(candidate),
        "max_fillable_units": max_fillable_units,
        "max_fillable_profit_cents": max_fillable_profit_cents,
        "expected_edge_bps": executable_edge_bps if executable_edge_bps is not None else theoretical_edge_bps,
        "theoretical_edge_bps": theoretical_edge_bps,
        "executable_edge_bps": executable_edge_bps,
        "expected_gross_profit_cents": expected_gross_profit_cents,
        "expected_fee_cents": expected_fee_cents,
        "expected_net_profit_cents": max_fillable_profit_cents,
        "total_notional_cents": total_notional_cents,
        "buy_notional_cents": buy_notional,
        "sell_notional_cents": sell_notional,
        "price_source": price_source,
        "diagnostic_class": "EXECUTABLE",
        "leg_checks": [dict(item) for item in leg_checks],
        "legs": leg_rows,
    }


def _build_window_key(relation_id: str, opportunity: Opportunity) -> str:
    leg_signature = "|".join(
        sorted(f"{leg.market_id}:{leg.token_id}:{leg.side.value}" for leg in opportunity.legs)
    )
    return f"{relation_id}|{leg_signature}"


def _build_relation_id(candidate: StructuralArbCandidate) -> str:
    relation_id = f"slugpair:{candidate.slug_left}|{candidate.slug_right}"
    if candidate.strike_low is not None and candidate.strike_high is not None:
        relation_id = (
            f"{relation_id}:"
            f"{candidate.strike_low}:{candidate.strike_high}:{candidate.boundary_relation or 'n/a'}"
        )
    return relation_id


def _extract_outcome_from_leg_id(leg_id: str) -> str:
    parts = leg_id.split("-")
    if len(parts) >= 2 and parts[1] in {"yes", "no"}:
        return parts[1]
    return "unknown"


def _candidate_question_map(candidate: StructuralArbCandidate) -> dict[str, str]:
    entries: dict[str, str] = {
        candidate.left_market.market_id: candidate.left_market.question,
        candidate.right_market.market_id: candidate.right_market.question,
    }
    if candidate.range_market is not None:
        entries[candidate.range_market.market_id] = candidate.range_market.question
    if candidate.lower_strike_market is not None:
        entries[candidate.lower_strike_market.market_id] = candidate.lower_strike_market.question
    if candidate.upper_strike_market is not None:
        entries[candidate.upper_strike_market.market_id] = candidate.upper_strike_market.question
    return entries


def _infer_asset_from_candidate(candidate: StructuralArbCandidate) -> str:
    for slug in (candidate.slug_left, candidate.slug_right):
        tokens = [token for token in re.split(r"[-_\s]+", slug.lower()) if token]
        for token in tokens:
            if token in {"price", "between", "on", "will", "close", "above", "below", "range"}:
                continue
            if token.isdigit():
                continue
            return token
    return "unknown"


def _should_stop_monitoring(*, started_at: float, duration_seconds: float) -> bool:
    if duration_seconds <= 0:
        return False
    elapsed = time.monotonic() - started_at
    return elapsed >= duration_seconds


def _to_probability(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        numeric = float(value)
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            numeric = float(text)
        except ValueError:
            return None
    else:
        return None

    if numeric < 0:
        return None
    if numeric <= 1:
        return numeric
    if numeric <= 100:
        return numeric / 100.0
    return None


def _to_float(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return float(text)
        except ValueError:
            return None
    return None


def _probability_to_ticks(probability: float) -> int:
    return max(1, min(99, int(round(probability * 100))))


def _ns_to_utc_iso(ts_ns: int) -> str:
    dt = datetime.fromtimestamp(ts_ns / 1_000_000_000, tz=timezone.utc)
    return dt.isoformat()


def _candidate_to_opportunity(
    candidate: StructuralArbCandidate,
    *,
    now_ns: int,
) -> _BuildOpportunityResult:
    if candidate.match_mode == "strict":
        return _strict_candidate_to_opportunity(candidate, now_ns=now_ns)

    left = candidate.left_market
    right = candidate.right_market
    left_quote = _outcome_quote(left, "yes")
    right_quote = _outcome_quote(right, "yes")
    if left_quote is None or right_quote is None:
        missing: list[str] = []
        if left_quote is None:
            missing.append(f"{left.market_id}:yes")
        if right_quote is None:
            missing.append(f"{right.market_id}:yes")
        return _BuildOpportunityResult(
            opportunity=None,
            snapshots={},
            theoretical_edge_bps=None,
            reason_code="CONVERT_MISSING_QUOTES",
            reason=f"missing yes quote(s): {','.join(missing)}",
            context={
                "mode": "non_strict",
                "missing_quotes": missing,
            },
        )

    left_price = left_quote.price_cents
    right_price = right_quote.price_cents

    if left_price <= right_price:
        buy_quote = left_quote
        sell_quote = right_quote
    else:
        buy_quote = right_quote
        sell_quote = left_quote

    buy_price = buy_quote.price_cents
    sell_price = sell_quote.price_cents

    total_price = max(1, buy_price + sell_price)
    price_diff = max(0, sell_price - buy_price)
    expected_edge_bps = max(1, (price_diff * 10_000) // total_price)

    opportunity = Opportunity(
        opportunity_id=f"struct-{buy_quote.market_id[:10]}-{sell_quote.market_id[:10]}-{now_ns}",
        detected_ts_ns=now_ns,
        expires_at_ns=now_ns + 2_000_000_000,
        expected_edge_bps=expected_edge_bps,
        confidence=0.95,
        target_package_units=1,
        min_package_units=1,
        max_package_units=1,
        legs=(
            OpportunityLeg(
                leg_id=f"buy-{buy_quote.outcome}-{buy_quote.market_id[:14]}",
                market_id=buy_quote.market_id,
                token_id=buy_quote.token_id,
                side=Side.BUY,
                quantity_ratio=1,
                reference_price_ticks=buy_price,
            ),
            OpportunityLeg(
                leg_id=f"sell-{sell_quote.outcome}-{sell_quote.market_id[:14]}",
                market_id=sell_quote.market_id,
                token_id=sell_quote.token_id,
                side=Side.SELL,
                quantity_ratio=1,
                reference_price_ticks=sell_price,
            ),
        ),
    )

    snapshots = {
        buy_quote.market_id: _build_synthetic_snapshot(
            market_id=buy_quote.market_id,
            mid_price_ticks=buy_price,
            now_ns=now_ns,
        ),
        sell_quote.market_id: _build_synthetic_snapshot(
            market_id=sell_quote.market_id,
            mid_price_ticks=sell_price,
            now_ns=now_ns,
        ),
    }

    return _BuildOpportunityResult(
        opportunity=opportunity,
        snapshots=snapshots,
        theoretical_edge_bps=expected_edge_bps,
        context={
            "mode": "non_strict",
            "variant": "yes_only",
            "buy_market_id": buy_quote.market_id,
            "sell_market_id": sell_quote.market_id,
        },
    )


def _strict_candidate_to_opportunity(
    candidate: StructuralArbCandidate,
    *,
    now_ns: int,
) -> _BuildOpportunityResult:
    lower = candidate.lower_strike_market
    upper = candidate.upper_strike_market
    range_market = candidate.range_market
    relation = candidate.boundary_relation

    if lower is None or upper is None or range_market is None:
        return _BuildOpportunityResult(
            opportunity=None,
            snapshots={},
            theoretical_edge_bps=None,
            reason_code="CONVERT_INVALID_RELATION",
            reason="strict candidate missing one of range/lower/upper markets",
            context={"mode": "strict"},
        )
    if relation not in {"above", "below"}:
        return _BuildOpportunityResult(
            opportunity=None,
            snapshots={},
            theoretical_edge_bps=None,
            reason_code="CONVERT_INVALID_RELATION",
            reason=f"unsupported boundary_relation={relation!r}",
            context={"mode": "strict"},
        )

    if relation == "above":
        target_market = lower
        companion_market = upper
    else:
        target_market = upper
        companion_market = lower

    target_yes = _outcome_quote(target_market, "yes")
    target_no = _outcome_quote(target_market, "no")
    companion_yes = _outcome_quote(companion_market, "yes")
    companion_no = _outcome_quote(companion_market, "no")
    range_yes = _outcome_quote(range_market, "yes")
    range_no = _outcome_quote(range_market, "no")

    variant_candidates = (
        _evaluate_strict_variant(
            label="yes_basis",
            left=target_yes,
            right_a=range_yes,
            right_b=companion_yes,
        ),
        _evaluate_strict_variant(
            label="range_no_basis",
            left=range_no,
            right_a=target_no,
            right_b=companion_yes,
        ),
        _evaluate_strict_variant(
            label="companion_no_basis",
            left=companion_no,
            right_a=target_no,
            right_b=range_yes,
        ),
    )

    selected: tuple[str, int, int, tuple[tuple[str, _OutcomeQuote], ...]] | None = None
    for variant in variant_candidates:
        if variant is None:
            continue
        if selected is None:
            selected = variant
            continue
        _, best_edge, best_no_count, _ = selected
        _, edge, no_count, _ = variant
        if edge > best_edge:
            selected = variant
            continue
        if edge == best_edge and no_count > best_no_count:
            selected = variant

    if selected is None:
        return _BuildOpportunityResult(
            opportunity=None,
            snapshots={},
            theoretical_edge_bps=None,
            reason_code="CONVERT_NO_VARIANT",
            reason="no strict equation variant produced non-zero residual",
            context={
                "mode": "strict",
                "boundary_relation": relation,
                "target_market_id": target_market.market_id,
                "companion_market_id": companion_market.market_id,
                "quotes_available": {
                    "target_yes": target_yes is not None,
                    "target_no": target_no is not None,
                    "companion_yes": companion_yes is not None,
                    "companion_no": companion_no is not None,
                    "range_yes": range_yes is not None,
                    "range_no": range_no is not None,
                },
            },
        )

    variant_label, expected_edge_bps, _, selected_legs = selected

    legs: list[OpportunityLeg] = []
    prices_by_market: dict[str, int] = {}
    for index, (action, quote) in enumerate(selected_legs, start=1):
        side = Side.BUY if action == "buy" else Side.SELL
        legs.append(
            OpportunityLeg(
                leg_id=f"{action}-{quote.outcome}-{index}-{quote.market_id[:12]}",
                market_id=quote.market_id,
                token_id=quote.token_id,
                side=side,
                quantity_ratio=1,
                reference_price_ticks=quote.price_cents,
            )
        )
        prices_by_market[quote.market_id] = quote.price_cents

    opportunity = Opportunity(
        opportunity_id=(
            f"strict-{variant_label}-{range_market.market_id[:8]}-{lower.market_id[:8]}-{upper.market_id[:8]}-{now_ns}"
        ),
        detected_ts_ns=now_ns,
        expires_at_ns=now_ns + 2_000_000_000,
        expected_edge_bps=expected_edge_bps,
        confidence=0.95,
        target_package_units=1,
        min_package_units=1,
        max_package_units=1,
        legs=tuple(legs),
    )

    snapshots = {
        market_id: _build_synthetic_snapshot(
            market_id=market_id,
            mid_price_ticks=price_cents,
            now_ns=now_ns,
        )
        for market_id, price_cents in prices_by_market.items()
    }

    return _BuildOpportunityResult(
        opportunity=opportunity,
        snapshots=snapshots,
        theoretical_edge_bps=expected_edge_bps,
        context={
            "mode": "strict",
            "boundary_relation": relation,
            "variant": variant_label,
            "target_market_id": target_market.market_id,
            "companion_market_id": companion_market.market_id,
            "range_market_id": range_market.market_id,
        },
    )


def _evaluate_strict_variant(
    *,
    label: str,
    left: _OutcomeQuote | None,
    right_a: _OutcomeQuote | None,
    right_b: _OutcomeQuote | None,
) -> tuple[str, int, int, tuple[tuple[str, _OutcomeQuote], ...]] | None:
    if left is None or right_a is None or right_b is None:
        return None

    residual = left.price_cents - right_a.price_cents - right_b.price_cents
    if residual == 0:
        return None

    total_price = max(1, left.price_cents + right_a.price_cents + right_b.price_cents)
    expected_edge_bps = max(1, (abs(residual) * 10_000) // total_price)
    no_count = sum(1 for quote in (left, right_a, right_b) if quote.outcome == "no")

    if residual > 0:
        legs = (
            ("sell", left),
            ("buy", right_a),
            ("buy", right_b),
        )
    else:
        legs = (
            ("buy", left),
            ("sell", right_a),
            ("sell", right_b),
        )

    return (label, expected_edge_bps, no_count, legs)


def _outcome_quote(market: SlugMarket, outcome: str) -> _OutcomeQuote | None:
    normalized_outcome = outcome.strip().lower()
    if normalized_outcome == "yes":
        token_id = market.yes_token_id or (market.token_ids[0] if market.token_ids else None)
        price_cents = market.yes_price_cents
        if price_cents is None and market.no_price_cents is not None:
            price_cents = _complement_price(market.no_price_cents)
    elif normalized_outcome == "no":
        token_id = market.no_token_id or (market.token_ids[1] if len(market.token_ids) >= 2 else None)
        price_cents = market.no_price_cents
        if price_cents is None and market.yes_price_cents is not None:
            price_cents = _complement_price(market.yes_price_cents)
    else:
        return None

    if token_id is None or price_cents is None:
        return None

    return _OutcomeQuote(
        market_id=market.market_id,
        token_id=token_id,
        price_cents=price_cents,
        outcome=normalized_outcome,
    )


def _complement_price(price_cents: int) -> int:
    return max(0, min(100, 100 - price_cents))


def _build_synthetic_snapshot(*, market_id: str, mid_price_ticks: int, now_ns: int) -> PricingSnapshot:
    bounded_mid = max(2, min(98, mid_price_ticks))
    best_bid = max(1, bounded_mid - 1)
    best_ask = max(best_bid + 1, bounded_mid + 1)

    return PricingSnapshot(
        market_id=market_id,
        ts_ns=now_ns,
        best_bid_ticks=best_bid,
        best_bid_size=1_000,
        best_ask_ticks=best_ask,
        best_ask_size=1_000,
        tick_size_ticks=1,
        is_uncertain=False,
    )


def _resolve_journal_dir(project_root: Path) -> Path:
    configured = os.environ.get("EXECUTOR_JOURNAL_DIR", "./data/journal")
    raw_path = Path(configured)
    if raw_path.is_absolute():
        return raw_path
    return (project_root / raw_path).resolve()


def _resolve_output_path(project_root: Path, env_name: str, default_relative_path: str) -> Path:
    configured = os.environ.get(env_name, default_relative_path)
    raw_path = Path(configured)
    if raw_path.is_absolute():
        return raw_path
    return (project_root / raw_path).resolve()


def _load_dotenv_file(path: Path) -> None:
    if not path.exists():
        return

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        normalized_key = key.strip()
        if not normalized_key:
            continue

        normalized_value = value.strip()
        if len(normalized_value) >= 2 and normalized_value[0] == normalized_value[-1] and normalized_value[0] in {'"', "'"}:
            normalized_value = normalized_value[1:-1]

        if normalized_key not in os.environ:
            os.environ[normalized_key] = normalized_value


def _env_bool(name: str, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    value = os.environ.get(name)
    if value is None or not value.strip():
        return default
    return int(value)


def _env_optional(name: str) -> str | None:
    value = os.environ.get(name)
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None


def _env_optional_int(name: str) -> int | None:
    value = _env_optional(name)
    if value is None:
        return None
    return int(value)


def _env_float(name: str, default: float) -> float:
    value = os.environ.get(name)
    if value is None or not value.strip():
        return default
    return float(value)


def _env_csv(name: str) -> tuple[str, ...]:
    value = _env_optional(name)
    if value is None:
        return tuple()
    return tuple(item.strip() for item in value.split(",") if item.strip())


if __name__ == "__main__":
    main()
