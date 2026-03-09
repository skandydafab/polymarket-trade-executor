from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
import sys
import time


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
    PlannerConfig,
    PolymarketAdapterConfig,
    PolymarketVenueAdapter,
    RecoveryCoordinator,
    RiskManagerConfig,
)
from executor.planner import ExecutionPlanner
from executor.polymarket_clob_client import PolymarketCLOBHttpClient, PolymarketCLOBHttpClientConfig
from executor.polymarket_sdk_signer import PyClobClientOrderSigner, PyClobClientOrderSignerConfig


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


def main() -> None:
    project_root = PROJECT_ROOT
    _load_dotenv_file(project_root / ".env")

    live_enabled = _env_bool("EXECUTOR_LIVE_TRADING_ENABLED", False)
    run_recovery_on_start = _env_bool("EXECUTOR_RUN_RECOVERY_ON_START", False)
    subscription_scope = _read_subscription_scope_from_env()

    logger = PrintLogger()
    planner = ExecutionPlanner(PlannerConfig())
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


def _resolve_journal_dir(project_root: Path) -> Path:
    configured = os.environ.get("EXECUTOR_JOURNAL_DIR", "./data/journal")
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


def _env_csv(name: str) -> tuple[str, ...]:
    value = _env_optional(name)
    if value is None:
        return tuple()
    return tuple(item.strip() for item in value.split(",") if item.strip())


if __name__ == "__main__":
    main()
