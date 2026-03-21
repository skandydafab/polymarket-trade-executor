# PolyymarketExecutor

Deterministic multi-leg execution engine for Polymarket-style CLOB trading, with explicit risk controls, auditable state transitions, and restart recovery support.

---

## 1) Purpose of this README

This document is intentionally written as a **handoff brief for another agent/reviewer**.

It covers:
- what the system currently does,
- what was recently added and hardened,
- where to inspect the code,
- and what risks/edge-cases should be reviewed before scaling live usage.

---

## 2) Current project snapshot

- Runtime target: Python 3.12
- Core pattern: deterministic planner + risk gate + single-writer executor + venue adapter + journal/replay
- Live integration path: `PolymarketVenueAdapter` + concrete HTTP CLOB client
- Optional signing path: SDK-backed signer (`py-clob-client`) or custom signer callback
- Current local test baseline: **62 passed**

Recent critical correctness work includes:
- `token_id` propagated from opportunity → planned leg → venue intent → journal/replay
- shared parseable client order ID generation via `ClientOrderIdFactory`
- concrete CLOB HTTP client implementation with robust payload normalization
- SDK signer helper with fallback-compatible invocation behavior
- executable env-driven wiring entrypoint for reproducible startup

---

## 3) System architecture and flow

High-level execution flow:

1. **Opportunity enters** with leg metadata and confidence window.
2. **Planner** validates books/snapshots and emits an `ExecutionPlan` or deterministic rejection.
3. **Risk manager** performs pre-trade checks and can hard block or auto-halt.
4. **Executor service** turns plan legs into venue intents, tracks package/leg lifecycle, and processes updates.
5. **Venue adapter/client** submit/cancel/poll normalize venue-specific payloads into internal events.
6. **State machine** applies strict transitions for each leg and package.
7. **Journal** records canonical events for replay and restart recovery.
8. **Recovery** reconstructs state, reconciles open orders, and can enforce protective halt.

---

## 4) Files to review first (agent checklist)

### Core behavior
- `executor/executor_service.py`
- `executor/state_machine.py`
- `executor/risk.py`
- `executor/planner.py`

### Venue integration
- `executor/polymarket_adapter.py`
- `executor/polymarket_clob_client.py`
- `executor/polymarket_sdk_signer.py`
- `executor/slug_structural_arb.py`
- `executor/venue.py`

### Reliability and recovery
- `executor/journal.py`
- `tests/replay/test_deterministic_replay.py`
- `tests/test_journal_recovery.py`

### Wiring and operations
- `examples/run_executor_from_env.py`
- `.env.example`

---

## 5) What to look out for (review risks)

### A. Ambiguous venue outcomes
Network timeout/transport errors on submit/cancel are treated as ambiguous until updates/reconciliation confirm final state.

Review that:
- ambiguous status paths never silently downgrade to success,
- retries do not violate idempotency,
- reconciliation actions remain operator-visible.

### B. Late fills after cancel/timeout
Late fills are explicitly modeled and should trigger unwind-required behavior when exposure appears after terminal-looking states.

Review that:
- late-fill transitions are monotonic and deterministic,
- fill accounting never regresses cumulative quantities,
- risk escalation occurs on asymmetric exposure.

### C. Recovery + halt interaction
Recovery can reconstruct active packages and detect unknown/missing open orders.

Review that:
- replay + reconciliation produce expected active/open sets,
- halt-after-recovery behavior matches desired operational policy,
- relation/package mapping remains stable across restart.

### D. SDK signer compatibility drift
`py-clob-client` signatures can vary by version.

Review that:
- signer method dispatch remains compatible with your deployed SDK version,
- private key is required only where expected,
- signed payload shape matches what your venue endpoint accepts.

### E. Env safety
Live mode is env-toggled and should not be enabled accidentally.

Review that:
- default mode remains paper-safe,
- `.env` is ignored in VCS,
- no secrets are logged.

### F. Slug pairing heuristic quality
Slug auto-pairing is heuristic-based at slug level, and strict market-level matching is required for structural arb execution.

Review that:
- chosen slug pairs match intended semantic relationships,
- strict mode finds both boundary strikes for each range market,
- fallback behavior is acceptable if you intentionally use non-strict match modes.

---

## 6) Running the project

### A. Setup

```powershell
Copy-Item .env.example .env
```

Fill `.env` with your real values before live mode.

### B. Recommended env-based executable

```powershell
.\.venv\Scripts\python.exe .\examples\run_executor_from_env.py
```

This entrypoint:
- loads `.env`,
- constructs planner/risk/adapter/journal/recovery/service,
- resolves market subscriptions from slug inputs via Gamma API,
- auto-pairs slugs using naming/date rules,
- builds structural-arb candidates from paired markets,
- starts service and performs a safe startup check,
- optionally runs recovery on startup.

### C. Market subscription scope

Preferred configuration is slug-only:

- `POLYMARKET_SUBSCRIBE_SLUGS`

Optional controls:

- `POLYMARKET_SUBSCRIBE_SLUG_PAIRS` (explicit pair overrides)
- `POLYMARKET_AUTO_PAIR_SLUGS` (default `true`)
- `POLYMARKET_STRUCT_ARB_MATCH_MODE` (`strict`, `text`, or `cross`; default `strict`)
- `POLYMARKET_STRUCT_ARB_MIN_SIMILARITY`
- `POLYMARKET_STRUCT_ARB_MAX_CANDIDATES`
- `POLYMARKET_STRUCT_ARB_MIN_EDGE_BPS`
- `POLYMARKET_STRUCT_ARB_MONITOR_INTERVAL_MS` (default `250`)
- `POLYMARKET_STRUCT_ARB_MONITOR_DURATION_SECONDS` (`<=0` means run until interrupted)
- `POLYMARKET_STRUCT_ARB_USE_PUBLIC_ORDERBOOK` (public top-of-book for fillable sizing)
- `POLYMARKET_PUBLIC_BOOK_TIMEOUT_MS`
- `POLYMARKET_STRUCT_ARB_LOG_JSONL_PATH`
- `POLYMARKET_STRUCT_ARB_LOG_CSV_PATH`
- `POLYMARKET_STRUCT_ARB_LOG_REJECTIONS`
- `POLYMARKET_STRUCT_ARB_LOG_REJECTIONS_JSONL_PATH`
- `POLYMARKET_STRUCT_ARB_EXECUTION_MODE` (`emit` or `execute`)

Format: comma-separated values.

Example:

```dotenv
POLYMARKET_SUBSCRIBE_SLUGS=bitcoin-price-on-march-22,bitcoin-price-between-march-22
POLYMARKET_AUTO_PAIR_SLUGS=true
POLYMARKET_STRUCT_ARB_MATCH_MODE=strict
POLYMARKET_STRUCT_ARB_MIN_EDGE_BPS=1
POLYMARKET_STRUCT_ARB_MONITOR_INTERVAL_MS=250
POLYMARKET_STRUCT_ARB_MONITOR_DURATION_SECONDS=300
POLYMARKET_STRUCT_ARB_USE_PUBLIC_ORDERBOOK=true
POLYMARKET_STRUCT_ARB_LOG_JSONL_PATH=./data/research/structural_arb_events.jsonl
POLYMARKET_STRUCT_ARB_LOG_CSV_PATH=./data/research/structural_arb_windows.csv
POLYMARKET_STRUCT_ARB_LOG_REJECTIONS=true
POLYMARKET_STRUCT_ARB_LOG_REJECTIONS_JSONL_PATH=./data/research/structural_arb_rejections.jsonl
POLYMARKET_STRUCT_ARB_MIN_SIMILARITY=0.40
POLYMARKET_STRUCT_ARB_EXECUTION_MODE=emit
```

Strict mode requirement:
- The range market must contain two strikes (for example, `between 88000 and 92000`).
- The paired non-between market set must contain both matching boundary strikes (`above 88000` and `above 92000`, or equivalently `below 92000` and `below 88000`).
- Only then does the runner emit a structural candidate, and in `execute` mode it creates a 3-leg package.
- Execution now evaluates both YES and NO outcome quotes/tokens for each market and selects the strongest valid strict equation variant.

Research logging output (offline and live):
- Opportunity identity: asset/slug-pair, market ids/questions, leg side, YES/NO outcome.
- Observation timing: per-sample UTC timestamp and per-window first/last seen times.
- Max fillable outcome: planner-derived max fillable units and max fillable net profit for that sample.
- Opportunity lifetime: window duration until price change/missing condition closes the window.
- Extra diagnostics: edge decomposition (gross/fees/net), top-of-book liquidity details, lifecycle markers (`opened`, `update`, `closed`, close reason), and split edge fields (`theoretical_edge_bps` vs `executable_edge_bps`).
- Rejection diagnostics: dedicated JSONL stream with explicit `reason_code`, `reason`, `diagnostic_class` (`INVALID_BOOK`, `PRICE_PROTECTION`, `BUILD_FAILURE`) plus candidate context (market ids, strikes, relation id), build context, and per-leg checks.

Runtime poll diagnostics:
- `structural_arb_poll` now emits `executable_candidates`, `rejected_by_class`, `rejected_by_reason`, and cumulative counters to separate market-condition failures from conversion/system issues.

Important: these JSONL/CSV logs are written in both paper mode and live mode, so attaching credentials does not disable research traces.

Manual scope remains available for upstream integrations:

- `POLYMARKET_SUBSCRIBE_MARKET_IDS`
- `POLYMARKET_SUBSCRIBE_TOKEN_IDS`

Note: the executor itself still consumes opportunities/snapshots; slug resolution and structural-arb candidate construction are orchestration helpers in `examples/run_executor_from_env.py`.

### D. Modes

- Paper mode (default):
   - `EXECUTOR_LIVE_TRADING_ENABLED=false`
   - uses `FakeVenueAdapter`

- Live mode:
   - `EXECUTOR_LIVE_TRADING_ENABLED=true`
   - uses `PolymarketVenueAdapter` + `PolymarketCLOBHttpClient`

- Optional startup recovery:
   - `EXECUTOR_RUN_RECOVERY_ON_START=true`

### E. Optional SDK signer dependency

```powershell
.\.venv\Scripts\python.exe -m pip install py-clob-client
```

### F. Docker (local and DigitalOcean droplet)

The repository now includes:
- `Dockerfile`
- `.dockerignore`
- `docker-compose.yml`

Quick start (local or remote Linux host):

```bash
cp .env.example .env
```

Set your desired values in `.env`.
For long-running deployment, set:

```dotenv
POLYMARKET_STRUCT_ARB_MONITOR_DURATION_SECONDS=0
```

Build and run:

```bash
docker compose build
docker compose up -d
```

View logs:

```bash
docker compose logs -f polyexecutor
```

Stop:

```bash
docker compose down
```

Persistence:
- `./data` is mounted to `/app/data` in the container.
- Research logs and journal output survive container restarts.
- Container restart policy is `unless-stopped`.

DigitalOcean droplet checklist:
1. Create an Ubuntu droplet and SSH in.
2. Install Docker Engine and Docker Compose plugin.
3. Clone this repository to the droplet.
4. Create `.env` from `.env.example` and set credentials when ready.
5. Run `docker compose up -d`.
6. Monitor with `docker compose logs -f polyexecutor`.

---

## 7) Test and validation commands

Run full suite:

```powershell
.\.venv\Scripts\python.exe -m pytest
```

Run deterministic minimal demo:

```powershell
.\.venv\Scripts\python.exe .\examples\minimal_executor_wiring.py
```

---

## 8) Suggested reviewer workflow (for another agent)

1. Run tests and confirm baseline.
2. Read `state_machine.py` transition logic with focus on failure/late-fill paths.
3. Inspect `polymarket_adapter.py` ambiguous outcomes and dedupe/out-of-order handling.
4. Inspect `journal.py` replay/recovery code and compatibility assumptions.
5. Validate `run_executor_from_env.py` toggles and defaults (paper vs live safety).
6. Confirm `.env.example` mapping matches all consumed env keys.

---

## 9) Known limitations / next checks

- Startup entrypoint currently validates wiring and lifecycle startup, but is intentionally conservative.
- Real-market rollout still requires strict monitoring and controlled position sizing.
- If SDK version changes, signer compatibility should be re-verified with a targeted smoke test.

---

## 10) Operational caution

Live trading can lose real funds. Keep limits conservative, use continuous monitoring, and only scale after repeated clean recovery and execution runs.
