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
- parses optional market subscription scope from env,
- starts service and performs a safe startup check,
- optionally runs recovery on startup.

### C. Market subscription scope

Put your target markets/tokens in `.env`:

- `POLYMARKET_SUBSCRIBE_MARKET_IDS`
- `POLYMARKET_SUBSCRIBE_TOKEN_IDS`

Format: comma-separated values.

Example:

```dotenv
POLYMARKET_SUBSCRIBE_MARKET_IDS=0xmarket_condition_id_example_a,0xmarket_condition_id_example_b
POLYMARKET_SUBSCRIBE_TOKEN_IDS=1234567890123456789012345678901234567890,9876543210987654321098765432109876543210
```

Note: the executor itself does not directly subscribe to data feeds. These env values are meant to define scope for your upstream market-data collector/detector, and `examples/run_executor_from_env.py` now parses and surfaces them on startup.

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
