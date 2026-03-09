# PolyymarketExecutor

Deterministic multi-leg execution core for Polymarket-style venue execution, including planning, risk controls, state machine transitions, venue abstraction, and journal/recovery.

## Status

- Ready for supervised, small-size live testing.
- `token_id` is propagated end-to-end through planning, execution, and journaling.
- Shared parseable client order IDs are used via `ClientOrderIdFactory`.
- Concrete HTTP CLOB client and optional SDK-backed signer are included.
- Current test baseline: `62 passed`.

## Quick start

1. Create and activate a venv (if needed).
2. Copy env template:

```powershell
Copy-Item .env.example .env
```

3. Fill required secrets in `.env`:
   - `POLYMARKET_API_URL`
   - `POLYMARKET_PRIVATE_KEY` (required for live signing)
   - optional API auth fields (`POLYMARKET_API_KEY`, `POLYMARKET_API_SECRET`, `POLYMARKET_PASSPHRASE`)

## Recommended executable entrypoint (from env)

Run:

```powershell
.\.venv\Scripts\python.exe .\examples\run_executor_from_env.py
```

Behavior:
- Loads `.env` automatically.
- Builds planner, risk manager, venue adapter/client, journal, recovery coordinator, and `ExecutorService`.
- Starts and stops the service safely for wiring validation.

Modes:
- Paper mode (default): `EXECUTOR_LIVE_TRADING_ENABLED=false` (uses `FakeVenueAdapter`).
- Live mode: `EXECUTOR_LIVE_TRADING_ENABLED=true` (uses `PolymarketVenueAdapter` + HTTP CLOB client).
- Optional startup recovery: `EXECUTOR_RUN_RECOVERY_ON_START=true`.

## SDK signer (optional but recommended for live)

Install dependency:

```powershell
.\.venv\Scripts\python.exe -m pip install py-clob-client
```

Relevant env keys:
- `POLYMARKET_USE_SDK_SIGNER=true`
- `POLYMARKET_CHAIN_ID=137`
- optional: `POLYMARKET_SIGNATURE_TYPE`, `POLYMARKET_FUNDER`, `POLYMARKET_MAKER`

If SDK signer is disabled or unavailable, the HTTP client still supports a custom signer callback.

## Other commands

Run tests:

```powershell
.\.venv\Scripts\python.exe -m pytest
```

Run deterministic minimal pipeline demo:

```powershell
.\.venv\Scripts\python.exe .\examples\minimal_executor_wiring.py
```

## Live rollout checklist

1. Start in paper mode and verify logs/journal behavior.
2. Move to tiny-size live orders with strict risk limits.
3. Run one-package tests first and validate both-leg behavior.
4. Confirm no ambiguous spikes and no unexpected late-fill handling issues.
5. Test restart + recovery path before scaling.

## Caution

Live trading can lose real funds. Keep limits conservative, monitor continuously, and scale only after repeated clean runs.
