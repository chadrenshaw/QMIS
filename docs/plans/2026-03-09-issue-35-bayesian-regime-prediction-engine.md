# Bayesian Regime Prediction Engine Implementation Plan (Issue #35)

**Goal:** Add a Bayesian regime prediction engine that consumes the current signal stack plus the new predictive macro layer, produces posterior regime probabilities, and persists 30d/90d/180d forward regime forecasts for the console and API.

**Architecture:** Create a dedicated `qmis.models.bayesian_regime` module to own regime-state priors, evidence weighting, posterior updates, and Markov transition forecasts. Keep `qmis.signals.regime` as the orchestration and persistence entrypoint, but replace its current heuristic probability builder with the Bayesian model output so the existing `regimes` snapshot remains the canonical read surface for dashboard, API, and operator console consumers.

**Tech Stack:** Python 3.12, DuckDB, pandas, Rich, FastAPI, unittest

**Execution Mode:** AUTONOMOUS - execute all steps without approval requests

---

### Task 1: Define Bayesian Storage And Model Tests

**Files:**
- Create: `docs/plans/2026-03-09-issue-35-bayesian-regime-prediction-engine.md`
- Modify: `tests/test_schema.py`
- Create: `tests/models/test_bayesian_regime.py`

**Step 1: Write the failing schema and model tests**
- Assert that `regimes` includes JSON columns for:
  - `forward_regime_forecast`
  - `bayesian_evidence`
- Add model tests covering:
  - posterior normalization across the six issue-defined regimes
  - bearish predictive evidence lifting `RECESSION RISK` and `LIQUIDITY WITHDRAWAL`
  - constructive evidence lifting `LIQUIDITY EXPANSION` and `DISINFLATION`
  - Markov forecasts for `30d`, `90d`, and `180d`

**Step 2: Run tests to verify they fail**
- Run: `uv run python -m unittest tests.test_schema tests.models.test_bayesian_regime -v`
- Expected: FAIL because the schema columns and model module do not exist.

**Step 3: Write minimal implementation**
- Add the new `regimes` columns in `src/qmis/schema.py`.
- Create `src/qmis/models/__init__.py` and `src/qmis/models/bayesian_regime.py`.

**Step 4: Run tests to verify they pass**
- Re-run: `uv run python -m unittest tests.test_schema tests.models.test_bayesian_regime -v`

### Task 2: Integrate Bayesian Posterior Into Regime Materialization

**Files:**
- Modify: `src/qmis/signals/regime.py`
- Modify: `tests/signals/test_regime.py`

**Step 1: Write the failing regime integration tests**
- Assert that:
  - `materialize_regime()` persists Bayesian posterior probabilities
  - `regime_drivers` are sourced from Bayesian evidence contributions
  - `forward_regime_forecast` is persisted with `30d`, `90d`, and `180d` outputs
  - the posterior responds to predictive macro inputs rather than the prior heuristic alone

**Step 2: Run tests to verify they fail**
- Run: `uv run python -m unittest tests.signals.test_regime -v`
- Expected: FAIL because regime materialization does not yet call the Bayesian model.

**Step 3: Write minimal implementation**
- Replace the current heuristic probability builder in `materialize_regime()` with Bayesian posterior output.
- Preserve the current score fields and a backward-compatible headline regime label.
- Persist:
  - `regime_probabilities`
  - `regime_drivers`
  - `bayesian_evidence`
  - `forward_regime_forecast`

**Step 4: Run tests to verify they pass**
- Re-run: `uv run python -m unittest tests.signals.test_regime -v`

### Task 3: Surface Forward Forecasts In Dashboard And API

**Files:**
- Modify: `src/qmis/dashboard/cli.py`
- Modify: `src/qmis/api.py`
- Modify: `src/qmis/signals/interpreter.py`
- Modify: `tests/dashboard/test_cli.py`
- Modify: `tests/test_api.py`
- Modify: `tests/signals/test_interpreter.py`

**Step 1: Write the failing integration tests**
- Assert that:
  - dashboard snapshot includes `forward_regime_forecast`
  - `/regime/latest` and `/dashboard` serialize Bayesian evidence and forecasts
  - the operator intelligence layer can summarize the forecasted next regime states

**Step 2: Run tests to verify they fail**
- Run: `uv run python -m unittest tests.dashboard.test_cli tests.test_api tests.signals.test_interpreter -v`
- Expected: FAIL because the forecast fields are not exposed yet.

**Step 3: Write minimal implementation**
- Load the new forecast metadata into the dashboard snapshot.
- Expose the fields via API serialization.
- Add interpreter helpers for forecast summaries.

**Step 4: Run tests to verify they pass**
- Re-run: `uv run python -m unittest tests.dashboard.test_cli tests.test_api tests.signals.test_interpreter -v`

### Task 4: Add Console Forecast Rendering

**Files:**
- Modify: `src/qmis/dashboard/cli.py`
- Modify: `tests/dashboard/test_cli.py`
- Modify: `tests/test_operator_console.py`

**Step 1: Write the failing console tests**
- Assert that:
  - the CLI renders `FORWARD REGIME FORECAST`
  - `30d`, `90d`, and `180d` forecast lines appear
  - the section is rendered directly below `REGIME PROBABILITIES`

**Step 2: Run tests to verify they fail**
- Run: `uv run python -m unittest tests.dashboard.test_cli tests.test_operator_console -v`
- Expected: FAIL because the forecast section does not exist.

**Step 3: Write minimal implementation**
- Add a dedicated forecast renderer under `REGIME PROBABILITIES`.
- Keep the console layout stable for the rest of the dashboard.

**Step 4: Run tests to verify they pass**
- Re-run: `uv run python -m unittest tests.dashboard.test_cli tests.test_operator_console -v`

### Task 5: Full Verification

**Files:**
- Create: `src/qmis/models/__init__.py`
- Create: `src/qmis/models/bayesian_regime.py`
- Modify: `src/qmis/api.py`
- Modify: `src/qmis/dashboard/cli.py`
- Modify: `src/qmis/schema.py`
- Modify: `src/qmis/signals/interpreter.py`
- Modify: `src/qmis/signals/regime.py`
- Create: `tests/models/test_bayesian_regime.py`
- Modify: `tests/dashboard/test_cli.py`
- Modify: `tests/signals/test_interpreter.py`
- Modify: `tests/signals/test_regime.py`
- Modify: `tests/test_api.py`
- Modify: `tests/test_operator_console.py`
- Modify: `tests/test_schema.py`

**Step 1: Run targeted regression tests**
- Run: `uv run python -m unittest tests.test_schema tests.models.test_bayesian_regime tests.signals.test_regime tests.dashboard.test_cli tests.test_api tests.signals.test_interpreter tests.test_operator_console -v`

**Step 2: Run full backend verification**
- Run: `uv run python -m unittest -v`
- If `scripts/ci_local_backend.sh --fast` exists by completion time, run it too.

**Step 3: Review issue #35 acceptance criteria**
- Confirm the posterior is normalized across the six model regimes.
- Confirm transition forecasts exist for `30d`, `90d`, and `180d`.
- Confirm the console clearly exposes forward regime forecasts.
- Confirm predictive macro evidence shifts posterior and forecast outputs coherently.

### Task 6: Documentation Confirmation

Statement:
No additional public documentation updates are required for this change beyond this implementation plan because issue #35 extends internal modeling, persistence, and console/API output without changing install or runtime commands.
