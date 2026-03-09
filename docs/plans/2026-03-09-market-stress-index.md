# Composite Market Stress Index Implementation Plan (Issue #27)

**Goal:** Add a persisted market stress index that summarizes the current macro stress regime and surfaces it prominently in the operator console.

**Architecture:** Introduce a new `qmis.signals.stress` analysis module that computes a composite score and stress level from existing QMIS inputs, persists the latest snapshot in DuckDB, and exposes it through the dashboard snapshot, interpreter layer, and FastAPI dashboard response. Keep the current `risk_monitor` output, but make the new market-stress snapshot the headline operator signal for systemic strain.

**Tech Stack:** Python 3.12, DuckDB, pandas, numpy, Rich, FastAPI, unittest

**Execution Mode:** AUTONOMOUS - execute all steps without approval requests

---

### Task 1: Schema And Snapshot Surface

**Files:**
- Create: `docs/plans/2026-03-09-market-stress-index.md`
- Modify: `src/qmis/schema.py`
- Test: `tests/test_schema.py`

**Step 1: Write the failing schema test**
- Assert that DuckDB bootstraps a `stress_snapshots` table with fields for timestamp, score, level, summary, and component metadata.

**Step 2: Run test to verify it fails**
- Run: `uv run python -m unittest tests.test_schema.QMISSchemaTests.test_bootstrap_database_creates_all_required_tables tests.test_schema.QMISSchemaTests.test_bootstrap_database_applies_spec_columns -v`

**Step 3: Add the `stress_snapshots` table**
- Keep the schema narrow and focused on the latest persisted market-stress artifact.

**Step 4: Run test to verify it passes**
- Re-run the schema test command.

**Step 5: Commit**
- Commit when schema support is green.

### Task 2: Stress Analysis Engine

**Files:**
- Create: `src/qmis/signals/stress.py`
- Modify: `scripts/run_analysis.py`
- Modify: `scripts/run_operator_console.py`
- Test: `tests/signals/test_stress.py`
- Test: `tests/test_operator_console.py`

**Step 1: Write the failing stress-engine tests**
- Cover score computation, missing-input degradation, optional credit-proxy handling, and DuckDB materialization.

**Step 2: Run test to verify it fails**
- Run: `uv run python -m unittest tests.signals.test_stress -v`

**Step 3: Write minimal implementation**
- Compute market stress from:
  - VIX level and spike
  - yield-curve stress
  - breadth deterioration
  - anomaly persistence or relationship-break pressure
  - optional credit stress when a credit proxy exists in `signals`
- Persist the latest stress snapshot.
- Wire `materialize_market_stress()` into analysis and operator refresh flows.

**Step 4: Run tests to verify they pass**
- Run: `uv run python -m unittest tests.signals.test_stress tests.test_operator_console.QMISOperatorConsoleTests -v`

**Step 5: Commit**
- Commit once analysis wiring and tests are green.

### Task 3: Dashboard, Interpreter, And API Integration

**Files:**
- Modify: `src/qmis/dashboard/cli.py`
- Modify: `src/qmis/signals/interpreter.py`
- Modify: `src/qmis/api.py`
- Modify: `tests/dashboard/test_cli.py`
- Modify: `tests/signals/test_interpreter.py`
- Modify: `tests/test_api.py`

**Step 1: Write the failing integration tests**
- Assert that dashboard snapshots load persisted stress state, `/dashboard` serializes it, and the operator console renders a prominent market-stress section.

**Step 2: Run tests to verify they fail**
- Run: `uv run python -m unittest tests.dashboard.test_cli tests.signals.test_interpreter tests.test_api -v`

**Step 3: Write minimal implementation**
- Load `stress_snapshots` into the dashboard snapshot.
- Add `market_stress` to the interpreter snapshot.
- Render a `MARKET STRESS` section in the CLI.
- Include the persisted stress object in the dashboard API response.

**Step 4: Run tests to verify they pass**
- Re-run the integration test command.

**Step 5: Commit**
- Commit once the persisted stress snapshot is visible end to end.

### Task 4: Full Backend Verification

**Files:**
- Modify: `scripts/run_analysis.py`
- Modify: `scripts/run_operator_console.py`
- Modify: `src/qmis/api.py`
- Modify: `src/qmis/dashboard/cli.py`
- Modify: `src/qmis/schema.py`
- Modify: `src/qmis/signals/interpreter.py`
- Create: `src/qmis/signals/stress.py`
- Create: `tests/signals/test_stress.py`

**Step 1: Run targeted regression tests**
- Run: `uv run python -m unittest tests.test_schema tests.signals.test_stress tests.signals.test_interpreter tests.dashboard.test_cli tests.test_api tests.test_operator_console -v`

**Step 2: Run full backend verification**
- Run: `uv run python -m unittest -v`

**Step 3: Review issue #27 acceptance criteria**
- Confirm deterministic score output, missing-input degradation, prominent console output, and documented weighting logic in code/tests.

**Step 4: Commit**
- Commit the final verified state if additional changes were required.

### Task 5: Documentation Confirmation

Statement:
No documentation updates required beyond this implementation plan because issue #27 changes internal analysis artifacts and operator/dashboard output, but does not change documented installation or public runtime commands.
