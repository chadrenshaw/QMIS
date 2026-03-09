# Market Breadth Health Score Implementation Plan (Issue #29)

**Goal:** Add a persisted breadth composite that scores current participation across the market, labels it as `STRONG`, `WEAKENING`, or `FRAGILE`, and uses that state in operator-facing interpretation and risk monitoring.

**Architecture:** Introduce a dedicated `qmis.signals.breadth` analysis module that computes a normalized breadth score from the existing breadth primitives already collected in `signals` and `features`, persists a latest snapshot in DuckDB, and exposes that snapshot through the dashboard snapshot, interpreter layer, and FastAPI dashboard response. Preserve the raw breadth metrics in the `signals` table and grouped dashboard data so the operator can drill into the underlying inputs.

**Tech Stack:** Python 3.12, DuckDB, pandas, Rich, FastAPI, unittest

**Execution Mode:** AUTONOMOUS - execute all steps without approval requests

---

### Task 1: Schema And Snapshot Surface

**Files:**
- Create: `docs/plans/2026-03-09-market-breadth-health-score.md`
- Modify: `src/qmis/schema.py`
- Test: `tests/test_schema.py`

**Step 1: Write the failing schema test**
- Assert that DuckDB bootstraps a `breadth_snapshots` table with score, state, summary, components, and missing-input metadata.

**Step 2: Run test to verify it fails**
- Run: `uv run python -m unittest tests.test_schema.QMISSchemaTests.test_bootstrap_database_creates_all_required_tables tests.test_schema.QMISSchemaTests.test_bootstrap_database_applies_spec_columns -v`

**Step 3: Add the `breadth_snapshots` table**
- Keep the table narrow and focused on the latest derived breadth-health artifact.

**Step 4: Run test to verify it passes**
- Re-run the schema test command.

### Task 2: Breadth Health Engine

**Files:**
- Create: `src/qmis/signals/breadth.py`
- Modify: `scripts/run_analysis.py`
- Modify: `scripts/run_operator_console.py`
- Test: `tests/signals/test_breadth.py`
- Test: `tests/test_operator_console.py`

**Step 1: Write the failing breadth-engine tests**
- Cover `STRONG`, `WEAKENING`, and `FRAGILE` scenarios.
- Cover missing-input degradation and DuckDB materialization.

**Step 2: Run test to verify it fails**
- Run: `uv run python -m unittest tests.signals.test_breadth tests.test_operator_console -v`

**Step 3: Write minimal implementation**
- Compute breadth health from:
  - `advance_decline_line` trend and z-score
  - `sp500_above_200dma`
  - `new_highs` versus `new_lows`
- Persist the latest breadth snapshot.
- Wire `materialize_breadth_health()` into analysis and operator refresh flows.

**Step 4: Run tests to verify they pass**
- Re-run the targeted test command.

### Task 3: Dashboard, Interpreter, And API Integration

**Files:**
- Modify: `src/qmis/dashboard/cli.py`
- Modify: `src/qmis/signals/interpreter.py`
- Modify: `src/qmis/api.py`
- Modify: `tests/dashboard/test_cli.py`
- Modify: `tests/signals/test_interpreter.py`
- Modify: `tests/test_api.py`

**Step 1: Write the failing integration tests**
- Assert that dashboard snapshots load persisted breadth health.
- Assert that `/dashboard` serializes it.
- Assert that operator risk monitoring improves beyond single-series breadth heuristics.

**Step 2: Run tests to verify they fail**
- Run: `uv run python -m unittest tests.dashboard.test_cli tests.signals.test_interpreter tests.test_api -v`

**Step 3: Write minimal implementation**
- Load `breadth_snapshots` into dashboard state.
- Add breadth health to interpreter outputs.
- Render a `BREADTH HEALTH` section in the CLI.
- Use the breadth composite in risk monitoring and warning generation while preserving raw breadth drill-down data.

**Step 4: Run tests to verify they pass**
- Re-run the integration test command.

### Task 4: Full Backend Verification

**Files:**
- Modify: `scripts/run_analysis.py`
- Modify: `scripts/run_operator_console.py`
- Modify: `src/qmis/api.py`
- Modify: `src/qmis/dashboard/cli.py`
- Modify: `src/qmis/schema.py`
- Modify: `src/qmis/signals/interpreter.py`
- Create: `src/qmis/signals/breadth.py`
- Create: `tests/signals/test_breadth.py`

**Step 1: Run targeted regression tests**
- Run: `uv run python -m unittest tests.test_schema tests.signals.test_breadth tests.signals.test_interpreter tests.dashboard.test_cli tests.test_api tests.test_operator_console -v`

**Step 2: Run full backend verification**
- Run: `uv run python -m unittest -v`

**Step 3: Review issue #29 acceptance criteria**
- Confirm the breadth score and discrete state are persisted.
- Confirm CLI interpretation consumes the breadth snapshot.
- Confirm risk monitoring uses the new breadth state instead of only `% above 200 DMA`.
- Confirm tests cover improving, weakening, and fragile scenarios.

### Task 5: Documentation Confirmation

Statement:
No public documentation updates are required beyond this implementation plan because issue #29 changes internal analysis artifacts and operator/dashboard output, but does not change installation or runtime command usage.
