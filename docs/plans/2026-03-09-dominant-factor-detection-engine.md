# Dominant Factor Detection Engine Implementation Plan (Issue #26)

**Goal:** Add a persisted dominant factor detection engine and surface its output in the QMIS operator console as primary market drivers.

**Architecture:** Introduce a new `qmis.signals.factors` analysis module that derives dominant factors from normalized cross-asset returns, persists the latest factor snapshot in DuckDB, and feeds the operator interpreter and Rich CLI from persisted factor data rather than ad hoc relationship grouping. Keep the current relationship and anomaly pipeline intact as supporting drill-down context.

**Tech Stack:** Python 3.12, DuckDB, pandas, numpy, Rich, unittest

**Execution Mode:** AUTONOMOUS - execute all steps without approval requests

---

### Task 1: Plan And Schema Surface

**Files:**
- Create: `docs/plans/2026-03-09-dominant-factor-detection-engine.md`
- Modify: `src/qmis/schema.py`
- Test: `tests/test_schema.py`

**Step 1: Write the failing schema test**
- Assert that DuckDB bootstraps a `factors` table with the required columns for dominant factor snapshots.

**Step 2: Run test to verify it fails**
- Run: `uv run python -m unittest tests.test_schema.QMISSchemaTests.test_bootstrap_database_creates_all_required_tables tests.test_schema.QMISSchemaTests.test_bootstrap_database_applies_spec_columns -v`

**Step 3: Add the `factors` table and migration-safe bootstrap support**
- Persist timestamp, factor name, component rank, strength, direction, summary, supporting assets, and loadings metadata.

**Step 4: Run test to verify it passes**
- Re-run the schema test command.

**Step 5: Commit**
- Commit once schema and schema tests are green.

### Task 2: Factor Engine Analysis

**Files:**
- Create: `src/qmis/signals/factors.py`
- Modify: `scripts/run_analysis.py`
- Modify: `scripts/run_operator_console.py`
- Test: `tests/signals/test_factors.py`
- Test: `tests/test_operator_console.py`

**Step 1: Write the failing factor-engine tests**
- Cover factor matrix preparation, dominant factor detection, theme mapping, direction labeling, and DuckDB materialization.

**Step 2: Run test to verify it fails**
- Run: `uv run python -m unittest tests.signals.test_factors -v`

**Step 3: Write minimal implementation**
- Build normalized return matrix from `signals`.
- Run PCA-like decomposition with numpy.
- Map components to known themes.
- Persist dominant factors into DuckDB.
- Wire `materialize_factors()` into the analysis scripts.

**Step 4: Run tests to verify they pass**
- Run: `uv run python -m unittest tests.signals.test_factors tests.test_operator_console.QMISOperatorConsoleTests -v`

**Step 5: Commit**
- Commit once factor analysis is green and wired into the runtime entrypoints.

### Task 3: Dashboard Snapshot And Interpreter Integration

**Files:**
- Modify: `src/qmis/dashboard/cli.py`
- Modify: `src/qmis/signals/interpreter.py`
- Modify: `tests/dashboard/test_cli.py`
- Modify: `tests/signals/test_interpreter.py`

**Step 1: Write the failing dashboard and interpreter tests**
- Assert that dashboard snapshots load persisted factors and that operator summaries render primary market drivers from factor data.

**Step 2: Run tests to verify they fail**
- Run: `uv run python -m unittest tests.dashboard.test_cli tests.signals.test_interpreter -v`

**Step 3: Write minimal implementation**
- Load factors into the dashboard snapshot.
- Replace heuristic driver output with persisted dominant factor output.
- Render a `PRIMARY MARKET DRIVERS` section in the Rich CLI.

**Step 4: Run tests to verify they pass**
- Re-run the dashboard and interpreter test command.

**Step 5: Commit**
- Commit once the factor-backed console output is green.

### Task 4: Full Backend Verification

**Files:**
- Modify: `scripts/run_analysis.py`
- Modify: `scripts/run_operator_console.py`
- Modify: `src/qmis/dashboard/cli.py`
- Modify: `src/qmis/schema.py`
- Modify: `src/qmis/signals/interpreter.py`
- Create: `src/qmis/signals/factors.py`
- Create: `tests/signals/test_factors.py`

**Step 1: Run targeted regression tests**
- Run: `uv run python -m unittest tests.test_schema tests.signals.test_factors tests.signals.test_interpreter tests.dashboard.test_cli tests.test_operator_console -v`

**Step 2: Run backend CI**
- Run: `scripts/ci_local_backend.sh --fast`

**Step 3: Review issue #26 against implementation**
- Confirm the factor engine, persistence, and CLI integration are satisfied.

**Step 4: Commit**
- Commit the final verified state if additional changes were required.

### Task 5: Documentation Confirmation

Statement:
No documentation updates required beyond this implementation plan because issue #26 changes internal analysis and console rendering behavior, but does not change the documented public API or runtime setup steps.
