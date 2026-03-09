# Issue #36 Macro Pressure Index Implementation Plan

**Goal:** Add a persisted Macro Pressure Index composite signal with console and alert integration so QMIS can surface systemic macro stress as a single early-warning indicator.

**Architecture:** Build a new `qmis.signals.macro_pressure` snapshot module that computes five normalized component stresses from existing signals/features, persists the latest MPI snapshot in DuckDB, then expose that snapshot through the dashboard loader, operator console, and alert rules. Keep the implementation aligned with the existing `stress`, `breadth`, `liquidity`, and `predictive` snapshot patterns so the new feature composes naturally with the current pipeline.

**Tech Stack:** Python 3.13, pandas, DuckDB, Rich console, unittest

**Execution Mode:** AUTONOMOUS - execute all steps without approval requests

---

### Task 1: Add schema and contract coverage for macro pressure snapshots

**Files:**
- Modify: `tests/test_schema.py`
- Create: `tests/signals/test_macro_pressure.py`

**Step 1: Write the failing schema test**
- Assert `macro_pressure_snapshots` exists.
- Assert the table columns are `ts`, `mpi_score`, `pressure_level`, `summary`, `components`, `primary_contributors`, `missing_inputs`.

**Step 2: Run test to verify it fails**
Run: `pytest tests/test_schema.py tests/signals/test_macro_pressure.py -q`

**Step 3: Write the failing macro pressure snapshot tests**
- Cover direct snapshot computation for elevated macro pressure.
- Cover persistence into DuckDB with normalized components and contributors.

**Step 4: Run test to verify it fails**
Run: `pytest tests/signals/test_macro_pressure.py -q`

### Task 2: Implement the macro pressure snapshot module

**Files:**
- Create: `src/qmis/signals/macro_pressure.py`
- Modify: `src/qmis/schema.py`

**Step 1: Write minimal implementation**
- Add component calculators for credit, volatility, breadth, liquidity, and yield-curve stress.
- Add composite MPI calculation and classification.
- Add persistence into `macro_pressure_snapshots`.

**Step 2: Run targeted tests**
Run: `pytest tests/test_schema.py tests/signals/test_macro_pressure.py -q`

### Task 3: Integrate MPI into dashboard loading and operator console rendering

**Files:**
- Modify: `src/qmis/dashboard/cli.py`
- Modify: `src/qmis/signals/interpreter.py`
- Modify: `scripts/run_operator_console.py`
- Modify: `tests/dashboard/test_cli.py`
- Modify: `tests/test_operator_console.py`

**Step 1: Write the failing integration tests**
- Extend dashboard loader test fixtures to seed `macro_pressure_snapshots`.
- Assert the loaded snapshot exposes `macro_pressure`.
- Assert rendered console output includes `MACRO PRESSURE INDEX` below `MARKET STRESS`.
- Assert operator-console refresh invokes the new materializer.

**Step 2: Run tests to verify they fail**
Run: `pytest tests/dashboard/test_cli.py tests/test_operator_console.py -q`

**Step 3: Write minimal integration code**
- Load the persisted MPI snapshot.
- Render score, classification, and primary contributors.
- Thread the snapshot through the operator intelligence layer.
- Refresh MPI during operator-console pipeline execution.

**Step 4: Run targeted tests**
Run: `pytest tests/dashboard/test_cli.py tests/test_operator_console.py -q`

### Task 4: Add MPI alerting and pipeline coverage

**Files:**
- Modify: `src/qmis/alerts/engine.py`
- Modify: `src/qmis/alerts/rules.py`
- Modify: `tests/alerts/test_rules.py`
- Modify: `tests/alerts/test_engine.py`

**Step 1: Write the failing alert tests**
- Assert `MPI > 70` emits a macro stress alert.
- Assert `MPI > 85` emits a systemic crisis alert.
- Assert alert materialization reads from the MPI snapshot table.

**Step 2: Run tests to verify they fail**
Run: `pytest tests/alerts/test_rules.py tests/alerts/test_engine.py -q`

**Step 3: Write minimal alert implementation**
- Load the latest MPI snapshot inside the alert engine.
- Extend rule evaluation with MPI threshold alerts and dedupe keys.

**Step 4: Run targeted tests**
Run: `pytest tests/alerts/test_rules.py tests/alerts/test_engine.py -q`

### Task 5: Documentation Confirmation

Statement:
No documentation updates required for this change beyond this implementation plan because the feature is internal code plus console output and there is no separate operator or API docs set in scope for issue #36.
