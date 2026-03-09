# Liquidity Composite State Model Implementation Plan (Issue #28)

**Goal:** Add a persisted liquidity composite that scores the current liquidity environment, labels it as `EXPANDING`, `NEUTRAL`, or `TIGHTENING`, and makes that state the operator-facing liquidity descriptor across QMIS.

**Architecture:** Introduce a dedicated `qmis.signals.liquidity` analysis module that computes a normalized liquidity score from existing QMIS inputs plus real yields, persists a latest snapshot in DuckDB, and exposes that snapshot through the dashboard snapshot, interpreter layer, factor engine, and FastAPI dashboard response. Preserve the existing regime `liquidity_score` for regime classification, but stop using it as the primary operator liquidity descriptor.

**Tech Stack:** Python 3.12, DuckDB, pandas, numpy, Rich, FastAPI, unittest

**Execution Mode:** AUTONOMOUS - execute all steps without approval requests

---

### Task 1: Schema And Data Inputs

**Files:**
- Create: `docs/plans/2026-03-09-liquidity-composite-state-model.md`
- Modify: `src/qmis/schema.py`
- Modify: `src/qmis/collectors/macro.py`
- Modify: `src/qmis/collectors/liquidity.py`
- Test: `tests/test_schema.py`
- Test: `tests/collectors/test_liquidity.py`

**Step 1: Write the failing tests**
- Assert that DuckDB bootstraps a `liquidity_snapshots` table.
- Assert that the liquidity collector now includes real yields as a first-class input.

**Step 2: Run test to verify it fails**
- Run: `uv run python -m unittest tests.test_schema tests.collectors.test_liquidity -v`

**Step 3: Add storage and collection support**
- Create the `liquidity_snapshots` table.
- Add a FRED real-yields series to macro collection and include it in the liquidity collector path.

**Step 4: Run tests to verify they pass**
- Re-run the schema and collector test command.

### Task 2: Liquidity Composite Engine

**Files:**
- Create: `src/qmis/signals/liquidity.py`
- Modify: `src/qmis/signals/factors.py`
- Modify: `scripts/run_analysis.py`
- Modify: `scripts/run_operator_console.py`
- Test: `tests/signals/test_liquidity.py`
- Test: `tests/signals/test_factors.py`
- Test: `tests/test_operator_console.py`

**Step 1: Write the failing engine tests**
- Cover normalized scoring, missing-input handling, state thresholds, persisted component metadata, and factor-engine consumption of the new liquidity state.

**Step 2: Run test to verify it fails**
- Run: `uv run python -m unittest tests.signals.test_liquidity tests.signals.test_factors tests.test_operator_console -v`

**Step 3: Write minimal implementation**
- Compute a weighted liquidity score from:
  - Fed balance sheet
  - M2 money supply
  - reverse repo usage
  - dollar index
  - real yields
- Persist the latest snapshot with component-level metadata and missing inputs.
- Wire `materialize_liquidity_state()` into the analysis and operator refresh flows.
- Allow the factor engine to consume the persisted liquidity state when assigning liquidity direction.

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
- Assert that dashboard snapshots load the persisted liquidity composite.
- Assert that `/dashboard` serializes it.
- Assert that operator-facing liquidity summaries use the persisted composite rather than the coarse regime score.

**Step 2: Run tests to verify they fail**
- Run: `uv run python -m unittest tests.dashboard.test_cli tests.signals.test_interpreter tests.test_api -v`

**Step 3: Write minimal implementation**
- Load `liquidity_snapshots` into dashboard state.
- Add the liquidity composite to interpreter outputs.
- Update the global state line, risk monitor, and watchlist to use the new liquidity state.
- Include the persisted liquidity object in the dashboard API response.

**Step 4: Run tests to verify they pass**
- Re-run the integration test command.

### Task 4: Full Backend Verification

**Files:**
- Modify: `scripts/run_analysis.py`
- Modify: `scripts/run_operator_console.py`
- Modify: `src/qmis/api.py`
- Modify: `src/qmis/collectors/liquidity.py`
- Modify: `src/qmis/collectors/macro.py`
- Modify: `src/qmis/dashboard/cli.py`
- Modify: `src/qmis/schema.py`
- Modify: `src/qmis/signals/factors.py`
- Modify: `src/qmis/signals/interpreter.py`
- Create: `src/qmis/signals/liquidity.py`
- Create: `tests/signals/test_liquidity.py`

**Step 1: Run targeted regression tests**
- Run: `uv run python -m unittest tests.test_schema tests.collectors.test_liquidity tests.signals.test_liquidity tests.signals.test_factors tests.signals.test_interpreter tests.dashboard.test_cli tests.test_api tests.test_operator_console -v`

**Step 2: Run full backend verification**
- Run: `uv run python -m unittest -v`

**Step 3: Review issue #28 acceptance criteria**
- Confirm the normalized liquidity score is persisted.
- Confirm the state is `EXPANDING`, `NEUTRAL`, or `TIGHTENING`.
- Confirm component weighting is explicit in code/tests.
- Confirm the factor engine and operator-facing summaries consume the new snapshot.

### Task 5: Documentation Confirmation

Statement:
No public documentation updates are required beyond this implementation plan because issue #28 changes internal analysis artifacts and operator/dashboard output, but does not change install or runtime command usage.
