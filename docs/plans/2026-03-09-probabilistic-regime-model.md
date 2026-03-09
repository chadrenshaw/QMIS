# Probabilistic Regime Model Implementation Plan (Issue #30)

**Goal:** Extend the regime layer so QMIS persists a normalized probability distribution across supported regimes while preserving the existing headline regime label for backward compatibility.

**Architecture:** Keep the current rule-based headline regime selection in `qmis.signals.regime`, but augment it with a probability vector and explanatory driver metadata derived from the current macro score stack plus the newer breadth, liquidity, stress, and factor snapshots. Persist those fields on the existing `regimes` row so the current CLI/API access pattern stays stable while gaining probabilistic detail.

**Tech Stack:** Python 3.12, DuckDB, pandas, Rich, FastAPI, unittest

**Execution Mode:** AUTONOMOUS - execute all steps without approval requests

---

### Task 1: Schema And Regime Snapshot Surface

**Files:**
- Create: `docs/plans/2026-03-09-probabilistic-regime-model.md`
- Modify: `src/qmis/schema.py`
- Test: `tests/test_schema.py`

**Step 1: Write the failing schema test**
- Assert that `regimes` includes JSON fields for the probability vector and regime-driver metadata.

**Step 2: Run test to verify it fails**
- Run: `uv run python -m unittest tests.test_schema.QMISSchemaTests.test_bootstrap_database_applies_spec_columns -v`

**Step 3: Add schema support**
- Extend `regimes` with probability and driver metadata columns.
- Add `ALTER TABLE` migrations so existing databases upgrade in place.

**Step 4: Run test to verify it passes**
- Re-run the schema test command.

### Task 2: Probabilistic Regime Engine

**Files:**
- Modify: `src/qmis/signals/regime.py`
- Test: `tests/signals/test_regime.py`

**Step 1: Write the failing regime tests**
- Cover:
  - normalized probabilities summing to 100
  - mixed signals producing non-trivial secondary regime probabilities
  - persisted probability and driver metadata on the latest regime row

**Step 2: Run test to verify it fails**
- Run: `uv run python -m unittest tests.signals.test_regime -v`

**Step 3: Write minimal implementation**
- Keep `determine_regime()` for backward-compatible headline selection.
- Add a probability builder that consumes:
  - macro scores
  - breadth state
  - liquidity composite
  - market stress
  - factor results
- Persist:
  - `regime_probabilities`
  - `regime_drivers`
- Keep `confidence` aligned with the top regime probability.

**Step 4: Run tests to verify they pass**
- Re-run the regime test command.

### Task 3: Dashboard, API, And CLI Integration

**Files:**
- Modify: `src/qmis/dashboard/cli.py`
- Modify: `src/qmis/api.py`
- Modify: `src/qmis/signals/interpreter.py`
- Modify: `tests/dashboard/test_cli.py`
- Modify: `tests/test_api.py`
- Modify: `tests/signals/test_interpreter.py`

**Step 1: Write the failing integration tests**
- Assert that:
  - dashboard snapshots include regime probabilities and drivers
  - `/regime/latest` and `/dashboard` serialize them
  - the CLI renders a probabilistic regime section

**Step 2: Run tests to verify they fail**
- Run: `uv run python -m unittest tests.dashboard.test_cli tests.test_api tests.signals.test_interpreter -v`

**Step 3: Write minimal implementation**
- Load the new regime metadata into the dashboard snapshot.
- Surface probabilities in the API.
- Add a `REGIME PROBABILITIES` console section.
- Include top regime probabilities in interpreter output for operator consumption.

**Step 4: Run tests to verify they pass**
- Re-run the integration test command.

### Task 4: Full Backend Verification

**Files:**
- Modify: `src/qmis/api.py`
- Modify: `src/qmis/dashboard/cli.py`
- Modify: `src/qmis/schema.py`
- Modify: `src/qmis/signals/interpreter.py`
- Modify: `src/qmis/signals/regime.py`
- Modify: `tests/dashboard/test_cli.py`
- Modify: `tests/signals/test_interpreter.py`
- Modify: `tests/signals/test_regime.py`
- Modify: `tests/test_api.py`
- Modify: `tests/test_schema.py`

**Step 1: Run targeted regression tests**
- Run: `uv run python -m unittest tests.test_schema tests.signals.test_regime tests.signals.test_interpreter tests.dashboard.test_cli tests.test_api -v`

**Step 2: Run full backend verification**
- Run: `uv run python -m unittest -v`

**Step 3: Review issue #30 acceptance criteria**
- Confirm the probability distribution is normalized and bounded.
- Confirm the headline regime label remains backward compatible.
- Confirm enough metadata is persisted to explain top regime movement.
- Confirm mixed-signal tests behave sensibly.

### Task 5: Documentation Confirmation

Statement:
No public documentation updates are required beyond this implementation plan because issue #30 changes internal analysis artifacts and operator/dashboard output, but does not change install or runtime command usage.
