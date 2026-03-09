# Rich CLI Dashboard Implementation Plan

**Goal:** Implement issue #16 by rendering a spec-aligned Rich CLI dashboard from the DuckDB-derived outputs.

**Architecture:** Build the dashboard in `qmis.dashboard.cli` as a snapshot loader plus a Rich renderer so the display logic stays separated from storage access. Expose the renderer through a new `scripts/run_dashboard.py` runtime entrypoint that reads the current database state and prints a stable dashboard suitable for local use or scheduled execution.

**Tech Stack:** Python 3.12, DuckDB, pandas, rich, unittest

**Execution Mode:** AUTONOMOUS - execute all steps without approval requests

---

### Task 1: Dashboard Tests

**Files:**
- Create: `tests/dashboard/test_cli.py`
- Modify: `tests/test_qmis_foundation.py`

**Step 1: Write the failing test**
- Add tests for loading a dashboard snapshot from DuckDB, rendering the Rich dashboard text with the required sections, and exposing a dry-run runtime entrypoint.

**Step 2: Run test to verify it fails**
Run: `uv run python -m unittest tests.dashboard.test_cli tests.test_qmis_foundation -v`
Expected: FAIL because the dashboard module and runtime script do not exist yet.

**Step 3: Write minimal implementation**
- Add the dashboard module and the smallest runtime wiring that satisfies the tests.

**Step 4: Run test to verify it passes**
Run: `uv run python -m unittest tests.dashboard.test_cli tests.test_qmis_foundation -v`
Expected: PASS

### Task 2: Dashboard Module And Runtime Entry Point

**Files:**
- Create: `src/qmis/dashboard/cli.py`
- Create: `scripts/run_dashboard.py`

**Step 1: Implement dashboard snapshot loading**
- Read the latest feature, signal, regime, and relationship outputs from DuckDB and derive anomaly summaries from the persisted relationship rows.

**Step 2: Implement Rich rendering**
- Render the macro dashboard, score summary, current regime, and relationship/anomaly summaries in a stable CLI format.

**Step 3: Implement the runtime entrypoint**
- Add a standard `--dry-run` runtime script that renders the live dashboard on normal execution.

### Task 3: Documentation Confirmation

Statement:
No additional documentation updates are required beyond this issue-specific plan because the spec already defines the dashboard contract and this plan records the concrete runtime and rendering approach.
