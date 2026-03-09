# Operator Console CLI Implementation Plan

**Goal:** Replace the prototype-only local CLI path with a current operator console that refreshes QMIS data and renders all active signal systems, trends, alerts, and derived state from DuckDB.

**Architecture:** Keep the existing collector, analysis, alert, and dashboard modules as the source of truth. Expand the dashboard snapshot/rendering layer so it can display every latest signal grouped by category, then add a small orchestration script that runs the full pipeline and renders the dashboard in one command.

**Tech Stack:** Python 3.12, DuckDB, pandas, Rich, unittest

**Execution Mode:** AUTONOMOUS - execute all steps without approval requests

---

### Task 1: Dashboard Rendering Coverage

**Files:**
- Modify: `tests/dashboard/test_cli.py`
- Modify: `src/qmis/dashboard/cli.py`

**Step 1: Write the failing tests**
- Add a regression test that seeds all current signal categories and asserts the rendered dashboard includes grouped sections and representative signal rows for each category.
- Add assertions that the snapshot exposes grouped signal data suitable for category rendering.

**Step 2: Run test to verify it fails**
Run: `uv run python -m unittest tests.dashboard.test_cli -v`
Expected: FAIL because the current dashboard only renders a small hardcoded signal subset.

**Step 3: Write minimal implementation**
- Extend the snapshot payload with grouped latest-signal rows that include category, trend, source, unit, timestamp, and value.
- Render one Rich table per populated category while preserving the existing regime, relationships, anomalies, and alerts sections.

**Step 4: Run test to verify it passes**
Run: `uv run python -m unittest tests.dashboard.test_cli -v`
Expected: PASS

### Task 2: One-Command Operator Console

**Files:**
- Create: `scripts/run_operator_console.py`
- Modify: `tests/test_qmis_foundation.py`
- Create: `tests/test_operator_console.py`

**Step 1: Write the failing tests**
- Add a focused test for a new script that supports `--dry-run`.
- Add an orchestration test proving the script refreshes collectors, analysis, alerts, and then renders the dashboard from the current DuckDB pipeline.

**Step 2: Run test to verify it fails**
Run: `uv run python -m unittest tests.test_operator_console tests.test_qmis_foundation -v`
Expected: FAIL because the script does not exist yet.

**Step 3: Write minimal implementation**
- Add a script that defaults to refreshing all collector groups, materializing analysis and alerts, and then rendering the current dashboard.
- Support `--dry-run` and `--no-refresh` so operators can inspect planned behavior or render existing state without refetching data.

**Step 4: Run test to verify it passes**
Run: `uv run python -m unittest tests.test_operator_console tests.test_qmis_foundation -v`
Expected: PASS

### Task 3: Documentation Updates

**Files:**
- Modify: `README.md`

**Step 1: Update documentation**
- Replace the legacy-script guidance with the new operator-console command.
- Document the refresh/no-refresh behavior and clarify that the dashboard reads the current DuckDB-backed QMIS pipeline.

**Step 2: Review for accuracy**
- Ensure the README commands match the final script names and behavior exactly.

### Task 4: Verification

**Files:**
- Modify: `src/qmis/dashboard/cli.py`
- Create: `scripts/run_operator_console.py`
- Modify: `README.md`
- Modify: `tests/dashboard/test_cli.py`
- Modify: `tests/test_qmis_foundation.py`
- Create: `tests/test_operator_console.py`

**Step 1: Run targeted verification**
Run: `uv run python -m unittest tests.dashboard.test_cli tests.test_operator_console tests.test_qmis_foundation -v`

**Step 2: Run broader verification**
Run: `uv run python -m unittest -v`

**Step 3: Confirm CLI behavior**
Run: `uv run python scripts/run_operator_console.py --dry-run`
