# Scheduling Entrypoints Implementation Plan

**Goal:** Implement issue #17 by making the collector, analysis, and alert scripts explicit, cron-friendly job surfaces with spec-defined cadence boundaries.

**Architecture:** Keep scheduling external to the process, but encode the supported cadence boundaries and job manifests in code so cron or systemd can call deterministic entrypoints. Centralize the schedule definitions in a small `qmis.scheduling` module, then have `run_collectors.py`, `run_analysis.py`, and `run_alerts.py` expose `--cadence` and `--list-jobs` surfaces around those definitions.

**Tech Stack:** Python 3.12, standard library, unittest

**Execution Mode:** AUTONOMOUS - execute all steps without approval requests

---

### Task 1: Scheduling Tests

**Files:**
- Create: `tests/test_scheduling_entrypoints.py`

**Step 1: Write the failing test**
- Add tests that require explicit collector cadence groups, listable scheduled job manifests, and cadence-aware analysis/alert dry-run paths.

**Step 2: Run test to verify it fails**
Run: `uv run python -m unittest tests.test_scheduling_entrypoints -v`
Expected: FAIL because the scheduling module and cadence-aware script behavior do not exist yet.

**Step 3: Write minimal implementation**
- Add the scheduling module and the smallest script changes that satisfy the tests.

**Step 4: Run test to verify it passes**
Run: `uv run python -m unittest tests.test_scheduling_entrypoints -v`
Expected: PASS

### Task 2: Scheduling Module And Script Finalization

**Files:**
- Create: `src/qmis/scheduling.py`
- Modify: `scripts/run_collectors.py`
- Modify: `scripts/run_analysis.py`
- Modify: `scripts/run_alerts.py`

**Step 1: Implement schedule definitions**
- Encode the spec cadences for `market-15m`, `daily` collectors, `daily` analysis, and `daily` alerts.

**Step 2: Finalize `run_collectors.py`**
- Add explicit cadence grouping and a listable schedule manifest.

**Step 3: Finalize `run_analysis.py` and `run_alerts.py`**
- Add cadence-aware entrypoint arguments and shared schedule-manifest output.

**Step 4: Preserve cron/systemd usability**
- Keep the scripts noninteractive, deterministic, and exit-code clean for scheduled invocation.

### Task 3: Documentation Confirmation

Statement:
No additional documentation updates are required beyond this issue-specific plan because the spec already defines the schedule contract and the new code surfaces make those cadence boundaries explicit for operators.
