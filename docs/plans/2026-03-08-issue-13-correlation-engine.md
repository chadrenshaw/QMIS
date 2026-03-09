# Cross-Domain Correlation Engine Implementation Plan

**Goal:** Implement issue #13 by computing cross-domain correlations with significance testing, confidence labeling, and relationship-state persistence into `relationships`.

**Architecture:** Keep the correlation computation and guardrail logic in `qmis.signals.correlations`, and extend the `relationships` table shape just enough to persist the required confidence label from the addendum. The engine should consume raw `signals`, compute pairwise correlations across the spec windows, apply multiple-testing and persistence guardrails, derive relationship states from cross-window behavior, and materialize the latest relationship snapshot idempotently.

**Tech Stack:** Python 3.12, pandas, numpy, scipy, DuckDB, unittest

**Execution Mode:** AUTONOMOUS - execute all steps without approval requests

---

### Task 1: Correlation Engine Tests

**Files:**
- Create: `tests/signals/test_correlations.py`
- Modify: `tests/test_schema.py`

**Step 1: Write the failing test**
- Add tests that require validated stable relationships, exploratory confidence labeling, broken short-term relationships, `5y` window support, and persistence of `confidence_label` in `relationships`.

**Step 2: Run test to verify it fails**
Run: `uv run python -m unittest tests.signals.test_correlations tests.test_schema -v`
Expected: FAIL because the correlation module and schema extension do not exist yet.

**Step 3: Write minimal implementation**
- Add the correlation module and the smallest schema/runtime updates that satisfy the tests.

**Step 4: Run test to verify it passes**
Run: `uv run python -m unittest tests.signals.test_correlations tests.test_schema -v`
Expected: PASS

### Task 2: Correlation Engine And Runtime Hook

**Files:**
- Create: `src/qmis/signals/correlations.py`
- Modify: `src/qmis/schema.py`
- Modify: `scripts/run_analysis.py`

**Step 1: Implement pairwise correlation computation**
- Compute pairwise raw-signal correlations for 30d, 90d, 365d, and 1825d windows using overlapping timestamps only.

**Step 2: Implement guardrails**
- Apply Bonferroni-style multiple-testing correction and a minimum persistence rule based on significant agreement across windows.

**Step 3: Implement relationship states and confidence labels**
- Emit `stable`, `emerging`, `weakening`, `broken`, and `exploratory` states plus `validated`, `statistically_significant`, `tentative`, `exploratory`, and `likely_spurious` confidence labels.

**Step 4: Implement materialization**
- Replace the current `relationships` rows idempotently with the latest correlation snapshot.

**Step 5: Hook correlation materialization into `run_analysis.py`**
- Preserve `--dry-run` and materialize relationships after features and regimes.

### Task 3: Documentation Confirmation

Statement:
No additional documentation updates are required beyond this issue-specific plan because the authoritative contracts remain in the specs and this plan captures the concrete storage/runtime choices needed for implementation.
