# Macro Scoring And Regime Engine Implementation Plan

**Goal:** Implement issue #12 by computing inflation, growth, liquidity, and risk scores from the latest features and materializing the current macro regime into the `regimes` table.

**Architecture:** Keep score calculation in `qmis.signals.scoring` and regime selection plus persistence in `qmis.signals.regime`. The scoring layer should consume the latest feature rows plus the latest raw yield inputs needed for the curve spread, while the regime layer should classify the current macro state, compute a bounded confidence score, and replace the `regimes` table contents idempotently with the latest regime snapshot.

**Tech Stack:** Python 3.12, pandas, numpy, scipy, DuckDB, unittest

**Execution Mode:** AUTONOMOUS - execute all steps without approval requests

---

### Task 1: Scoring And Regime Tests

**Files:**
- Create: `tests/signals/test_scoring.py`
- Create: `tests/signals/test_regime.py`

**Step 1: Write the failing test**
- Add tests that require macro score computation from feature trends, regime classification across all spec labels, and regime-row persistence into DuckDB.

**Step 2: Run test to verify it fails**
Run: `uv run python -m unittest tests.signals.test_scoring tests.signals.test_regime -v`
Expected: FAIL because the scoring and regime modules do not exist yet.

**Step 3: Write minimal implementation**
- Add the signal modules and the smallest materialization path that satisfies the tests.

**Step 4: Run test to verify it passes**
Run: `uv run python -m unittest tests.signals.test_scoring tests.signals.test_regime -v`
Expected: PASS

### Task 2: Signal Modules And Analysis Runtime Hook

**Files:**
- Create: `src/qmis/signals/scoring.py`
- Create: `src/qmis/signals/regime.py`
- Modify: `scripts/run_analysis.py`

**Step 1: Implement score calculation**
- Compute inflation, growth, liquidity, and risk scores from the latest feature trends plus the latest yield-curve inputs.

**Step 2: Implement regime classification and confidence**
- Support every spec regime label and generate a bounded confidence output suitable for the `regimes` table.

**Step 3: Implement regime materialization**
- Read the latest `features` and required `signals`, then replace the `regimes` table contents idempotently with the latest regime row.

**Step 4: Hook regime materialization into `run_analysis.py`**
- Preserve `--dry-run` and run regime materialization after feature materialization during non-dry-run analysis execution.

### Task 3: Documentation Confirmation

Statement:
No additional documentation updates are required beyond this issue-specific plan because the macro scoring and regime contract is already defined in the specs and this plan records the concrete scoring/materialization split.
