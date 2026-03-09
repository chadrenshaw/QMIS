# Feature Engine Implementation Plan

**Goal:** Implement issue #11 by computing derived features from raw `signals` rows and materializing them into the `features` table.

**Architecture:** Keep the numerical helpers split across the spec-aligned feature modules and centralize orchestration in `qmis.features.normalization`. `trends.py` should own percent change, slope, and trend labeling; `momentum.py` should expose moving-average helpers used by normalization logic; `volatility.py` should own rolling volatility and drawdown math; and `normalization.py` should read `signals`, build per-series feature rows, and replace the `features` table contents idempotently.

**Tech Stack:** Python 3.12, pandas, numpy, scipy, DuckDB, unittest

**Execution Mode:** AUTONOMOUS - execute all steps without approval requests

---

### Task 1: Feature Engine Tests

**Files:**
- Create: `tests/features/test_trends.py`
- Create: `tests/features/test_momentum.py`
- Create: `tests/features/test_volatility.py`
- Create: `tests/features/test_normalization.py`

**Step 1: Write the failing test**
- Add tests that require 30d/90d/365d percent changes, moving averages, rolling volatility, slope, z-score, drawdown, trend labels, and DuckDB materialization into `features`.

**Step 2: Run test to verify it fails**
Run: `uv run python -m unittest tests.features.test_trends tests.features.test_momentum tests.features.test_volatility tests.features.test_normalization -v`
Expected: FAIL because the feature modules do not exist yet.

**Step 3: Write minimal implementation**
- Add the feature modules and the smallest materialization path that satisfies the tests.

**Step 4: Run test to verify it passes**
Run: `uv run python -m unittest tests.features.test_trends tests.features.test_momentum tests.features.test_volatility tests.features.test_normalization -v`
Expected: PASS

### Task 2: Feature Modules And Analysis Runtime Hook

**Files:**
- Create: `src/qmis/features/trends.py`
- Create: `src/qmis/features/momentum.py`
- Create: `src/qmis/features/volatility.py`
- Create: `src/qmis/features/normalization.py`
- Modify: `scripts/run_analysis.py`

**Step 1: Implement numerical helpers**
- Add moving average, percent change, z-score, rolling volatility, slope, drawdown, and trend-label helpers with the spec thresholds.

**Step 2: Implement feature materialization**
- Read `signals`, compute per-series feature rows, and replace the `features` table contents idempotently.

**Step 3: Hook the feature engine into `run_analysis.py`**
- Preserve `--dry-run` and add a real feature-materialization execution path for non-dry-run use.

### Task 3: Documentation Confirmation

Statement:
No additional documentation updates are required beyond this issue-specific plan because the feature-engine contract is already defined in the specs and this plan records the module split and materialization strategy.
