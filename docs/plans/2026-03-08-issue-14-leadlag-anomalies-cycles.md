# Lead-Lag, Anomaly Detection, And Cycle Analysis Implementation Plan

**Goal:** Implement issue #14 by adding lag discovery, broken-relationship anomaly detection, and FFT-based cycle analysis on top of the current Phase 5 signal engine.

**Architecture:** Persist lead-lag discoveries in the existing `relationships` table by writing nonzero `lag_days` rows alongside the zero-lag correlation rows. Keep anomaly detection as a derived module over persisted relationship-state transitions, and keep cycle analysis in `qmis.signals.cycles` as structured outputs rather than forcing spectral results into the wrong storage table.

**Tech Stack:** Python 3.12, pandas, numpy, scipy, DuckDB, unittest

**Execution Mode:** AUTONOMOUS - execute all steps without approval requests

---

### Task 1: Lead-Lag, Anomaly, And Cycle Tests

**Files:**
- Create: `tests/signals/test_leadlag.py`
- Create: `tests/signals/test_anomalies.py`
- Create: `tests/signals/test_cycles.py`

**Step 1: Write the failing test**
- Add tests for best-lag detection over a synthetic shifted series, anomaly detection from relationship-state transitions, and FFT cycle detection against a known lunar-like period.

**Step 2: Run test to verify it fails**
Run: `uv run python -m unittest tests.signals.test_leadlag tests.signals.test_anomalies tests.signals.test_cycles -v`
Expected: FAIL because the modules do not exist yet.

**Step 3: Write minimal implementation**
- Add the signal modules and the smallest runtime integration that satisfies the tests.

**Step 4: Run test to verify it passes**
Run: `uv run python -m unittest tests.signals.test_leadlag tests.signals.test_anomalies tests.signals.test_cycles -v`
Expected: PASS

### Task 2: Signal Modules And Analysis Runtime Hook

**Files:**
- Create: `src/qmis/signals/leadlag.py`
- Create: `src/qmis/signals/anomalies.py`
- Create: `src/qmis/signals/cycles.py`
- Modify: `scripts/run_analysis.py`

**Step 1: Implement lead-lag detection**
- Sweep lags from `-365` to `+365` days, find the strongest lagged relationship, and materialize the results into `relationships` with nonzero `lag_days`.

**Step 2: Implement anomaly detection**
- Derive broken/weakening anomalies from the relationship-state evolution already persisted by the correlation engine.

**Step 3: Implement FFT cycle analysis**
- Detect dominant periods with FFT, compare them to known periodicities, and return interpretable confidence-labeled outputs.

**Step 4: Hook lead-lag materialization into `run_analysis.py`**
- Preserve `--dry-run` and append lead-lag rows after zero-lag relationship materialization.

### Task 3: Documentation Confirmation

Statement:
No additional documentation updates are required beyond this issue-specific plan because the authoritative behavioral contracts remain in the specs and this plan captures the concrete persistence/runtime choices for this phase.
