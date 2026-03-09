# Predictive Macro Signals Layer Implementation Plan (Issue #34)

**Goal:** Add a predictive macro signals layer that computes forward-looking macro indicators, feeds them into the existing regime engine, and renders a new forward macro section in the operator console.

**Architecture:** Extend the current collector layer to ingest the missing macro and market inputs needed for predictive indicators, then add a new `qmis.signals.predictive` module that computes a persisted predictive snapshot from latest signals and features. Keep the current regime engine as the persistence and orchestration point, but let it consume the predictive snapshot so the headline regime and probability distribution incorporate forward-looking macro pressure. Surface the resulting classification in the Rich dashboard snapshot and console.

**Tech Stack:** Python 3.12, DuckDB, pandas, requests, yfinance, Rich, unittest

**Execution Mode:** AUTONOMOUS - execute all steps without approval requests

---

### Task 1: Plan And Test The Input Surface

**Files:**
- Create: `docs/plans/2026-03-09-issue-34-predictive-macro-signals-layer.md`
- Modify: `tests/collectors/test_macro.py`
- Modify: `tests/collectors/test_market.py`

**Step 1: Write the failing collector tests**
- Add macro collector assertions for:
  - `DGS2`
  - `BAMLH0A0HYM2`
  - `BAA10YM`
  - `STLFSI4`
  - `T10YIE`
- Add market collector assertions for:
  - `VIX3M`
  - `VIX6M`
  - semiconductor leadership proxy
  - small-cap leadership proxy
  - bank leadership proxy
  - transportation leadership proxy
  - agriculture / broad commodity proxies used by the commodity pressure signal

**Step 2: Run tests to verify they fail**
- Run: `uv run python -m unittest tests.collectors.test_macro tests.collectors.test_market -v`
- Expected: FAIL because the new series mappings do not exist yet.

**Step 3: Write the minimal collector changes**
- Expand `src/qmis/collectors/macro.py` series definitions and yield-curve treasury support.
- Expand `src/qmis/collectors/market.py` ticker coverage for volatility term structure, leadership rotation, and commodity basket proxies.

**Step 4: Run tests to verify they pass**
- Re-run: `uv run python -m unittest tests.collectors.test_macro tests.collectors.test_market -v`

### Task 2: Add Predictive Signal Computation

**Files:**
- Create: `src/qmis/signals/predictive.py`
- Modify: `src/qmis/schema.py`
- Modify: `tests/signals/test_predictive.py`

**Step 1: Write the failing predictive signal tests**
- Cover:
  - yield-curve state classification from `yield_10y`, `yield_2y`, and `yield_3m`
  - credit spread widening and stress score
  - financial conditions trend
  - real-rate trend and shock
  - global liquidity score
  - volatility term structure state
  - manufacturing momentum
  - leadership rotation
  - commodity inflation pressure
  - persisted predictive snapshot shape

**Step 2: Run tests to verify they fail**
- Run: `uv run python -m unittest tests.signals.test_predictive -v`
- Expected: FAIL because the predictive module and storage table do not exist.

**Step 3: Write minimal implementation**
- Add a `predictive_snapshots` table to `src/qmis/schema.py`.
- Implement `src/qmis/signals/predictive.py` with:
  - `compute_yield_curve_signals()`
  - `compute_credit_spreads()`
  - `compute_financial_conditions()`
  - `compute_real_rate_signals()`
  - `compute_global_liquidity()`
  - `compute_volatility_term_structure()`
  - `compute_manufacturing_momentum()`
  - `compute_leadership_rotation()`
  - `compute_commodity_pressure()`
  - a materializer that loads latest signals / features and persists a single predictive snapshot

**Step 4: Run tests to verify they pass**
- Re-run: `uv run python -m unittest tests.signals.test_predictive -v`

### Task 3: Feed Predictive Signals Into Regime Materialization

**Files:**
- Modify: `src/qmis/signals/regime.py`
- Modify: `tests/signals/test_regime.py`

**Step 1: Write the failing regime integration tests**
- Assert that:
  - predictive snapshot data is materialized during regime refresh
  - recession-sensitive forward signals raise `RECESSION RISK` / `LIQUIDITY WITHDRAWAL` probabilities
  - supportive forward signals lift `LIQUIDITY EXPANSION` / `DISINFLATION` probabilities

**Step 2: Run tests to verify they fail**
- Run: `uv run python -m unittest tests.signals.test_regime -v`
- Expected: FAIL because regime materialization does not yet consume predictive signals.

**Step 3: Write minimal implementation**
- Materialize the predictive snapshot as part of `materialize_regime()`.
- Extend the regime probability builder to incorporate predictive signal evidence and attach predictive drivers.

**Step 4: Run tests to verify they pass**
- Re-run: `uv run python -m unittest tests.signals.test_regime -v`

### Task 4: Add Dashboard Snapshot And Console Rendering

**Files:**
- Modify: `src/qmis/dashboard/cli.py`
- Modify: `src/qmis/signals/interpreter.py`
- Modify: `tests/dashboard/test_cli.py`
- Modify: `tests/test_operator_console.py`

**Step 1: Write the failing dashboard tests**
- Assert that:
  - dashboard snapshot includes predictive macro data
  - console rendering includes `FORWARD MACRO SIGNALS`
  - the new section appears before `REGIME PROBABILITIES`

**Step 2: Run tests to verify they fail**
- Run: `uv run python -m unittest tests.dashboard.test_cli tests.test_operator_console -v`
- Expected: FAIL because the snapshot and renderer do not expose predictive signals.

**Step 3: Write minimal implementation**
- Load the latest predictive snapshot into `load_dashboard_snapshot()`.
- Add operator-facing interpretation helpers for the forward macro layer.
- Render the new `FORWARD MACRO SIGNALS` section above `REGIME PROBABILITIES`.

**Step 4: Run tests to verify they pass**
- Re-run: `uv run python -m unittest tests.dashboard.test_cli tests.test_operator_console -v`

### Task 5: Full Verification

**Files:**
- Modify: `src/qmis/collectors/macro.py`
- Modify: `src/qmis/collectors/market.py`
- Modify: `src/qmis/dashboard/cli.py`
- Modify: `src/qmis/schema.py`
- Modify: `src/qmis/signals/interpreter.py`
- Modify: `src/qmis/signals/predictive.py`
- Modify: `src/qmis/signals/regime.py`
- Modify: `tests/collectors/test_macro.py`
- Modify: `tests/collectors/test_market.py`
- Modify: `tests/dashboard/test_cli.py`
- Modify: `tests/signals/test_predictive.py`
- Modify: `tests/signals/test_regime.py`
- Modify: `tests/test_operator_console.py`

**Step 1: Run targeted regression tests**
- Run: `uv run python -m unittest tests.collectors.test_macro tests.collectors.test_market tests.signals.test_predictive tests.signals.test_regime tests.dashboard.test_cli tests.test_operator_console -v`

**Step 2: Run full backend verification**
- Run: `scripts/ci_local_backend.sh --fast`

**Step 3: Review issue #34 acceptance criteria**
- Confirm forward macro classifications are available.
- Confirm the regime engine consumes predictive inputs.
- Confirm console output clearly surfaces the next-regime indicators.

### Task 6: Documentation Confirmation

Statement:
No additional operator or README documentation updates are required beyond this implementation plan because issue #34 adds internal signal computation and console output without changing install or runtime commands.
