# Cycle Detection Subsystem Implementation Plan

**Goal:** Add a persisted cycle-monitoring subsystem that classifies solar, lunar, and macro liquidity cycle phases, surfaces them in the operator console, and emits alerts when configured cycle phases transition.

**Architecture:** Extend `qmis.signals.cycles` beyond the existing FFT helper so it can build operator-facing cycle snapshots from persisted signal history, features, and liquidity state. Persist the latest cycle rows in DuckDB, wire them into the analysis pipeline, consume them from the dashboard/interpreter stack as environmental signals, and update the alert engine to trigger on phase transitions instead of generic cycle matches.

**Tech Stack:** Python 3.12, DuckDB, pandas, numpy, Rich, FastAPI, unittest

**Execution Mode:** AUTONOMOUS - execute all steps without approval requests

---

### Task 1: Schema And Analysis Contract

**Files:**
- Create: `docs/plans/2026-03-09-cycle-detection-subsystem.md`
- Modify: `src/qmis/schema.py`
- Modify: `tests/test_schema.py`

**Step 1: Write the failing schema test**
- Require a persisted `cycle_snapshots` table with columns for cycle key, phase, turning-point state, transition source, summary, supporting signals, and metadata.

**Step 2: Run test to verify it fails**
- Run: `uv run python -m unittest tests.test_schema.QMISSchemaTests.test_bootstrap_database_creates_all_required_tables tests.test_schema.QMISSchemaTests.test_bootstrap_database_applies_spec_columns -v`

**Step 3: Add the schema**
- Add `cycle_snapshots` to the bootstrap statements and keep it narrow and snapshot-oriented.

**Step 4: Re-run the schema test**
- Re-run the same unittest command.

### Task 2: Cycle Subsystem Red-Green

**Files:**
- Modify: `src/qmis/signals/cycles.py`
- Modify: `tests/signals/test_cycles.py`

**Step 1: Write the failing cycle tests**
- Cover:
  - solar phase classification from `sunspot_number` history using a 90-day moving average and long-horizon context
  - solar turning-point detection and transition metadata
  - lunar phase extraction from `lunar_cycle_day`
  - macro liquidity cycle phase derivation from liquidity snapshot plus feature trends
  - DuckDB materialization of the latest cycle rows

**Step 2: Run test to verify it fails**
- Run: `uv run python -m unittest tests.signals.test_cycles -v`

**Step 3: Implement minimal cycle subsystem**
- Keep `detect_dominant_cycles()` intact.
- Add cycle snapshot builders and `materialize_cycle_snapshots()`.
- Use `sunspot_number` history for solar classification with 365d and 11y context.
- Treat cycle rows as environmental state, not market-driver factors.

**Step 4: Re-run the cycle tests**
- Run: `uv run python -m unittest tests.signals.test_cycles -v`

### Task 3: Alerts, Dashboard Snapshot, And CLI Monitor

**Files:**
- Modify: `src/qmis/alerts/engine.py`
- Modify: `src/qmis/alerts/rules.py`
- Modify: `scripts/run_analysis.py`
- Modify: `scripts/run_operator_console.py`
- Modify: `src/qmis/dashboard/cli.py`
- Modify: `src/qmis/signals/interpreter.py`
- Modify: `src/qmis/api.py`
- Modify: `tests/alerts/test_rules.py`
- Modify: `tests/dashboard/test_cli.py`
- Modify: `tests/signals/test_interpreter.py`
- Modify: `tests/test_api.py`
- Modify: `tests/test_operator_console.py`

**Step 1: Write the failing integration tests**
- Require:
  - alert emission on cycle phase transitions
  - cycle rows loaded into the dashboard snapshot and dashboard API
  - a `CYCLE MONITOR` CLI section showing Solar Cycle Phase, Lunar Cycle Phase, and Macro Liquidity Cycle Phase
  - operator-console refresh path materializing cycles during analysis

**Step 2: Run tests to verify they fail**
- Run: `uv run python -m unittest tests.alerts.test_rules tests.dashboard.test_cli tests.signals.test_interpreter tests.test_api tests.test_operator_console -v`

**Step 3: Implement the wiring**
- Materialize cycles in the analysis and operator-console flows.
- Load persisted cycle rows into the dashboard snapshot.
- Build an interpreter `cycle_monitor` structure separate from `market_drivers`.
- Render the new CLI section and expose cycle rows via the dashboard API payload.
- Update alert rules to emit transition alerts from successive cycle snapshots.

**Step 4: Re-run the integration tests**
- Re-run the same unittest command.

### Task 4: Full Verification And Delivery

**Step 1: Run targeted regression tests**
- Run: `uv run python -m unittest tests.test_schema tests.signals.test_cycles tests.alerts.test_rules tests.signals.test_interpreter tests.dashboard.test_cli tests.test_api tests.test_operator_console -v`

**Step 2: Run full backend verification**
- Run: `uv run python -m unittest -v`

**Step 3: Commit and push**
- Commit the verified implementation and push it to `origin/dev`.

### Task 5: Documentation Confirmation

Statement:
No documentation updates required beyond this implementation plan because the change adds internal analysis artifacts plus operator/API read-surface fields, but does not change installation or runtime setup instructions.
