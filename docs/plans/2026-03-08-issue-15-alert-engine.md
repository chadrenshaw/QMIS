# Issue 15 Alert Engine Implementation Plan

**Goal:** Implement a persisted alert engine and explicit rule catalog for regime, threshold, correlation, anomaly, and cycle alerts.

**Architecture:** Add a repo-local `alerts` materialization layer that evaluates rules from persisted `regimes`, `relationships`, `signals`, and derived cycle/anomaly inputs, writes deduplicated alerts into DuckDB, and exposes those results through the existing alert runtime and dashboard/API read models. Keep rule evaluation pure and testable in `qmis.alerts.rules`, and keep orchestration/persistence in `qmis.alerts.engine`.

**Tech Stack:** DuckDB, pandas, FastAPI read model integration, Rich CLI integration, unittest

**Execution Mode:** AUTONOMOUS - execute all steps without approval requests

---

### Task 1: Add failing schema and alert-rule tests

**Files:**
- Modify: `tests/test_schema.py`
- Create: `tests/alerts/test_rules.py`
- Create: `tests/alerts/test_engine.py`

**Step 1: Write the failing tests**
- Add schema coverage for a persisted `alerts` table.
- Add rule coverage for regime change, threshold, correlation, anomaly, and cycle alerts.
- Add engine coverage for deduplicated alert materialization.

**Step 2: Run tests to verify they fail**
- Run: `uv run python -m unittest tests.test_schema tests.alerts.test_rules tests.alerts.test_engine -v`

### Task 2: Extend schema for persisted alerts

**Files:**
- Modify: `src/qmis/schema.py`
- Modify: `tests/test_schema.py`

**Step 1: Add the `alerts` table and migrations**
- Store deduplicated alert outputs with explicit identifiers, severity, titles, messages, and metadata.

**Step 2: Re-run schema tests**
- Run: `uv run python -m unittest tests.test_schema -v`

### Task 3: Implement explicit alert rules

**Files:**
- Create: `src/qmis/alerts/rules.py`
- Create: `tests/alerts/test_rules.py`

**Step 1: Implement pure alert evaluators**
- Regime change alerts
- Threshold alerts
- Correlation discovery alerts
- Relationship break alerts
- Cycle alerts

**Step 2: Re-run rule tests**
- Run: `uv run python -m unittest tests.alerts.test_rules -v`

### Task 4: Implement alert materialization engine

**Files:**
- Create: `src/qmis/alerts/engine.py`
- Create: `tests/alerts/test_engine.py`

**Step 1: Load persisted inputs and evaluate rules**
- Read the latest required state from DuckDB.

**Step 2: Deduplicate and materialize current alerts**
- Replace the current alert snapshot with stable rule-key-based rows.

**Step 3: Re-run engine tests**
- Run: `uv run python -m unittest tests.alerts.test_engine -v`

### Task 5: Integrate alert runtime and dashboard/API surfaces

**Files:**
- Modify: `scripts/run_alerts.py`
- Modify: `src/qmis/dashboard/cli.py`
- Modify: `src/qmis/api.py`
- Modify: `tests/dashboard/test_cli.py`
- Modify: `tests/test_api.py`
- Modify: `tests/test_qmis_foundation.py`

**Step 1: Replace the alert runtime scaffold**
- Have `run_alerts.py` execute the materialization engine and emit a meaningful summary.

**Step 2: Feed persisted alerts into dashboard/API read models**
- Replace the placeholder alert summary with real alert rows and a summary status.

**Step 3: Re-run targeted integration tests**
- Run: `uv run python -m unittest tests.dashboard.test_cli tests.test_api tests.test_qmis_foundation -v`

### Task 6: Full validation

**Files:**
- Modify: no new files expected

**Step 1: Run backend suite**
- Run: `uv run python -m unittest -v`

**Step 2: Run frontend suite**
- Run: `cd web && npm test -- --run`

**Step 3: Run frontend build**
- Run: `cd web && npm run build`

### Task 7: Documentation Confirmation

Statement:
No documentation updates required for this change beyond the persisted alert outputs now exposed through existing runtime and dashboard surfaces.
