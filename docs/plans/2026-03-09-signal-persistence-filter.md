# Signal Persistence Filter Implementation Plan (Issue #32)

**Goal:** Add a shared persistence filter so QMIS promotes only sufficiently durable factors, divergences, and relationship-break alerts while keeping raw detections visible for debugging.

**Architecture:** Introduce a small persistence module that centralizes configurable thresholds and persistence metadata generation by signal family. Use that shared metadata to enrich raw detections, gate alert promotion, and filter operator-facing drivers, divergences, and relationship-shift narratives without removing the underlying raw rows from dashboard and API snapshots.

**Tech Stack:** Python 3.12, DuckDB, pandas, FastAPI, Rich, unittest

**Execution Mode:** AUTONOMOUS - execute all steps without approval requests

---

### Task 1: Persistence Metadata Tests

**Files:**
- Create: `tests/signals/test_persistence.py`
- Modify: `tests/signals/test_divergence.py`
- Modify: `tests/signals/test_anomalies.py`

**Step 1: Write the failing tests**
- Cover:
  - transient, emerging, and persistent persistence classifications
  - divergence detections staying available as raw rows even when they fail promotion
  - relationship anomalies carrying persistence metadata

**Step 2: Run tests to verify they fail**
- Run: `uv run python -m unittest tests.signals.test_persistence tests.signals.test_divergence tests.signals.test_anomalies -v`

**Step 3: Write minimal implementation**
- Add a shared persistence helper module with configurable thresholds by family.
- Enrich raw divergence and anomaly outputs with persistence metadata instead of filtering them out.

**Step 4: Run tests to verify they pass**
- Re-run: `uv run python -m unittest tests.signals.test_persistence tests.signals.test_divergence tests.signals.test_anomalies -v`

### Task 2: Promotion Gating For Drivers And Relationship Alerts

**Files:**
- Modify: `src/qmis/signals/factors.py`
- Modify: `src/qmis/signals/interpreter.py`
- Modify: `src/qmis/alerts/rules.py`
- Modify: `tests/signals/test_factors.py`
- Modify: `tests/signals/test_interpreter.py`
- Modify: `tests/alerts/test_rules.py`

**Step 1: Write the failing tests**
- Assert that:
  - factors expose persistence metadata
  - non-persistent factors remain in raw data but are not promoted into primary market drivers
  - non-persistent relationship anomalies do not produce alerts

**Step 2: Run tests to verify they fail**
- Run: `uv run python -m unittest tests.signals.test_factors tests.signals.test_interpreter tests.alerts.test_rules -v`

**Step 3: Write minimal implementation**
- Compute factor persistence from existing feature windows.
- Gate `market_drivers`, `relationship_shifts`, and alert-producing relationship changes on family-specific persistence thresholds.
- Include persistence metadata in promoted summaries and alert metadata.

**Step 4: Run tests to verify they pass**
- Re-run: `uv run python -m unittest tests.signals.test_factors tests.signals.test_interpreter tests.alerts.test_rules -v`

### Task 3: Dashboard And API Snapshot Integration

**Files:**
- Modify: `src/qmis/dashboard/cli.py`
- Modify: `src/qmis/api.py`
- Modify: `tests/dashboard/test_cli.py`
- Modify: `tests/test_api.py`

**Step 1: Write the failing integration tests**
- Assert that:
  - dashboard snapshots expose persistence metadata on raw detections
  - promoted operator output reflects the persistence filter
  - API responses preserve raw rows for debugging while promoted narratives stay filtered

**Step 2: Run tests to verify they fail**
- Run: `uv run python -m unittest tests.dashboard.test_cli tests.test_api -v`

**Step 3: Write minimal implementation**
- Enrich dashboard snapshot factors/divergences/anomalies with persistence metadata.
- Ensure the API serializes those enriched raw rows cleanly.
- Keep operator-facing panels driven by the filtered interpreter output.

**Step 4: Run tests to verify they pass**
- Re-run: `uv run python -m unittest tests.dashboard.test_cli tests.test_api -v`

### Task 4: Full Backend Verification

**Files:**
- Create: `src/qmis/signals/persistence.py`
- Modify: `src/qmis/alerts/rules.py`
- Modify: `src/qmis/api.py`
- Modify: `src/qmis/dashboard/cli.py`
- Modify: `src/qmis/signals/anomalies.py`
- Modify: `src/qmis/signals/divergence.py`
- Modify: `src/qmis/signals/factors.py`
- Modify: `src/qmis/signals/interpreter.py`
- Modify: `tests/alerts/test_rules.py`
- Modify: `tests/dashboard/test_cli.py`
- Modify: `tests/signals/test_anomalies.py`
- Modify: `tests/signals/test_divergence.py`
- Modify: `tests/signals/test_factors.py`
- Modify: `tests/signals/test_interpreter.py`
- Modify: `tests/signals/test_persistence.py`
- Modify: `tests/test_api.py`

**Step 1: Run targeted regression tests**
- Run: `uv run python -m unittest tests.signals.test_persistence tests.signals.test_anomalies tests.signals.test_divergence tests.signals.test_factors tests.signals.test_interpreter tests.alerts.test_rules tests.dashboard.test_cli tests.test_api -v`

**Step 2: Run full backend verification**
- Run: `uv run python -m unittest -v`

**Step 3: Review issue #32 acceptance criteria**
- Confirm noisy one-window promotions are suppressed.
- Confirm persistence metadata is visible alongside raw detections.
- Confirm alert engine and operator snapshot use the filter consistently.
- Confirm transient, emerging, and persistent cases are covered by tests.

### Task 5: Documentation Confirmation

Statement:
No public documentation updates are required beyond this implementation plan because issue #32 changes internal signal-promotion rules and operator output, but does not change install or runtime command usage.
