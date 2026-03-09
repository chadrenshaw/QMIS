# Macro Collector Implementation Plan

**Goal:** Implement issue #4 by adding the QMIS macro collector that fetches the spec-defined FRED macro series and persists normalized raw signal rows into DuckDB.

**Architecture:** Keep macro ingestion isolated in `qmis.collectors.macro`, mirroring the existing market collector shape so later analysis jobs can consume one normalized `signals` table regardless of source. The collector should fetch a bounded set of FRED series, normalize timestamps, units, and metadata consistently, and integrate into the shared collector runtime without introducing regime or liquidity logic yet.

**Tech Stack:** Python 3.12, fredapi, requests, pandas, DuckDB, unittest

**Execution Mode:** AUTONOMOUS - execute all steps without approval requests

---

### Task 1: Macro Collector Tests

**Files:**
- Create: `tests/collectors/test_macro.py`

**Step 1: Write the failing test**
- Add tests that require normalization of FRED series payloads into macro signal rows and persistence of those rows into the `signals` table.

**Step 2: Run test to verify it fails**
Run: `uv run python -m unittest tests.collectors.test_macro -v`
Expected: FAIL because the macro collector module does not exist yet.

**Step 3: Write minimal implementation**
- Add the collector module and the smallest persistence API that satisfies the tests.

**Step 4: Run test to verify it passes**
Run: `uv run python -m unittest tests.collectors.test_macro -v`
Expected: PASS

### Task 2: Collector Module And Runtime Hook

**Files:**
- Create: `src/qmis/collectors/macro.py`
- Modify: `scripts/run_collectors.py`
- Modify: `pyproject.toml`

**Step 1: Implement macro series mapping**
- Add the six spec-defined macro indicators with stable normalized series names and metadata.

**Step 2: Implement FRED fetch and normalization**
- Fetch each series through `fredapi`, normalize daily and lower-frequency timestamps into `signals` rows, and preserve source metadata.

**Step 3: Implement persistence**
- Insert normalized rows into DuckDB using the existing storage/schema bootstrap.

**Step 4: Hook the collector into `run_collectors.py`**
- Preserve `--dry-run` and add the macro execution path for non-dry-run use alongside the market collector.

### Task 3: Documentation Confirmation

Statement:
No additional documentation updates are required beyond this issue-specific plan because the macro collector contract is already defined in the specs and issue body.
