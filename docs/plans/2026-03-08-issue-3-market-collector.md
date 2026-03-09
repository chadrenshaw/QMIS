# Market Collector Implementation Plan

**Goal:** Implement issue #3 by adding the QMIS market collector that fetches the spec-defined market assets with yfinance and persists normalized raw signal rows into DuckDB.

**Architecture:** Keep market ingestion isolated in `qmis.collectors.market`, with one normalization pass turning Yahoo Finance daily close data into `signals` table rows. Integrate the collector into the existing `scripts/run_collectors.py` scaffold without broadening scope into macro, liquidity, or analysis logic.

**Tech Stack:** Python 3.12, yfinance, pandas, DuckDB, requests, unittest

**Execution Mode:** AUTONOMOUS - execute all steps without approval requests

---

### Task 1: Market Collector Tests

**Files:**
- Create: `tests/collectors/test_market.py`

**Step 1: Write the failing test**
- Add tests that require normalization of a yfinance multi-index payload into market signal rows and persistence of those rows into the `signals` table.

**Step 2: Run test to verify it fails**
Run: `uv run python -m unittest tests.collectors.test_market -v`
Expected: FAIL because the market collector module does not exist yet.

**Step 3: Write minimal implementation**
- Add the collector module and the smallest persistence API that satisfies the tests.

**Step 4: Run test to verify it passes**
Run: `uv run python -m unittest tests.collectors.test_market -v`
Expected: PASS

### Task 2: Collector Module And Runtime Hook

**Files:**
- Create: `src/qmis/collectors/market.py`
- Modify: `scripts/run_collectors.py`

**Step 1: Implement market asset mapping**
- Add the six spec-defined market assets and stable normalized series names.

**Step 2: Implement data fetch and normalization**
- Fetch daily closes from yfinance and transform them into `signals` rows.

**Step 3: Implement persistence**
- Insert normalized rows into DuckDB using the existing storage/schema bootstrap.

**Step 4: Hook the collector into `run_collectors.py`**
- Preserve `--dry-run` and add the minimal market execution path for non-dry-run use.

### Task 3: Documentation Confirmation

Statement:
No additional documentation updates are required beyond this issue-specific plan because the market collector contract is already defined in the specs and issue body.
