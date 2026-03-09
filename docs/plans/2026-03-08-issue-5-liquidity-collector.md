# Liquidity Collector Implementation Plan

**Goal:** Implement issue #5 by adding the QMIS liquidity collector that fetches the spec-defined liquidity inputs and persists normalized raw signal rows into DuckDB.

**Architecture:** Keep liquidity ingestion isolated in `qmis.collectors.liquidity`, but reuse the established market and macro fetch patterns rather than inventing a third source client stack. The collector should persist only the score inputs named by the spec under category `liquidity`, with stable series names and units that line up with later score computation without prematurely computing `liquidity_score` or `global_liquidity_score`.

**Tech Stack:** Python 3.12, fredapi, yfinance, requests, pandas, DuckDB, unittest

**Execution Mode:** AUTONOMOUS - execute all steps without approval requests

---

### Task 1: Liquidity Collector Tests

**Files:**
- Create: `tests/collectors/test_liquidity.py`

**Step 1: Write the failing test**
- Add tests that require normalization of the four liquidity inputs into `signals` rows and persistence into the DuckDB `signals` table.

**Step 2: Run test to verify it fails**
Run: `uv run python -m unittest tests.collectors.test_liquidity -v`
Expected: FAIL because the liquidity collector module does not exist yet.

**Step 3: Write minimal implementation**
- Add the collector module and the smallest persistence API that satisfies the tests.

**Step 4: Run test to verify it passes**
Run: `uv run python -m unittest tests.collectors.test_liquidity -v`
Expected: PASS

### Task 2: Collector Module And Runtime Hook

**Files:**
- Create: `src/qmis/collectors/liquidity.py`
- Modify: `scripts/run_collectors.py`

**Step 1: Implement liquidity series mapping**
- Add Fed balance sheet, M2 money supply, reverse repo usage, and dollar index with stable normalized series names.

**Step 2: Implement fetch and normalization**
- Reuse the existing FRED and yfinance collector helpers to fetch only the required liquidity inputs and normalize them under category `liquidity`.

**Step 3: Implement persistence**
- Insert normalized liquidity rows into DuckDB using the shared storage and schema bootstrap.

**Step 4: Hook the collector into `run_collectors.py`**
- Preserve `--dry-run` and add the liquidity execution path for non-dry-run use alongside the market and macro collectors.

### Task 3: Documentation Confirmation

Statement:
No additional documentation updates are required beyond this issue-specific plan because the liquidity collector contract is already defined in the specs and issue body.
