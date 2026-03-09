# Breadth Collector Implementation Plan

**Goal:** Implement issue #8 by selecting a concrete breadth-data provider strategy and persisting the spec-defined breadth metrics into DuckDB.

**Architecture:** Resolve the source-selection spike by separating membership from pricing. Use a public S&P 500 constituents CSV as the membership source and yfinance as the price-history source, then derive breadth metrics locally into normalized `signals` rows under category `breadth`. This keeps the collector aligned with the spec without introducing a paid or speculative breadth provider.

**Tech Stack:** Python 3.12, requests, pandas, yfinance, DuckDB, unittest

**Execution Mode:** AUTONOMOUS - execute all steps without approval requests

---

### Task 1: Breadth Collector Tests

**Files:**
- Create: `tests/collectors/test_breadth.py`

**Step 1: Write the failing test**
- Add tests that require breadth metric derivation from constituent membership plus price history and persistence into the DuckDB `signals` table.

**Step 2: Run test to verify it fails**
Run: `uv run python -m unittest tests.collectors.test_breadth -v`
Expected: FAIL because the breadth collector module does not exist yet.

**Step 3: Write minimal implementation**
- Add the collector module and the smallest persistence API that satisfies the tests.

**Step 4: Run test to verify it passes**
Run: `uv run python -m unittest tests.collectors.test_breadth -v`
Expected: PASS

### Task 2: Collector Module And Runtime Hook

**Files:**
- Create: `src/qmis/collectors/breadth.py`
- Modify: `scripts/run_collectors.py`

**Step 1: Implement provider selection in code**
- Document and implement the chosen breadth provider approach: public S&P 500 constituent CSV plus yfinance price history.

**Step 2: Implement breadth derivation**
- Derive `sp500_above_200dma`, `advance_decline_line`, `new_highs`, and `new_lows` from constituent daily closes.

**Step 3: Implement persistence**
- Insert normalized breadth rows into DuckDB using the shared storage and schema bootstrap.

**Step 4: Hook the collector into `run_collectors.py`**
- Preserve `--dry-run` and add the breadth execution path for non-dry-run use alongside the existing collectors.

### Task 3: Documentation Confirmation

Statement:
No additional documentation updates are required beyond this issue-specific plan because the breadth collector contract is already defined in the specs and this plan explicitly records the selected provider strategy.
