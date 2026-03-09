# QMIS Standalone Macro Engine Implementation Plan

**Goal:** Build the standalone Python 3.12 QMIS macro market engine that fetches live market data, derives macro signals, classifies the current regime, and renders a rich dashboard with alerts.

**Architecture:** Implement the script as a small set of pure calculation and formatting helpers layered beneath a thin CLI entrypoint. Keep data acquisition isolated so tests can exercise trend, score, regime, and alert logic without hitting the network.

**Tech Stack:** Python 3.12, pandas, numpy, yfinance, requests, rich, scipy, unittest

**Execution Mode:** AUTONOMOUS - execute all steps without approval requests

---

### Task 1: Plan And Test Harness

**Files:**
- Create: `docs/plans/2026-03-08-qmis-standalone-macro-engine.md`
- Create: `tests/test_macro_sentiment_engine.py`

**Step 1: Write the failing test**
- Add tests covering trend classification, macro scoring, regime selection, and alert generation.

**Step 2: Run test to verify it fails**
Run: `uv run python -m unittest tests.test_macro_sentiment_engine -v`
Expected: FAIL because the script does not yet expose the required implementation.

**Step 3: Write minimal implementation**
- Add the public helpers required by the tests in `scripts/macro_sentiment_engine.py`.

**Step 4: Run test to verify it passes**
Run: `uv run python -m unittest tests.test_macro_sentiment_engine -v`
Expected: PASS

### Task 2: Live Data Pipeline And Dashboard

**Files:**
- Modify: `scripts/macro_sentiment_engine.py`
- Modify: `pyproject.toml`

**Step 1: Implement data fetching**
- Fetch one year of daily market data for all required tickers with `yfinance`.
- Normalize close-price handling and latest-value extraction.

**Step 2: Implement trend and derived indicator calculations**
- Compute percent change, linear regression slope, and direction for 12M / 3M / 1M.
- Derive yield-curve state, inflation/growth/risk signals, and structured output payloads.

**Step 3: Implement dashboard rendering**
- Render summary panels, score tables, sparkline output, and alert panels with `rich`.
- Keep the command cron-friendly by returning `0` on success and non-zero on fatal fetch failures.

**Step 4: Verify script execution**
Run: `uv run python scripts/macro_sentiment_engine.py`
Expected: dashboard output using live data

### Task 3: Documentation Confirmation

Statement:
No documentation updates required for this change because the request is limited to a standalone executable script and dependency wiring.
