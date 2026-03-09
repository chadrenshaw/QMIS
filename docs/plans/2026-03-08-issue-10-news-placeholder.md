# News Collector Placeholder Implementation Plan

**Goal:** Implement issue #10 by adding the spec-required `collectors/news.py` placeholder without inventing a news source that is not defined in the authoritative docs.

**Architecture:** Keep the placeholder isolated and explicit. The module should expose a minimal collector interface plus a clear failure path that explains why the collector is not yet runnable. It should not be wired into the normal collector runtime, because doing so would turn an intentionally unresolved design choice into a runtime failure for the rest of the system.

**Tech Stack:** Python 3.12, standard library, unittest

**Execution Mode:** AUTONOMOUS - execute all steps without approval requests

---

### Task 1: Placeholder Tests

**Files:**
- Create: `tests/collectors/test_news.py`

**Step 1: Write the failing test**
- Add tests that require a placeholder module, a dedicated not-configured exception, and a clear runtime failure path if the placeholder is invoked.

**Step 2: Run test to verify it fails**
Run: `uv run python -m unittest tests.collectors.test_news -v`
Expected: FAIL because the news collector module does not exist yet.

**Step 3: Write minimal implementation**
- Add the placeholder module and the smallest explicit error path that satisfies the tests.

**Step 4: Run test to verify it passes**
Run: `uv run python -m unittest tests.collectors.test_news -v`
Expected: PASS

### Task 2: Placeholder Module

**Files:**
- Create: `src/qmis/collectors/news.py`

**Step 1: Implement placeholder exception and metadata**
- Add a dedicated exception and stable explanatory text describing the missing source/provider decision.

**Step 2: Implement stub interface**
- Add a thin collector entrypoint that raises the dedicated error when invoked.

**Step 3: Keep runtime isolated**
- Do not wire the placeholder into `scripts/run_collectors.py` until a source is chosen.

### Task 3: Documentation Confirmation

Statement:
No additional documentation updates are required beyond this issue-specific plan because the placeholder exists specifically to preserve the spec-aligned layout while source selection remains unresolved.
