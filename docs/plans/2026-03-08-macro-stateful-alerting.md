# Macro Stateful Alerting Implementation Plan

**Goal:** Extend the macro sentiment engine with SQLite-backed signal state tracking and consolidated ntfy alerts that only fire on state transitions.

**Architecture:** Keep the existing market-analysis pipeline intact, then add a persistence layer that stores discrete signal states and recent alert events. Each run compares the current snapshot with the stored snapshot, records only transitions, keeps recent events visible for 24 hours, and emits a single ntfy notification summarizing new changes.

**Tech Stack:** Python 3.12, sqlite3, pandas, requests, rich, unittest

**Execution Mode:** AUTONOMOUS - execute all steps without approval requests

---

### Task 1: Transition Tests

**Files:**
- Modify: `tests/test_macro_sentiment_engine.py`
- Test: `tests/test_macro_sentiment_engine.py`

**Step 1: Write the failing test**
- Add tests for first-run baselining, later signal transitions, 24-hour active-event visibility, and consolidated ntfy message delivery.

**Step 2: Run test to verify it fails**
Run: `uv run python -m unittest tests.test_macro_sentiment_engine -v`
Expected: FAIL because the transition-tracking and ntfy functions do not exist yet.

**Step 3: Write minimal implementation**
- Add the smallest SQLite and alert-delivery API needed to satisfy the tests.

**Step 4: Run test to verify it passes**
Run: `uv run python -m unittest tests.test_macro_sentiment_engine -v`
Expected: PASS

### Task 2: Stateful Alerting Runtime

**Files:**
- Modify: `scripts/macro_sentiment_engine.py`

**Step 1: Add SQLite persistence**
- Create and initialize state tables for current signal values and recent alert events.

**Step 2: Add signal snapshot generation**
- Convert current market analysis into stable discrete signal values and event metadata.

**Step 3: Add consolidated ntfy delivery**
- POST one summary message per run to `https://ntfy.chadlee.org/markets` when new transitions exist.

**Step 4: Add dashboard recent-change section**
- Show active alert events from the last 24 hours without resending them.

**Step 5: Verify script execution**
Run: `uv run python scripts/macro_sentiment_engine.py --no-ntfy`
Expected: dashboard output with stateful recent-change handling

### Task 3: Documentation Confirmation

Statement:
No documentation updates required for this change because the request is limited to runtime alerting behavior in the standalone script.
