# Operator Briefing Console Implementation Plan

**Goal:** Redesign the QMIS CLI operator console so it reads like a compressed intelligence briefing instead of a collection of raw statistical tables.

**Architecture:** Keep the existing DuckDB snapshot loader and data pipeline intact, but expand `src/qmis/signals/interpreter.py` so it derives compact briefing sections from the loaded snapshot. Rework `src/qmis/dashboard/cli.py` to render line-oriented Rich panels and narrative sections in a fixed operator-first order.

**Tech Stack:** Python 3.12, Rich, DuckDB snapshot loader, unittest

**Execution Mode:** AUTONOMOUS - execute all steps without approval requests

---

### Task 1: Update briefing-oriented tests

**Files:**
- Modify: `tests/signals/test_interpreter.py`
- Modify: `tests/dashboard/test_cli.py`

**Step 1: Write the failing tests**
- Change interpreter expectations from table-oriented section data to briefing-oriented summaries.
- Change dashboard render expectations to assert the new section headings and one-line summary content.

**Step 2: Run test to verify it fails**
Run: `uv run python -m unittest tests.signals.test_interpreter tests.dashboard.test_cli -v`
Expected: FAIL because the current interpreter and CLI still emit the previous snapshot layout.

### Task 2: Extend interpreter summaries

**Files:**
- Modify: `src/qmis/signals/interpreter.py`
- Test: `tests/signals/test_interpreter.py`

**Step 1: Write minimal implementation**
- Add helpers for:
  - global state line
  - market pulse direction summary
  - cosmic state line
  - narrative market drivers
  - compressed relationship shift summaries
  - risk monitor categorical severity
  - warning signal selection
  - experimental signal visibility only when strong experimental correlations exist

**Step 2: Run focused tests**
Run: `uv run python -m unittest tests.signals.test_interpreter -v`
Expected: PASS

### Task 3: Rebuild CLI rendering around the briefing

**Files:**
- Modify: `src/qmis/dashboard/cli.py`
- Test: `tests/dashboard/test_cli.py`

**Step 1: Write minimal implementation**
- Replace the current multi-table render order with the new section order:
  - Global State
  - Market Pulse
  - Cosmic State
  - Market Drivers
  - Relationship Shifts
  - Risk Monitor
  - Warning Signals
  - Experimental Signals
- Reduce table usage in favor of Rich text, panels, and compact grids.

**Step 2: Run focused tests**
Run: `uv run python -m unittest tests.dashboard.test_cli -v`
Expected: PASS

### Task 4: Validation

**Files:**
- Modify: none
- Test: `tests/signals/test_interpreter.py`
- Test: `tests/dashboard/test_cli.py`

**Step 1: Run targeted verification**
Run: `uv run python -m unittest tests.signals.test_interpreter tests.dashboard.test_cli -v`

**Step 2: Run full verification**
Run: `uv run python -m unittest -v`

**Step 3: Smoke-check operator console**
Run: `uv run python scripts/run_operator_console.py --no-refresh`

### Task 5: Documentation Confirmation

Statement:
No documentation updates required for this change.
