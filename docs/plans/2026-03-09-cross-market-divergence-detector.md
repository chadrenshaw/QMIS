# Cross-Market Divergence Detector Implementation Plan (Issue #31)

**Goal:** Detect and rank a small set of regime-relevant cross-market divergences from existing QMIS relationship and feature data, then surface them in operator-facing outputs.

**Architecture:** Add a focused divergence module on top of the current `relationships` plus `anomalies` pipeline instead of creating a parallel engine. The detector will map canonical divergence templates to current relationship windows and recent feature trends, return operator-ready findings with persistence evidence, and feed those findings into the dashboard snapshot, API, interpreter, and CLI.

**Tech Stack:** Python 3.12, DuckDB, pandas, FastAPI, Rich, unittest

**Execution Mode:** AUTONOMOUS - execute all steps without approval requests

---

### Task 1: Divergence Detection Tests

**Files:**
- Create: `tests/signals/test_divergence.py`

**Step 1: Write the failing test**
- Cover:
  - canonical divergence detection for equities vs copper, gold vs yields, and crypto vs liquidity
  - ranking by strength
  - persistence filtering so single-window noise does not become a divergence finding

**Step 2: Run test to verify it fails**
- Run: `uv run python -m unittest tests.signals.test_divergence -v`

**Step 3: Write minimal implementation**
- Add a divergence detector module that:
  - consumes existing relationship windows and latest features
  - maps canonical templates to known asset pairs
  - computes expected direction, observed divergence, persistence, and strength

**Step 4: Run test to verify it passes**
- Re-run: `uv run python -m unittest tests.signals.test_divergence -v`

### Task 2: Snapshot, API, And CLI Surface

**Files:**
- Modify: `src/qmis/dashboard/cli.py`
- Modify: `src/qmis/api.py`
- Modify: `tests/dashboard/test_cli.py`
- Modify: `tests/test_api.py`

**Step 1: Write the failing integration tests**
- Assert that:
  - dashboard snapshot includes `divergences`
  - `/divergences` exposes the new findings
  - `/dashboard` serializes them
  - the CLI renders a cross-market divergence section

**Step 2: Run tests to verify they fail**
- Run: `uv run python -m unittest tests.dashboard.test_cli tests.test_api -v`

**Step 3: Write minimal implementation**
- Compute divergences during dashboard snapshot loading.
- Add a dedicated API endpoint and dashboard serialization for divergences.
- Add a `CROSS-MARKET DIVERGENCES` console panel.

**Step 4: Run tests to verify they pass**
- Re-run: `uv run python -m unittest tests.dashboard.test_cli tests.test_api -v`

### Task 3: Interpreter And Risk Monitor Integration

**Files:**
- Modify: `src/qmis/signals/interpreter.py`
- Modify: `tests/signals/test_interpreter.py`

**Step 1: Write the failing interpreter test**
- Assert that:
  - divergence findings are included in operator intelligence
  - the risk monitor gets a divergence risk row
  - warning signals/narrative output prioritize the top divergence when present

**Step 2: Run test to verify it fails**
- Run: `uv run python -m unittest tests.signals.test_interpreter -v`

**Step 3: Write minimal implementation**
- Add divergence summaries to the operator intelligence payload.
- Add divergence risk classification to the risk monitor.
- Use top divergences in warning and watchlist narratives.

**Step 4: Run test to verify it passes**
- Re-run: `uv run python -m unittest tests.signals.test_interpreter -v`

### Task 4: Full Backend Verification

**Files:**
- Create: `src/qmis/signals/divergence.py`
- Modify: `src/qmis/api.py`
- Modify: `src/qmis/dashboard/cli.py`
- Modify: `src/qmis/signals/interpreter.py`
- Modify: `tests/dashboard/test_cli.py`
- Modify: `tests/signals/test_divergence.py`
- Modify: `tests/signals/test_interpreter.py`
- Modify: `tests/test_api.py`

**Step 1: Run targeted regression tests**
- Run: `uv run python -m unittest tests.signals.test_divergence tests.signals.test_interpreter tests.dashboard.test_cli tests.test_api -v`

**Step 2: Run full backend verification**
- Run: `uv run python -m unittest -v`

**Step 3: Review issue #31 acceptance criteria**
- Confirm predefined divergence templates are detected and ranked.
- Confirm persistence filtering suppresses noise.
- Confirm CLI/API/operator intelligence all expose the new divergence findings.
- Confirm the risk monitor and warning narratives include divergence context.

### Task 5: Documentation Confirmation

Statement:
No public documentation updates are required beyond this implementation plan because issue #31 changes internal analytical outputs and operator console content, but does not change install or runtime command usage.
