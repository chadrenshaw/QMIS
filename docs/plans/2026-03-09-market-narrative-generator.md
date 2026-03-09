# Market Narrative Generator Implementation Plan (Issue #33)

**Goal:** Add a structured market narrative generator that turns the latest operator snapshot into a short, evidence-traceable macro narrative for the CLI and API.

**Architecture:** Introduce `src/qmis/signals/narrative.py` as a deterministic sentence-builder on top of existing structured outputs: regime probabilities, factors, market stress, breadth, liquidity, divergences, and warnings. Feed that narrative into the operator intelligence payload and render it in the CLI while also serializing it through the dashboard API without displacing the underlying structured panels.

**Tech Stack:** Python 3.12, DuckDB, FastAPI, Rich, unittest

**Execution Mode:** AUTONOMOUS - execute all steps without approval requests

---

### Task 1: Narrative Generator Tests

**Files:**
- Create: `tests/signals/test_narrative.py`

**Step 1: Write the failing tests**
- Cover:
  - narrative generation from structured snapshot evidence
  - factor/divergence changes causing the narrative to adapt
  - concise output that stays grounded in available data

**Step 2: Run test to verify it fails**
- Run: `uv run python -m unittest tests.signals.test_narrative -v`

**Step 3: Write minimal implementation**
- Add `src/qmis/signals/narrative.py`.
- Build short deterministic sentences from structured evidence only.
- Return narrative metadata that remains traceable to the supporting signals.

**Step 4: Run test to verify it passes**
- Re-run: `uv run python -m unittest tests.signals.test_narrative -v`

### Task 2: Operator Snapshot Integration

**Files:**
- Modify: `src/qmis/signals/interpreter.py`
- Modify: `tests/signals/test_interpreter.py`

**Step 1: Write the failing interpreter tests**
- Assert that:
  - operator intelligence includes the market narrative
  - the narrative adapts to the dominant factor and divergence context already present in the snapshot

**Step 2: Run test to verify it fails**
- Run: `uv run python -m unittest tests.signals.test_interpreter -v`

**Step 3: Write minimal implementation**
- Generate the narrative from the snapshot and add it to `build_operator_snapshot`.
- Keep narrative evidence alongside the rendered text.

**Step 4: Run test to verify it passes**
- Re-run: `uv run python -m unittest tests.signals.test_interpreter -v`

### Task 3: CLI And API Surface

**Files:**
- Modify: `src/qmis/dashboard/cli.py`
- Modify: `src/qmis/api.py`
- Modify: `tests/dashboard/test_cli.py`
- Modify: `tests/test_api.py`

**Step 1: Write the failing integration tests**
- Assert that:
  - the CLI renders a `MARKET NARRATIVE` section
  - `/dashboard` returns the narrative
  - the narrative coexists with the structured signals already exposed

**Step 2: Run tests to verify they fail**
- Run: `uv run python -m unittest tests.dashboard.test_cli tests.test_api -v`

**Step 3: Write minimal implementation**
- Add a Rich panel for the narrative without removing existing panels.
- Serialize the narrative through the dashboard API.

**Step 4: Run tests to verify they pass**
- Re-run: `uv run python -m unittest tests.dashboard.test_cli tests.test_api -v`

### Task 4: Full Backend Verification

**Files:**
- Create: `src/qmis/signals/narrative.py`
- Modify: `src/qmis/api.py`
- Modify: `src/qmis/dashboard/cli.py`
- Modify: `src/qmis/signals/interpreter.py`
- Modify: `tests/dashboard/test_cli.py`
- Modify: `tests/signals/test_interpreter.py`
- Modify: `tests/signals/test_narrative.py`
- Modify: `tests/test_api.py`

**Step 1: Run targeted regression tests**
- Run: `uv run python -m unittest tests.signals.test_narrative tests.signals.test_interpreter tests.dashboard.test_cli tests.test_api -v`

**Step 2: Run full backend verification**
- Run: `uv run python -m unittest -v`

**Step 3: Review issue #33 acceptance criteria**
- Confirm the narrative is short and grounded in available evidence.
- Confirm it adapts when factors or divergences change.
- Confirm the operator console exposes it without replacing structured data.
- Confirm API/dashboard surfaces receive the same narrative.

### Task 5: Documentation Confirmation

Statement:
No public documentation updates are required beyond this implementation plan because issue #33 changes operator interpretation output and dashboard/API payloads, but does not change install or runtime command usage.
