# Optional FastAPI Read Surface Implementation Plan

**Goal:** Implement issue #18 by adding an optional read-only FastAPI surface over the current DuckDB-derived outputs.

**Architecture:** Keep the API thin and read-only in `qmis.api`, with a small app factory that accepts a database path for tests and local runtime use. The endpoints should read from the same derived tables the CLI dashboard uses, expose only GET routes, and derive anomaly output from persisted relationships rather than inventing a separate alerts store.

**Tech Stack:** Python 3.12, FastAPI, Uvicorn, DuckDB, pandas, unittest

**Execution Mode:** AUTONOMOUS - execute all steps without approval requests

---

### Task 1: API Tests

**Files:**
- Create: `tests/test_api.py`

**Step 1: Write the failing test**
- Add tests for the read-only app factory, health endpoint, latest regime endpoint, latest signal snapshot endpoint, relationship listing, and anomaly/dashboard reads from DuckDB.

**Step 2: Run test to verify it fails**
Run: `uv run python -m unittest tests.test_api -v`
Expected: FAIL because the API module and dependency surface do not exist yet.

**Step 3: Write minimal implementation**
- Add the FastAPI module and the smallest dependency/runtime changes that satisfy the tests.

**Step 4: Run test to verify it passes**
Run: `uv run python -m unittest tests.test_api -v`
Expected: PASS

### Task 2: Read-Only API Module

**Files:**
- Create: `src/qmis/api.py`
- Modify: `pyproject.toml`

**Step 1: Add FastAPI dependencies**
- Add `fastapi` and `uvicorn` to the project dependencies so the optional API can run under `uv`.

**Step 2: Implement the app factory**
- Expose a read-only app with GET routes for health, current regime, latest signal snapshot, relationships, anomalies, and dashboard snapshot.

**Step 3: Keep storage aligned**
- Read directly from DuckDB-derived outputs and reuse existing anomaly/dashboard helpers where appropriate.

### Task 3: Documentation Confirmation

Statement:
No additional documentation updates are required beyond this issue-specific plan because the API surface is explicitly optional in the spec and the endpoints are derived directly from the existing stored model.
