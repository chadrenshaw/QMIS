# DuckDB Bootstrap Implementation Plan

**Goal:** Implement issue #2 by adding the authoritative DuckDB storage layer and schema bootstrap for `signals`, `features`, `relationships`, and `regimes`.

**Architecture:** Keep storage concerns isolated in `qmis.storage` and `qmis.schema`, with config providing the default repo-local database path. Schema creation should be idempotent and callable from later collectors, analysis jobs, or tests without side effects beyond creating the database file and required tables.

**Tech Stack:** Python 3.12, DuckDB, pathlib, unittest

**Execution Mode:** AUTONOMOUS - execute all steps without approval requests

---

### Task 1: Storage And Schema Tests

**Files:**
- Create: `tests/test_storage.py`
- Create: `tests/test_schema.py`

**Step 1: Write the failing test**
- Add tests that require a DuckDB connection helper, repo-local default path resolution, idempotent schema bootstrap, and the expected spec table columns.

**Step 2: Run test to verify it fails**
Run: `uv run python -m unittest tests.test_storage tests.test_schema -v`
Expected: FAIL because the storage and schema modules do not exist yet.

**Step 3: Write minimal implementation**
- Add `src/qmis/storage.py` and `src/qmis/schema.py` with the smallest stable bootstrap API that satisfies the tests.

**Step 4: Run test to verify it passes**
Run: `uv run python -m unittest tests.test_storage tests.test_schema -v`
Expected: PASS

### Task 2: DuckDB Dependency And Bootstrap Integration

**Files:**
- Modify: `pyproject.toml`
- Create: `src/qmis/storage.py`
- Create: `src/qmis/schema.py`

**Step 1: Add DuckDB dependency**
- Update the project dependencies so the runtime can import DuckDB.

**Step 2: Implement storage connection helper**
- Create a connection function that ensures the repo-local database directory exists.

**Step 3: Implement schema bootstrap**
- Add idempotent DDL for `signals`, `features`, `relationships`, and `regimes`.

**Step 4: Verify direct bootstrap**
Run: `uv run python - <<'PY' ...`
Expected: database file exists and tables are queryable

### Task 3: Documentation Confirmation

Statement:
No additional documentation updates are required beyond this issue-specific plan because the storage contract is already described in the authoritative specs.
