# QMIS Foundation Runtime Implementation Plan

**Goal:** Implement issue #1 by creating the spec-aligned `src/qmis/` package skeleton and top-level runtime entrypoints for collectors, analysis, and alerts.

**Architecture:** Add the minimal package and runtime scaffolding required by the spec without implementing collectors or signal logic yet. The work centers on a shared config module, shared logging, namespace packages for later modules, and runnable scripts that load the package cleanly from the repository’s `src/` layout.

**Tech Stack:** Python 3.12, uv, rich, standard library logging/dataclasses/pathlib

**Execution Mode:** AUTONOMOUS - execute all steps without approval requests

---

### Task 1: Foundation Tests

**Files:**
- Create: `tests/test_qmis_foundation.py`
- Modify: `tests/test_macro_sentiment_engine.py`

**Step 1: Write the failing test**
- Add tests that prove `src/qmis/` imports from the `src` layout, runtime scripts resolve the package, and config paths are rooted at the repository rather than a hardcoded absolute path.

**Step 2: Run test to verify it fails**
Run: `uv run python -m unittest tests.test_qmis_foundation tests.test_macro_sentiment_engine -v`
Expected: FAIL because the package and runtime scripts do not exist yet.

**Step 3: Write minimal implementation**
- Add package modules and runtime entrypoints required by the tests.

**Step 4: Run test to verify it passes**
Run: `uv run python -m unittest tests.test_qmis_foundation tests.test_macro_sentiment_engine -v`
Expected: PASS

### Task 2: Package Skeleton And Runtime Entry Points

**Files:**
- Create: `src/qmis/__init__.py`
- Create: `src/qmis/config.py`
- Create: `src/qmis/logger.py`
- Create: `src/qmis/collectors/__init__.py`
- Create: `src/qmis/features/__init__.py`
- Create: `src/qmis/signals/__init__.py`
- Create: `src/qmis/alerts/__init__.py`
- Create: `src/qmis/dashboard/__init__.py`
- Create: `scripts/run_collectors.py`
- Create: `scripts/run_analysis.py`
- Create: `scripts/run_alerts.py`

**Step 1: Implement package config**
- Provide repo-root path resolution and shared defaults needed by later issues.

**Step 2: Implement shared logging**
- Provide one logging setup function for scripts and package jobs.

**Step 3: Implement runtime scripts**
- Add thin entrypoints for collectors, analysis, and alerts with `--dry-run` behavior and package-backed execution.

**Step 4: Verify imports and scripts**
Run: `uv run python scripts/run_collectors.py --dry-run`
Expected: exit `0` with runtime scaffold output

### Task 3: Documentation Confirmation

Statement:
No additional documentation updates are required beyond this issue-specific plan because the authoritative system design already lives in the spec documents.
