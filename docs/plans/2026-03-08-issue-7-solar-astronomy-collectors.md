# Solar And Astronomy Collectors Implementation Plan

**Goal:** Implement issue #7 by adding deterministic astronomy derivations and live solar activity ingestion, then persist those signals into DuckDB.

**Architecture:** Split the work into two collectors with separate responsibilities. `qmis.collectors.solar` should fetch live solar activity data from NOAA JSON feeds and normalize daily aggregates into `signals`, while `qmis.collectors.astronomy` should derive daily lunar, zodiac, and event-style astronomy series locally using ephemeris libraries so the output is deterministic and testable.

**Tech Stack:** Python 3.12, requests, pandas, DuckDB, astral, ephem, skyfield, unittest

**Execution Mode:** AUTONOMOUS - execute all steps without approval requests

---

### Task 1: Solar And Astronomy Tests

**Files:**
- Create: `tests/collectors/test_solar.py`
- Create: `tests/collectors/test_astronomy.py`

**Step 1: Write the failing test**
- Add tests that require daily solar aggregates from NOAA payloads and deterministic astronomy signal derivations for a fixed date.

**Step 2: Run test to verify it fails**
Run: `uv run python -m unittest tests.collectors.test_solar tests.collectors.test_astronomy -v`
Expected: FAIL because the collectors do not exist yet.

**Step 3: Write minimal implementation**
- Add the collector modules and the smallest persistence APIs that satisfy the tests.

**Step 4: Run test to verify it passes**
Run: `uv run python -m unittest tests.collectors.test_solar tests.collectors.test_astronomy -v`
Expected: PASS

### Task 2: Collector Modules And Runtime Hook

**Files:**
- Create: `src/qmis/collectors/solar.py`
- Create: `src/qmis/collectors/astronomy.py`
- Modify: `scripts/run_collectors.py`
- Modify: `pyproject.toml`

**Step 1: Implement solar source parsing**
- Fetch NOAA JSON feeds for sunspots, solar radio flux, Kp, and edited events, then aggregate them into daily rows with stable metadata.

**Step 2: Implement astronomy derivations**
- Derive lunar cycle day, phase angle, illumination, moon distance, moon declination, solar longitude, zodiac index, zodiac sign, axial tilt, precession angle, and daily new/full-moon event flags.

**Step 3: Implement persistence**
- Insert normalized solar and astronomy rows into DuckDB using the shared storage and schema bootstrap.

**Step 4: Hook both collectors into `run_collectors.py`**
- Preserve `--dry-run` and add solar plus astronomy execution paths for non-dry-run use alongside the existing collectors.

### Task 3: Documentation Confirmation

Statement:
No additional documentation updates are required beyond this issue-specific plan because the solar and astronomy collector contracts are already defined in the specs and issue body.
