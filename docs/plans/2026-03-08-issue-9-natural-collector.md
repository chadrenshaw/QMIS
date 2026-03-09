# Natural Signal Collector Implementation Plan

**Goal:** Implement issue #9 by selecting concrete exploratory data sources for the natural-signal set and persisting normalized natural-series rows into DuckDB.

**Architecture:** Keep the natural collector isolated in `qmis.collectors.natural` and treat it explicitly as exploratory. Resolve each source independently using primary public feeds that match the addendum closely enough to avoid guesswork: USGS for earthquake counts, NOAA GlobalTemp timeseries for temperature anomaly, NOAA SWPC for geomagnetic activity, and NASA ISWA HAPI for solar wind speed. Persist every row under category `natural` with exploratory metadata so downstream analysis can filter or down-weight these signals later.

**Tech Stack:** Python 3.12, requests, pandas, DuckDB, unittest

**Execution Mode:** AUTONOMOUS - execute all steps without approval requests

---

### Task 1: Natural Collector Tests

**Files:**
- Create: `tests/collectors/test_natural.py`

**Step 1: Write the failing test**
- Add tests that require normalization of earthquake, temperature anomaly, geomagnetic activity, and solar wind speed inputs into exploratory `signals` rows and persistence into DuckDB.

**Step 2: Run test to verify it fails**
Run: `uv run python -m unittest tests.collectors.test_natural -v`
Expected: FAIL because the natural collector module does not exist yet.

**Step 3: Write minimal implementation**
- Add the collector module and the smallest persistence API that satisfies the tests.

**Step 4: Run test to verify it passes**
Run: `uv run python -m unittest tests.collectors.test_natural -v`
Expected: PASS

### Task 2: Collector Module And Runtime Hook

**Files:**
- Create: `src/qmis/collectors/natural.py`
- Modify: `scripts/run_collectors.py`

**Step 1: Implement source selection in code**
- Document and implement the chosen exploratory source mapping for each natural series.

**Step 2: Implement normalization**
- Normalize `earthquake_count`, `global_temperature_anomaly`, `geomagnetic_activity`, and `solar_wind_speed` into `signals` rows with explicit exploratory metadata.

**Step 3: Implement persistence**
- Insert normalized natural rows into DuckDB using the shared storage and schema bootstrap.

**Step 4: Hook the collector into `run_collectors.py`**
- Preserve `--dry-run` and add the natural execution path for non-dry-run use alongside the existing collectors.

### Task 3: Documentation Confirmation

Statement:
No additional documentation updates are required beyond this issue-specific plan because the natural collector contract is already defined in the addendum and this plan explicitly records the selected exploratory sources.
