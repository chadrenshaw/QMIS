# Issue 21 Web Dashboard Views Implementation Plan

**Goal:** Build the first complete browser dashboard for QMIS with regime, signal, relationship, anomaly, and selected-history views that reach practical parity with the CLI for remote monitoring.

**Architecture:** Extend the existing FastAPI read surface with a compact dashboard-history payload derived from persisted `signals` and `regimes`, then consume that payload from the Vite/React dashboard created in issue `#20`. Keep the UI read-only and snapshot-driven so it stays compatible with the single-container static-asset deployment planned in issue `#19`.

**Tech Stack:** FastAPI, DuckDB, pandas, React, TypeScript, Vite, Tailwind CSS, TanStack Query, Recharts, Vitest, unittest

**Execution Mode:** AUTONOMOUS - execute all steps without approval requests

---

### Task 1: Add backend dashboard history coverage

**Files:**
- Modify: `tests/test_api.py`
- Test: `tests/test_api.py`

**Step 1: Write the failing test**
- Add assertions for a dashboard payload that includes compact signal history, score history, and explicit freshness metadata.

**Step 2: Run test to verify it fails**
- Run: `uv run python -m unittest tests.test_api -v`

**Step 3: Write minimal implementation**
- Extend the read API only enough to satisfy the contract required by the browser dashboard.

**Step 4: Run test to verify it passes**
- Run: `uv run python -m unittest tests.test_api -v`

### Task 2: Implement backend history payload and dashboard metadata

**Files:**
- Modify: `src/qmis/api.py`
- Modify: `src/qmis/dashboard/cli.py`

**Step 1: Add compact history helpers**
- Add persisted-history queries for selected signals and regime scores.

**Step 2: Add dashboard response fields**
- Include series history, score history, timestamps, freshness/staleness markers, and empty-safe defaults.

**Step 3: Verify backend behavior**
- Run: `uv run python -m unittest tests.test_api -v`

### Task 3: Add failing frontend view test

**Files:**
- Modify: `web/src/App.test.tsx`
- Test: `web/src/App.test.tsx`

**Step 1: Write the failing test**
- Assert the browser dashboard renders historical chart sections, recent signal groups, relationship/anomaly summaries, and a degraded alert panel state when alert-engine data is unavailable.

**Step 2: Run test to verify it fails**
- Run: `cd web && npm test -- --run`

### Task 4: Implement browser dashboard views

**Files:**
- Modify: `web/src/App.tsx`
- Modify: `web/src/api/queries.ts`
- Modify: `web/src/components/RelationshipTable.tsx`
- Create: `web/src/components/StatusBadge.tsx`
- Create: `web/src/components/SectionHeading.tsx`
- Create: `web/src/components/HistoryChart.tsx`
- Create: `web/src/components/AlertSummaryPanel.tsx`
- Modify: `web/src/index.css`
- Modify: `web/src/test/setup.ts`

**Step 1: Add typed dashboard query shapes**
- Wire the frontend to the richer dashboard payload without inventing raw collector access.

**Step 2: Implement overview and chart sections**
- Show regime, confidence, macro scores, selected history charts, and explicit timestamps/units.

**Step 3: Implement signal, relationship, anomaly, and alert panels**
- Group major signals by model layer, render relationship/anomaly summaries, and show an explicit unavailable state for alerts until issue `#15` lands.

**Step 4: Verify frontend test**
- Run: `cd web && npm test -- --run`

**Step 5: Verify production build**
- Run: `cd web && npm run build`

### Task 5: Full validation

**Files:**
- Modify: no new files expected

**Step 1: Run targeted scaffold acceptance**
- Run: `uv run python -m unittest tests.test_web_scaffold -v`

**Step 2: Run full backend suite**
- Run: `uv run python -m unittest -v`

**Step 3: Re-run frontend checks**
- Run: `cd web && npm test -- --run`
- Run: `cd web && npm run build`

### Task 6: Documentation Confirmation

Statement:
No documentation updates required for this change beyond the existing frontend command coverage already present in `README.md`.
