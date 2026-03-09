# Web Dashboard Scaffold Implementation Plan

**Goal:** Implement issue #20 by scaffolding a React + TypeScript + Vite + Tailwind frontend that can consume the QMIS read API and build to static assets for single-container deployment.

**Architecture:** Create a `web/` SPA with a small API client layer, TanStack Query hooks, a responsive operator-console shell, and a minimal chart-powered overview using current API outputs. Keep the runtime simple: Vite for local development and static build output for later serving by the Python app/container.

**Tech Stack:** React, TypeScript, Vite, Tailwind CSS, TanStack Query, Recharts, Vitest, Testing Library

**Execution Mode:** AUTONOMOUS - execute all steps without approval requests

---

### Task 1: Scaffold Acceptance Test

**Files:**
- Create: `tests/test_web_scaffold.py`

**Step 1: Write the failing test**
- Add an acceptance test that requires the `web/` scaffold files, required scripts/dependencies, API client layer, and documented frontend commands.

**Step 2: Run test to verify it fails**
Run: `uv run python -m unittest tests.test_web_scaffold -v`
Expected: FAIL because the web scaffold does not exist yet.

**Step 3: Write minimal implementation**
- Add the frontend scaffold and the minimal documentation needed to satisfy the acceptance test.

**Step 4: Run test to verify it passes**
Run: `uv run python -m unittest tests.test_web_scaffold -v`
Expected: PASS

### Task 2: Frontend Scaffold And Build Pipeline

**Files:**
- Create: `web/package.json`
- Create: `web/tsconfig.json`
- Create: `web/tsconfig.node.json`
- Create: `web/vite.config.ts`
- Create: `web/postcss.config.js`
- Create: `web/tailwind.config.ts`
- Create: `web/index.html`
- Create: `web/src/main.tsx`
- Create: `web/src/App.tsx`
- Create: `web/src/index.css`
- Create: `web/src/env.d.ts`
- Create: `web/src/api/client.ts`
- Create: `web/src/api/queries.ts`
- Create: `web/src/components/*`
- Create: `web/src/test/setup.ts`
- Create: `web/src/App.test.tsx`
- Create: `web/public/*` as needed
- Modify: `README.md`

**Step 1: Add package/config files**
- Define the Vite, TypeScript, Tailwind, and test tooling setup.

**Step 2: Add the API/query layer**
- Add a typed client and TanStack Query hooks for the existing QMIS read endpoints.

**Step 3: Build the app shell**
- Create a responsive dashboard shell with loading/error/empty states and a desktop-first operator-console visual theme.

**Step 4: Add a basic frontend test**
- Verify the shell renders and uses mocked query data safely.

**Step 5: Document dev/build commands**
- Add the local frontend boot and production build commands to `README.md`.

### Task 3: Frontend Verification

**Files:**
- Use the created frontend files

**Step 1: Install frontend dependencies**
- Run `npm install` in `web/`.

**Step 2: Run frontend tests**
- Run `npm test -- --run` in `web/`.

**Step 3: Run production build**
- Run `npm run build` in `web/`.

### Task 4: Documentation Updates

**Files:**
- Modify: `README.md`

**Step 1: Update documentation**
- Document frontend dev, test, and build commands so the scaffold is operable locally.

**Step 2: Review for accuracy**
- Ensure the documented commands match the final scaffold exactly.
