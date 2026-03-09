## Issue #19 Plan

**Goal:** Package QMIS into a single Dockerized deployment that serves the FastAPI read API and built web dashboard from one container, persists durable state on a mounted volume, and runs scheduled jobs in-container.

### Workstreams

1. Runtime configuration
- Add environment-driven overrides for DB, logs, durable data, and built frontend asset paths.
- Keep repo-local defaults intact for local development.

2. API/static serving
- Extend the FastAPI app so `/` serves the built frontend when `web/dist` is present.
- Keep existing JSON API endpoints unchanged.
- Serve built asset files from the same process.

3. Container runtime
- Add a multi-stage `Dockerfile` that builds the frontend and installs the Python runtime.
- Add `.dockerignore` to keep the build context deterministic.
- Add `docker/entrypoint.sh` to:
  - create mounted runtime directories
  - optionally bootstrap jobs on startup
  - run scheduled collector, analysis, and alert loops
  - host uvicorn and clean up on shutdown

4. Deployment contract
- Add `docker-compose.yml` with a mounted volume and documented environment contract.
- Update `README.md` with:
  - `docker build`
  - `docker run`
  - `docker compose up`
  - required environment variables

### Verification

- `uv run python -m unittest -v`
- `cd web && npm test -- --run`
- `cd web && npm run build`
- `docker build -t qmis:local .`
- `docker run --rm -p 8000:8000 qmis:local` smoke check
- `docker compose config`
