# QMIS

QMIS is a DuckDB-backed macro signal engine with collectors, analysis jobs, a Rich CLI dashboard, and an optional read-only FastAPI surface.

## Local CLI

The current local operator workflow is DuckDB-backed. Use the operator console to refresh all collectors, run analysis and alert materialization, then render the consolidated CLI dashboard with the current market, macro, liquidity, crypto, breadth, astronomy, and natural signal systems.

```bash
uv sync --frozen
uv run python scripts/run_operator_console.py
```

If you want to render the dashboard from the existing database state without refetching data:

```bash
uv run python scripts/run_operator_console.py --no-refresh
```

For the split runtime entrypoints, use:

```bash
uv run python scripts/run_collectors.py --cadence all
uv run python scripts/run_analysis.py
uv run python scripts/run_alerts.py
uv run python scripts/run_dashboard.py
```

## Frontend Scaffold

The browser dashboard scaffold lives in `web/` and builds as a static Vite app so it can later be served by the Python container.

```bash
cd web
npm install
npm run dev
```

For a production build:

```bash
cd web
npm run build
```

For frontend tests:

```bash
cd web
npm test -- --run
```

## Container Runtime

QMIS can be packaged as a single container that serves the FastAPI read API and the built frontend from the same process.

Code repository:

- `https://gitea.chadlee.org/crenshaw/QMIS`

Published image path:

- `gitea.chadlee.org/crenshaw/qmis:latest`

### Runtime Environment

- `QMIS_DATA_ROOT`
  Container-mounted durable root for DuckDB, logs, and runtime data.
  Default: `/var/lib/qmis`
- `QMIS_DB_PATH`
  Optional explicit DuckDB path override.
- `QMIS_LOG_DIR`
  Optional explicit log directory override.
- `QMIS_RUNTIME_DATA_DIR`
  Optional explicit runtime data directory override.
- `QMIS_WEB_DIST_DIR`
  Built frontend asset directory.
  Default in container: `/app/web/dist`
- `QMIS_ENABLE_SCHEDULER`
  `1` to run collector, analysis, and alert loops in-container.
- `QMIS_BOOTSTRAP_ON_START`
  `1` to run one startup pass before the steady-state schedule loops begin.
- `FRED_API_KEY`
  Optional FRED API key for macro collectors.

### Build The Image

```bash
docker build -t qmis:local .
```

The Dockerfile also accepts CI metadata build args so Woodpecker can stamp the image with the source repo URL and commit SHA.

### Run The Container

```bash
docker run --rm \
  -p 8000:8000 \
  -v "$(pwd)/runtime:/var/lib/qmis" \
  -e QMIS_DATA_ROOT=/var/lib/qmis \
  -e QMIS_ENABLE_SCHEDULER=1 \
  -e QMIS_BOOTSTRAP_ON_START=1 \
  -e FRED_API_KEY="${FRED_API_KEY:-}" \
  qmis:local
```

After startup:

- API health: `http://localhost:8000/health`
- browser dashboard: `http://localhost:8000/`

### Docker Compose

```bash
docker compose up
```

The included [docker-compose.yml](/Users/crenshaw/Projects/QMIS/docker-compose.yml) defaults to pulling `gitea.chadlee.org/crenshaw/qmis:latest` and mounts `./runtime` into `/var/lib/qmis`, so DuckDB and logs survive container restarts.

To run Compose against a locally built image instead of the published registry image:

```bash
QMIS_IMAGE=qmis:local docker compose up
```

## Woodpecker CI/CD

The repository now expects container build and publishing to happen in Woodpecker via [.woodpecker.yaml](/Users/crenshaw/Projects/QMIS/.woodpecker.yaml).

Pipeline behavior:

- run backend tests with `uv run python -m unittest -v`
- run frontend tests and production build
- verify the container build on pull requests, pushes, and tags
- publish `dev` to `gitea.chadlee.org/crenshaw/qmis` on pushes to `dev`
- upload `docker-compose.yml` and `docker/deploy.sh` to `infra-docker.zocalo.net:/srv/docker/qmis`
- execute the remote deploy script from `/srv/docker/qmis` on `dev` pushes
- publish `latest` to `gitea.chadlee.org/crenshaw/qmis` on pushes to `main`
- publish semver-style tags to the same Gitea registry on tag events

Required Woodpecker secrets:

- `gitea_registry_username`
- `gitea_registry_password`
- `REGISTRY_USER`
- `REGISTRY_PASS`
- `DEV_DOCKER_HOST_IP`
- `DOCKER_HOST_USER`
- `DOCKER_HOST_SSH_KEY`

The registry credentials must have permission to push packages to the same Gitea instance that hosts the code repository.

The remote deploy steps target `infra-docker.zocalo.net` and copy runtime assets into `/srv/docker/qmis` before running `/srv/docker/qmis/deploy.sh`. The script uses that directory as its working directory, pulls `gitea.chadlee.org/crenshaw/qmis:dev`, and restarts the `qmis` service with the checked-in [docker-compose.yml](/Users/crenshaw/Projects/QMIS/docker-compose.yml).
