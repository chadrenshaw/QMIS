# Macro Sentiment Engine

QMIS is a DuckDB-backed macro signal engine with collectors, analysis jobs, a Rich CLI dashboard, and an optional read-only FastAPI surface.

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
docker compose up --build
```

The included [docker-compose.yml](/Users/crenshaw/Projects/macro-sentiment-engine/docker-compose.yml) mounts `./runtime` into `/var/lib/qmis`, so DuckDB and logs survive container restarts.
