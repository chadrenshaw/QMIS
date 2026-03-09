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
