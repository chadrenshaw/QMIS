FROM node:22-bookworm-slim AS web-build

WORKDIR /build/web
COPY web/package.json web/package-lock.json ./
RUN npm ci
COPY web/ ./
RUN npm run build


FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_LINK_MODE=copy \
    PATH="/app/.venv/bin:${PATH}" \
    PYTHONPATH="/app/src" \
    QMIS_DATA_ROOT="/var/lib/qmis" \
    QMIS_WEB_DIST_DIR="/app/web/dist" \
    PORT="8000"

WORKDIR /app

COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev --no-install-project

COPY src ./src
COPY scripts ./scripts
COPY main.py ./
COPY docker/entrypoint.sh /entrypoint.sh
COPY --from=web-build /build/web/dist ./web/dist

RUN chmod +x /entrypoint.sh \
    && mkdir -p /var/lib/qmis/db /var/lib/qmis/logs /var/lib/qmis/data

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=10s --start-period=20s --retries=3 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/health', timeout=5).read()"

ENTRYPOINT ["/entrypoint.sh"]
