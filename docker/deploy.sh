#!/bin/bash

set -euo pipefail

COMPOSE_FILE="${1:-docker-compose.yml}"
DEPLOY_DIR="/srv/docker/qmis"
SERVICE_NAME="qmis"
IMAGE_REF="${QMIS_IMAGE:-gitea.chadlee.org/crenshaw/qmis:dev}"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

rollback_previous_image() {
    if [ -z "${PREVIOUS_IMAGE_ID:-}" ]; then
        log_warn "No previous image found for rollback"
        return
    fi

    log_warn "Rolling back ${SERVICE_NAME} to image ${PREVIOUS_IMAGE_ID}"
    if ! QMIS_IMAGE="${PREVIOUS_IMAGE_ID}" docker compose -f "${COMPOSE_FILE}" up -d --no-deps "${SERVICE_NAME}"; then
        log_error "Rollback failed"
        exit 1
    fi
}

cd "${DEPLOY_DIR}" || {
    log_error "Failed to change to deployment directory: ${DEPLOY_DIR}"
    exit 1
}

if [ ! -f "${COMPOSE_FILE}" ]; then
    log_error "Compose file not found: ${DEPLOY_DIR}/${COMPOSE_FILE}"
    exit 1
fi

log_info "Starting QMIS deploy in ${DEPLOY_DIR}"
log_info "Deploying image ${IMAGE_REF}"

PREVIOUS_IMAGE_ID="$(docker compose -f "${COMPOSE_FILE}" images -q "${SERVICE_NAME}" 2>/dev/null | head -n 1 || true)"

log_info "Pulling latest image for ${SERVICE_NAME}"
if ! QMIS_IMAGE="${IMAGE_REF}" docker compose -f "${COMPOSE_FILE}" pull qmis; then
    log_error "Failed to pull image ${IMAGE_REF}"
    exit 1
fi

log_info "Restarting ${SERVICE_NAME}"
if ! QMIS_IMAGE="${IMAGE_REF}" docker compose -f "${COMPOSE_FILE}" up -d --no-deps qmis; then
    log_error "Deploy failed"
    rollback_previous_image
    exit 1
fi

MAX_RETRIES=12
RETRY_INTERVAL=5
STATE="missing"

log_info "Verifying container state"
for _ in $(seq 1 "${MAX_RETRIES}"); do
    STATE="$(QMIS_IMAGE="${IMAGE_REF}" docker compose -f "${COMPOSE_FILE}" ps --format '{{.State}}' "${SERVICE_NAME}" | head -n 1 || echo "missing")"
    if [ "${STATE}" = "running" ]; then
        log_info "Deployment successful"
        docker image prune -f >/dev/null 2>&1 || true
        exit 0
    fi
    sleep "${RETRY_INTERVAL}"
done

log_error "${SERVICE_NAME} failed to reach running state"
rollback_previous_image
exit 1
