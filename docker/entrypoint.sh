#!/bin/sh
set -eu

APP_DIR="${QMIS_APP_DIR:-/app}"
DATA_ROOT="${QMIS_DATA_ROOT:-/var/lib/qmis}"
HOST="${QMIS_HOST:-0.0.0.0}"
PORT="${PORT:-8000}"
ENABLE_SCHEDULER="${QMIS_ENABLE_SCHEDULER:-1}"
BOOTSTRAP_ON_START="${QMIS_BOOTSTRAP_ON_START:-1}"
MARKET_INTERVAL_SECONDS="${QMIS_MARKET_INTERVAL_SECONDS:-900}"
DAILY_INTERVAL_SECONDS="${QMIS_DAILY_INTERVAL_SECONDS:-86400}"

export PYTHONPATH="${APP_DIR}/src${PYTHONPATH:+:${PYTHONPATH}}"
export QMIS_DATA_ROOT="${DATA_ROOT}"

mkdir -p "${DATA_ROOT}/db" "${DATA_ROOT}/logs" "${DATA_ROOT}/data"

pids=""

log() {
  printf '%s %s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "$*"
}

run_job() {
  name="$1"
  shift
  log "starting ${name}"
  if "$@"; then
    log "completed ${name}"
  else
    log "failed ${name}"
    return 1
  fi
}

run_loop() {
  name="$1"
  interval="$2"
  initial_delay="$3"
  shift 3

  (
    if [ "$initial_delay" -gt 0 ]; then
      sleep "$initial_delay"
    fi
    while :; do
      run_job "$name" "$@" || true
      sleep "$interval"
    done
  ) &

  pids="${pids} $!"
}

cleanup() {
  trap - INT TERM EXIT
  if [ -n "${pids}" ]; then
    kill ${pids} 2>/dev/null || true
    wait ${pids} 2>/dev/null || true
  fi
}

trap 'cleanup; exit 0' INT TERM
trap cleanup EXIT

if [ "${BOOTSTRAP_ON_START}" = "1" ]; then
  run_job "collectors:market-15m bootstrap" uv run python "${APP_DIR}/scripts/run_collectors.py" --cadence market-15m || true
  run_job "collectors:daily bootstrap" uv run python "${APP_DIR}/scripts/run_collectors.py" --cadence daily || true
  run_job "analysis:daily bootstrap" uv run python "${APP_DIR}/scripts/run_analysis.py" --cadence daily || true
  run_job "alerts:daily bootstrap" uv run python "${APP_DIR}/scripts/run_alerts.py" --cadence daily || true
fi

if [ "${ENABLE_SCHEDULER}" = "1" ]; then
  market_initial_delay=0
  daily_initial_delay=0
  if [ "${BOOTSTRAP_ON_START}" = "1" ]; then
    market_initial_delay="${MARKET_INTERVAL_SECONDS}"
    daily_initial_delay="${DAILY_INTERVAL_SECONDS}"
  fi

  run_loop "collectors:market-15m" "${MARKET_INTERVAL_SECONDS}" "${market_initial_delay}" \
    uv run python "${APP_DIR}/scripts/run_collectors.py" --cadence market-15m
  run_loop "collectors:daily" "${DAILY_INTERVAL_SECONDS}" "${daily_initial_delay}" \
    uv run python "${APP_DIR}/scripts/run_collectors.py" --cadence daily
  run_loop "analysis:daily" "${DAILY_INTERVAL_SECONDS}" "${daily_initial_delay}" \
    uv run python "${APP_DIR}/scripts/run_analysis.py" --cadence daily
  run_loop "alerts:daily" "${DAILY_INTERVAL_SECONDS}" "${daily_initial_delay}" \
    uv run python "${APP_DIR}/scripts/run_alerts.py" --cadence daily
fi

log "starting uvicorn on ${HOST}:${PORT}"
uv run uvicorn qmis.api:app --host "${HOST}" --port "${PORT}" &
api_pid=$!
pids="${pids} ${api_pid}"

wait "${api_pid}"
status=$?
cleanup
exit "${status}"
