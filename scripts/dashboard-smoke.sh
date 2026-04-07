#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUTPUT_DIR="${ROOT_DIR}/output/playwright/dashboard-smoke"
SEED_SQL="${ROOT_DIR}/scripts/dashboard-smoke-seed.sql"
CLASSPATH_FILE="${ROOT_DIR}/build/jobq/test-runtime-classpath.txt"
PLAYWRIGHT_SCRIPT="${ROOT_DIR}/scripts/dashboard-smoke-browser.mjs"
PLAYWRIGHT_PACKAGE_DIR="${ROOT_DIR}/scripts/node_modules/playwright"
APP_LOG="${OUTPUT_DIR}/app.log"
APP_PORT="${JOBQ_SMOKE_APP_PORT:-18080}"
DB_PORT="${JOBQ_SMOKE_DB_PORT:-55432}"
DB_CONTAINER="jobq-dashboard-smoke-db"
APP_PID=""
JAVA_BIN=""

log() {
  printf '[dashboard-smoke] %s\n' "$*"
}

fail() {
  log "ERROR: $*"
  exit 1
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || fail "Required command not found: $1"
}

port_in_use() {
  local port=$1
  if command -v lsof >/dev/null 2>&1; then
    lsof -i "tcp:${port}" >/dev/null 2>&1
    return $?
  fi
  if command -v ss >/dev/null 2>&1; then
    ss -ltn | awk '{print $4}' | grep -Eq "[:.]${port}$"
    return $?
  fi
  return 1
}

cleanup() {
  local exit_code=$?

  if [[ $exit_code -ne 0 && -f "${APP_LOG}" ]]; then
    log "Smoke test failed. Last application log lines:"
    tail -n 200 "${APP_LOG}" || true
  fi

  if [[ -n "${APP_PID}" ]]; then
    kill "${APP_PID}" >/dev/null 2>&1 || true
    for _ in $(seq 1 10); do
      if ! kill -0 "${APP_PID}" >/dev/null 2>&1; then
        break
      fi
      sleep 1
    done
    kill -9 "${APP_PID}" >/dev/null 2>&1 || true
    wait "${APP_PID}" >/dev/null 2>&1 || true
  fi

  docker rm -f "${DB_CONTAINER}" >/dev/null 2>&1 || true
}

trap cleanup EXIT

wait_for_http() {
  local url=$1
  local attempts=${2:-60}
  local delay_seconds=${3:-1}

  for _ in $(seq 1 "${attempts}"); do
    if curl -fsS -u admin:supersecret "${url}" >/dev/null 2>&1; then
      return 0
    fi
    sleep "${delay_seconds}"
  done

  fail "Timed out waiting for ${url}"
}

wait_for_postgres() {
  for _ in $(seq 1 60); do
    if docker exec "${DB_CONTAINER}" pg_isready -U jobq -d jobq >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done

  fail "PostgreSQL container did not become ready"
}

java_is_compatible() {
  local candidate=$1
  local version_output
  local major_version

  [[ -x "${candidate}" ]] || return 1

  version_output="$("${candidate}" -version 2>&1 | head -n 1)"
  major_version="$(printf '%s\n' "${version_output}" | sed -E 's/.*version "([0-9]+).*/\1/')"

  [[ -n "${major_version}" && "${major_version}" =~ ^[0-9]+$ && "${major_version}" -ge 25 ]]
}

resolve_java_bin() {
  if [[ -n "${JOBQ_SMOKE_JAVA_BIN:-}" ]] && java_is_compatible "${JOBQ_SMOKE_JAVA_BIN}"; then
    JAVA_BIN="${JOBQ_SMOKE_JAVA_BIN}"
    return
  fi

  if [[ -n "${JAVA_HOME:-}" ]] && java_is_compatible "${JAVA_HOME}/bin/java"; then
    JAVA_BIN="${JAVA_HOME}/bin/java"
    return
  fi

  if command -v /usr/libexec/java_home >/dev/null 2>&1; then
    local detected_java_home
    detected_java_home="$(/usr/libexec/java_home -v 25 2>/dev/null || true)"
    if [[ -n "${detected_java_home}" ]] && java_is_compatible "${detected_java_home}/bin/java"; then
      JAVA_BIN="${detected_java_home}/bin/java"
      return
    fi
  fi

  local candidate
  shopt -s nullglob
  for candidate in \
    "${HOME}"/Library/Java/JavaVirtualMachines/*/bin/java \
    /Library/Java/JavaVirtualMachines/*/Contents/Home/bin/java \
    /Library/Java/JavaVirtualMachines/*/bin/java
  do
    if java_is_compatible "${candidate}"; then
      JAVA_BIN="${candidate}"
      shopt -u nullglob
      return
    fi
  done
  shopt -u nullglob

  JAVA_BIN="$(command -v java || true)"
  [[ -n "${JAVA_BIN}" ]] || fail "Java 25 runtime not found. Set JAVA_HOME or JOBQ_SMOKE_JAVA_BIN."
}

verify_java_version() {
  local version_output
  local major_version
  version_output="$("${JAVA_BIN}" -version 2>&1 | head -n 1)"
  major_version="$(printf '%s\n' "${version_output}" | sed -E 's/.*version "([0-9]+).*/\1/')"

  if [[ -z "${major_version}" || ! "${major_version}" =~ ^[0-9]+$ ]]; then
    fail "Unable to determine Java version from: ${version_output}"
  fi

  if (( major_version < 25 )); then
    fail "Java 25 is required for the dashboard smoke test. Current runtime: ${version_output}"
  fi
}

ensure_node_dependencies() {
  [[ -f "${ROOT_DIR}/scripts/package-lock.json" ]] || fail "Missing scripts/package-lock.json. Run npm install in scripts/."

  if [[ ! -d "${PLAYWRIGHT_PACKAGE_DIR}" ]]; then
    log "Installing dashboard smoke Node dependencies"
    (cd "${ROOT_DIR}/scripts" && npm ci >/dev/null)
  fi
}

main() {
  require_command docker
  require_command curl
  require_command node
  require_command npm
  resolve_java_bin
  verify_java_version

  mkdir -p "${OUTPUT_DIR}"
  find "${OUTPUT_DIR}" -mindepth 1 -maxdepth 1 -exec rm -rf {} + 2>/dev/null || true

  if port_in_use "${APP_PORT}"; then
    fail "Port ${APP_PORT} is already in use. Override JOBQ_SMOKE_APP_PORT if needed."
  fi
  if port_in_use "${DB_PORT}"; then
    fail "Port ${DB_PORT} is already in use. Override JOBQ_SMOKE_DB_PORT if needed."
  fi

  ensure_node_dependencies

  if [[ "${JOBQ_SMOKE_INSTALL_BROWSER:-0}" == "1" ]]; then
    log "Installing Playwright browser runtime"
    npm exec --prefix "${ROOT_DIR}/scripts" playwright install chromium >/dev/null
  fi

  log "Preparing test runtime classpath"
  (cd "${ROOT_DIR}" && ./gradlew --no-daemon writeTestRuntimeClasspath >/dev/null)
  [[ -s "${CLASSPATH_FILE}" ]] || fail "Missing runtime classpath file: ${CLASSPATH_FILE}"

  log "Starting PostgreSQL container"
  docker run --rm -d \
    --name "${DB_CONTAINER}" \
    -e POSTGRES_DB=jobq \
    -e POSTGRES_USER=jobq \
    -e POSTGRES_PASSWORD=jobq \
    -p "${DB_PORT}:5432" \
    postgres:17-alpine >/dev/null
  wait_for_postgres

  log "Starting dashboard app"
  "${JAVA_BIN}" -cp "$(cat "${CLASSPATH_FILE}")" \
    com.jobq.TestApplication \
    --server.port="${APP_PORT}" \
    --spring.datasource.url="jdbc:postgresql://localhost:${DB_PORT}/jobq" \
    --spring.datasource.username=jobq \
    --spring.datasource.password=jobq \
    --spring.jpa.hibernate.ddl-auto=none \
    --spring.jpa.open-in-view=false \
    --jobq.dashboard.enabled=true \
    --jobq.dashboard.username=admin \
    --jobq.dashboard.password=supersecret \
    --jobq.background-job-server.enabled=false \
    >"${APP_LOG}" 2>&1 &
  APP_PID=$!

  wait_for_http "http://localhost:${APP_PORT}/jobq/dashboard"

  log "Seeding dashboard data"
  docker exec -i "${DB_CONTAINER}" psql -v ON_ERROR_STOP=1 -U jobq -d jobq < "${SEED_SQL}" >/dev/null

  log "Running Playwright smoke assertions"
  JOBQ_SMOKE_OUTPUT_DIR="${OUTPUT_DIR}" \
  JOBQ_SMOKE_URL="http://admin:supersecret@localhost:${APP_PORT}/jobq/dashboard" \
  JOBQ_SMOKE_BROWSER="${JOBQ_SMOKE_BROWSER:-chromium}" \
  node "${PLAYWRIGHT_SCRIPT}"

  log "Dashboard smoke test passed"
}

main "$@"
