#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${ENV_FILE:-${ROOT_DIR}/.env}"
COMPOSE=(docker compose --env-file "${ENV_FILE}" --project-directory "${ROOT_DIR}")
REQUIRED_VARIABLES=(
  COMPOSE_PROJECT_NAME POSTGRES_IMAGE POSTGRES_USER POSTGRES_PASSWORD POSTGRES_DB POSTGRES_PORT
  AIRFLOW_DB MLFLOW_DB DBT_SCHEMA DBT_THREADS SILVER_ROOT
  AIRFLOW_IMAGE AIRFLOW_UID AIRFLOW_PORT MLFLOW_IMAGE MLFLOW_PORT
  SERVICE_HEALTH_TIMEOUT
)

usage() {
  echo "Usage: scripts/local.sh {check-path|validate-env|preflight|config|up|up-airflow|up-mlflow|health|ps|logs|down|clean}" >&2
}

check_path() {
  local project_path="${PROJECT_DIR_OVERRIDE:-${ROOT_DIR}}"
  case "${project_path}" in
    /mnt/*)
      echo "Checkout detected under ${project_path}. Move it to the WSL2 Linux filesystem (for example ~/projects) to avoid slow I/O, permission issues, and CRLF surprises." >&2
      return 1
      ;;
  esac
}

validate_env() {
  if [[ ! -f "${ENV_FILE}" ]]; then
    echo "Missing ${ENV_FILE}. Copy .env.example to .env and keep it local." >&2
    return 1
  fi

  local variable
  for variable in "${REQUIRED_VARIABLES[@]}"; do
    if ! grep -Eq "^[[:space:]]*${variable}=[^[:space:]].*$" "${ENV_FILE}"; then
      echo "Missing required variable: ${variable}" >&2
      return 1
    fi
  done
}

preflight() {
  check_path
  validate_env
  command -v docker >/dev/null || {
    echo "Docker CLI is unavailable. Install Docker Desktop and enable WSL integration." >&2
    return 1
  }
  docker compose version >/dev/null || {
    echo "Docker CLI is unavailable or does not provide Docker Compose. Update Docker Desktop." >&2
    return 1
  }
  docker info >/dev/null 2>&1 || {
    echo "Docker daemon is unavailable. Start Docker Desktop and enable this WSL2 distribution under Settings > Resources > WSL Integration." >&2
    return 1
  }
}

compose_config() {
  validate_env
  "${COMPOSE[@]}" -f "${ROOT_DIR}/docker-compose.yml" config --quiet
}

wait_for_health() {
  local service="$1"
  local timeout
  timeout="$(grep -E '^SERVICE_HEALTH_TIMEOUT=' "${ENV_FILE}" | tail -1 | cut -d= -f2-)"
  local deadline=$((SECONDS + timeout))
  local container_id status

  while ((SECONDS < deadline)); do
    container_id="$("${COMPOSE[@]}" -f "${ROOT_DIR}/docker-compose.yml" ps -q "${service}")"
    if [[ -n "${container_id}" ]]; then
      status="$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "${container_id}")"
      if [[ "${status}" == "healthy" || "${status}" == "running" ]]; then
        return 0
      fi
      if [[ "${status}" == "unhealthy" || "${status}" == "exited" || "${status}" == "dead" ]]; then
        echo "Service ${service} reached terminal state: ${status}" >&2
        return 1
      fi
    fi
    sleep 2
  done

  echo "Timed out waiting for ${service} health after ${timeout}s." >&2
  return 1
}

check_databases() {
  local expected actual
  expected="$(grep -E '^(POSTGRES_DB|AIRFLOW_DB|MLFLOW_DB)=' "${ENV_FILE}" | cut -d= -f2- | sort)"
  actual="$("${COMPOSE[@]}" -f "${ROOT_DIR}/docker-compose.yml" exec -T postgres \
    sh -c 'psql --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" --tuples-only --no-align --command="SELECT datname FROM pg_database"' \
    | grep -Fxf <(printf '%s\n' "${expected}") | sort)"
  if [[ "${actual}" != "${expected}" ]]; then
    echo "Expected application, Airflow, and MLflow databases were not all found." >&2
    return 1
  fi
}

up_postgres() {
  preflight
  compose_config
  "${COMPOSE[@]}" -f "${ROOT_DIR}/docker-compose.yml" up -d postgres
  wait_for_health postgres
  check_databases
  echo "PostgreSQL is healthy and all local databases are ready."
}

profiles_up() {
  local profile="$1"
  shift
  up_postgres
  "${COMPOSE[@]}" -f "${ROOT_DIR}/docker-compose.yml" --profile "${profile}" up -d "$@"
  local service
  for service in "$@"; do
    wait_for_health "${service}"
  done
}

command_name="${1:-}"
case "${command_name}" in
  check-path) check_path ;;
  validate-env) validate_env ;;
  preflight) preflight ;;
  config) compose_config ;;
  up) up_postgres ;;
  up-airflow) profiles_up airflow airflow-webserver airflow-scheduler ;;
  up-mlflow) profiles_up mlflow mlflow ;;
  health)
    preflight
    wait_for_health postgres
    check_databases
    ;;
  ps)
    preflight
    "${COMPOSE[@]}" -f "${ROOT_DIR}/docker-compose.yml" --profile airflow --profile mlflow ps
    ;;
  logs)
    preflight
    service="${2:-}"
    [[ -n "${service}" ]] || { echo "Specify a service name." >&2; exit 1; }
    "${COMPOSE[@]}" -f "${ROOT_DIR}/docker-compose.yml" --profile airflow --profile mlflow logs --tail=200 "${service}"
    ;;
  down)
    validate_env
    "${COMPOSE[@]}" -f "${ROOT_DIR}/docker-compose.yml" --profile airflow --profile mlflow down --remove-orphans
    ;;
  clean)
    validate_env
    if [[ "${CONFIRM_CLEAN:-}" != "yes" ]]; then
      echo "Destructive cleanup refused. This removes project containers, PostgreSQL volumes, and mlruns/. Re-run with CONFIRM_CLEAN=yes." >&2
      exit 1
    fi
    "${COMPOSE[@]}" -f "${ROOT_DIR}/docker-compose.yml" --profile airflow --profile mlflow down --volumes --remove-orphans
    rm -rf -- "${ROOT_DIR}/mlruns"
    ;;
  *) usage; exit 2 ;;
esac
