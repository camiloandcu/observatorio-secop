#!/usr/bin/env bash
set -euo pipefail

for database_name in "${AIRFLOW_DB}" "${MLFLOW_DB}"; do
  psql --set ON_ERROR_STOP=1 \
    --username "${POSTGRES_USER}" \
    --dbname "${POSTGRES_DB}" \
    --set database_name="${database_name}" <<'SQL'
SELECT format('CREATE DATABASE %I', :'database_name')
WHERE NOT EXISTS (
  SELECT FROM pg_database WHERE datname = :'database_name'
)\gexec
SQL
done
