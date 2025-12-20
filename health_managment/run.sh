#!/usr/bin/with-contenv bashio
set -euo pipefail

export PG_HOST="$(bashio::config 'pg_host')"
export PG_PORT="$(bashio::config 'pg_port')"
export PG_DB="$(bashio::config 'pg_db')"
export PG_USER="$(bashio::config 'pg_user')"
export PG_PASSWORD="$(bashio::config 'pg_password')"
export PG_SSLMODE="$(bashio::config 'pg_sslmode')"

bashio::log.info "Starting Health App API on :8000"
exec uvicorn app.main:app --host 0.0.0.0 --port 8000
