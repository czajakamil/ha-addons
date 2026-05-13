#!/usr/bin/with-contenv bashio
set -euo pipefail

export MEALPILOT_DB="/data/mealpilot.db"
export MEALPILOT_STATIC_DIR="/app/static"
export MEALPILOT_ADMIN_USERNAME="$(bashio::config 'admin_username')"
export MEALPILOT_ADMIN_PASSWORD="$(bashio::config 'admin_password')"
export MEALPILOT_AI_API_URL="$(bashio::config 'ai_api_url')"
export MEALPILOT_AI_API_KEY="$(bashio::config 'ai_api_key')"
export MEALPILOT_AI_MODEL="$(bashio::config 'ai_model')"
export MEALPILOT_CORS_ORIGINS="$(bashio::config 'cors_origins')"

if bashio::config.true 'require_cf_access'; then
    export MEALPILOT_REQUIRE_CF_ACCESS="1"
else
    export MEALPILOT_REQUIRE_CF_ACCESS="0"
fi

if bashio::config.true 'cookie_secure'; then
    export MEALPILOT_COOKIE_SECURE="1"
else
    export MEALPILOT_COOKIE_SECURE="0"
fi

bashio::log.info "Starting MealPilot on :8000 (DB=${MEALPILOT_DB})"
exec python -m uvicorn app.main:app --host 0.0.0.0 --port 8000
