#!/usr/bin/with-contenv bashio

bashio::log.info "Starting Cookidoo Today (FastAPI/Uvicorn) ..."

exec python -m uvicorn app.server:app --host 0.0.0.0 --port 8099