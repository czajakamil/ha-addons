#!/usr/bin/with-contenv bashio

bashio::log.info "Starting CatPrint server..."

export PRINTER_ADDRESS="$(bashio::config 'printer_address')"
export PRINTER_NAME="$(bashio::config 'printer_name')"
export PORT=5123
export HOST=0.0.0.0
export DB_PATH="/data/catprint.db"

exec python /app/app.py
