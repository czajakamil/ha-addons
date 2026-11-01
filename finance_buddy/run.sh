#!/bin/bash
set -euo pipefail

OPTIONS=/data/options.json
IMPORT_DIR=/share/finance_buddy/import

option() {
    jq -r --arg key "$1" '.[$key] // empty' "$OPTIONS"
}

# Jednorazowe przeniesienie istniejących danych, tylko gdy baza jeszcze nie istnieje.
# Niczego nie nadpisuje, a katalog zmienia nazwę, żeby nie został w udziale Samby.
if [ ! -e "$FINANCE_DB_PATH" ] && [ -f "$IMPORT_DIR/finance.sqlite3" ]; then
    echo "Importing existing data from $IMPORT_DIR"
    cp "$IMPORT_DIR/finance.sqlite3" "$FINANCE_DB_PATH.tmp"
    if [ "$(sqlite3 "$FINANCE_DB_PATH.tmp" 'PRAGMA integrity_check;')" != "ok" ]; then
        rm -f "$FINANCE_DB_PATH.tmp"*
        echo "Import database failed integrity_check, refusing to start." >&2
        exit 1
    fi
    if [ -d "$IMPORT_DIR/attachments" ]; then
        mkdir -p "$FINANCE_ATTACHMENTS_DIR"
        cp -R "$IMPORT_DIR/attachments/." "$FINANCE_ATTACHMENTS_DIR/"
    fi
    mv "$FINANCE_DB_PATH.tmp" "$FINANCE_DB_PATH"
    mv "$IMPORT_DIR" "$IMPORT_DIR.done-$(date +%Y%m%d-%H%M%S)"
    echo "Import finished. Delete the $IMPORT_DIR.done-* folder from the share once verified."
fi

mkdir -p /data/logs /data/.keyring

# Logowanie załatwia Home Assistant (Ingress); apka nie pokazuje ekranu passkey.
# Bezpieczne tylko razem z ograniczeniem w nginx.conf i backendem na 127.0.0.1.
export FINANCE_DEV_LOGIN=1
export FINANCE_DEV_LOGIN_USERNAME="$(option username)"

export FINANCE_BACKUP_DIR="$(option backup_dir)"
export RECEIPT_EXTRACTOR="$(option receipt_extractor)"
export FINANCE_LOG_LEVEL="$(option log_level)"

YNAB_API_KEY="$(option ynab_api_key)"
if [ -n "$YNAB_API_KEY" ]; then
    export YNAB_API_KEY
fi

mkdir -p "$FINANCE_BACKUP_DIR"

echo "Starting Finance Buddy (db=$FINANCE_DB_PATH, backups=$FINANCE_BACKUP_DIR)"
exec /usr/bin/supervisord -n -c /etc/supervisor/conf.d/supervisord.conf
