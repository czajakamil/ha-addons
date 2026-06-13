#!/usr/bin/env bash
# Startuje izolowaną instancję MealPilot na potrzeby browser E2E:
#  - świeża baza SQLite (za każdym razem od zera → admin + demo-przepisy z seedu),
#  - znany admin (e2e_admin / E2eAdminPass1234) provisionowany przez env,
#  - backend serwuje też zbudowany frontend pod tym samym originem.
# Dzięki temu testy nie zależą od Twojego dev-kontenera ani Twojego hasła.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"   # katalog mealpilot/
FE="$ROOT/frontend"
BE="$ROOT/backend"
PORT="${E2E_PORT:-8765}"

# 1. Zbuduj frontend (waliduje też typy TS) → frontend/dist
if [ -z "${E2E_SKIP_BUILD:-}" ]; then
  ( cd "$FE" && npm run build )
fi

# 2. Świeża baza za każdym uruchomieniem (katalog tymczasowy — XXXXXX musi być
#    na końcu wzorca, bo BSD/macOS mktemp nie obsługuje suffiksu po iksach).
DBDIR="$(mktemp -d "${TMPDIR:-/tmp}/mealpilot-e2e-XXXXXX")"
DB="$DBDIR/test.db"

export MEALPILOT_DB="$DB"
export MEALPILOT_STATIC_DIR="$FE/dist"
export MEALPILOT_ADMIN_USERNAME="${E2E_USERNAME:-e2e_admin}"
export MEALPILOT_ADMIN_PASSWORD="${E2E_PASSWORD:-E2eAdminPass1234}"
export MEALPILOT_SECRET="e2e-secret"

PY="$BE/env/bin/python3.12"
[ -x "$PY" ] || PY="python3"

exec "$PY" -m uvicorn app.main:app --app-dir "$BE" --host 127.0.0.1 --port "$PORT"
