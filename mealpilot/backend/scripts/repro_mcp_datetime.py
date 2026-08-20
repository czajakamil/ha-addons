"""Repro/regression dla buga: TypeError can't subtract offset-naive and offset-aware datetimes.

Odtwarza scenariusz, w którym `api_key.last_used_at` jest NAIVE (tak jak po
odczycie z SQLite przy drugim żądaniu) i sprawdza, że `_resolve_user` tego nie
wywraca. Uruchom z katalogu backend:

    MEALPILOT_DB=/tmp/mealpilot_test.db ./env/bin/python scripts/repro_mcp_datetime.py
"""

import hashlib
import os
import tempfile
from datetime import UTC, datetime, timedelta

# Izolowana baza, zanim cokolwiek z app zaimportuje engine.
os.environ.setdefault("MEALPILOT_DB", os.path.join(tempfile.gettempdir(), "mealpilot_repro.db"))
if os.path.exists(os.environ["MEALPILOT_DB"]):
    os.remove(os.environ["MEALPILOT_DB"])

from app import models
from app.db import SessionLocal, engine
from app.routers.mcp_sse import _resolve_user

RAW_KEY = "mp_test_key_local"


def seed(last_used):
    models.Base.metadata.create_all(bind=engine)
    db = SessionLocal()
    try:
        user = models.User(username="repro", password_hash="x", is_active=1)
        db.add(user)
        db.flush()
        db.add(
            models.ApiKey(
                user_id=user.id,
                name="repro",
                prefix="mp_test",
                key_hash=hashlib.sha256(RAW_KEY.encode()).hexdigest(),
                last_used_at=last_used,
            )
        )
        db.commit()
    finally:
        db.close()


def main():
    # Kluczowy przypadek: NAIVE datetime (jak po odczycie z SQLite przy 2. żądaniu).
    naive_past = datetime.now(UTC).replace(tzinfo=None) - timedelta(hours=1)
    seed(last_used=naive_past)

    user = _resolve_user(RAW_KEY)
    # Nie dotykamy leniwych atrybutów — obiekt jest odłączony po zamknięciu sesji.
    # Sam fakt, że nie poleciał TypeError, jest dowodem poprawki.
    assert isinstance(user, models.User)
    print("OK: _resolve_user obsłużył naive last_used_at bez TypeError")

    # Sanity: zły klucz nadal odrzucany.
    try:
        _resolve_user("mp_zly_klucz")
    except Exception as exc:  # HTTPException 401
        print(f"OK: zły klucz odrzucony ({type(exc).__name__})")
    else:
        raise SystemExit("FAIL: zły klucz powinien zostać odrzucony")

    print("\nWSZYSTKO PRZESZŁO ✅")


if __name__ == "__main__":
    main()
