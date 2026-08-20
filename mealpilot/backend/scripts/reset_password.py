"""Awaryjny reset hasła użytkownika — uruchamiaj wewnątrz kontenera.

Użycie:
    python -m scripts.reset_password <username> [<new_password>]

Jeśli hasło nie zostanie podane, zostanie wygenerowane losowe i wypisane.
"""

import secrets
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app import models
from app.db import SessionLocal
from app.security import hash_password


def main() -> int:
    if len(sys.argv) < 2:
        print(__doc__, file=sys.stderr)
        return 2

    username = sys.argv[1]
    new_password = sys.argv[2] if len(sys.argv) >= 3 else secrets.token_urlsafe(12)

    db = SessionLocal()
    try:
        user = db.query(models.User).filter(models.User.username == username).one_or_none()
        if not user:
            print(f"User not found: {username}", file=sys.stderr)
            return 1
        user.password_hash = hash_password(new_password)
        user.is_active = 1
        db.commit()
        print(f"Password reset for '{username}'.")
        if len(sys.argv) < 3:
            print(f"New password: {new_password}")
        return 0
    finally:
        db.close()


if __name__ == "__main__":
    raise SystemExit(main())
