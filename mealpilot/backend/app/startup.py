"""
Runs once before uvicorn starts: mkdir, create_all, alembic upgrade, provision admin.
Exit code non-zero on failure so the HA addon supervisor sees a failed start.
"""

import logging
import os
import sys
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

from alembic import command as alembic_command
from alembic.config import Config as AlembicConfig
from alembic.runtime.migration import MigrationContext

from .db import Base, engine, DB_PATH, SessionLocal
from .images import IMAGES_DIR
from . import models
from .security import hash_password, verify_password
from .seed import seed_for_user

_PASSWORD_MIN = 12


def _alembic_cfg() -> AlembicConfig:
    cfg = AlembicConfig()
    migrations_dir = Path(__file__).parent.parent / "migrations"
    cfg.set_main_option("script_location", str(migrations_dir))
    cfg.set_main_option("sqlalchemy.url", str(engine.url))
    return cfg


def _run_migrations() -> None:
    logger.info("Running database migrations…")
    cfg = _alembic_cfg()
    with engine.connect() as conn:
        ctx = MigrationContext.configure(conn)
        current = ctx.get_current_revision()
    if current is None:
        # Brand-new or pre-Alembic DB — stamp head so future migrations don't replay history.
        alembic_command.stamp(cfg, "head")
    else:
        alembic_command.upgrade(cfg, "head")
    logger.info("Migrations complete.")


def _password_strong_enough(pw: str) -> bool:
    return (
        len(pw) >= _PASSWORD_MIN
        and any(c.isalpha() for c in pw)
        and any(c.isdigit() for c in pw)
    )


def _provision_admin() -> None:
    username = os.environ.get("MEALPILOT_ADMIN_USERNAME", "").strip()
    password = os.environ.get("MEALPILOT_ADMIN_PASSWORD", "").strip()
    if not username or not password:
        return
    if not _password_strong_enough(password):
        logger.error(
            "MEALPILOT_ADMIN_PASSWORD is too weak (min. %d characters, at least one letter "
            "and one digit). Update the password in the add-on options and restart.",
            _PASSWORD_MIN,
        )
        return

    logger.info("Provisioning admin user '%s'…", username)
    db = SessionLocal()
    try:
        user = db.query(models.User).filter(models.User.username == username).one_or_none()
        if user is None:
            user = models.User(
                username=username,
                password_hash=hash_password(password),
                role="admin",
                is_active=1,
            )
            db.add(user)
            db.commit()
            db.refresh(user)
            seed_for_user(db, user.id)
            logger.info("Admin user created.")
        else:
            if not verify_password(password, user.password_hash):
                user.password_hash = hash_password(password)
                db.commit()
                logger.info("Admin password updated.")
    finally:
        db.close()


def main() -> None:
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    IMAGES_DIR.mkdir(parents=True, exist_ok=True)
    Base.metadata.create_all(bind=engine)
    _run_migrations()
    _provision_admin()
    logger.info("Startup complete — handing off to uvicorn.")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.exception("Startup failed")
        sys.exit(1)
