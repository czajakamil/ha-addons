"""Database helpers (engine, sessions, initialization)."""

from __future__ import annotations

import contextlib
import errno
import logging
import os
import time
from pathlib import Path
from typing import TYPE_CHECKING

from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker

from .config import PROJECT_ROOT, settings
from .models import Base

if TYPE_CHECKING:
    from alembic.config import Config as AlembicConfig

log = logging.getLogger(__name__)


def make_engine(db_path: str | None = None):
    raw_path = db_path or settings.db_path

    connect_args: dict[str, object] = {}
    if raw_path == ":memory:":
        database_url = "sqlite:///:memory:"
    else:
        db_file = Path(raw_path).expanduser()
        if not db_file.is_absolute():
            db_file = (PROJECT_ROOT / db_file).resolve()
        else:
            db_file = db_file.resolve()

        db_file.parent.mkdir(parents=True, exist_ok=True)
        database_url = f"sqlite:///{db_file}"
        connect_args = {"timeout": 30}

    engine = create_engine(
        database_url,
        future=True,
        echo=False,
        connect_args=connect_args,
    )

    @event.listens_for(engine, "connect")
    def _set_sqlite_pragmas(dbapi_conn, _):  # pragma: no cover - exercised at runtime
        cur = dbapi_conn.cursor()
        cur.execute("PRAGMA foreign_keys = ON;")
        cur.execute("PRAGMA journal_mode = WAL;")
        cur.execute("PRAGMA synchronous = NORMAL;")
        cur.execute("PRAGMA busy_timeout = 5000;")
        cur.close()

    return engine


engine = make_engine()
SessionLocal = sessionmaker(bind=engine, expire_on_commit=False, autoflush=False)


def _resolve_sqlite_db_path(db_path: str) -> Path | None:
    if db_path == ":memory:":
        return None

    db_file = Path(db_path).expanduser()
    if db_file.is_absolute():
        return db_file.resolve()
    return (PROJECT_ROOT / db_file).resolve()


def _lock_file_handle(lock_file):
    lock_file.seek(0)
    if os.name == "nt":
        import msvcrt

        try:
            msvcrt.locking(lock_file.fileno(), msvcrt.LK_NBLCK, 1)
        except OSError as exc:
            if exc.errno in (errno.EACCES, errno.EAGAIN, errno.EDEADLK):
                raise BlockingIOError from exc
            raise
    else:
        import fcntl

        try:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            if exc.errno in (errno.EACCES, errno.EAGAIN):
                raise BlockingIOError from exc
            raise


def _unlock_file_handle(lock_file):
    lock_file.seek(0)
    if os.name == "nt":
        import msvcrt

        msvcrt.locking(lock_file.fileno(), msvcrt.LK_UNLCK, 1)
    else:
        import fcntl

        fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


@contextlib.contextmanager
def _sqlite_migration_lock(timeout_seconds: float = 15.0, poll_interval: float = 0.1):
    if engine.dialect.name != "sqlite":
        yield
        return

    db_file = _resolve_sqlite_db_path(settings.db_path)
    if db_file is None:
        yield
        return

    lock_path = db_file.with_suffix(db_file.suffix + ".migrate.lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)

    start = time.monotonic()
    with lock_path.open("a+", encoding="utf-8") as lock_file:
        while True:
            try:
                _lock_file_handle(lock_file)
                break
            except BlockingIOError as exc:
                if time.monotonic() - start >= timeout_seconds:
                    raise TimeoutError(
                        f"Timed out waiting for SQLite migration lock: {lock_path}"
                    ) from exc
                time.sleep(poll_interval)

        try:
            yield
        finally:
            _unlock_file_handle(lock_file)


def _alembic_config() -> AlembicConfig:
    """Build an Alembic config that targets this project's migrations.

    We deliberately construct ``Config()`` without an ``.ini`` file: env.py
    derives the database URL from ``settings`` and skips ``fileConfig`` when no
    config file is set, so we don't clobber the app's logging setup. Only the
    script location needs to be pinned so it works regardless of CWD.
    """
    from alembic.config import Config as AlembicConfig

    cfg = AlembicConfig()
    cfg.set_main_option("script_location", str(PROJECT_ROOT / "backend" / "alembic"))
    return cfg


def _stamp_if_unmanaged() -> None:
    """Adopt a pre-Alembic database created via ``create_all``.

    Older dev databases were built directly from the models (no Alembic), so
    they carry the current schema but aren't pinned to a revision. Running
    ``upgrade`` on such a DB would fail ("table already exists"). If we find
    application tables but no *actual* current revision (no ``alembic_version``
    table, or an empty one left over from an aborted run), we stamp it at head —
    but only after checking that every table and column the models declare is
    there. A brand-new (empty) DB is left alone so that ``upgrade`` can build
    it from scratch.
    """
    from alembic import command
    from alembic.runtime.migration import MigrationContext
    from sqlalchemy import inspect

    inspector = inspect(engine)
    app_tables = set(inspector.get_table_names()) - {"alembic_version"}
    if not app_tables:
        return

    with engine.connect() as conn:
        current = MigrationContext.configure(conn).get_current_revision()
    if current is not None:
        return

    # Stamping tells Alembic "every migration is applied". If the DB predates
    # some of them, that claim is false and the missing columns would only
    # surface later as query errors — so refuse instead of guessing.
    missing = _missing_schema(inspector)
    if missing:
        raise RuntimeError(
            "Database has no Alembic revision and does not match the current "
            "schema, so it can't be adopted automatically. Missing: " + ", ".join(missing)
        )

    log.info("Adopting pre-Alembic database: stamping at head.")
    command.stamp(_alembic_config(), "head")


def _missing_schema(inspector) -> list[str]:
    """Tables/columns the models declare but the database lacks."""
    existing_tables = set(inspector.get_table_names())
    missing: list[str] = []
    for table in Base.metadata.sorted_tables:
        if table.name not in existing_tables:
            missing.append(table.name)
            continue
        existing_columns = {col["name"] for col in inspector.get_columns(table.name)}
        missing.extend(
            f"{table.name}.{col.name}" for col in table.columns if col.name not in existing_columns
        )
    return missing


def init_db():
    """Bring the schema to the latest Alembic revision; return the engine.

    In-memory databases (used by tests) can't be shared across the separate
    connection Alembic opens, so they fall back to ``create_all``.
    """
    if settings.db_path == ":memory:":
        Base.metadata.create_all(engine)
        return engine

    from alembic import command

    with _sqlite_migration_lock():
        _stamp_if_unmanaged()
        command.upgrade(_alembic_config(), "head")
    return engine


def get_db_session():
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()
