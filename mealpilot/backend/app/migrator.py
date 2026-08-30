"""Uruchamianie migracji Alembica z wnętrza aplikacji.

Dlaczego w ogóle ten moduł istnieje, zamiast wołania ``alembic upgrade head``
z shella: obraz add-onu nie zawiera ``alembic.ini`` — Dockerfile kopiuje tylko
``backend/app``, ``backend/mcp_server.py`` i ``backend/scripts``. Runtime buduje
więc ``Config`` w kodzie i wskazuje na ``app/migrations``, które jedzie razem
z pakietem ``app``. ``backend/alembic.ini`` jest wyłącznie dla CLI dewelopera.

--------------------------------------------------------------------------- #
Jak dodać zmianę schematu (to zastępuje dopisywanie ALTER TABLE do _migrate())
--------------------------------------------------------------------------- #

1. Zmień model w ``app/models.py``.
2. ``cd backend && MEALPILOT_DB=/tmp/autogen.db env/bin/alembic upgrade head``
   (świeża baza doprowadzona do bieżącego heada — punkt odniesienia dla diffa).
3. ``MEALPILOT_DB=/tmp/autogen.db env/bin/alembic revision --autogenerate -m "opis"``
4. Przejrzyj wygenerowany plik w ``app/migrations/versions/`` — autogeneracja
   pod SQLite bywa niedokładna przy indeksach i nazwanych constraintach — i
   dopisz sensowny ``downgrade()``.
5. ``MEALPILOT_DB=/tmp/autogen.db env/bin/alembic upgrade head`` i
   ``... alembic downgrade -1`` żeby sprawdzić obie strony.
6. ``env/bin/python -m pytest tests/integration/test_alembic.py`` — test
   antydryfowy pilnuje, że migracje i modele opisują ten sam schemat.
"""

from __future__ import annotations

import logging
from pathlib import Path

from alembic import command
from alembic.config import Config
from alembic.migration import MigrationContext
from alembic.script import ScriptDirectory
from sqlalchemy import Connection, Engine

logger = logging.getLogger(__name__)

MIGRATIONS_DIR = Path(__file__).resolve().parent / "migrations"

VERSION_TABLE = "alembic_version"


def alembic_config(connection: Connection | None = None) -> Config:
    """``Config`` zbudowany w kodzie, bez pliku ini.

    ``connection`` trafia do ``config.attributes``, skąd bierze je ``env.py`` —
    dzięki temu migracje jadą po połączeniu aplikacji (a więc z pragmami
    ustawianymi przez listener w ``app/db.py``), a nie po własnym engine'ie.
    """
    cfg = Config()
    cfg.set_main_option("script_location", str(MIGRATIONS_DIR))
    if connection is not None:
        cfg.attributes["connection"] = connection
    return cfg


def _script() -> ScriptDirectory:
    return ScriptDirectory.from_config(alembic_config())


def base_revision() -> str:
    """Id rewizji bazowej (korzenia łańcucha) — celu dla ``stamp`` przy adopcji."""
    return _script().get_base()


def head_revision() -> str:
    return _script().get_current_head()


def current_revisions(engine_: Engine) -> set[str]:
    with engine_.connect() as conn:
        return set(MigrationContext.configure(conn).get_current_heads())


def has_pending(engine_: Engine) -> bool:
    """True, gdy ``upgrade head`` faktycznie miałby coś do zrobienia."""
    return current_revisions(engine_) != set(_script().get_heads())


def exec_outside_transaction(conn: Connection, sql: str) -> list:
    """Wykonuje `sql` w autocommicie i zwraca wiersze.

    ``exec_driver_sql`` autostartuje transakcję SQLAlchemy (choć pysqlite nie
    wysyła jeszcze BEGIN), więc od razu ją zamykamy — inaczej późniejsze
    ``conn.begin()`` rzuca InvalidRequestError, a PRAGMA i tak jest ignorowana
    wewnątrz transakcji.
    """
    result = conn.exec_driver_sql(sql)
    rows = list(result.fetchall()) if result.returns_rows else []
    conn.rollback()
    return rows


def _run(engine_: Engine, action) -> None:
    """Wykonuje `action(cfg)` na jednym połączeniu, w jednej transakcji.

    ``PRAGMA foreign_keys=OFF`` na czas migracji jest wymuszone przez SQLite,
    nie przez wygodę: ``render_as_batch`` przebudowuje tabelę przez kopię
    (``CREATE _alembic_tmp_x`` → ``DROP TABLE x`` → ``RENAME``), a ``DROP TABLE``
    przy włączonych kluczach obcych wywala się na wierszach potomnych (np.
    ``shopping_item_recipes`` → ``shopping_items``). Po commicie klucze wracają,
    a ``foreign_key_check`` mówi w logu, gdyby batch coś jednak zerwał.
    """
    with engine_.connect() as conn:
        is_sqlite = conn.dialect.name == "sqlite"
        if is_sqlite:
            exec_outside_transaction(conn, "PRAGMA foreign_keys=OFF")
        try:
            with conn.begin():
                action(alembic_config(conn))
        finally:
            if is_sqlite:
                exec_outside_transaction(conn, "PRAGMA foreign_keys=ON")
                for row in exec_outside_transaction(conn, "PRAGMA foreign_key_check"):
                    logger.warning("Naruszenie klucza obcego po migracji: %s", tuple(row))


def upgrade_head(engine_: Engine) -> None:
    _run(engine_, lambda cfg: command.upgrade(cfg, "head"))


def downgrade(engine_: Engine, revision: str) -> None:
    """Cofa migracje do `revision`.

    Runtime nigdy tego nie woła — add-on jedzie tylko w przód. Istnieje po to,
    żeby testy mogły sprawdzić, że każda rewizja ma działający ``downgrade()``,
    i żeby deweloper mógł cofnąć świeżo napisaną rewizję bez ``alembic.ini``.
    """
    _run(engine_, lambda cfg: command.downgrade(cfg, revision))


def stamp(engine_: Engine, revision: str) -> None:
    """Zapisuje `revision` w ``alembic_version`` bez wykonywania migracji."""
    _run(engine_, lambda cfg: command.stamp(cfg, revision))
