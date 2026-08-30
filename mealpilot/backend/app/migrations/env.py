"""Punkt wejścia Alembica — wspólny dla CLI dewelopera i dla startu aplikacji.

Dwa tryby połączenia:

* **runtime** — ``app/migrator.py`` podstawia gotowe ``Connection`` przez
  ``config.attributes["connection"]``. Migracje jadą wtedy po tym samym
  engine'ie co aplikacja, więc obowiązują pragmy z ``app/db.py`` (WAL,
  ``busy_timeout``) — bez tego migracja na zajętej bazie kończyłaby się
  natychmiastowym „database is locked”. Klucze obce są na czas przebudowy
  tabel wyłączane i przywracane (szczegóły w ``app/migrator.py``).
* **CLI** — brak podstawionego połączenia, więc bierzemy wprost ``app.db.engine``
  (ten sam ``MEALPILOT_DB``, ten sam listener pragm).

``render_as_batch=True`` jest obowiązkowe: SQLite nie potrafi ``ALTER TABLE DROP
COLUMN`` ani zdjąć constraintu, więc Alembic musi przebudować tabelę przez
kopię. ``compare_type=True`` domyka autogenerację o zmiany typów kolumn.
"""

from logging.config import fileConfig

from alembic import context

from app import models  # noqa: F401  (import rejestruje wszystkie tabele w Base.metadata)
from app.db import Base, engine
from app.migrator import exec_outside_transaction

config = context.config

# Ustawiony tylko w trybie CLI (alembic.ini). Runtime buduje Config w kodzie,
# więc nie ma pliku ini i nie ruszamy konfiguracji logowania aplikacji.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata

_CONTEXT_OPTS = {
    "target_metadata": target_metadata,
    "render_as_batch": True,
    "compare_type": True,
    "compare_server_default": True,
}


def run_migrations_offline() -> None:
    """Tryb `--sql`: generuje skrypt bez łączenia się z bazą."""
    context.configure(
        url=str(engine.url),
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        **_CONTEXT_OPTS,
    )
    with context.begin_transaction():
        context.run_migrations()


def _run(connection) -> None:
    context.configure(connection=connection, **_CONTEXT_OPTS)
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    connection = config.attributes.get("connection")
    if connection is not None:
        # Runtime: transakcją i pragmami zarządza `app/migrator.py`.
        _run(connection)
        return
    # CLI: to samo, co robi migrator — bez `foreign_keys=OFF` przebudowa tabeli
    # w trybie batch wywala się na `DROP TABLE` z wierszami potomnymi.
    with engine.connect() as conn:
        exec_outside_transaction(conn, "PRAGMA foreign_keys=OFF")
        try:
            _run(conn)
        finally:
            exec_outside_transaction(conn, "PRAGMA foreign_keys=ON")


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
