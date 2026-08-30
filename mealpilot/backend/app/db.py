import os

from sqlalchemy import create_engine, event
from sqlalchemy.orm import declarative_base, sessionmaker

DB_PATH = os.environ.get("MEALPILOT_DB", "/data/mealpilot.db")
DB_URL = f"sqlite:///{DB_PATH}"

engine = create_engine(
    DB_URL,
    connect_args={"check_same_thread": False},
    future=True,
)


@event.listens_for(engine, "connect")
def _set_sqlite_pragmas(dbapi_connection, _connection_record) -> None:
    """Per-connection SQLite setup.

    SQLite disables foreign keys by default *per connection*, so every
    ``ondelete="CASCADE"`` in models.py was pure decoration until now — deleting
    a parent row silently left its children behind. It has to be re-issued on
    each new pooled connection; there is no database-level equivalent.

    WAL + busy_timeout go together with it: endpoints are synchronous and run in
    FastAPI's threadpool over a single ``check_same_thread=False`` connection
    pool, so concurrent requests really do overlap. WAL lets readers run while a
    writer holds the write lock, and busy_timeout makes a writer wait instead of
    failing instantly with "database is locked".
    """
    cursor = dbapi_connection.cursor()
    try:
        cursor.execute("PRAGMA foreign_keys=ON")
        # WAL is persistent (stored in the db header) but harmless to re-issue;
        # it is a no-op for in-memory databases.
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA busy_timeout=5000")
    finally:
        cursor.close()


SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
Base = declarative_base()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
