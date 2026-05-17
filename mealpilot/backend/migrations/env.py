import sys
from pathlib import Path
from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool

# Make `app` importable when running alembic CLI from backend/
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.db import Base, DB_URL  # noqa: E402
from app import models  # noqa: F401,E402 — registers all ORM models on Base

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Allow the URL to be injected programmatically (from main.py lifespan)
if not config.get_main_option("sqlalchemy.url", fallback=None):
    config.set_main_option("sqlalchemy.url", DB_URL)

target_metadata = Base.metadata


def run_migrations_offline() -> None:
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
