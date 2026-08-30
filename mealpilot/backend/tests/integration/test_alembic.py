"""Alembic: brak dryfu, adopcja starej bazy, świeża instalacja, downgrade.

Najważniejszy jest tu `test_migracje_nie_dryfuja_od_modeli`: bez niego rewizje
i `app/models.py` rozjadą się po kilku miesiącach i nikt tego nie zauważy, bo
testy jadą na `Base.metadata.create_all()`, a nie na migracjach.
"""

import pytest
from alembic.autogenerate import compare_metadata
from alembic.migration import MigrationContext
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, inspect, text

from app import main, migrator
from app.db import Base
from app.db import engine as app_engine
from app.main import _run_migrations, app

from ..conftest import ADMIN_PASSWORD, ADMIN_USERNAME
from .test_migrations import _legacy_db

pytestmark = [pytest.mark.integration, pytest.mark.smoke]


@pytest.fixture
def blank_engine(tmp_path):
    """Engine na nieistniejący jeszcze plik bazy (stan „świeża instalacja")."""
    eng = create_engine(f"sqlite:///{tmp_path / 'fresh.db'}", future=True)
    yield eng
    eng.dispose()


def _alembic_version(engine_) -> str | None:
    if migrator.VERSION_TABLE not in inspect(engine_).get_table_names():
        return None
    with engine_.connect() as conn:
        return conn.execute(text(f"SELECT version_num FROM {migrator.VERSION_TABLE}")).scalar()


def _schema_diff(engine_) -> list:
    with engine_.connect() as conn:
        ctx = MigrationContext.configure(
            conn, opts={"compare_type": True, "compare_server_default": True, "render_as_batch": True}
        )
        return compare_metadata(ctx, Base.metadata)


# --------------------------------------------------------------------------- #
# Antydryf
# --------------------------------------------------------------------------- #


def test_migracje_nie_dryfuja_od_modeli(blank_engine):
    """`upgrade head` na pustej bazie musi dać dokładnie to, co `Base.metadata`.

    Porównanie robi ta sama autogeneracja, której używa `alembic revision
    --autogenerate`: pusta lista różnic = brak dryfu. Gdy ten test padnie,
    znaczy to, że ktoś zmienił model bez rewizji (albo rewizję bez modelu).
    """
    migrator.upgrade_head(blank_engine)

    diff = _schema_diff(blank_engine)
    assert diff == [], f"Migracje rozjechały się z modelami: {diff}"


def test_head_jest_osiagniety_i_nie_ma_zaleglosci(blank_engine):
    migrator.upgrade_head(blank_engine)

    assert _alembic_version(blank_engine) == migrator.head_revision()
    assert migrator.has_pending(blank_engine) is False


# --------------------------------------------------------------------------- #
# Adopcja istniejącej bazy (ścieżka 2: są tabele, nie ma alembic_version)
# --------------------------------------------------------------------------- #


def test_stara_baza_zostaje_zaadoptowana_bez_utraty_danych(tmp_path):
    eng = _legacy_db(tmp_path)
    assert _alembic_version(eng) is None

    _run_migrations(eng)

    assert _alembic_version(eng) == migrator.head_revision()
    with eng.connect() as conn:
        assert conn.execute(text("SELECT username FROM users")).scalar() == "stary"
        assert conn.execute(text("SELECT title FROM recipes")).scalar() == "Stary przepis"
    eng.dispose()


def test_adopcja_usuwa_martwe_kolumny_ustawien_agenta(tmp_path):
    eng = _legacy_db(tmp_path)
    assert {"endpoint", "api_key"} <= {c["name"] for c in inspect(eng).get_columns("agent_settings")}

    _run_migrations(eng)

    cols = {c["name"] for c in inspect(eng).get_columns("agent_settings")}
    assert "endpoint" not in cols and "api_key" not in cols
    eng.dispose()


def test_adopcja_zdejmuje_martwy_constraint_listy_zakupow(tmp_path):
    """Baza, która ma jeszcze `uq_shop_user_week_name_unit`, traci go przy starcie."""
    eng = _legacy_db(tmp_path)
    with eng.begin() as conn:
        conn.execute(text("DROP TABLE shopping_items"))
        conn.execute(
            text(
                "CREATE TABLE shopping_items ("
                " id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER,"
                " week_start VARCHAR, name VARCHAR, qty FLOAT, unit VARCHAR,"
                " category VARCHAR, checked INTEGER,"
                " CONSTRAINT uq_shop_user_week_name_unit UNIQUE (user_id, week_start, name, unit))"
            )
        )
        conn.execute(
            text("INSERT INTO shopping_items (user_id, week_start, name, unit) VALUES (1, '2026-07-06', 'ryż', 'g')")
        )

    _run_migrations(eng)

    assert inspect(eng).get_unique_constraints("shopping_items") == []
    with eng.connect() as conn:
        # Przebudowa tabeli w trybie batch nie może zgubić wierszy.
        assert conn.execute(text("SELECT name FROM shopping_items")).scalar() == "ryż"
    eng.dispose()


def test_powtorny_start_na_zaadoptowanej_bazie_jest_no_opem(tmp_path, monkeypatch):
    eng = _legacy_db(tmp_path)
    _run_migrations(eng)

    def _explode(*_args, **_kwargs):
        raise AssertionError("_migrate() nie może być wołany na bazie z alembic_version")

    monkeypatch.setattr(main, "_migrate", _explode)
    _run_migrations(eng)  # drugi start: sam upgrade head, bez zaległości

    assert _alembic_version(eng) == migrator.head_revision()
    eng.dispose()


# --------------------------------------------------------------------------- #
# Świeża instalacja (ścieżka 1: brak jakichkolwiek tabel)
# --------------------------------------------------------------------------- #


def test_swieza_instalacja_startuje_na_samych_migracjach():
    """Pusta baza aplikacji: schemat buduje `upgrade head`, nie `create_all()`."""
    Base.metadata.drop_all(bind=app_engine)
    with app_engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {migrator.VERSION_TABLE}"))
    assert inspect(app_engine).get_table_names() == []

    with TestClient(app) as c:  # lifespan -> _run_migrations
        assert _alembic_version(app_engine) == migrator.head_revision()

        r = c.post("/api/auth/setup", json={"username": ADMIN_USERNAME, "password": ADMIN_PASSWORD})
        assert r.status_code == 201, r.text
        r = c.post("/api/recipes", json={"title": "Naleśniki", "servings": 2, "ingredients": [], "steps": []})
        assert r.status_code == 201, r.text
        assert r.json()["title"] == "Naleśniki"

    assert _schema_diff(app_engine) == []


# --------------------------------------------------------------------------- #
# Downgrade
# --------------------------------------------------------------------------- #


def test_downgrade_ostatniej_rewizji_wykonuje_sie(blank_engine):
    migrator.upgrade_head(blank_engine)

    migrator.downgrade(blank_engine, "-1")

    assert _alembic_version(blank_engine) == migrator.base_revision()
    cols = {c["name"] for c in inspect(blank_engine).get_columns("agent_settings")}
    assert {"endpoint", "api_key"} <= cols
    assert {c["name"] for c in inspect(blank_engine).get_unique_constraints("shopping_items")} == {
        "uq_shop_user_week_name_unit"
    }

    migrator.upgrade_head(blank_engine)  # i z powrotem w przód
    assert _schema_diff(blank_engine) == []


def test_downgrade_do_zera_kasuje_caly_schemat(blank_engine):
    migrator.upgrade_head(blank_engine)

    migrator.downgrade(blank_engine, "base")

    assert inspect(blank_engine).get_table_names() == [migrator.VERSION_TABLE]
