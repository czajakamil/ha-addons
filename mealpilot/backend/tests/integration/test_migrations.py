"""Migracja w miejscu: stara baza (sprzed multi-user / meal-prep) musi dać się
podnieść na nowym schemacie bez utraty danych. _migrate() robi ręczne ALTER TABLE,
więc to test regresyjny dla aktualizacji add-onu między wersjami.
"""

import json
from pathlib import Path

import pytest
from sqlalchemy import MetaData, String, create_engine, event, inspect, text

from app.db import Base
from app.main import _migrate


def _enable_wal(dbapi_connection, _connection_record) -> None:
    """Kopia listenera z `app/db.py` — testowe engine'y nie mają go z automatu."""
    cursor = dbapi_connection.cursor()
    try:
        cursor.execute("PRAGMA journal_mode=WAL")
    finally:
        cursor.close()


pytestmark = [pytest.mark.integration, pytest.mark.smoke]


def _legacy_db(tmp_path):
    """Tworzy bazę w „starym" kształcie (przed kolumnami AI/owner/meal-prep)."""
    db_file = tmp_path / "legacy.db"
    eng = create_engine(f"sqlite:///{db_file}", future=True)
    with eng.begin() as conn:
        conn.execute(
            text(
                "CREATE TABLE users ("
                " id INTEGER PRIMARY KEY AUTOINCREMENT,"
                " username VARCHAR NOT NULL,"
                " password_hash VARCHAR NOT NULL,"
                " role VARCHAR NOT NULL DEFAULT 'user',"
                " is_active INTEGER NOT NULL DEFAULT 1,"
                " session_version INTEGER NOT NULL DEFAULT 0,"
                " created_at DATETIME)"
            )
        )
        # Schemat pośredni: user_id już istnieje (dodany we wcześniejszej wersji),
        # ale brakuje kolumn owner_*/meal-prep/AI dodawanych później.
        conn.execute(
            text(
                "CREATE TABLE recipes ("
                " id VARCHAR PRIMARY KEY,"
                " user_id INTEGER,"
                " title VARCHAR NOT NULL,"
                " tags JSON,"
                " servings INTEGER,"
                " ingredients JSON,"
                " steps JSON)"
            )
        )
        conn.execute(
            text(
                "CREATE TABLE meal_plan_entries ("
                " id INTEGER PRIMARY KEY AUTOINCREMENT,"
                " user_id INTEGER,"
                " week_start VARCHAR, day INTEGER, meal VARCHAR,"
                " recipe_id VARCHAR, servings INTEGER)"
            )
        )
        conn.execute(
            text(
                "CREATE TABLE shopping_items ("
                " id INTEGER PRIMARY KEY AUTOINCREMENT,"
                " user_id INTEGER,"
                " week_start VARCHAR, name VARCHAR, qty FLOAT,"
                " unit VARCHAR, category VARCHAR, checked INTEGER)"
            )
        )
        conn.execute(
            text(
                "CREATE TABLE agent_settings ("
                " user_id INTEGER PRIMARY KEY, endpoint VARCHAR,"
                " api_key VARCHAR, model VARCHAR, system_prompt VARCHAR)"
            )
        )
        # Dane z ery jednego użytkownika.
        conn.execute(text("INSERT INTO users (username, password_hash) VALUES ('stary', 'hash')"))
        conn.execute(text("INSERT INTO recipes (id, user_id, title, servings) VALUES ('r1', 1, 'Stary przepis', 2)"))
    return eng


def test_migrate_adds_new_columns_and_keeps_data(tmp_path):
    eng = _legacy_db(tmp_path)

    _migrate(eng)

    insp = inspect(eng)
    user_cols = {c["name"] for c in insp.get_columns("users")}
    assert {
        "can_use_ai",
        "ai_monthly_token_limit",
        "ai_used_tokens_this_month",
        "ai_usage_period_start",
    } <= user_cols

    recipe_cols = {c["name"] for c in insp.get_columns("recipes")}
    assert {
        "image_filename",
        "owner_user_id",
        "owner_household_id",
        "meal_types",
        "is_meal_prep",
        "meal_prep_days",
        "meal_prep_steps",
    } <= recipe_cols

    shop_cols = {c["name"] for c in insp.get_columns("shopping_items")}
    assert {"is_custom", "owner_user_id"} <= shop_cols

    # Dane przetrwały migrację.
    with eng.begin() as conn:
        assert conn.execute(text("SELECT username FROM users")).scalar() == "stary"
        assert conn.execute(text("SELECT title FROM recipes")).scalar() == "Stary przepis"


def test_migrate_is_idempotent(tmp_path):
    eng = _legacy_db(tmp_path)
    _migrate(eng)
    _migrate(eng)  # drugi przebieg nie może rzucić (kolumny już istnieją)
    insp = inspect(eng)
    assert "can_use_ai" in {c["name"] for c in insp.get_columns("users")}


# --------------------------------------------------------------------------- #
# Id przepisów: slug/UUID -> liczba
# --------------------------------------------------------------------------- #

_TEXT_RECIPE_ID_COLUMNS = (
    ("recipes", "id"),
    ("meal_plan_entries", "recipe_id"),
    ("recipe_ratings", "recipe_id"),
    ("recipe_notes", "recipe_id"),
    ("shopping_item_recipes", "recipe_id"),
)


def _text_id_db(tmp_path):
    """Baza w kształcie sprzed migracji: aktualny schemat, ale id przepisu tekstowe."""
    eng = create_engine(f"sqlite:///{tmp_path / 'textids.db'}", future=True)
    meta = MetaData()
    for table in Base.metadata.tables.values():
        table.to_metadata(meta)
    for table_name, column in _TEXT_RECIPE_ID_COLUMNS:
        meta.tables[table_name].c[column].type = String()
    meta.create_all(eng)
    return eng


@pytest.fixture
def text_id_db(tmp_path):
    """Dane w tej bazie odwzorowują realny stan: slugi, UUID-y i osierocone powiązania."""
    eng = _text_id_db(tmp_path)
    t = Base.metadata.tables
    recipe = {
        "user_id": 1,
        "owner_user_id": 1,
        "tags": [],
        "servings": 2,
        "ingredients": [{"name": "ryż", "qty": 100, "unit": "g"}],
        "steps": [],
        "meal_types": [],
        "meal_prep_steps": [],
    }
    with eng.begin() as conn:
        conn.execute(t["users"].insert(), {"id": 1, "username": "ala", "password_hash": "h"})
        conn.execute(
            t["recipes"].insert(),
            [
                {**recipe, "id": "kurczak-curry", "title": "Kurczak curry"},
                {**recipe, "id": "3f1b0c2e-uuid", "title": "Owsianka"},
            ],
        )
        conn.execute(
            t["meal_plan_entries"].insert(),
            [
                {
                    "user_id": 1,
                    "owner_user_id": 1,
                    "week_start": "2026-07-06",
                    "day": 0,
                    "meal": "Obiad",
                    "recipe_id": "kurczak-curry",
                    "servings": 2,
                },
                # Wskazuje na nieistniejący przepis — osierocony wiersz nie może wywrócić migracji.
                {
                    "user_id": 1,
                    "owner_user_id": 1,
                    "week_start": "2026-07-06",
                    "day": 1,
                    "meal": "Obiad",
                    "recipe_id": "skasowany",
                    "servings": 1,
                },
            ],
        )
        conn.execute(t["recipe_ratings"].insert(), {"recipe_id": "3f1b0c2e-uuid", "user_id": 1, "rating": 5})
        conn.execute(t["recipe_notes"].insert(), {"recipe_id": "3f1b0c2e-uuid", "user_id": 1, "note": "za słone"})
        conn.execute(
            t["week_templates"].insert(),
            {
                "user_id": 1,
                "owner_user_id": 1,
                "name": "T",
                "entries": [
                    {"day": 0, "meal": "Obiad", "recipe_id": "3f1b0c2e-uuid", "servings": 1},
                    {"day": 1, "meal": "Obiad", "recipe_id": "skasowany", "servings": 1},
                ],
            },
        )
        conn.execute(
            t["agent_settings"].insert(),
            {"user_id": 1, "ui_prefs": {"favorite_recipe_ids": ["3f1b0c2e-uuid", "skasowany"]}},
        )
    return eng


def test_recipe_ids_become_integers_and_relations_follow(text_id_db):
    _migrate(text_id_db)

    id_col = next(c for c in inspect(text_id_db).get_columns("recipes") if c["name"] == "id")
    assert str(id_col["type"]).upper().startswith("INT")

    with text_id_db.begin() as conn:
        by_title = dict(conn.execute(text("SELECT title, id FROM recipes")).all())
        assert set(by_title) == {"Kurczak curry", "Owsianka"}
        assert all(isinstance(v, int) for v in by_title.values())

        # Wpis planu wskazuje teraz na nowe id; osierocony został pominięty.
        plan = conn.execute(text("SELECT day, recipe_id FROM meal_plan_entries")).all()
        assert plan == [(0, by_title["Kurczak curry"])]

        assert conn.execute(text("SELECT recipe_id FROM recipe_ratings")).scalar() == by_title["Owsianka"]
        assert conn.execute(text("SELECT recipe_id FROM recipe_notes")).scalar() == by_title["Owsianka"]

        # Id ukryte w blobach JSON też są przemapowane, a martwe odwołania usunięte.
        entries = json.loads(conn.execute(text("SELECT entries FROM week_templates")).scalar())
        assert entries == [{"day": 0, "meal": "Obiad", "recipe_id": by_title["Owsianka"], "servings": 1}]
        prefs = json.loads(conn.execute(text("SELECT ui_prefs FROM agent_settings")).scalar())
        assert prefs["favorite_recipe_ids"] == [by_title["Owsianka"]]


def test_recipes_stay_writable_after_the_id_migration(text_id_db):
    _migrate(text_id_db)
    with text_id_db.begin() as conn:
        highest = conn.execute(text("SELECT MAX(id) FROM recipes")).scalar()
        conn.execute(
            text(
                "INSERT INTO recipes (user_id, owner_user_id, title, tags, servings, prep_time, cook_time,"
                " kcal, p, f, c, hue, ingredients, steps, meal_types, is_meal_prep, meal_prep_steps)"
                " VALUES (1, 1, 'Nowy', '[]', 1, 0, 0, 0, 0, 0, 0, 40, '[]', '[]', '[]', 0, '[]')"
            )
        )
        assert conn.execute(text("SELECT id FROM recipes WHERE title = 'Nowy'")).scalar() == highest + 1


def test_recipe_id_migration_is_idempotent(text_id_db):
    _migrate(text_id_db)
    with text_id_db.begin() as conn:
        before = conn.execute(text("SELECT id, title FROM recipes ORDER BY id")).all()

    _migrate(text_id_db)  # drugi przebieg nie może przenumerować niczego ponownie
    with text_id_db.begin() as conn:
        assert conn.execute(text("SELECT id, title FROM recipes ORDER BY id")).all() == before


# --------------------------------------------------------------------------- #
# Kopia zapasowa sprzed migracji id
# --------------------------------------------------------------------------- #


def test_pre_migration_backup_contains_rows_committed_into_the_wal(tmp_path):
    """Kopia musi zawierać dane zapisane tuż przed migracją — także te z WAL.

    Regresja: `app/db.py` włącza `journal_mode=WAL`, więc świeże commity leżą w
    pliku `<db>-wal` aż do checkpointu. `shutil.copy2` samego `.db` kopiował więc
    stan sprzed tych zapisów — a to jedyna siatka bezpieczeństwa przed
    nieodwracalnym przebudowaniem tabel przepisów.
    """
    eng = _text_id_db(tmp_path)

    # Ten sam tryb dziennika co w produkcji; dispose() wymusza nowe połączenia,
    # żeby PRAGMA w ogóle się wykonała (WAL zapisuje się w nagłówku pliku).
    event.listen(eng, "connect", _enable_wal)
    eng.dispose()

    t = Base.metadata.tables
    with eng.begin() as conn:
        conn.execute(t["users"].insert(), {"id": 1, "username": "ala", "password_hash": "h"})
        conn.execute(
            t["recipes"].insert(),
            {
                "id": "tuz-przed-migracja",
                "user_id": 1,
                "owner_user_id": 1,
                "title": "Zapisany tuż przed migracją",
                "tags": [],
                "servings": 1,
                "ingredients": [],
                "steps": [],
                "meal_types": [],
                "meal_prep_steps": [],
            },
        )

    assert Path(f"{tmp_path / 'textids.db'}-wal").exists(), "test bez WAL nie sprawdza tego, co miał sprawdzać"

    _migrate(eng)

    backup = Path(f"{tmp_path / 'textids.db'}.pre-int-ids.bak")
    assert backup.exists()
    # Kopia jest samowystarczalna: żadnych plików obok, do których trzeba by sięgać.
    assert not Path(f"{backup}-wal").exists()

    restored = create_engine(f"sqlite:///{backup}", future=True)
    with restored.begin() as conn:
        rows = conn.execute(text("SELECT id, title FROM recipes")).all()
    restored.dispose()
    # Stan SPRZED migracji: wiersz jest, a jego id jest jeszcze tekstowe.
    assert rows == [("tuz-przed-migracja", "Zapisany tuż przed migracją")]
