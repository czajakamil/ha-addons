"""Migracja w miejscu: stara baza (sprzed multi-user / meal-prep) musi dać się
podnieść na nowym schemacie bez utraty danych. _migrate() robi ręczne ALTER TABLE,
więc to test regresyjny dla aktualizacji add-onu między wersjami.
"""

import pytest
from sqlalchemy import create_engine, inspect, text

from app.main import _migrate

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
        conn.execute(
            text(
                "INSERT INTO recipes (id, user_id, title, servings) "
                "VALUES ('r1', 1, 'Stary przepis', 2)"
            )
        )
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
