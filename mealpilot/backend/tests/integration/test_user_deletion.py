"""Kasowanie użytkownika: nic po nim nie zostaje, a to co wspólne — zostaje.

Kasowanie usera zostawiało wiersze w `api_keys`, `agent_settings`,
`agent_conversations`/`agent_messages`/`agent_tool_uses`, `recipe_ratings`
i `recipe_notes`. To nie były niegroźne śmieci: klucze główne to zwykłe
INTEGER PRIMARY KEY (bez AUTOINCREMENT), więc SQLite nadaje id ponownie, jeśli
skasowany wiersz miał najwyższe. Następny założony użytkownik dziedziczył
wtedy cudzy — wyciekły — klucz API i cudze rozmowy z agentem.
"""

import pytest
from sqlalchemy import func, text

from app import models
from app.db import engine

pytestmark = pytest.mark.integration

WEEK = "2026-09-07"  # poniedziałek
NEW_PASSWORD = "Haslo12345678"


def _recipe(client, title="Danie"):
    r = client.post(
        "/api/recipes",
        json={
            "title": title,
            "servings": 1,
            "ingredients": [{"name": "x", "qty": 1, "unit": "g"}],
            "steps": ["y"],
        },
    )
    assert r.status_code == 201, r.text
    return r.json()["id"]


def _fill_account(client) -> dict:
    """Zostaw po użytkowniku wiersz w każdej tabeli, która się na niego powołuje."""
    recipe_id = _recipe(client)
    assert client.put(f"/api/recipes/{recipe_id}/rating", json={"rating": 5}).status_code == 200
    assert client.put(f"/api/recipes/{recipe_id}/note", json={"note": "sekret"}).status_code == 200
    assert (
        client.put(
            f"/api/plan/{WEEK}",
            json=[{"day": 0, "meal": "Obiad", "recipe_id": recipe_id, "servings": 1}],
        ).status_code
        == 200
    )
    assert client.post(f"/api/shopping/{WEEK}/generate").status_code == 200
    assert (
        client.post(
            "/api/templates",
            json={"name": "T", "entries": [{"day": 0, "meal": "Obiad", "recipe_id": recipe_id, "servings": 1}]},
        ).status_code
        == 201
    )
    assert client.put("/api/settings/agent", json={"model": "gpt", "system_prompt": ""}).status_code == 200

    key = client.post("/api/auth/api-keys", json={"name": "desktop"})
    assert key.status_code == 201, key.text

    conv_id = client.post("/api/agent/conversations", json={"title": "Rozmowa"}).json()["id"]
    msg = client.post(
        f"/api/agent/conversations/{conv_id}/messages",
        json={
            "role": "user",
            "content": "cześć",
            "tool_uses": [{"tool_use_id": "tu1", "tool_name": "list_recipes", "input": {}}],
        },
    )
    assert msg.status_code == 200, msg.text
    assert msg.json()["tool_uses"], "test wymaga realnego tool_use do osierocenia"

    return {
        "recipe_id": recipe_id,
        "conv_id": conv_id,
        "msg_id": msg.json()["id"],
        "raw_key": key.json()["key"],
    }


# --------------------------------------------------------------------------- #
# Wymuszanie kluczy obcych
# --------------------------------------------------------------------------- #


def test_sqlite_connections_enforce_foreign_keys():
    """Bez tego każde `ondelete="CASCADE"` w models.py jest tylko dekoracją."""
    with engine.connect() as conn:
        assert conn.execute(text("PRAGMA foreign_keys")).scalar() == 1
        assert conn.execute(text("PRAGMA busy_timeout")).scalar() == 5000


# --------------------------------------------------------------------------- #
# Przejęcie konta przez ponownie nadane id
# --------------------------------------------------------------------------- #


def test_deleted_user_key_cannot_authenticate_as_the_id_successor(admin_client, make_user, new_client, db_session):
    victim, victim_id = make_user("ofiara")
    raw = victim.post("/api/auth/api-keys", json={"name": "desktop"}).json()["key"]

    probe = new_client()
    assert probe.get("/api/auth/me", headers={"X-MealPilot-Token": raw}).status_code == 200

    # Sedno scenariusza: ofiara ma najwyższe id, więc SQLite nada je ponownie.
    assert victim_id == db_session.query(func.max(models.User.id)).scalar()
    assert admin_client.delete(f"/api/admin/users/{victim_id}").status_code == 204

    heir = admin_client.post("/api/admin/users", json={"username": "spadkobierca", "password": NEW_PASSWORD})
    assert heir.status_code == 201, heir.text
    assert heir.json()["id"] == victim_id, "bez ponownego nadania id test niczego nie sprawdza"

    assert probe.get("/api/auth/me", headers={"X-MealPilot-Token": raw}).status_code == 401


def test_id_successor_does_not_inherit_agent_conversations(admin_client, make_user, new_client):
    victim, victim_id = make_user("ofiara_rozmow")
    victim.post("/api/agent/conversations", json={"title": "Prywatna rozmowa"})

    assert admin_client.delete(f"/api/admin/users/{victim_id}").status_code == 204
    heir = admin_client.post("/api/admin/users", json={"username": "spadkobierca2", "password": NEW_PASSWORD})
    assert heir.json()["id"] == victim_id

    c = new_client()
    assert c.post("/api/auth/login", json={"username": "spadkobierca2", "password": NEW_PASSWORD}).status_code == 200
    assert c.get("/api/agent/conversations").json() == []
    assert c.get("/api/auth/api-keys").json() == []


# --------------------------------------------------------------------------- #
# Brak osieroconych wierszy
# --------------------------------------------------------------------------- #


def test_delete_user_leaves_no_orphan_rows(admin_client, make_user, db_session):
    user, uid = make_user("smieciarz")
    ids = _fill_account(user)

    assert admin_client.delete(f"/api/admin/users/{uid}").status_code == 204
    db_session.expire_all()

    q = db_session.query
    assert q(models.ApiKey).filter(models.ApiKey.user_id == uid).count() == 0
    assert db_session.get(models.AgentSettings, uid) is None
    assert q(models.AgentConversation).filter(models.AgentConversation.user_id == uid).count() == 0
    assert q(models.AgentMessage).filter(models.AgentMessage.conversation_id == ids["conv_id"]).count() == 0
    assert q(models.AgentToolUse).filter(models.AgentToolUse.message_id == ids["msg_id"]).count() == 0
    assert q(models.RecipeRating).filter(models.RecipeRating.user_id == uid).count() == 0
    assert q(models.RecipeNote).filter(models.RecipeNote.user_id == uid).count() == 0
    assert q(models.Recipe).filter(models.Recipe.created_by == uid).count() == 0
    assert db_session.get(models.Recipe, ids["recipe_id"]) is None
    assert q(models.MealPlanEntry).filter(models.MealPlanEntry.created_by == uid).count() == 0
    assert q(models.MealPlanEntry).filter(models.MealPlanEntry.recipe_id == ids["recipe_id"]).count() == 0
    assert q(models.ShoppingItem).filter(models.ShoppingItem.created_by == uid).count() == 0
    assert q(models.ShoppingItemRecipe).filter(models.ShoppingItemRecipe.recipe_id == ids["recipe_id"]).count() == 0
    assert q(models.WeekTemplate).filter(models.WeekTemplate.created_by == uid).count() == 0
    assert q(models.HouseholdMember).filter(models.HouseholdMember.user_id == uid).count() == 0


def test_delete_user_keeps_other_accounts_intact(admin_client, make_user, db_session):
    doomed, doomed_id = make_user("znika")
    survivor, survivor_id = make_user("zostaje")
    _fill_account(doomed)
    kept = _recipe(survivor, "Nietykalny")
    assert survivor.put(f"/api/recipes/{kept}/rating", json={"rating": 4}).status_code == 200

    assert admin_client.delete(f"/api/admin/users/{doomed_id}").status_code == 204
    db_session.expire_all()

    assert db_session.get(models.Recipe, kept) is not None
    assert db_session.query(models.RecipeRating).filter(models.RecipeRating.user_id == survivor_id).count() == 1
    assert survivor.get("/api/auth/me").status_code == 200


# --------------------------------------------------------------------------- #
# Przepisy współdzielone z household
# --------------------------------------------------------------------------- #


@pytest.fixture
def household(admin_client):
    r = admin_client.post("/api/admin/households", json={"name": "Dom"})
    assert r.status_code == 201
    return r.json()["id"]


def _assign(admin_client, user_id, household_id):
    r = admin_client.put(
        f"/api/admin/users/{user_id}/household",
        json={"household_id": household_id, "can_edit": True},
    )
    assert r.status_code == 200, r.text


def test_shared_recipes_are_handed_over_to_a_remaining_member(admin_client, make_user, household, db_session):
    alice, alice_id = make_user("hh_odchodzi")
    bob, bob_id = make_user("hh_zostaje")
    _assign(admin_client, alice_id, household)
    _assign(admin_client, bob_id, household)

    shared = _recipe(alice, "Wspólny")
    assert alice.put(f"/api/recipes/{shared}/ownership", json={"share_with_household": True}).status_code == 200
    private = _recipe(alice, "Prywatny")

    # Bob wpina wspólny przepis w swój plan — po usunięciu Alice nie może zawisnąć.
    assert (
        bob.put(
            f"/api/plan/{WEEK}",
            json=[{"day": 0, "meal": "Obiad", "recipe_id": shared, "servings": 1}],
        ).status_code
        == 200
    )

    assert admin_client.delete(f"/api/admin/users/{alice_id}").status_code == 204
    db_session.expire_all()

    row = db_session.get(models.Recipe, shared)
    assert row is not None, "wspólny przepis household nie może zniknąć razem z autorem"
    assert row.created_by == bob_id
    assert row.owner_household_id == household
    assert db_session.get(models.Recipe, private) is None

    entries = bob.get(f"/api/plan/{WEEK}").json()["entries"]
    assert [e["recipe_id"] for e in entries] == [shared]
    assert bob.get(f"/api/recipes/{shared}").status_code == 200


def test_shared_recipes_die_with_the_last_member(admin_client, make_user, household, db_session):
    solo, solo_id = make_user("hh_ostatni")
    _assign(admin_client, solo_id, household)

    shared = _recipe(solo, "Wspólny bez następcy")
    assert solo.put(f"/api/recipes/{shared}/ownership", json={"share_with_household": True}).status_code == 200

    assert admin_client.delete(f"/api/admin/users/{solo_id}").status_code == 204
    db_session.expire_all()

    assert db_session.get(models.Recipe, shared) is None
