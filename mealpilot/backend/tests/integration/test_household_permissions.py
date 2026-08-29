"""Regresje uprawnień household, które naprawiła konsolidacja warstwy narzędzi.

Wcześniej agent/MCP miały własny model uprawnień: członek bez `can_edit` mógł
edytować wspólny przepis, a przebudowa planu/listy zamieniała wiersze household
na prywatne. Teraz jedna implementacja obsługuje REST i narzędzia — te testy to
sprawdzają, w tym asercjami na samym ORM.
"""

import asyncio

import pytest

from app import models
from app.services import registry
from app.services.errors import Forbidden

pytestmark = pytest.mark.integration

WEEK = "2026-07-06"  # poniedziałek


@pytest.fixture
def household(admin_client):
    """Tworzy household i zwraca jego id."""
    r = admin_client.post("/api/admin/households", json={"name": "Dom"})
    assert r.status_code == 201
    return r.json()["id"]


def _assign(admin_client, user_id, household_id, can_edit):
    r = admin_client.put(
        f"/api/admin/users/{user_id}/household",
        json={"household_id": household_id, "can_edit": can_edit},
    )
    assert r.status_code == 200, r.text


def _recipe_payload(rid, title="Wspólny", ingredients=None, servings=1):
    return {
        "id": rid,
        "title": title,
        "servings": servings,
        "ingredients": ingredients if ingredients is not None else [{"name": "sól", "qty": 1, "unit": "g"}],
        "steps": ["Krok"],
    }


def _invoke(db, user, name, args=None):
    return asyncio.run(registry.invoke(db, user, name, args or {}))


def _user(db, user_id):
    return db.get(models.User, user_id)


@pytest.fixture
def shared_setup(admin_client, make_user, household, db_session):
    """Alice (can_edit) i Bob (bez can_edit) w jednym household, ze wspólnym przepisem."""
    alice, alice_id = make_user("alice_hh")
    bob, bob_id = make_user("bob_hh")
    _assign(admin_client, alice_id, household, can_edit=True)
    _assign(admin_client, bob_id, household, can_edit=False)

    assert alice.post("/api/recipes", json=_recipe_payload("wspolny")).status_code == 201
    assert alice.put("/api/recipes/wspolny/ownership", json={"share_with_household": True}).status_code == 200

    return {
        "household": household,
        "alice": alice,
        "alice_id": alice_id,
        "alice_user": _user(db_session, alice_id),
        "bob": bob,
        "bob_id": bob_id,
        "bob_user": _user(db_session, bob_id),
        "recipe_id": "wspolny",
    }


# --------------------------------------------------------------------------- #
# Edycja przez warstwę narzędzi respektuje can_edit
# --------------------------------------------------------------------------- #


def test_member_without_can_edit_cannot_update_shared_recipe_via_tools(shared_setup, db_session):
    with pytest.raises(Forbidden):
        _invoke(db_session, shared_setup["bob_user"], "update_recipe", {"recipe_id": "wspolny", "title": "Hack"})
    db_session.rollback()
    assert db_session.get(models.Recipe, "wspolny").title == "Wspólny"


def test_member_without_can_edit_cannot_delete_shared_recipe_via_tools(shared_setup, db_session):
    with pytest.raises(Forbidden):
        _invoke(db_session, shared_setup["bob_user"], "delete_recipe", {"recipe_id": "wspolny"})
    db_session.rollback()
    assert db_session.get(models.Recipe, "wspolny") is not None


def test_member_with_can_edit_can_update_shared_recipe_via_tools(shared_setup, db_session):
    out = _invoke(db_session, shared_setup["alice_user"], "update_recipe", {"recipe_id": "wspolny", "title": "Nowy"})
    assert out["title"] == "Nowy"


def test_member_without_can_edit_can_still_read_shared_recipe(shared_setup, db_session):
    bob = shared_setup["bob_user"]
    assert _invoke(db_session, bob, "get_recipe", {"recipe_id": "wspolny"})["id"] == "wspolny"
    found = _invoke(db_session, bob, "search_recipes", {"query": "wspólny"})
    assert "wspolny" in {i["id"] for i in found["items"]}


def test_tool_layer_hides_a_personal_recipe_from_other_members(shared_setup, db_session):
    """Prywatny przepis Alicji nie jest widoczny dla Boba nawet w tym samym household."""
    from app.services.errors import NotFound

    shared_setup["alice"].post("/api/recipes", json=_recipe_payload("prywatny_alicji"))
    with pytest.raises(NotFound):
        _invoke(db_session, shared_setup["bob_user"], "get_recipe", {"recipe_id": "prywatny_alicji"})


# --------------------------------------------------------------------------- #
# Ownership wierszy planu przeżywa przebudowę tygodnia
# --------------------------------------------------------------------------- #


def _household_entry(db, *, created_by, household_id, day, meal, recipe_id, servings=2):
    """Wiersz planu należący do household — REST tworzy tylko prywatne."""
    entry = models.MealPlanEntry(
        created_by=created_by,
        owner_user_id=None,
        owner_household_id=household_id,
        week_start=WEEK,
        day=day,
        meal=meal,
        recipe_id=recipe_id,
        servings=servings,
    )
    db.add(entry)
    db.commit()
    db.refresh(entry)
    return entry


def _entry_at(db, day, meal):
    return (
        db.query(models.MealPlanEntry)
        .filter(
            models.MealPlanEntry.week_start == WEEK,
            models.MealPlanEntry.day == day,
            models.MealPlanEntry.meal == meal,
        )
        .one_or_none()
    )


def test_add_plan_entry_keeps_household_rows_household_owned(shared_setup, db_session):
    hh = shared_setup["household"]
    original = _household_entry(
        db_session, created_by=shared_setup["alice_id"], household_id=hh, day=0, meal="Obiad", recipe_id="wspolny"
    )

    _invoke(
        db_session,
        shared_setup["alice_user"],
        "add_plan_entry",
        {"week_start": WEEK, "day": 2, "meal": "Obiad", "recipe_id": "wspolny", "servings": 1},
    )

    survivor = _entry_at(db_session, 0, "Obiad")
    assert survivor is not None and survivor.id == original.id
    assert survivor.owner_household_id == hh
    assert survivor.owner_user_id is None


def test_remove_plan_entry_keeps_household_rows_household_owned(shared_setup, db_session):
    hh = shared_setup["household"]
    keep = _household_entry(
        db_session, created_by=shared_setup["alice_id"], household_id=hh, day=0, meal="Obiad", recipe_id="wspolny"
    )
    drop = _household_entry(
        db_session, created_by=shared_setup["alice_id"], household_id=hh, day=1, meal="Obiad", recipe_id="wspolny"
    )

    _invoke(
        db_session,
        shared_setup["alice_user"],
        "remove_plan_entry",
        {"week_start": WEEK, "day": 1, "meal": "Obiad"},
    )

    assert db_session.get(models.MealPlanEntry, drop.id) is None
    survivor = db_session.get(models.MealPlanEntry, keep.id)
    assert survivor is not None
    assert survivor.owner_household_id == hh
    assert survivor.owner_user_id is None


def test_set_week_plan_updates_household_rows_in_place(shared_setup, db_session):
    hh = shared_setup["household"]
    original = _household_entry(
        db_session,
        created_by=shared_setup["alice_id"],
        household_id=hh,
        day=0,
        meal="Obiad",
        recipe_id="wspolny",
        servings=2,
    )

    _invoke(
        db_session,
        shared_setup["alice_user"],
        "set_week_plan",
        {
            "week_start": WEEK,
            "entries": [{"day": 0, "meal": "Obiad", "recipe_id": "wspolny", "servings": 4}],
        },
    )

    survivor = _entry_at(db_session, 0, "Obiad")
    assert survivor is not None
    # Ten sam wiersz zaktualizowany w miejscu — nie skasowany i odtworzony jako prywatny.
    assert survivor.id == original.id
    assert survivor.servings == 4
    assert survivor.owner_household_id == hh
    assert survivor.owner_user_id is None


def test_member_without_can_edit_cannot_rewrite_household_plan(shared_setup, db_session):
    hh = shared_setup["household"]
    _household_entry(
        db_session, created_by=shared_setup["alice_id"], household_id=hh, day=0, meal="Obiad", recipe_id="wspolny"
    )

    with pytest.raises(Forbidden):
        _invoke(
            db_session,
            shared_setup["bob_user"],
            "set_week_plan",
            {"week_start": WEEK, "entries": []},
        )
    db_session.rollback()
    assert _entry_at(db_session, 0, "Obiad") is not None


# --------------------------------------------------------------------------- #
# REST i warstwa narzędzi widzą to samo
# --------------------------------------------------------------------------- #


def test_rest_and_tools_agree_on_shared_plan_visibility(shared_setup, db_session):
    _household_entry(
        db_session,
        created_by=shared_setup["alice_id"],
        household_id=shared_setup["household"],
        day=0,
        meal="Obiad",
        recipe_id="wspolny",
        servings=2,
    )

    via_rest = shared_setup["bob"].get(f"/api/plan/{WEEK}")
    assert via_rest.status_code == 200
    rest_entries = [(e["day"], e["meal"], e["recipe_id"], e["servings"]) for e in via_rest.json()["entries"]]

    via_tool = _invoke(db_session, shared_setup["bob_user"], "get_week_plan", {"week_start": WEEK})
    tool_entries = [(e["day"], e["meal"], e["recipe_id"], e["servings"]) for e in via_tool["entries"]]

    assert rest_entries == tool_entries == [(0, "Obiad", "wspolny", 2)]


def test_rest_and_tools_agree_on_shared_shopping_visibility(shared_setup, db_session):
    item = models.ShoppingItem(
        created_by=shared_setup["alice_id"],
        owner_user_id=None,
        owner_household_id=shared_setup["household"],
        week_start=WEEK,
        name="Masło",
        qty=1,
        unit="szt",
        category="Nabiał",
        checked=0,
        is_custom=1,
    )
    db_session.add(item)
    db_session.commit()

    via_rest = shared_setup["bob"].get(f"/api/shopping/{WEEK}")
    assert via_rest.status_code == 200
    assert [i["name"] for i in via_rest.json()] == ["Masło"]

    via_tool = _invoke(db_session, shared_setup["bob_user"], "get_shopping_list", {"week_start": WEEK})
    assert [i["name"] for i in via_tool["items"]] == ["Masło"]
    assert {i["id"] for i in via_tool["items"]} == {i["id"] for i in via_rest.json()}


# --------------------------------------------------------------------------- #
# Regeneracja listy zakupów
# --------------------------------------------------------------------------- #


def test_regeneration_keeps_ids_and_preserves_checked(shared_setup, db_session):
    alice = shared_setup["alice_user"]
    shared_setup["alice"].post(
        "/api/recipes",
        json=_recipe_payload(
            "zakupy",
            title="Na zakupy",
            ingredients=[{"name": "ryż", "qty": 100, "unit": "g"}, {"name": "cebula", "qty": 2, "unit": "szt"}],
        ),
    )
    _invoke(
        db_session,
        alice,
        "add_plan_entry",
        {"week_start": WEEK, "day": 0, "meal": "Obiad", "recipe_id": "zakupy", "servings": 1},
    )

    first = _invoke(db_session, alice, "generate_shopping_list", {"week_start": WEEK})
    by_name = {i["name"]: i for i in first["items"]}
    assert set(by_name) == {"ryż", "cebula"}

    _invoke(db_session, alice, "check_shopping_item", {"item_id": by_name["ryż"]["id"], "checked": True})

    second = _invoke(db_session, alice, "generate_shopping_list", {"week_start": WEEK})
    again = {i["name"]: i for i in second["items"]}
    assert {n: i["id"] for n, i in again.items()} == {n: i["id"] for n, i in by_name.items()}
    assert again["ryż"]["checked"] is True
    assert again["cebula"]["checked"] is False


def test_regeneration_drops_items_no_longer_in_the_plan(shared_setup, db_session):
    alice = shared_setup["alice_user"]
    shared_setup["alice"].post(
        "/api/recipes",
        json=_recipe_payload("zakupy2", title="Na zakupy 2", ingredients=[{"name": "ryż", "qty": 100, "unit": "g"}]),
    )
    _invoke(
        db_session,
        alice,
        "add_plan_entry",
        {"week_start": WEEK, "day": 0, "meal": "Obiad", "recipe_id": "zakupy2", "servings": 1},
    )
    generated = _invoke(db_session, alice, "generate_shopping_list", {"week_start": WEEK})
    assert [i["name"] for i in generated["items"]] == ["ryż"]

    manual = _invoke(db_session, alice, "add_shopping_item", {"week_start": WEEK, "name": "Masło", "qty": 1})
    _invoke(db_session, alice, "remove_plan_entry", {"week_start": WEEK, "day": 0, "meal": "Obiad"})

    after = _invoke(db_session, alice, "generate_shopping_list", {"week_start": WEEK})
    # Pozycja z planu znika, ręcznie dopisana zostaje (z tym samym id).
    assert [(i["id"], i["name"]) for i in after["items"]] == [(manual["id"], "Masło")]
