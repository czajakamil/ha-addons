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


def _make_recipe(client, title="Wspólny", ingredients=None, servings=1):
    """Create a recipe and return the id the server assigned."""
    r = client.post(
        "/api/recipes",
        json={
            "title": title,
            "servings": servings,
            "ingredients": ingredients if ingredients is not None else [{"name": "sól", "qty": 1, "unit": "g"}],
            "steps": ["Krok"],
        },
    )
    assert r.status_code == 201, r.text
    return r.json()["id"]


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

    recipe_id = _make_recipe(alice)
    assert alice.put(f"/api/recipes/{recipe_id}/ownership", json={"share_with_household": True}).status_code == 200

    return {
        "household": household,
        "alice": alice,
        "alice_id": alice_id,
        "alice_user": _user(db_session, alice_id),
        "bob": bob,
        "bob_id": bob_id,
        "bob_user": _user(db_session, bob_id),
        "recipe_id": recipe_id,
    }


# --------------------------------------------------------------------------- #
# Edycja przez warstwę narzędzi respektuje can_edit
# --------------------------------------------------------------------------- #


def test_member_without_can_edit_cannot_update_shared_recipe_via_tools(shared_setup, db_session):
    with pytest.raises(Forbidden):
        _invoke(
            db_session,
            shared_setup["bob_user"],
            "update_recipe",
            {"recipe_id": shared_setup["recipe_id"], "title": "Hack"},
        )
    db_session.rollback()
    assert db_session.get(models.Recipe, shared_setup["recipe_id"]).title == "Wspólny"


def test_member_without_can_edit_cannot_delete_shared_recipe_via_tools(shared_setup, db_session):
    with pytest.raises(Forbidden):
        _invoke(db_session, shared_setup["bob_user"], "delete_recipe", {"recipe_id": shared_setup["recipe_id"]})
    db_session.rollback()
    assert db_session.get(models.Recipe, shared_setup["recipe_id"]) is not None


def test_member_with_can_edit_can_update_shared_recipe_via_tools(shared_setup, db_session):
    out = _invoke(
        db_session,
        shared_setup["alice_user"],
        "update_recipe",
        {"recipe_id": shared_setup["recipe_id"], "title": "Nowy"},
    )
    assert out["title"] == "Nowy"


def test_member_without_can_edit_can_still_read_shared_recipe(shared_setup, db_session):
    bob = shared_setup["bob_user"]
    rid = shared_setup["recipe_id"]
    assert _invoke(db_session, bob, "get_recipe", {"recipe_id": rid})["id"] == rid
    found = _invoke(db_session, bob, "search_recipes", {"query": "wspólny"})
    assert rid in {i["id"] for i in found["items"]}


def test_tool_layer_hides_a_personal_recipe_from_other_members(shared_setup, db_session):
    """Prywatny przepis Alicji nie jest widoczny dla Boba nawet w tym samym household."""
    from app.services.errors import NotFound

    private_id = _make_recipe(shared_setup["alice"], title="Prywatny Alicji")
    with pytest.raises(NotFound):
        _invoke(db_session, shared_setup["bob_user"], "get_recipe", {"recipe_id": private_id})


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
        db_session,
        created_by=shared_setup["alice_id"],
        household_id=hh,
        day=0,
        meal="Obiad",
        recipe_id=shared_setup["recipe_id"],
    )

    _invoke(
        db_session,
        shared_setup["alice_user"],
        "add_plan_entry",
        {"week_start": WEEK, "day": 2, "meal": "Obiad", "recipe_id": shared_setup["recipe_id"], "servings": 1},
    )

    survivor = _entry_at(db_session, 0, "Obiad")
    assert survivor is not None and survivor.id == original.id
    assert survivor.owner_household_id == hh
    assert survivor.owner_user_id is None


def test_remove_plan_entry_keeps_household_rows_household_owned(shared_setup, db_session):
    hh = shared_setup["household"]
    keep = _household_entry(
        db_session,
        created_by=shared_setup["alice_id"],
        household_id=hh,
        day=0,
        meal="Obiad",
        recipe_id=shared_setup["recipe_id"],
    )
    drop = _household_entry(
        db_session,
        created_by=shared_setup["alice_id"],
        household_id=hh,
        day=1,
        meal="Obiad",
        recipe_id=shared_setup["recipe_id"],
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
        recipe_id=shared_setup["recipe_id"],
        servings=2,
    )

    _invoke(
        db_session,
        shared_setup["alice_user"],
        "set_week_plan",
        {
            "week_start": WEEK,
            "entries": [{"day": 0, "meal": "Obiad", "recipe_id": shared_setup["recipe_id"], "servings": 4}],
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
        db_session,
        created_by=shared_setup["alice_id"],
        household_id=hh,
        day=0,
        meal="Obiad",
        recipe_id=shared_setup["recipe_id"],
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
        recipe_id=shared_setup["recipe_id"],
        servings=2,
    )

    via_rest = shared_setup["bob"].get(f"/api/plan/{WEEK}")
    assert via_rest.status_code == 200
    rest_entries = [(e["day"], e["meal"], e["recipe_id"], e["servings"]) for e in via_rest.json()["entries"]]

    via_tool = _invoke(db_session, shared_setup["bob_user"], "get_week_plan", {"week_start": WEEK})
    tool_entries = [(e["day"], e["meal"], e["recipe_id"], e["servings"]) for e in via_tool["entries"]]

    assert rest_entries == tool_entries == [(0, "Obiad", shared_setup["recipe_id"], 2)]


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
    rid = _make_recipe(
        shared_setup["alice"],
        title="Na zakupy",
        ingredients=[{"name": "ryż", "qty": 100, "unit": "g"}, {"name": "cebula", "qty": 2, "unit": "szt"}],
    )
    _invoke(
        db_session,
        alice,
        "add_plan_entry",
        {"week_start": WEEK, "day": 0, "meal": "Obiad", "recipe_id": rid, "servings": 1},
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
    rid = _make_recipe(
        shared_setup["alice"], title="Na zakupy 2", ingredients=[{"name": "ryż", "qty": 100, "unit": "g"}]
    )
    _invoke(
        db_session,
        alice,
        "add_plan_entry",
        {"week_start": WEEK, "day": 0, "meal": "Obiad", "recipe_id": rid, "servings": 1},
    )
    generated = _invoke(db_session, alice, "generate_shopping_list", {"week_start": WEEK})
    assert [i["name"] for i in generated["items"]] == ["ryż"]

    manual = _invoke(db_session, alice, "add_shopping_item", {"week_start": WEEK, "name": "Masło", "qty": 1})
    _invoke(db_session, alice, "remove_plan_entry", {"week_start": WEEK, "day": 0, "meal": "Obiad"})

    after = _invoke(db_session, alice, "generate_shopping_list", {"week_start": WEEK})
    # Pozycja z planu znika, ręcznie dopisana zostaje (z tym samym id).
    assert [(i["id"], i["name"]) for i in after["items"]] == [(manual["id"], "Masło")]


# --------------------------------------------------------------------------- #
# Lista zakupów dziedziczy własność planu
# --------------------------------------------------------------------------- #


@pytest.fixture
def shared_week(shared_setup, db_session):
    """Wspólny (household) plan na WEEK: jeden przepis household, 100 g ryżu."""
    rid = _make_recipe(
        shared_setup["alice"],
        title="Wspólny obiad",
        ingredients=[{"name": "ryż", "qty": 100, "unit": "g"}],
        servings=1,
    )
    assert (
        shared_setup["alice"].put(f"/api/recipes/{rid}/ownership", json={"share_with_household": True}).status_code
        == 200
    )
    _household_entry(
        db_session,
        created_by=shared_setup["alice_id"],
        household_id=shared_setup["household"],
        day=0,
        meal="Obiad",
        recipe_id=rid,
        servings=1,
    )
    return rid


def _shopping_rows(db, week_start=WEEK):
    db.expire_all()
    return db.query(models.ShoppingItem).filter(models.ShoppingItem.week_start == week_start).all()


def test_household_plan_generates_a_household_owned_list(shared_setup, shared_week, db_session):
    items = shared_setup["alice"].post(f"/api/shopping/{WEEK}/generate").json()
    assert [i["name"] for i in items] == ["ryż"]

    rows = _shopping_rows(db_session)
    assert len(rows) == 1
    assert rows[0].owner_household_id == shared_setup["household"]
    assert rows[0].owner_user_id is None

    # Drugi członek widzi DOKŁADNIE tę samą pozycję (to samo id), nie własną kopię.
    bob_items = shared_setup["bob"].get(f"/api/shopping/{WEEK}").json()
    assert [i["id"] for i in bob_items] == [i["id"] for i in items]


def test_personal_plan_still_generates_a_private_list(shared_setup, db_session):
    """Bez wspólnego planu zachowanie jest dokładnie takie jak dotąd: lista prywatna."""
    rid = _make_recipe(
        shared_setup["alice"], title="Prywatny obiad", ingredients=[{"name": "ryż", "qty": 100, "unit": "g"}]
    )
    # PUT /api/plan tworzy wpisy prywatne.
    assert (
        shared_setup["alice"]
        .put(f"/api/plan/{WEEK}", json=[{"day": 0, "meal": "Obiad", "recipe_id": rid, "servings": 1}])
        .status_code
        == 200
    )
    items = shared_setup["alice"].post(f"/api/shopping/{WEEK}/generate").json()
    assert [i["name"] for i in items] == ["ryż"]

    rows = _shopping_rows(db_session)
    assert rows[0].owner_user_id == shared_setup["alice_id"]
    assert rows[0].owner_household_id is None
    assert shared_setup["bob"].get(f"/api/shopping/{WEEK}").json() == []


def test_mixed_week_puts_a_line_touched_by_a_household_recipe_on_the_shared_list(shared_setup, shared_week, db_session):
    """Reguła: jeden household'owy przepis w linii wystarczy, by pozycja była wspólna."""
    private_rid = _make_recipe(
        shared_setup["alice"], title="Tylko moje", ingredients=[{"name": "kakao", "qty": 10, "unit": "g"}]
    )
    # Prywatny wpis planu obok istniejącego wpisu household.
    entries = [
        {"day": 0, "meal": "Obiad", "recipe_id": shared_week, "servings": 1},
        {"day": 1, "meal": "Obiad", "recipe_id": private_rid, "servings": 1},
    ]
    assert shared_setup["alice"].put(f"/api/plan/{WEEK}", json=entries).status_code == 200

    shared_setup["alice"].post(f"/api/shopping/{WEEK}/generate")
    by_name = {r.name: r for r in _shopping_rows(db_session)}
    assert by_name["ryż"].owner_household_id == shared_setup["household"]
    assert by_name["kakao"].owner_user_id == shared_setup["alice_id"]
    assert by_name["kakao"].owner_household_id is None


def test_member_without_can_edit_can_check_off_a_shared_item(shared_setup, shared_week):
    items = shared_setup["alice"].post(f"/api/shopping/{WEEK}/generate").json()
    item_id = items[0]["id"]

    r = shared_setup["bob"].patch(f"/api/shopping/{WEEK}/items/{item_id}", json={"checked": True})
    assert r.status_code == 200, r.text
    assert r.json()["checked"] is True

    # Odhaczenie jest widoczne dla drugiego członka — to jedna lista, nie dwie.
    alice_items = shared_setup["alice"].get(f"/api/shopping/{WEEK}").json()
    assert alice_items[0]["checked"] is True


def test_member_without_can_edit_still_cannot_add_or_delete_on_a_shared_list(shared_setup, shared_week):
    items = shared_setup["alice"].post(f"/api/shopping/{WEEK}/generate").json()
    item_id = items[0]["id"]

    assert shared_setup["bob"].post(f"/api/shopping/{WEEK}/items", json={"name": "wino", "qty": 1}).status_code == 403
    assert shared_setup["bob"].delete(f"/api/shopping/{WEEK}/items/{item_id}").status_code == 403
    assert shared_setup["bob"].delete(f"/api/shopping/{WEEK}").status_code == 403
    assert len(shared_setup["alice"].get(f"/api/shopping/{WEEK}").json()) == 1


def test_regeneration_by_the_other_member_does_not_duplicate_the_list(
    admin_client, shared_setup, shared_week, db_session
):
    _assign(admin_client, shared_setup["bob_id"], shared_setup["household"], can_edit=True)

    first = shared_setup["alice"].post(f"/api/shopping/{WEEK}/generate").json()
    second = shared_setup["bob"].post(f"/api/shopping/{WEEK}/generate").json()

    assert [i["id"] for i in first] == [i["id"] for i in second]
    assert len(_shopping_rows(db_session)) == len(first) == 1


def test_manual_item_on_a_shared_week_lands_on_the_shared_list(shared_setup, shared_week, db_session):
    shared_setup["alice"].post(f"/api/shopping/{WEEK}/generate")
    added = shared_setup["alice"].post(f"/api/shopping/{WEEK}/items", json={"name": "wino", "qty": 1}).json()

    row = next(r for r in _shopping_rows(db_session) if r.id == added["id"])
    assert row.owner_household_id == shared_setup["household"]
    assert row.owner_user_id is None
    assert added["id"] in {i["id"] for i in shared_setup["bob"].get(f"/api/shopping/{WEEK}").json()}


def test_tool_layer_lets_a_member_without_can_edit_check_off_a_shared_item(shared_setup, shared_week, db_session):
    """Ta sama reguła „widzi ⇒ może odhaczyć" obowiązuje agenta i MCP."""
    items = shared_setup["alice"].post(f"/api/shopping/{WEEK}/generate").json()
    item_id = items[0]["id"]

    out = _invoke(db_session, shared_setup["bob_user"], "check_shopping_item", {"item_id": item_id, "checked": True})
    assert out["checked"] is True

    # ...ale dopisanie i usunięcie nadal nie.
    with pytest.raises(Forbidden):
        _invoke(db_session, shared_setup["bob_user"], "add_shopping_item", {"week_start": WEEK, "name": "wino"})
    db_session.rollback()
    with pytest.raises(Forbidden):
        _invoke(db_session, shared_setup["bob_user"], "delete_shopping_item", {"item_id": item_id})
    db_session.rollback()
    assert db_session.get(models.ShoppingItem, item_id) is not None
