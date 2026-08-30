import pytest

pytestmark = pytest.mark.integration

WEEK = "2026-07-06"


def _recipe(client, title, ingredients, servings=2):
    """Create a recipe and return the id the server assigned."""
    r = client.post(
        "/api/recipes",
        json={
            "title": title,
            "servings": servings,
            "ingredients": ingredients,
            "steps": ["x"],
        },
    )
    assert r.status_code == 201, r.text
    return r.json()["id"]


def test_plan_rejects_unknown_recipe(admin_client):
    r = admin_client.put(
        WEEK and f"/api/plan/{WEEK}",
        json=[
            {"day": 0, "meal": "Obiad", "recipe_id": 999999, "servings": 1},
        ],
    )
    assert r.status_code == 400


def test_plan_put_is_idempotent_replace(admin_client):
    rid = _recipe(admin_client, "p1", [{"name": "ryż", "qty": 100, "unit": "g"}])
    entries = [{"day": 0, "meal": "Obiad", "recipe_id": rid, "servings": 1}]
    admin_client.put(f"/api/plan/{WEEK}", json=entries)
    # Drugie PUT z tą samą zawartością nie duplikuje.
    r = admin_client.put(f"/api/plan/{WEEK}", json=entries)
    assert len(r.json()["entries"]) == 1
    assert admin_client.get(f"/api/plan/{WEEK}").json()["entries"][0]["recipe_id"] == rid


def test_plan_day_out_of_range_rejected(admin_client):
    rid = _recipe(admin_client, "p2", [{"name": "x", "qty": 1, "unit": "g"}])
    r = admin_client.put(
        f"/api/plan/{WEEK}",
        json=[
            {"day": 9, "meal": "Obiad", "recipe_id": rid, "servings": 1},
        ],
    )
    assert r.status_code == 422


def test_shopping_consolidates_and_scales(admin_client):
    # 200 g ryżu na 2 porcje przepisu; w planie 2 sloty po 2 porcje (scale=1 każdy) → 400 g.
    rid = _recipe(admin_client, "s1", [{"name": "ryż", "qty": 200, "unit": "g"}], servings=2)
    admin_client.put(
        f"/api/plan/{WEEK}",
        json=[
            {"day": 0, "meal": "Obiad", "recipe_id": rid, "servings": 2},
            {"day": 1, "meal": "Obiad", "recipe_id": rid, "servings": 2},
        ],
    )
    items = admin_client.post(f"/api/shopping/{WEEK}/generate").json()
    rice = [i for i in items if i["name"].lower() == "ryż"]
    assert len(rice) == 1  # skonsolidowane
    assert rice[0]["qty"] == 400.0
    assert rice[0]["unit"] == "g"


def test_shopping_normalizes_kg_to_g(admin_client):
    rid = _recipe(admin_client, "s2", [{"name": "mąka", "qty": 1, "unit": "kg"}], servings=1)
    admin_client.put(
        f"/api/plan/{WEEK}",
        json=[
            {"day": 0, "meal": "Obiad", "recipe_id": rid, "servings": 1},
        ],
    )
    items = admin_client.post(f"/api/shopping/{WEEK}/generate").json()
    flour = next(i for i in items if i["name"].lower() == "mąka")
    assert flour["unit"] == "g"
    assert flour["qty"] == 1000.0


def test_shopping_checkbox_persists_across_regenerate(admin_client):
    rid = _recipe(admin_client, "s3", [{"name": "sól", "qty": 5, "unit": "g"}], servings=1)
    admin_client.put(
        f"/api/plan/{WEEK}",
        json=[
            {"day": 0, "meal": "Obiad", "recipe_id": rid, "servings": 1},
        ],
    )
    items = admin_client.post(f"/api/shopping/{WEEK}/generate").json()
    item_id = items[0]["id"]
    admin_client.patch(f"/api/shopping/items/{item_id}", json={"checked": True})
    # Regeneracja zachowuje stan odhaczenia.
    items2 = admin_client.post(f"/api/shopping/{WEEK}/generate").json()
    assert items2[0]["checked"] is True


def test_custom_item_add_and_delete(admin_client):
    r = admin_client.post(f"/api/shopping/{WEEK}/items", json={"name": "papier", "qty": 2, "unit": "szt"})
    assert r.status_code == 200
    item = r.json()
    assert item["is_custom"] is True
    # Dodanie tej samej nazwy+jednostki sumuje ilość.
    r2 = admin_client.post(f"/api/shopping/{WEEK}/items", json={"name": "papier", "qty": 3, "unit": "szt"})
    assert r2.json()["qty"] == 5.0
    assert admin_client.delete(f"/api/shopping/items/{item['id']}").status_code == 204


def test_clear_shopping_list(admin_client):
    admin_client.post(f"/api/shopping/{WEEK}/items", json={"name": "x", "qty": 1, "unit": "szt"})
    assert admin_client.delete(f"/api/shopping/{WEEK}").status_code == 204
    assert admin_client.get(f"/api/shopping/{WEEK}").json() == []


def test_generate_with_empty_plan_clears_auto_items(admin_client):
    # Brak planu → auto-pozycje usunięte, custom zostają.
    admin_client.post(f"/api/shopping/{WEEK}/items", json={"name": "custom", "qty": 1, "unit": "szt"})
    items = admin_client.post(f"/api/shopping/{WEEK}/generate").json()
    assert all(i["is_custom"] for i in items)


def test_generated_item_tracks_source_recipes(admin_client):
    # Dwa przepisy w planie dzielą składnik → jedna pozycja z oboma recipe_ids.
    src1 = _recipe(admin_client, "src1", [{"name": "czosnek", "qty": 1, "unit": "szt"}], servings=1)
    src2 = _recipe(admin_client, "src2", [{"name": "czosnek", "qty": 2, "unit": "szt"}], servings=1)
    admin_client.put(
        f"/api/plan/{WEEK}",
        json=[
            {"day": 0, "meal": "Obiad", "recipe_id": src1, "servings": 1},
            {"day": 1, "meal": "Obiad", "recipe_id": src2, "servings": 1},
        ],
    )
    items = admin_client.post(f"/api/shopping/{WEEK}/generate").json()
    garlic = next(i for i in items if i["name"].lower() == "czosnek")
    assert set(garlic["recipe_ids"]) == {src1, src2}

    # Regeneracja nie duplikuje wpisów źródłowych.
    items2 = admin_client.post(f"/api/shopping/{WEEK}/generate").json()
    garlic2 = next(i for i in items2 if i["name"].lower() == "czosnek")
    assert sorted(garlic2["recipe_ids"]) == sorted([src1, src2])


def test_manual_add_from_recipe_tracks_and_merges_source(admin_client):
    src3 = _recipe(admin_client, "src3", [{"name": "cebula", "qty": 1, "unit": "szt"}], servings=1)
    src4 = _recipe(admin_client, "src4", [{"name": "cebula", "qty": 1, "unit": "szt"}], servings=1)
    r1 = admin_client.post(
        f"/api/shopping/{WEEK}/items",
        json={
            "name": "cebula",
            "qty": 1,
            "unit": "szt",
            "recipe_id": src3,
        },
    )
    assert r1.json()["recipe_ids"] == [src3]

    r2 = admin_client.post(
        f"/api/shopping/{WEEK}/items",
        json={
            "name": "cebula",
            "qty": 1,
            "unit": "szt",
            "recipe_id": src4,
        },
    )
    item = r2.json()
    assert item["qty"] == 2.0
    assert sorted(item["recipe_ids"]) == sorted([src3, src4])


def test_deleting_recipe_removes_it_from_shopping_item_sources(admin_client):
    src5 = _recipe(admin_client, "src5", [{"name": "masło", "qty": 1, "unit": "szt"}], servings=1)
    added = admin_client.post(
        f"/api/shopping/{WEEK}/items",
        json={
            "name": "masło",
            "qty": 1,
            "unit": "szt",
            "recipe_id": src5,
        },
    ).json()
    assert added["recipe_ids"] == [src5]

    assert admin_client.delete(f"/api/recipes/{src5}").status_code == 204
    items = admin_client.get(f"/api/shopping/{WEEK}").json()
    butter = next(i for i in items if i["id"] == added["id"])
    assert butter["recipe_ids"] == []


OTHER_WEEK = "2026-07-13"  # kolejny poniedziałek


def test_manual_topup_of_a_generated_item_survives_regeneration(admin_client):
    """Ręczna nadwyżka doliczona do pozycji z planu nie znika przy regeneracji."""
    rid = _recipe(admin_client, "topup", [{"name": "ryż", "qty": 100, "unit": "g"}], servings=1)
    admin_client.put(f"/api/plan/{WEEK}", json=[{"day": 0, "meal": "Obiad", "recipe_id": rid, "servings": 1}])
    generated = admin_client.post(f"/api/shopping/{WEEK}/generate").json()
    rice = next(i for i in generated if i["name"] == "ryż")
    assert rice["qty"] == 100.0
    assert rice["is_custom"] is False

    topped = admin_client.post(f"/api/shopping/{WEEK}/items", json={"name": "ryż", "qty": 50, "unit": "g"}).json()
    assert topped["id"] == rice["id"]
    assert topped["qty"] == 150.0
    # Doliczenie ręcznej ilości przejmuje pozycję: przestaje być przeliczana z planu.
    assert topped["is_custom"] is True

    again = admin_client.post(f"/api/shopping/{WEEK}/generate").json()
    rows = [i for i in again if i["name"] == "ryż"]
    assert len(rows) == 1  # bez duplikatu obok pozycji ręcznej
    assert rows[0]["id"] == rice["id"]
    assert rows[0]["qty"] == 150.0


def test_manual_item_shadowing_a_planned_ingredient_does_not_break_generate(admin_client):
    """Pozycja ręczna dodana PRZED generowaniem przejmuje linię (wcześniej: 500 z UNIQUE)."""
    rid = _recipe(admin_client, "shadow", [{"name": "ryż", "qty": 100, "unit": "g"}], servings=1)
    admin_client.put(f"/api/plan/{WEEK}", json=[{"day": 0, "meal": "Obiad", "recipe_id": rid, "servings": 1}])
    manual = admin_client.post(f"/api/shopping/{WEEK}/items", json={"name": "ryż", "qty": 50, "unit": "g"}).json()

    r = admin_client.post(f"/api/shopping/{WEEK}/generate")
    assert r.status_code == 200, r.text
    rows = [i for i in r.json() if i["name"] == "ryż"]
    assert [(i["id"], i["qty"]) for i in rows] == [(manual["id"], 50.0)]


def test_manual_add_matches_existing_item_case_insensitively(admin_client):
    first = admin_client.post(f"/api/shopping/{WEEK}/items", json={"name": "Cukier", "qty": 1, "unit": "szt"}).json()
    second = admin_client.post(f"/api/shopping/{WEEK}/items", json={"name": "cukier", "qty": 2, "unit": "szt"}).json()
    assert second["id"] == first["id"]
    assert second["qty"] == 3.0


# --------------------------------------------------------------------------- #
# week_start w ścieżce jest częścią tożsamości zasobu
# --------------------------------------------------------------------------- #


def test_patch_through_the_wrong_week_is_404(admin_client):
    item = admin_client.post(f"/api/shopping/{WEEK}/items", json={"name": "papier", "qty": 1, "unit": "szt"}).json()

    wrong = admin_client.patch(f"/api/shopping/{OTHER_WEEK}/items/{item['id']}", json={"checked": True})
    assert wrong.status_code == 404
    assert admin_client.get(f"/api/shopping/{WEEK}").json()[0]["checked"] is False

    ok = admin_client.patch(f"/api/shopping/{WEEK}/items/{item['id']}", json={"checked": True})
    assert ok.status_code == 200
    assert ok.json()["checked"] is True


def test_delete_through_the_wrong_week_is_404(admin_client):
    item = admin_client.post(f"/api/shopping/{WEEK}/items", json={"name": "mydło", "qty": 1, "unit": "szt"}).json()

    assert admin_client.delete(f"/api/shopping/{OTHER_WEEK}/items/{item['id']}").status_code == 404
    assert len(admin_client.get(f"/api/shopping/{WEEK}").json()) == 1

    assert admin_client.delete(f"/api/shopping/{WEEK}/items/{item['id']}").status_code == 204
    assert admin_client.get(f"/api/shopping/{WEEK}").json() == []


def test_week_agnostic_routes_still_work(admin_client):
    """Warianty /items/{id} bez tygodnia zostają — używa ich warstwa narzędzi."""
    item = admin_client.post(f"/api/shopping/{WEEK}/items", json={"name": "gąbka", "qty": 1, "unit": "szt"}).json()
    assert admin_client.patch(f"/api/shopping/items/{item['id']}", json={"checked": True}).status_code == 200
    assert admin_client.delete(f"/api/shopping/items/{item['id']}").status_code == 204
