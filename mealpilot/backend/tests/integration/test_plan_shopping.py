import pytest

pytestmark = pytest.mark.integration

WEEK = "2026-07-06"


def _recipe(client, rid, ingredients, servings=2):
    return client.post(
        "/api/recipes",
        json={
            "id": rid,
            "title": rid,
            "servings": servings,
            "ingredients": ingredients,
            "steps": ["x"],
        },
    )


def test_plan_rejects_unknown_recipe(admin_client):
    r = admin_client.put(
        WEEK and f"/api/plan/{WEEK}",
        json=[
            {"day": 0, "meal": "Obiad", "recipe_id": "nie-istnieje", "servings": 1},
        ],
    )
    assert r.status_code == 400


def test_plan_put_is_idempotent_replace(admin_client):
    _recipe(admin_client, "p1", [{"name": "ryż", "qty": 100, "unit": "g"}])
    entries = [{"day": 0, "meal": "Obiad", "recipe_id": "p1", "servings": 1}]
    admin_client.put(f"/api/plan/{WEEK}", json=entries)
    # Drugie PUT z tą samą zawartością nie duplikuje.
    r = admin_client.put(f"/api/plan/{WEEK}", json=entries)
    assert len(r.json()["entries"]) == 1
    assert admin_client.get(f"/api/plan/{WEEK}").json()["entries"][0]["recipe_id"] == "p1"


def test_plan_day_out_of_range_rejected(admin_client):
    _recipe(admin_client, "p2", [{"name": "x", "qty": 1, "unit": "g"}])
    r = admin_client.put(
        f"/api/plan/{WEEK}",
        json=[
            {"day": 9, "meal": "Obiad", "recipe_id": "p2", "servings": 1},
        ],
    )
    assert r.status_code == 422


def test_shopping_consolidates_and_scales(admin_client):
    # 200 g ryżu na 2 porcje przepisu; w planie 2 sloty po 2 porcje (scale=1 każdy) → 400 g.
    _recipe(admin_client, "s1", [{"name": "ryż", "qty": 200, "unit": "g"}], servings=2)
    admin_client.put(
        f"/api/plan/{WEEK}",
        json=[
            {"day": 0, "meal": "Obiad", "recipe_id": "s1", "servings": 2},
            {"day": 1, "meal": "Obiad", "recipe_id": "s1", "servings": 2},
        ],
    )
    items = admin_client.post(f"/api/shopping/{WEEK}/generate").json()
    rice = [i for i in items if i["name"].lower() == "ryż"]
    assert len(rice) == 1  # skonsolidowane
    assert rice[0]["qty"] == 400.0
    assert rice[0]["unit"] == "g"


def test_shopping_normalizes_kg_to_g(admin_client):
    _recipe(admin_client, "s2", [{"name": "mąka", "qty": 1, "unit": "kg"}], servings=1)
    admin_client.put(
        f"/api/plan/{WEEK}",
        json=[
            {"day": 0, "meal": "Obiad", "recipe_id": "s2", "servings": 1},
        ],
    )
    items = admin_client.post(f"/api/shopping/{WEEK}/generate").json()
    flour = next(i for i in items if i["name"].lower() == "mąka")
    assert flour["unit"] == "g"
    assert flour["qty"] == 1000.0


def test_shopping_checkbox_persists_across_regenerate(admin_client):
    _recipe(admin_client, "s3", [{"name": "sól", "qty": 5, "unit": "g"}], servings=1)
    admin_client.put(
        f"/api/plan/{WEEK}",
        json=[
            {"day": 0, "meal": "Obiad", "recipe_id": "s3", "servings": 1},
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
    _recipe(admin_client, "src1", [{"name": "czosnek", "qty": 1, "unit": "szt"}], servings=1)
    _recipe(admin_client, "src2", [{"name": "czosnek", "qty": 2, "unit": "szt"}], servings=1)
    admin_client.put(
        f"/api/plan/{WEEK}",
        json=[
            {"day": 0, "meal": "Obiad", "recipe_id": "src1", "servings": 1},
            {"day": 1, "meal": "Obiad", "recipe_id": "src2", "servings": 1},
        ],
    )
    items = admin_client.post(f"/api/shopping/{WEEK}/generate").json()
    garlic = next(i for i in items if i["name"].lower() == "czosnek")
    assert set(garlic["recipe_ids"]) == {"src1", "src2"}

    # Regeneracja nie duplikuje wpisów źródłowych.
    items2 = admin_client.post(f"/api/shopping/{WEEK}/generate").json()
    garlic2 = next(i for i in items2 if i["name"].lower() == "czosnek")
    assert sorted(garlic2["recipe_ids"]) == ["src1", "src2"]


def test_manual_add_from_recipe_tracks_and_merges_source(admin_client):
    _recipe(admin_client, "src3", [{"name": "cebula", "qty": 1, "unit": "szt"}], servings=1)
    _recipe(admin_client, "src4", [{"name": "cebula", "qty": 1, "unit": "szt"}], servings=1)
    r1 = admin_client.post(
        f"/api/shopping/{WEEK}/items",
        json={
            "name": "cebula",
            "qty": 1,
            "unit": "szt",
            "recipe_id": "src3",
        },
    )
    assert r1.json()["recipe_ids"] == ["src3"]

    r2 = admin_client.post(
        f"/api/shopping/{WEEK}/items",
        json={
            "name": "cebula",
            "qty": 1,
            "unit": "szt",
            "recipe_id": "src4",
        },
    )
    item = r2.json()
    assert item["qty"] == 2.0
    assert sorted(item["recipe_ids"]) == ["src3", "src4"]


def test_deleting_recipe_removes_it_from_shopping_item_sources(admin_client):
    _recipe(admin_client, "src5", [{"name": "masło", "qty": 1, "unit": "szt"}], servings=1)
    added = admin_client.post(
        f"/api/shopping/{WEEK}/items",
        json={
            "name": "masło",
            "qty": 1,
            "unit": "szt",
            "recipe_id": "src5",
        },
    ).json()
    assert added["recipe_ids"] == ["src5"]

    assert admin_client.delete("/api/recipes/src5").status_code == 204
    items = admin_client.get(f"/api/shopping/{WEEK}").json()
    butter = next(i for i in items if i["id"] == added["id"])
    assert butter["recipe_ids"] == []
