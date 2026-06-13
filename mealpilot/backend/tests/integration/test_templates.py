import pytest

pytestmark = pytest.mark.integration

WEEK = "2026-08-03"


def _recipe(client, rid):
    return client.post("/api/recipes", json={
        "id": rid, "title": rid, "servings": 1,
        "ingredients": [{"name": "x", "qty": 1, "unit": "g"}], "steps": ["y"],
    })


def test_create_template_rejects_unknown_recipe(admin_client):
    r = admin_client.post("/api/templates", json={
        "name": "T", "entries": [{"day": 0, "meal": "Obiad", "recipe_id": "ghost", "servings": 1}],
    })
    assert r.status_code == 400


def test_create_and_apply_template(admin_client):
    _recipe(admin_client, "t1")
    r = admin_client.post("/api/templates", json={
        "name": "Mój tydzień",
        "entries": [{"day": 0, "meal": "Obiad", "recipe_id": "t1", "servings": 2}],
    })
    assert r.status_code == 201
    tpl_id = r.json()["id"]

    r = admin_client.post(f"/api/templates/{tpl_id}/apply/{WEEK}")
    assert r.status_code == 200
    entries = r.json()["entries"]
    assert len(entries) == 1
    assert entries[0]["recipe_id"] == "t1"
    assert entries[0]["servings"] == 2


def test_apply_skips_deleted_recipes(admin_client):
    _recipe(admin_client, "t2")
    _recipe(admin_client, "t3")
    tpl_id = admin_client.post("/api/templates", json={
        "name": "T", "entries": [
            {"day": 0, "meal": "Obiad", "recipe_id": "t2", "servings": 1},
            {"day": 1, "meal": "Obiad", "recipe_id": "t3", "servings": 1},
        ],
    }).json()["id"]
    admin_client.delete("/api/recipes/t3")
    entries = admin_client.post(f"/api/templates/{tpl_id}/apply/{WEEK}").json()["entries"]
    ids = {e["recipe_id"] for e in entries}
    assert ids == {"t2"}


def test_delete_template(admin_client):
    _recipe(admin_client, "t4")
    tpl_id = admin_client.post("/api/templates", json={
        "name": "T", "entries": [{"day": 0, "meal": "Obiad", "recipe_id": "t4", "servings": 1}],
    }).json()["id"]
    assert admin_client.delete(f"/api/templates/{tpl_id}").status_code == 204
    assert admin_client.delete(f"/api/templates/{tpl_id}").status_code == 404


def test_template_not_visible_to_other_user(make_user):
    alice, _ = make_user("ta")
    bob, _ = make_user("tb")
    _recipe(alice, "tpriv")
    tpl_id = alice.post("/api/templates", json={
        "name": "T", "entries": [{"day": 0, "meal": "Obiad", "recipe_id": "tpriv", "servings": 1}],
    }).json()["id"]
    assert tpl_id not in {t["id"] for t in bob.get("/api/templates").json()}
    assert bob.delete(f"/api/templates/{tpl_id}").status_code == 404
