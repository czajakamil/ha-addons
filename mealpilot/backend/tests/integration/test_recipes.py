import pytest

from app.images import IMAGES_DIR

pytestmark = pytest.mark.integration


def _make_recipe(client, rid="r1", **over):
    payload = {
        "id": rid,
        "title": "Przepis",
        "servings": 2,
        "tags": ["lunch"],
        "ingredients": [{"name": "ryż", "qty": 100, "unit": "g"}],
        "steps": ["Ugotuj ryż"],
        "meal_types": ["Obiad"],
        **over,
    }
    return client.post("/api/recipes", json=payload)


def test_create_duplicate_id_conflicts(admin_client):
    assert _make_recipe(admin_client, "dup").status_code == 201
    assert _make_recipe(admin_client, "dup").status_code == 409


def test_create_requires_title(admin_client):
    r = admin_client.post("/api/recipes", json={"id": "x"})
    assert r.status_code == 422


def test_legacy_string_steps_coerced(admin_client):
    _make_recipe(admin_client, "steps1", steps=["Krok A", "Krok B"])
    body = admin_client.get("/api/recipes/steps1").json()
    assert body["steps"] == [
        {"text": "Krok A", "duration_minutes": None},
        {"text": "Krok B", "duration_minutes": None},
    ]


def test_rating_upsert_and_aggregate(admin_client):
    _make_recipe(admin_client, "rate1")
    r = admin_client.put("/api/recipes/rate1/rating", json={"rating": 4})
    assert r.status_code == 200

    body = admin_client.get("/api/recipes/rate1").json()
    assert body["my_rating"] == 4
    assert body["avg_rating"] == 4.0
    assert body["rating_count"] == 1

    # Upsert nadpisuje, nie tworzy drugiego wpisu.
    admin_client.put("/api/recipes/rate1/rating", json={"rating": 2})
    body = admin_client.get("/api/recipes/rate1").json()
    assert body["my_rating"] == 2
    assert body["rating_count"] == 1


def test_rating_out_of_range_rejected(admin_client):
    _make_recipe(admin_client, "rate2")
    assert admin_client.put("/api/recipes/rate2/rating", json={"rating": 9}).status_code == 422


def test_rating_delete(admin_client):
    _make_recipe(admin_client, "rate3")
    admin_client.put("/api/recipes/rate3/rating", json={"rating": 5})
    assert admin_client.delete("/api/recipes/rate3/rating").status_code == 204
    body = admin_client.get("/api/recipes/rate3").json()
    assert body["my_rating"] is None
    assert body["rating_count"] == 0


def test_filter_by_min_my_rating(admin_client):
    _make_recipe(admin_client, "f1")
    _make_recipe(admin_client, "f2")
    admin_client.put("/api/recipes/f1/rating", json={"rating": 5})
    admin_client.put("/api/recipes/f2/rating", json={"rating": 2})
    ids = {r["id"] for r in admin_client.get("/api/recipes", params={"min_my_rating": 4}).json()}
    assert "f1" in ids and "f2" not in ids


def test_tags_and_meal_types_meta(admin_client):
    _make_recipe(admin_client, "m1", tags=["alfa", "beta"], meal_types=["Śniadanie"])
    tags = admin_client.get("/api/recipes/meta/tags").json()["tags"]
    mts = admin_client.get("/api/recipes/meta/meal_types").json()["meal_types"]
    assert "alfa" in tags and "beta" in tags
    assert "Śniadanie" in mts


def test_note_round_trip(admin_client):
    _make_recipe(admin_client, "note1")
    r = admin_client.put("/api/recipes/note1/note", json={"note": "moja prywatna notatka"})
    assert r.status_code == 200
    assert admin_client.get("/api/recipes/note1").json()["my_note"] == "moja prywatna notatka"
    assert admin_client.delete("/api/recipes/note1/note").status_code == 204
    assert admin_client.get("/api/recipes/note1").json()["my_note"] is None


def test_image_upload_and_delete(admin_client):
    _make_recipe(admin_client, "img1")
    png = b"\x89PNG\r\n\x1a\n" + b"0" * 64
    r = admin_client.post(
        "/api/recipes/img1/image",
        files={"file": ("photo.png", png, "image/png")},
    )
    assert r.status_code == 200
    assert r.json()["image_filename"] == "img1.png"
    assert (IMAGES_DIR / "img1.png").exists()

    r = admin_client.delete("/api/recipes/img1/image")
    assert r.status_code == 200
    assert r.json()["image_filename"] is None
    assert not (IMAGES_DIR / "img1.png").exists()


def test_image_unsupported_type_rejected(admin_client):
    _make_recipe(admin_client, "img2")
    r = admin_client.post(
        "/api/recipes/img2/image",
        files={"file": ("doc.txt", b"hello", "text/plain")},
    )
    assert r.status_code == 415


def test_get_missing_recipe_404(admin_client):
    assert admin_client.get("/api/recipes/nie-ma").status_code == 404
