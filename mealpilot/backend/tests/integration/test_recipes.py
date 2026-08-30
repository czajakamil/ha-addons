import pytest

from app.images import IMAGES_DIR

pytestmark = pytest.mark.integration


def _make_recipe(client, title="Przepis", **over):
    """Create a recipe and return the id the server assigned."""
    payload = {
        "title": title,
        "servings": 2,
        "tags": ["lunch"],
        "ingredients": [{"name": "ryż", "qty": 100, "unit": "g"}],
        "steps": ["Ugotuj ryż"],
        "meal_types": ["Obiad"],
        **over,
    }
    r = client.post("/api/recipes", json=payload)
    assert r.status_code == 201, r.text
    return r.json()["id"]


def test_same_title_gets_distinct_ids(admin_client):
    # Ids are surrogate keys, so a repeated title is not a conflict.
    first = _make_recipe(admin_client, "Duplikat")
    second = _make_recipe(admin_client, "Duplikat")
    assert first != second
    assert admin_client.get(f"/api/recipes/{second}").json()["title"] == "Duplikat"


def test_client_supplied_id_is_ignored(admin_client):
    rid = _make_recipe(admin_client, "Z własnym id", id="wlasne-id")
    assert isinstance(rid, int)


def test_create_requires_title(admin_client):
    r = admin_client.post("/api/recipes", json={})
    assert r.status_code == 422


def test_renaming_keeps_the_id(admin_client):
    rid = _make_recipe(admin_client, "Stara nazwa")
    assert admin_client.put(f"/api/recipes/{rid}", json={"title": "Nowa nazwa"}).status_code == 200
    body = admin_client.get(f"/api/recipes/{rid}").json()
    assert body["id"] == rid
    assert body["title"] == "Nowa nazwa"


def test_legacy_string_steps_coerced(admin_client):
    rid = _make_recipe(admin_client, "steps1", steps=["Krok A", "Krok B"])
    body = admin_client.get(f"/api/recipes/{rid}").json()
    assert body["steps"] == [
        {"text": "Krok A", "duration_minutes": None},
        {"text": "Krok B", "duration_minutes": None},
    ]


def test_rating_upsert_and_aggregate(admin_client):
    rid = _make_recipe(admin_client, "rate1")
    r = admin_client.put(f"/api/recipes/{rid}/rating", json={"rating": 4})
    assert r.status_code == 200

    body = admin_client.get(f"/api/recipes/{rid}").json()
    assert body["my_rating"] == 4
    assert body["avg_rating"] == 4.0
    assert body["rating_count"] == 1

    # Upsert nadpisuje, nie tworzy drugiego wpisu.
    admin_client.put(f"/api/recipes/{rid}/rating", json={"rating": 2})
    body = admin_client.get(f"/api/recipes/{rid}").json()
    assert body["my_rating"] == 2
    assert body["rating_count"] == 1


def test_rating_out_of_range_rejected(admin_client):
    rid = _make_recipe(admin_client, "rate2")
    assert admin_client.put(f"/api/recipes/{rid}/rating", json={"rating": 9}).status_code == 422


def test_rating_delete(admin_client):
    rid = _make_recipe(admin_client, "rate3")
    admin_client.put(f"/api/recipes/{rid}/rating", json={"rating": 5})
    assert admin_client.delete(f"/api/recipes/{rid}/rating").status_code == 204
    body = admin_client.get(f"/api/recipes/{rid}").json()
    assert body["my_rating"] is None
    assert body["rating_count"] == 0


def test_filter_by_min_my_rating(admin_client):
    f1 = _make_recipe(admin_client, "f1")
    f2 = _make_recipe(admin_client, "f2")
    admin_client.put(f"/api/recipes/{f1}/rating", json={"rating": 5})
    admin_client.put(f"/api/recipes/{f2}/rating", json={"rating": 2})
    ids = {r["id"] for r in admin_client.get("/api/recipes", params={"min_my_rating": 4}).json()}
    assert f1 in ids and f2 not in ids


def test_tags_and_meal_types_meta(admin_client):
    _make_recipe(admin_client, "m1", tags=["alfa", "beta"], meal_types=["Śniadanie"])
    tags = admin_client.get("/api/recipes/meta/tags").json()["tags"]
    mts = admin_client.get("/api/recipes/meta/meal_types").json()["meal_types"]
    assert "alfa" in tags and "beta" in tags
    assert "Śniadanie" in mts


def test_note_round_trip(admin_client):
    rid = _make_recipe(admin_client, "note1")
    r = admin_client.put(f"/api/recipes/{rid}/note", json={"note": "moja prywatna notatka"})
    assert r.status_code == 200
    assert admin_client.get(f"/api/recipes/{rid}").json()["my_note"] == "moja prywatna notatka"
    assert admin_client.delete(f"/api/recipes/{rid}/note").status_code == 204
    assert admin_client.get(f"/api/recipes/{rid}").json()["my_note"] is None


def test_image_upload_and_delete(admin_client):
    rid = _make_recipe(admin_client, "img1")
    png = b"\x89PNG\r\n\x1a\n" + b"0" * 64
    r = admin_client.post(
        f"/api/recipes/{rid}/image",
        files={"file": ("photo.png", png, "image/png")},
    )
    assert r.status_code == 200
    assert r.json()["image_filename"] == f"{rid}.png"
    assert (IMAGES_DIR / f"{rid}.png").exists()

    r = admin_client.delete(f"/api/recipes/{rid}/image")
    assert r.status_code == 200
    assert r.json()["image_filename"] is None
    assert not (IMAGES_DIR / f"{rid}.png").exists()


def test_image_unsupported_type_rejected(admin_client):
    rid = _make_recipe(admin_client, "img2")
    r = admin_client.post(
        f"/api/recipes/{rid}/image",
        files={"file": ("doc.txt", b"hello", "text/plain")},
    )
    assert r.status_code == 415


def test_get_missing_recipe_404(admin_client):
    assert admin_client.get("/api/recipes/999999").status_code == 404


def test_get_non_numeric_recipe_id_rejected(admin_client):
    assert admin_client.get("/api/recipes/kurczak-z-ryzem").status_code == 422


# --------------------------------------------------------------------------- #
# REST przez warstwę serwisów (ta sama normalizacja co agent i MCP)
# --------------------------------------------------------------------------- #


def test_create_normalizes_steps_like_the_tool_layer(admin_client):
    """POST idzie przez `recipes_svc.create_recipe`, więc `coerce_steps` działa tu tak samo."""
    rid = _make_recipe(admin_client, "norm1", steps=["Krok"], meal_prep_steps=["Przygotuj"])
    body = admin_client.get(f"/api/recipes/{rid}").json()
    assert body["steps"] == [{"text": "Krok", "duration_minutes": None}]
    assert body["meal_prep_steps"] == [{"text": "Przygotuj", "duration_minutes": None}]


def test_create_rejects_blank_title(admin_client):
    r = admin_client.post("/api/recipes", json={"title": "   "})
    assert r.status_code == 400, r.text
    assert r.json()["error"] == "invalid_request"


def test_update_explicit_null_on_required_field_is_422(admin_client):
    """Jawne `null` w polu NOT NULL to błąd klienta, nie 500 z bazy."""
    rid = _make_recipe(admin_client, "null1")
    r = admin_client.put(f"/api/recipes/{rid}", json={"title": None})
    assert r.status_code == 422, r.text
    assert "title" in r.json()["detail"]
    # Nic nie zostało zapisane.
    assert admin_client.get(f"/api/recipes/{rid}").json()["title"] == "null1"


def test_update_explicit_null_lists_every_offending_field(admin_client):
    rid = _make_recipe(admin_client, "null2")
    r = admin_client.put(f"/api/recipes/{rid}", json={"tags": None, "servings": None})
    assert r.status_code == 422
    detail = r.json()["detail"]
    assert "servings" in detail and "tags" in detail


def test_update_explicit_null_clears_a_nullable_field(admin_client):
    """`meal_prep_days` jest nullowalne — edytor czyści je wysyłając `null`."""
    rid = _make_recipe(admin_client, "null3", is_meal_prep=True, meal_prep_days=3)
    assert admin_client.get(f"/api/recipes/{rid}").json()["meal_prep_days"] == 3

    r = admin_client.put(f"/api/recipes/{rid}", json={"meal_prep_days": None})
    assert r.status_code == 200, r.text
    assert r.json()["meal_prep_days"] is None
    assert admin_client.get(f"/api/recipes/{rid}").json()["meal_prep_days"] is None


def test_update_response_keeps_the_orm_shape(admin_client):
    """Kontrakt HTTP bez zmian: PUT zwraca ten sam kształt co GET."""
    rid = _make_recipe(admin_client, "shape1")
    updated = admin_client.put(f"/api/recipes/{rid}", json={"title": "shape2", "servings": 4})
    assert updated.status_code == 200
    assert updated.json() == admin_client.get(f"/api/recipes/{rid}").json()


def test_update_of_someone_elses_recipe_is_rejected(make_user):
    ala, _ = make_user("ala_upd")
    bob, _ = make_user("bob_upd")
    rid = _make_recipe(ala, "cudzy")
    r = bob.put(f"/api/recipes/{rid}", json={"title": "przejęty"})
    assert r.status_code in (403, 404), r.text
    assert ala.get(f"/api/recipes/{rid}").json()["title"] == "cudzy"


def test_text_search_ranks_title_matches_first(admin_client):
    _make_recipe(admin_client, "Zupa pomidorowa")
    _make_recipe(admin_client, "Sałatka", ingredients=[{"name": "pomidor", "qty": 2, "unit": "szt"}])
    titles = [r["title"] for r in admin_client.get("/api/recipes", params={"q": "pomidor"}).json()]
    assert titles[0] == "Zupa pomidorowa"
    assert "Sałatka" in titles


def test_text_search_respects_other_filters(admin_client):
    _make_recipe(admin_client, "Kurczak szybki", tags=["szybkie"])
    _make_recipe(admin_client, "Kurczak wolny", tags=["wolne"])
    found = admin_client.get("/api/recipes", params={"q": "kurczak", "tags": "szybkie"}).json()
    assert [r["title"] for r in found] == ["Kurczak szybki"]
