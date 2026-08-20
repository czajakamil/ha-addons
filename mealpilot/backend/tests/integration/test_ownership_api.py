import pytest

pytestmark = pytest.mark.integration


def _recipe_payload(rid, title="Wspólny"):
    return {
        "id": rid,
        "title": title,
        "servings": 1,
        "ingredients": [{"name": "sól", "qty": 1, "unit": "g"}],
        "steps": ["Krok"],
    }


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


def test_personal_recipe_not_visible_to_others(make_user):
    alice, _ = make_user("alice")
    bob, _ = make_user("bob")
    assert alice.post("/api/recipes", json=_recipe_payload("priv1")).status_code == 201
    # Bob nie widzi prywatnego przepisu Alicji.
    assert bob.get("/api/recipes/priv1").status_code == 404
    ids = {r["id"] for r in bob.get("/api/recipes").json()}
    assert "priv1" not in ids


def test_sharing_to_household_makes_visible(admin_client, make_user, household):
    alice, alice_id = make_user("alice2")
    bob, bob_id = make_user("bob2")
    _assign(admin_client, alice_id, household, can_edit=True)
    _assign(admin_client, bob_id, household, can_edit=False)

    alice.post("/api/recipes", json=_recipe_payload("shared1"))
    # Zanim udostępni — bob nie widzi.
    assert bob.get("/api/recipes/shared1").status_code == 404

    r = alice.put("/api/recipes/shared1/ownership", json={"share_with_household": True})
    assert r.status_code == 200
    assert r.json()["owner_household_id"] == household
    # Teraz bob (członek) widzi.
    assert bob.get("/api/recipes/shared1").status_code == 200


def test_household_member_without_can_edit_cannot_edit(admin_client, make_user, household):
    alice, alice_id = make_user("alice3")
    bob, bob_id = make_user("bob3")
    _assign(admin_client, alice_id, household, can_edit=True)
    _assign(admin_client, bob_id, household, can_edit=False)
    alice.post("/api/recipes", json=_recipe_payload("shared2"))
    alice.put("/api/recipes/shared2/ownership", json={"share_with_household": True})

    # Bob widzi, ale nie może edytować (brak can_edit, nie jest twórcą).
    assert bob.put("/api/recipes/shared2", json={"title": "Hack"}).status_code == 403
    # Twórca (alice) może.
    assert alice.put("/api/recipes/shared2", json={"title": "OK"}).status_code == 200


def test_member_with_can_edit_can_edit_shared(admin_client, make_user, household):
    alice, alice_id = make_user("alice4")
    bob, bob_id = make_user("bob4")
    _assign(admin_client, alice_id, household, can_edit=True)
    _assign(admin_client, bob_id, household, can_edit=True)
    alice.post("/api/recipes", json=_recipe_payload("shared3"))
    alice.put("/api/recipes/shared3/ownership", json={"share_with_household": True})
    assert (
        bob.put("/api/recipes/shared3", json={"title": "Edytowane przez Boba"}).status_code == 200
    )


def test_cannot_edit_invisible_personal_recipe(make_user):
    alice, _ = make_user("alice5")
    bob, _ = make_user("bob5")
    alice.post("/api/recipes", json=_recipe_payload("priv2"))
    # Niewidoczny → 404 (nie zdradzamy istnienia).
    assert bob.put("/api/recipes/priv2", json={"title": "x"}).status_code == 404
    assert bob.delete("/api/recipes/priv2").status_code == 404


def test_sharing_without_household_rejected(make_user):
    alice, _ = make_user("alice6")
    alice.post("/api/recipes", json=_recipe_payload("priv3"))
    r = alice.put("/api/recipes/priv3/ownership", json={"share_with_household": True})
    assert r.status_code == 400


def test_only_creator_can_repin_ownership(admin_client, make_user, household):
    alice, alice_id = make_user("alice7")
    bob, bob_id = make_user("bob7")
    _assign(admin_client, alice_id, household, can_edit=True)
    _assign(admin_client, bob_id, household, can_edit=True)
    alice.post("/api/recipes", json=_recipe_payload("shared4"))
    alice.put("/api/recipes/shared4/ownership", json={"share_with_household": True})
    # Bob widzi przepis, ale nie jest twórcą → nie może zmienić ownershipu (404).
    assert (
        bob.put("/api/recipes/shared4/ownership", json={"share_with_household": False}).status_code
        == 404
    )
