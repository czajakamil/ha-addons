import pytest

pytestmark = pytest.mark.integration


def _make_recipe(client, title="Wspólny"):
    """Create a recipe and return the id the server assigned."""
    r = client.post(
        "/api/recipes",
        json={
            "title": title,
            "servings": 1,
            "ingredients": [{"name": "sól", "qty": 1, "unit": "g"}],
            "steps": ["Krok"],
        },
    )
    assert r.status_code == 201, r.text
    return r.json()["id"]


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
    priv = _make_recipe(alice)
    # Bob nie widzi prywatnego przepisu Alicji.
    assert bob.get(f"/api/recipes/{priv}").status_code == 404
    ids = {r["id"] for r in bob.get("/api/recipes").json()}
    assert priv not in ids


def test_sharing_to_household_makes_visible(admin_client, make_user, household):
    alice, alice_id = make_user("alice2")
    bob, bob_id = make_user("bob2")
    _assign(admin_client, alice_id, household, can_edit=True)
    _assign(admin_client, bob_id, household, can_edit=False)

    rid = _make_recipe(alice)
    # Zanim udostępni — bob nie widzi.
    assert bob.get(f"/api/recipes/{rid}").status_code == 404

    r = alice.put(f"/api/recipes/{rid}/ownership", json={"share_with_household": True})
    assert r.status_code == 200
    assert r.json()["owner_household_id"] == household
    # Teraz bob (członek) widzi.
    assert bob.get(f"/api/recipes/{rid}").status_code == 200


def test_household_member_without_can_edit_cannot_edit(admin_client, make_user, household):
    alice, alice_id = make_user("alice3")
    bob, bob_id = make_user("bob3")
    _assign(admin_client, alice_id, household, can_edit=True)
    _assign(admin_client, bob_id, household, can_edit=False)
    rid = _make_recipe(alice)
    alice.put(f"/api/recipes/{rid}/ownership", json={"share_with_household": True})

    # Bob widzi, ale nie może edytować (brak can_edit, nie jest twórcą).
    assert bob.put(f"/api/recipes/{rid}", json={"title": "Hack"}).status_code == 403
    # Twórca (alice) może.
    assert alice.put(f"/api/recipes/{rid}", json={"title": "OK"}).status_code == 200


def test_member_with_can_edit_can_edit_shared(admin_client, make_user, household):
    alice, alice_id = make_user("alice4")
    bob, bob_id = make_user("bob4")
    _assign(admin_client, alice_id, household, can_edit=True)
    _assign(admin_client, bob_id, household, can_edit=True)
    rid = _make_recipe(alice)
    alice.put(f"/api/recipes/{rid}/ownership", json={"share_with_household": True})
    assert bob.put(f"/api/recipes/{rid}", json={"title": "Edytowane przez Boba"}).status_code == 200


def test_cannot_edit_invisible_personal_recipe(make_user):
    alice, _ = make_user("alice5")
    bob, _ = make_user("bob5")
    rid = _make_recipe(alice)
    # Niewidoczny → 404 (nie zdradzamy istnienia).
    assert bob.put(f"/api/recipes/{rid}", json={"title": "x"}).status_code == 404
    assert bob.delete(f"/api/recipes/{rid}").status_code == 404


def test_sharing_without_household_rejected(make_user):
    alice, _ = make_user("alice6")
    rid = _make_recipe(alice)
    r = alice.put(f"/api/recipes/{rid}/ownership", json={"share_with_household": True})
    assert r.status_code == 400


def test_only_creator_can_repin_ownership(admin_client, make_user, household):
    alice, alice_id = make_user("alice7")
    bob, bob_id = make_user("bob7")
    _assign(admin_client, alice_id, household, can_edit=True)
    _assign(admin_client, bob_id, household, can_edit=True)
    rid = _make_recipe(alice)
    alice.put(f"/api/recipes/{rid}/ownership", json={"share_with_household": True})
    # Bob widzi przepis, ale nie jest twórcą → nie może zmienić ownershipu (404).
    assert bob.put(f"/api/recipes/{rid}/ownership", json={"share_with_household": False}).status_code == 404
