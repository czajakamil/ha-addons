import pytest

from app import models

pytestmark = pytest.mark.integration


def test_non_admin_forbidden(make_user):
    user, _ = make_user("zwykly")
    assert user.get("/api/admin/users").status_code == 403
    assert user.get("/api/admin/households").status_code == 403


def test_create_user_and_duplicate(admin_client):
    r = admin_client.post("/api/admin/users", json={"username": "nowy", "password": "Haslo12345678"})
    assert r.status_code == 201
    r = admin_client.post("/api/admin/users", json={"username": "nowy", "password": "Haslo12345678"})
    assert r.status_code == 409


def test_update_user_role_and_username(admin_client, make_user):
    _, uid = make_user("przed", login=False)
    r = admin_client.patch(f"/api/admin/users/{uid}", json={"username": "po", "role": "admin"})
    assert r.status_code == 200
    assert r.json()["username"] == "po"
    assert r.json()["role"] == "admin"


def test_cannot_demote_last_admin(admin_client):
    me = admin_client.get("/api/auth/me").json()
    r = admin_client.patch(f"/api/admin/users/{me['id']}", json={"role": "user"})
    assert r.status_code == 400


def test_cannot_disable_last_admin(admin_client):
    me = admin_client.get("/api/auth/me").json()
    r = admin_client.patch(f"/api/admin/users/{me['id']}", json={"is_active": False})
    assert r.status_code == 400


def test_cannot_delete_self(admin_client):
    me = admin_client.get("/api/auth/me").json()
    assert admin_client.delete(f"/api/admin/users/{me['id']}").status_code == 400


def test_delete_user_removes_resources(admin_client, make_user, db_session):
    user, uid = make_user("dokasacji")
    user.post("/api/recipes", json={
        "id": "rdel", "title": "x", "servings": 1,
        "ingredients": [{"name": "a", "qty": 1, "unit": "g"}], "steps": ["s"],
    })
    assert admin_client.delete(f"/api/admin/users/{uid}").status_code == 204
    assert db_session.get(models.User, uid) is None
    assert db_session.query(models.Recipe).filter(models.Recipe.created_by == uid).count() == 0


def test_ai_limits_disable_blocks_user(admin_client, make_user):
    user, uid = make_user("ai_off")
    r = admin_client.put(f"/api/admin/users/{uid}/ai-limits", json={"can_use_ai": False})
    assert r.status_code == 200
    assert r.json()["can_use_ai"] is False
    # User nie przejdzie bramki quota.
    assert user.post("/api/agent/usage/check").status_code == 403


def test_ai_usage_report_and_reset(admin_client, make_user):
    user, uid = make_user("ai_usage")
    user.post("/api/agent/usage/report", json={"tokens": 100, "cost_cents": 10})
    listed = {u["id"]: u for u in admin_client.get("/api/admin/users").json()}
    assert listed[uid]["ai_used_tokens_this_month"] == 100

    assert admin_client.post(f"/api/admin/users/{uid}/ai-usage/reset").status_code == 204
    listed = {u["id"]: u for u in admin_client.get("/api/admin/users").json()}
    assert listed[uid]["ai_used_tokens_this_month"] == 0


def test_ai_token_limit_enforced(admin_client, make_user):
    user, uid = make_user("ai_limit")
    admin_client.put(f"/api/admin/users/{uid}/ai-limits", json={"ai_monthly_token_limit": 50})
    user.post("/api/agent/usage/report", json={"tokens": 50})
    assert user.post("/api/agent/usage/check").status_code == 429


# --- Households ------------------------------------------------------------
def test_household_crud(admin_client):
    r = admin_client.post("/api/admin/households", json={"name": "Rodzina"})
    assert r.status_code == 201
    hid = r.json()["id"]

    r = admin_client.patch(f"/api/admin/households/{hid}", json={"name": "Rodzinka"})
    assert r.json()["name"] == "Rodzinka"

    assert admin_client.delete(f"/api/admin/households/{hid}").status_code == 204
    assert hid not in {h["id"] for h in admin_client.get("/api/admin/households").json()}


def test_household_member_assignment_and_listing(admin_client, make_user):
    _, uid = make_user("czlonek", login=False)
    hid = admin_client.post("/api/admin/households", json={"name": "H"}).json()["id"]
    r = admin_client.put(f"/api/admin/users/{uid}/household", json={"household_id": hid, "can_edit": True})
    assert r.status_code == 200
    assert r.json()["household_id"] == hid
    assert r.json()["can_edit_in_household"] is True

    members = admin_client.get(f"/api/admin/households/{hid}/members").json()
    assert any(m["user_id"] == uid and m["can_edit"] for m in members)
    listed = {h["id"]: h for h in admin_client.get("/api/admin/households").json()}
    assert listed[hid]["member_count"] == 1


def test_deleting_household_reassigns_resources(admin_client, make_user, db_session):
    user, uid = make_user("dom_user")
    hid = admin_client.post("/api/admin/households", json={"name": "H"}).json()["id"]
    admin_client.put(f"/api/admin/users/{uid}/household", json={"household_id": hid, "can_edit": True})
    user.post("/api/recipes", json={
        "id": "rh", "title": "x", "servings": 1,
        "ingredients": [{"name": "a", "qty": 1, "unit": "g"}], "steps": ["s"],
    })
    user.put("/api/recipes/rh/ownership", json={"share_with_household": True})

    assert admin_client.delete(f"/api/admin/households/{hid}").status_code == 204
    # Przepis wraca do twórcy jako prywatny.
    rec = db_session.get(models.Recipe, "rh")
    assert rec.owner_household_id is None
    assert rec.owner_user_id == uid
