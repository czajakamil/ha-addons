import pytest

from tests.conftest import ADMIN_PASSWORD, ADMIN_USERNAME

pytestmark = pytest.mark.integration


def test_setup_only_once(admin_client):
    r = admin_client.post(
        "/api/auth/setup",
        json={"username": "drugi", "password": "AdminPass1234"},
    )
    assert r.status_code == 403


def test_setup_rejects_weak_password(client):
    r = client.post("/api/auth/setup", json={"username": "admin", "password": "krotkie"})
    assert r.status_code == 422


def test_setup_rejects_bad_username(client):
    r = client.post(
        "/api/auth/setup",
        json={"username": "ma spację", "password": "AdminPass1234"},
    )
    assert r.status_code == 422


def test_login_wrong_password(admin_client, new_client):
    c = new_client()
    r = c.post("/api/auth/login", json={"username": ADMIN_USERNAME, "password": "złe-hasło-123"})
    assert r.status_code == 401


def test_me_requires_auth(client):
    assert client.get("/api/auth/me").status_code == 401


def test_logout_clears_session(admin_client):
    assert admin_client.get("/api/auth/me").status_code == 200
    assert admin_client.post("/api/auth/logout").status_code == 204
    assert admin_client.get("/api/auth/me").status_code == 401


def test_login_rate_limited_after_10_attempts(admin_client, new_client):
    c = new_client()
    for _ in range(10):
        r = c.post("/api/auth/login", json={"username": "nieistnieje", "password": "cokolwiek123"})
        assert r.status_code == 401
    r = c.post("/api/auth/login", json={"username": "nieistnieje", "password": "cokolwiek123"})
    assert r.status_code == 429
    assert "Retry-After" in r.headers


def test_change_password_invalidates_other_sessions_and_keys(make_user, new_client):
    c1, uid = make_user("bob")
    # Drugi klient zalogowany jako ten sam user (osobna sesja).
    c2 = new_client()
    assert c2.post("/api/auth/login", json={"username": "bob", "password": "UserPass1234"}).status_code == 200
    # Bob tworzy klucz API.
    assert c1.post("/api/auth/api-keys", json={"name": "k"}).status_code == 201

    r = c1.post(
        "/api/auth/change-password",
        json={"old_password": "UserPass1234", "new_password": "NoweHaslo9999"},
    )
    assert r.status_code == 204

    # Sesja wywołującego dalej działa, druga jest unieważniona.
    assert c1.get("/api/auth/me").status_code == 200
    assert c2.get("/api/auth/me").status_code == 401
    # Klucze API skasowane po rotacji hasła.
    assert c1.get("/api/auth/api-keys").json() == []


def test_change_password_wrong_old(make_user):
    c1, _ = make_user("carol")
    r = c1.post(
        "/api/auth/change-password",
        json={"old_password": "zła-stara-1234", "new_password": "NoweHaslo9999"},
    )
    assert r.status_code == 400


def test_api_key_authenticates_requests(make_user, new_client):
    c1, _ = make_user("dave")
    created = c1.post("/api/auth/api-keys", json={"name": "automation"})
    assert created.status_code == 201
    raw = created.json()["key"]
    assert raw.startswith("mp_")

    # Świeży klient bez ciasteczka, uwierzytelniony tylko nagłówkiem.
    c = new_client()
    r = c.get("/api/auth/me", headers={"X-MealPilot-Token": raw})
    assert r.status_code == 200
    assert r.json()["username"] == "dave"


def test_api_key_revoked_after_delete(make_user, new_client):
    c1, _ = make_user("erin")
    created = c1.post("/api/auth/api-keys", json={"name": "k"}).json()
    raw = created["key"]
    assert c1.delete(f"/api/auth/api-keys/{created['id']}").status_code == 204

    c = new_client()
    r = c.get("/api/auth/me", headers={"X-MealPilot-Token": raw})
    assert r.status_code == 401


def test_invalid_api_key_rejected(new_client):
    c = new_client()
    r = c.get("/api/auth/me", headers={"X-MealPilot-Token": "mp_nieprawidlowy"})
    assert r.status_code == 401
