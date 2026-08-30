"""Klucz API sięga tylko po dane domenowe — nie po samo konto.

Klucz `X-MealPilot-Token` był akceptowany na wszystkich endpointach. Klucz
wygenerowany do Claude Desktop mógł więc bić w `/api/auth/api-keys` (mnożyć
i kasować klucze), a klucz admina w `/api/admin/*`. Zakres "read" filtrował
tylko po metodzie HTTP, więc klucz "write" był de facto pełną władzą nad kontem.
"""

import pytest

pytestmark = pytest.mark.integration

WEEK = "2026-09-07"
DEFAULT_PASSWORD = "UserPass1234"


def _key(client, name="desktop", scope="write") -> dict:
    r = client.post("/api/auth/api-keys", json={"name": name, "scope": scope})
    assert r.status_code == 201, r.text
    return {"X-MealPilot-Token": r.json()["key"]}


def test_api_key_reaches_domain_endpoints(make_user, new_client):
    owner, _ = make_user("klucz_domena")
    headers = _key(owner)
    c = new_client()

    assert c.get("/api/recipes", headers=headers).status_code == 200
    assert c.get(f"/api/plan/{WEEK}", headers=headers).status_code == 200
    assert c.get(f"/api/shopping/{WEEK}", headers=headers).status_code == 200
    assert c.get("/api/templates", headers=headers).status_code == 200
    assert c.get("/api/settings/agent", headers=headers).status_code == 200
    assert (
        c.post(
            "/api/recipes",
            json={"title": "Z klucza", "servings": 1, "ingredients": [], "steps": []},
            headers=headers,
        ).status_code
        == 201
    )


def test_api_key_may_still_check_its_own_identity(make_user, new_client):
    """`/api/auth/me` to health-check tożsamości — czytelny, bez żadnej władzy."""
    owner, _ = make_user("klucz_tozsamosc")
    headers = _key(owner)
    c = new_client()

    r = c.get("/api/auth/me", headers=headers)
    assert r.status_code == 200
    assert r.json()["username"] == "klucz_tozsamosc"


def test_api_key_cannot_manage_the_account(make_user, new_client):
    owner, _ = make_user("klucz_konto")
    headers = _key(owner)
    c = new_client()

    assert c.get("/api/auth/api-keys", headers=headers).status_code == 403
    assert c.post("/api/auth/api-keys", json={"name": "kolejny"}, headers=headers).status_code == 403
    assert c.delete("/api/auth/api-keys/1", headers=headers).status_code == 403
    r = c.post(
        "/api/auth/change-password",
        json={"old_password": DEFAULT_PASSWORD, "new_password": "ZupelnieInne9999"},
        headers=headers,
    )
    assert r.status_code == 403
    assert "sesji" in r.json()["detail"]

    # Hasło faktycznie się nie zmieniło.
    assert owner.get("/api/auth/me").status_code == 200


def test_admin_api_key_cannot_reach_the_admin_panel(admin_client, new_client):
    headers = _key(admin_client, name="admin-key")
    c = new_client()

    assert c.get("/api/admin/users", headers=headers).status_code == 403
    assert c.get("/api/admin/households", headers=headers).status_code == 403
    assert (
        c.post(
            "/api/admin/users",
            json={"username": "podstawiony", "password": "Haslo12345678"},
            headers=headers,
        ).status_code
        == 403
    )


def test_api_key_cannot_drive_the_ai_agent(make_user, new_client):
    """Rozmowy i limity AI to konto, nie dane domenowe — poza zasięgiem klucza."""
    owner, _ = make_user("klucz_agent")
    headers = _key(owner)
    c = new_client()

    assert c.get("/api/agent/conversations", headers=headers).status_code == 403
    assert c.post("/api/agent/conversations", json={}, headers=headers).status_code == 403
    assert c.get("/api/agent/usage", headers=headers).status_code == 403


def test_invalid_key_on_a_blocked_path_is_unauthorized_not_forbidden(new_client):
    """Ważność klucza sprawdzamy przed ścieżką — inaczej 403 zdradzałby, że klucz jest dobry."""
    c = new_client()
    assert c.get("/api/admin/users", headers={"X-MealPilot-Token": "mp_nieprawidlowy"}).status_code == 401


def test_cookie_session_keeps_full_access(admin_client):
    """UI zarządza kluczami przez ciasteczko — nic z powyższego go nie dotyczy."""
    assert admin_client.get("/api/admin/users").status_code == 200
    assert admin_client.get("/api/admin/households").status_code == 200
    assert admin_client.get("/api/auth/api-keys").status_code == 200
    assert admin_client.post("/api/auth/api-keys", json={"name": "przez-ciasteczko"}).status_code == 201
    assert admin_client.get("/api/agent/conversations").status_code == 200
    assert admin_client.get("/api/recipes").status_code == 200
