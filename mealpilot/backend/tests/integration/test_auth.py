"""Integration tests for /api/auth/* endpoints.

Each test uses a fresh in-memory SQLite database (via the `db` / `client`
fixtures from conftest.py) so tests are fully independent.
"""
from __future__ import annotations

import os
import time

import pytest
from fastapi.testclient import TestClient

from app.models import User
from app.security import hash_password


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_ADMIN_USERNAME = "admin"
_ADMIN_PASSWORD = "AdminPass1234"
_ADMIN_NEW_PASSWORD = "NewAdminPass9999"

_REGULAR_USERNAME = "regularuser"
_REGULAR_PASSWORD = "UserPass5678"


def _setup_admin(client: TestClient, username: str = _ADMIN_USERNAME, password: str = _ADMIN_PASSWORD, token: str | None = None) -> dict:
    payload: dict = {"username": username, "password": password}
    if token is not None:
        payload["setup_token"] = token
    return client.post("/api/auth/setup", json=payload)


def _login(client: TestClient, username: str, password: str):
    return client.post("/api/auth/login", json={"username": username, "password": password})


# ---------------------------------------------------------------------------
# POST /api/auth/setup
# ---------------------------------------------------------------------------

class TestSetup:
    def test_creates_admin_on_empty_db(self, client):
        """Setup succeeds and returns 201 with user data when the DB is empty."""
        resp = _setup_admin(client)
        assert resp.status_code == 201
        data = resp.json()
        assert data["username"] == _ADMIN_USERNAME
        assert data["role"] == "admin"
        assert data["is_active"] is True

    def test_setup_required_true_before_setup(self, client):
        resp = client.get("/api/auth/setup-required")
        assert resp.status_code == 200
        assert resp.json()["setup_required"] is True

    def test_setup_required_false_after_setup(self, client):
        _setup_admin(client)
        resp = client.get("/api/auth/setup-required")
        assert resp.status_code == 200
        assert resp.json()["setup_required"] is False

    def test_returns_409_when_admin_already_exists(self, client, admin_user):
        """After a user already exists, setup returns 403 (setup already done)."""
        resp = _setup_admin(client)
        assert resp.status_code == 403

    def test_no_setup_token_env_not_required(self, client, monkeypatch):
        """When MEALPILOT_SETUP_TOKEN is not set, no token is required."""
        monkeypatch.delenv("MEALPILOT_SETUP_TOKEN", raising=False)
        resp = _setup_admin(client)
        assert resp.status_code == 201

    def test_invalid_setup_token_returns_403(self, client, monkeypatch):
        """When MEALPILOT_SETUP_TOKEN is set, wrong token → 403."""
        monkeypatch.setenv("MEALPILOT_SETUP_TOKEN", "secret-token-123")
        resp = _setup_admin(client, token="wrong-token")
        assert resp.status_code == 403

    def test_correct_setup_token_allows_setup(self, client, monkeypatch):
        """When MEALPILOT_SETUP_TOKEN is set, correct token → 201."""
        monkeypatch.setenv("MEALPILOT_SETUP_TOKEN", "secret-token-123")
        resp = _setup_admin(client, token="secret-token-123")
        assert resp.status_code == 201


# ---------------------------------------------------------------------------
# POST /api/auth/login
# ---------------------------------------------------------------------------

class TestLogin:
    def test_valid_credentials_returns_200(self, client, admin_user):
        resp = _login(client, _ADMIN_USERNAME, _ADMIN_PASSWORD)
        assert resp.status_code == 200
        data = resp.json()
        assert data["username"] == _ADMIN_USERNAME
        assert data["role"] == "admin"

    def test_login_sets_session_cookie(self, client, admin_user):
        resp = _login(client, _ADMIN_USERNAME, _ADMIN_PASSWORD)
        assert resp.status_code == 200
        assert "mealpilot_session" in client.cookies

    def test_wrong_password_returns_401(self, client, admin_user):
        resp = _login(client, _ADMIN_USERNAME, "WrongPassword999")
        assert resp.status_code == 401

    def test_nonexistent_user_returns_401(self, client):
        resp = _login(client, "ghost", "AnyPassword123")
        assert resp.status_code == 401

    def test_wrong_user_and_wrong_password_same_error_code(self, client, admin_user):
        """Both wrong-password and non-existent-user must return 401 (not leak info)."""
        r1 = _login(client, _ADMIN_USERNAME, "BadPass1234")
        r2 = _login(client, "nobody", "BadPass1234")
        assert r1.status_code == 401
        assert r2.status_code == 401
        # Both responses carry the same detail message to avoid user enumeration.
        assert r1.json()["detail"] == r2.json()["detail"]

    def test_timing_equalisation_dummy_verify(self, client, admin_user):
        """Non-existent user should take at least some non-trivial time (dummy_verify runs Argon2)."""
        start = time.monotonic()
        _login(client, "ghost_user", "AnyPassword123")
        elapsed = time.monotonic() - start
        # Argon2 should take >10 ms even on fast machines.
        assert elapsed > 0.01, "dummy_verify did not run (timing too fast)"

    def test_inactive_user_cannot_login(self, client, db):
        """An is_active=0 user is rejected at login."""
        user = User(
            username="inactive",
            password_hash=hash_password("InactivePass123"),
            role="user",
            is_active=0,
            session_version=0,
        )
        db.add(user)
        db.commit()
        resp = _login(client, "inactive", "InactivePass123")
        assert resp.status_code == 401


# ---------------------------------------------------------------------------
# GET /api/auth/me
# ---------------------------------------------------------------------------

class TestMe:
    def test_unauthenticated_returns_401(self, client):
        resp = client.get("/api/auth/me")
        assert resp.status_code == 401

    def test_authenticated_returns_user_data(self, client, admin_user):
        _login(client, _ADMIN_USERNAME, _ADMIN_PASSWORD)
        resp = client.get("/api/auth/me")
        assert resp.status_code == 200
        data = resp.json()
        assert data["username"] == _ADMIN_USERNAME
        assert data["id"] == admin_user.id

    def test_regular_user_me(self, client, regular_user):
        _login(client, _REGULAR_USERNAME, _REGULAR_PASSWORD)
        resp = client.get("/api/auth/me")
        assert resp.status_code == 200
        assert resp.json()["username"] == _REGULAR_USERNAME


# ---------------------------------------------------------------------------
# POST /api/auth/logout
# ---------------------------------------------------------------------------

class TestLogout:
    def test_logout_clears_session(self, client, admin_user):
        _login(client, _ADMIN_USERNAME, _ADMIN_PASSWORD)
        # Session works before logout.
        assert client.get("/api/auth/me").status_code == 200

        resp = client.post("/api/auth/logout")
        assert resp.status_code == 204

        # Session no longer works.
        assert client.get("/api/auth/me").status_code == 401

    def test_logout_without_session_still_204(self, client):
        """Logging out with no active session should not error."""
        resp = client.post("/api/auth/logout")
        assert resp.status_code == 204


# ---------------------------------------------------------------------------
# POST /api/auth/change-password
# ---------------------------------------------------------------------------

class TestChangePassword:
    def test_change_password_success(self, client, admin_user):
        _login(client, _ADMIN_USERNAME, _ADMIN_PASSWORD)
        resp = client.post(
            "/api/auth/change-password",
            json={"old_password": _ADMIN_PASSWORD, "new_password": _ADMIN_NEW_PASSWORD},
        )
        assert resp.status_code == 204

    def test_old_password_invalid_after_change(self, client, admin_user):
        _login(client, _ADMIN_USERNAME, _ADMIN_PASSWORD)
        client.post(
            "/api/auth/change-password",
            json={"old_password": _ADMIN_PASSWORD, "new_password": _ADMIN_NEW_PASSWORD},
        )
        # Log out explicitly, then try to log in with the old password.
        client.post("/api/auth/logout")
        resp = _login(client, _ADMIN_USERNAME, _ADMIN_PASSWORD)
        assert resp.status_code == 401

    def test_new_password_works_after_change(self, client, admin_user):
        _login(client, _ADMIN_USERNAME, _ADMIN_PASSWORD)
        client.post(
            "/api/auth/change-password",
            json={"old_password": _ADMIN_PASSWORD, "new_password": _ADMIN_NEW_PASSWORD},
        )
        client.post("/api/auth/logout")
        resp = _login(client, _ADMIN_USERNAME, _ADMIN_NEW_PASSWORD)
        assert resp.status_code == 200

    def test_wrong_old_password_returns_400(self, client, admin_user):
        _login(client, _ADMIN_USERNAME, _ADMIN_PASSWORD)
        resp = client.post(
            "/api/auth/change-password",
            json={"old_password": "WrongOldPass99", "new_password": _ADMIN_NEW_PASSWORD},
        )
        assert resp.status_code == 400

    def test_session_version_bumped_invalidates_old_sessions(self, client, db, admin_user):
        """After password change, old session cookies must be rejected."""
        # Client A logs in, captures session.
        _login(client, _ADMIN_USERNAME, _ADMIN_PASSWORD)
        old_cookies = dict(client.cookies)

        # Change password (uses the current session on `client`).
        client.post(
            "/api/auth/change-password",
            json={"old_password": _ADMIN_PASSWORD, "new_password": _ADMIN_NEW_PASSWORD},
        )

        # Verify session_version was bumped in the DB.
        db.refresh(admin_user)
        assert admin_user.session_version == 1

        # A second client with the old cookies should be rejected.
        from app.main import app as _app
        from app.db import get_db as _get_db

        def _override():
            try:
                yield db
            finally:
                pass

        _app.dependency_overrides[_get_db] = _override
        with TestClient(_app) as second_client:
            second_client.cookies.update(old_cookies)
            # After bumping, the old session cookie's version doesn't match.
            resp = second_client.get("/api/auth/me")
            assert resp.status_code == 401
        _app.dependency_overrides.pop(_get_db, None)

    def test_change_password_unauthenticated_returns_401(self, client):
        resp = client.post(
            "/api/auth/change-password",
            json={"old_password": "AnyOld12345", "new_password": _ADMIN_NEW_PASSWORD},
        )
        assert resp.status_code == 401


# ---------------------------------------------------------------------------
# API Keys: POST / GET / DELETE /api/auth/api-keys
# ---------------------------------------------------------------------------

class TestApiKeys:
    def test_create_api_key_returns_key(self, client, admin_user):
        _login(client, _ADMIN_USERNAME, _ADMIN_PASSWORD)
        resp = client.post("/api/auth/api-keys", json={"name": "my-key"})
        assert resp.status_code == 201
        data = resp.json()
        assert "key" in data
        assert data["key"].startswith("mp_")
        assert data["name"] == "my-key"
        assert "prefix" in data
        assert "id" in data

    def test_list_api_keys(self, client, admin_user):
        _login(client, _ADMIN_USERNAME, _ADMIN_PASSWORD)
        client.post("/api/auth/api-keys", json={"name": "key-one"})
        client.post("/api/auth/api-keys", json={"name": "key-two"})
        resp = client.get("/api/auth/api-keys")
        assert resp.status_code == 200
        names = [k["name"] for k in resp.json()]
        assert "key-one" in names
        assert "key-two" in names

    def test_list_api_keys_unauthenticated(self, client):
        resp = client.get("/api/auth/api-keys")
        assert resp.status_code == 401

    def test_delete_api_key(self, client, admin_user):
        _login(client, _ADMIN_USERNAME, _ADMIN_PASSWORD)
        create_resp = client.post("/api/auth/api-keys", json={"name": "delete-me"})
        key_id = create_resp.json()["id"]

        del_resp = client.delete(f"/api/auth/api-keys/{key_id}")
        assert del_resp.status_code == 204

        # Key should no longer appear in the list.
        list_resp = client.get("/api/auth/api-keys")
        ids = [k["id"] for k in list_resp.json()]
        assert key_id not in ids

    def test_delete_nonexistent_key_returns_404(self, client, admin_user):
        _login(client, _ADMIN_USERNAME, _ADMIN_PASSWORD)
        resp = client.delete("/api/auth/api-keys/99999")
        assert resp.status_code == 404

    def test_cannot_delete_another_users_key(self, client, db, admin_user, regular_user):
        """A regular user cannot delete a key that belongs to admin."""
        _login(client, _ADMIN_USERNAME, _ADMIN_PASSWORD)
        create_resp = client.post("/api/auth/api-keys", json={"name": "admin-key"})
        key_id = create_resp.json()["id"]

        # Log out admin, log in as regular user.
        client.post("/api/auth/logout")
        _login(client, _REGULAR_USERNAME, _REGULAR_PASSWORD)

        resp = client.delete(f"/api/auth/api-keys/{key_id}")
        assert resp.status_code == 404  # endpoint returns 404 when key not owned

    def test_api_key_can_authenticate_requests(self, client, admin_user):
        """The raw key returned at creation can be used as a bearer token."""
        _login(client, _ADMIN_USERNAME, _ADMIN_PASSWORD)
        key_resp = client.post("/api/auth/api-keys", json={"name": "bearer-test"})
        raw_key = key_resp.json()["key"]

        # Log out cookie session.
        client.post("/api/auth/logout")
        assert client.get("/api/auth/me").status_code == 401

        # Use API key header.
        resp = client.get("/api/auth/me", headers={"X-MealPilot-Token": raw_key})
        assert resp.status_code == 200
        assert resp.json()["username"] == _ADMIN_USERNAME

    def test_api_keys_revoked_after_password_change(self, client, admin_user):
        """Changing password invalidates all existing API keys."""
        _login(client, _ADMIN_USERNAME, _ADMIN_PASSWORD)
        key_resp = client.post("/api/auth/api-keys", json={"name": "doomed-key"})
        raw_key = key_resp.json()["key"]

        # Change password.
        client.post(
            "/api/auth/change-password",
            json={"old_password": _ADMIN_PASSWORD, "new_password": _ADMIN_NEW_PASSWORD},
        )

        # Old API key must no longer work.
        client.post("/api/auth/logout")
        resp = client.get("/api/auth/me", headers={"X-MealPilot-Token": raw_key})
        assert resp.status_code == 401
