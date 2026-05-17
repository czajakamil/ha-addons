"""Integration tests for /api/households (household memory/settings).

Endpoints covered:
  GET  /api/households/{household_id}/memory
  PATCH /api/households/{household_id}/memory
"""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app.models import Household, HouseholdMember, User
from app.security import hash_password

# Re-use fixtures from conftest.py (db, client, admin_user, regular_user, login)
# Additional helpers defined locally.


# ---------------------------------------------------------------------------
# Local helpers
# ---------------------------------------------------------------------------

def create_user(db, username: str, password: str, is_admin: bool = False) -> User:
    user = User(
        username=username,
        password_hash=hash_password(password),
        role="admin" if is_admin else "user",
        is_active=1,
        session_version=0,
    )
    db.add(user)
    db.commit()
    db.refresh(user)
    return user


def create_household(db, name: str = "Test Household") -> Household:
    hh = Household(name=name)
    db.add(hh)
    db.commit()
    db.refresh(hh)
    return hh


def add_member(db, user_id: int, household_id: int, can_edit: bool = False) -> HouseholdMember:
    m = HouseholdMember(user_id=user_id, household_id=household_id, can_edit=can_edit)
    db.add(m)
    db.commit()
    db.refresh(m)
    return m


def login(client: TestClient, username: str, password: str) -> dict:
    resp = client.post("/api/auth/login", json={"username": username, "password": password})
    assert resp.status_code == 200, f"login failed: {resp.text}"
    return dict(client.cookies)


# ---------------------------------------------------------------------------
# Tests: GET /api/households/{household_id}/memory
# ---------------------------------------------------------------------------

class TestGetHouseholdMemory:
    def test_get_memory_lazy_creates_empty_settings(self, client, db):
        """First GET of household memory creates an empty HouseholdSettings row."""
        user = create_user(db, "member1", "Password1234")
        hh = create_household(db)
        add_member(db, user.id, hh.id, can_edit=True)
        login(client, "member1", "Password1234")

        resp = client.get(f"/api/households/{hh.id}/memory")

        assert resp.status_code == 200
        data = resp.json()
        assert data["shared_restrictions"] == []
        assert data["shared_dislikes"] == []
        assert data["planning_notes"] is None
        assert data["servings_default"] is None

    def test_get_memory_requires_membership(self, client, db):
        """A user who is not a member of the household gets 404."""
        outsider = create_user(db, "outsider", "Password1234")
        hh = create_household(db)
        login(client, "outsider", "Password1234")

        resp = client.get(f"/api/households/{hh.id}/memory")

        assert resp.status_code == 404

    def test_get_memory_unauthenticated_returns_401(self, client, db):
        """Unauthenticated request returns 401."""
        hh = create_household(db)

        resp = client.get(f"/api/households/{hh.id}/memory")

        assert resp.status_code == 401

    def test_get_memory_returns_existing_data(self, client, db):
        """GET returns previously-stored memory values."""
        user = create_user(db, "member2", "Password1234")
        hh = create_household(db)
        add_member(db, user.id, hh.id, can_edit=True)
        login(client, "member2", "Password1234")

        # First PATCH to populate data
        client.patch(
            f"/api/households/{hh.id}/memory",
            json={"shared_restrictions": ["gluten-free"], "servings_default": 4},
        )

        resp = client.get(f"/api/households/{hh.id}/memory")

        assert resp.status_code == 200
        data = resp.json()
        assert data["shared_restrictions"] == ["gluten-free"]
        assert data["servings_default"] == 4

    def test_get_memory_nonexistent_household_returns_404(self, client, db):
        """GET on a non-existent household returns 404 for any logged-in user."""
        user = create_user(db, "member3", "Password1234")
        login(client, "member3", "Password1234")

        resp = client.get("/api/households/99999/memory")

        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Tests: PATCH /api/households/{household_id}/memory
# ---------------------------------------------------------------------------

class TestPatchHouseholdMemory:
    def test_patch_by_member_with_can_edit_true_succeeds(self, client, db):
        """Member with can_edit=True can update household memory."""
        user = create_user(db, "editor", "Password1234")
        hh = create_household(db)
        add_member(db, user.id, hh.id, can_edit=True)
        login(client, "editor", "Password1234")

        resp = client.patch(
            f"/api/households/{hh.id}/memory",
            json={
                "shared_restrictions": ["vegan"],
                "shared_dislikes": ["mushrooms"],
                "planning_notes": "Buy organic",
                "servings_default": 3,
            },
        )

        assert resp.status_code == 200
        data = resp.json()
        assert data["shared_restrictions"] == ["vegan"]
        assert data["shared_dislikes"] == ["mushrooms"]
        assert data["planning_notes"] == "Buy organic"
        assert data["servings_default"] == 3

    def test_patch_by_member_with_can_edit_false_returns_403(self, client, db):
        """Member with can_edit=False cannot update household memory."""
        user = create_user(db, "readonly_member", "Password1234")
        hh = create_household(db)
        add_member(db, user.id, hh.id, can_edit=False)
        login(client, "readonly_member", "Password1234")

        resp = client.patch(
            f"/api/households/{hh.id}/memory",
            json={"shared_restrictions": ["nut-free"]},
        )

        assert resp.status_code == 403

    def test_patch_by_non_member_returns_404(self, client, db):
        """A user not in the household gets 404 on PATCH."""
        outsider = create_user(db, "outsider2", "Password1234")
        hh = create_household(db)
        login(client, "outsider2", "Password1234")

        resp = client.patch(
            f"/api/households/{hh.id}/memory",
            json={"shared_restrictions": ["dairy-free"]},
        )

        assert resp.status_code == 404

    def test_patch_unauthenticated_returns_401(self, client, db):
        """Unauthenticated PATCH returns 401."""
        hh = create_household(db)

        resp = client.patch(
            f"/api/households/{hh.id}/memory",
            json={"shared_restrictions": ["sugar-free"]},
        )

        assert resp.status_code == 401

    def test_patch_partial_update_preserves_other_fields(self, client, db):
        """Patching only one field does not overwrite others."""
        user = create_user(db, "partial_editor", "Password1234")
        hh = create_household(db)
        add_member(db, user.id, hh.id, can_edit=True)
        login(client, "partial_editor", "Password1234")

        # Initial full patch
        client.patch(
            f"/api/households/{hh.id}/memory",
            json={"shared_restrictions": ["gluten-free"], "servings_default": 2},
        )

        # Partial patch — only planning_notes
        resp = client.patch(
            f"/api/households/{hh.id}/memory",
            json={"planning_notes": "Cook Sunday"},
        )

        assert resp.status_code == 200
        data = resp.json()
        assert data["shared_restrictions"] == ["gluten-free"]
        assert data["servings_default"] == 2
        assert data["planning_notes"] == "Cook Sunday"

    def test_patch_by_admin_user_who_is_member_with_can_edit_true(self, client, db):
        """Admin user who is a member with can_edit=True can patch."""
        admin = create_user(db, "hh_admin", "AdminPass1234", is_admin=True)
        hh = create_household(db)
        add_member(db, admin.id, hh.id, can_edit=True)
        login(client, "hh_admin", "AdminPass1234")

        resp = client.patch(
            f"/api/households/{hh.id}/memory",
            json={"shared_dislikes": ["cilantro"]},
        )

        assert resp.status_code == 200
        assert resp.json()["shared_dislikes"] == ["cilantro"]

    def test_patch_empty_payload_is_noop(self, client, db):
        """PATCH with no fields set returns current state unchanged."""
        user = create_user(db, "noop_member", "Password1234")
        hh = create_household(db)
        add_member(db, user.id, hh.id, can_edit=True)
        login(client, "noop_member", "Password1234")

        # Set initial data
        client.patch(
            f"/api/households/{hh.id}/memory",
            json={"servings_default": 5},
        )

        # No-op patch
        resp = client.patch(f"/api/households/{hh.id}/memory", json={})

        assert resp.status_code == 200
        assert resp.json()["servings_default"] == 5
