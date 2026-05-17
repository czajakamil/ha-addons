"""Integration tests for /api/admin/users and /api/admin/households.

Endpoints covered (admin_users router):
  GET    /api/admin/users
  POST   /api/admin/users
  PATCH  /api/admin/users/{user_id}
  DELETE /api/admin/users/{user_id}
  PUT    /api/admin/users/{user_id}/ai-limits
  POST   /api/admin/users/{user_id}/ai-usage/reset
  PUT    /api/admin/users/{user_id}/household

Endpoints covered (admin_households router):
  GET    /api/admin/households
  POST   /api/admin/households
  PATCH  /api/admin/households/{household_id}
  DELETE /api/admin/households/{household_id}
  GET    /api/admin/households/{household_id}/members
"""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app.models import Household, HouseholdMember, User
from app.security import hash_password


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


def login(client: TestClient, username: str, password: str) -> dict:
    resp = client.post("/api/auth/login", json={"username": username, "password": password})
    assert resp.status_code == 200, f"login failed: {resp.text}"
    return dict(client.cookies)


# ---------------------------------------------------------------------------
# Tests: GET /api/admin/users
# ---------------------------------------------------------------------------

class TestListUsers:
    def test_admin_sees_all_users(self, client, db):
        """Admin receives a list containing all users with AI usage stats."""
        admin = create_user(db, "admin", "AdminPass1234", is_admin=True)
        create_user(db, "user1", "UserPass5678")
        create_user(db, "user2", "UserPass5678")
        login(client, "admin", "AdminPass1234")

        resp = client.get("/api/admin/users")

        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 3
        usernames = {u["username"] for u in data}
        assert {"admin", "user1", "user2"} == usernames
        # Each item must include AI usage fields
        for u in data:
            assert "can_use_ai" in u
            assert "ai_used_tokens_this_month" in u
            assert "ai_used_cost_cents_this_month" in u

    def test_regular_user_returns_403(self, client, db):
        """Non-admin gets 403."""
        create_user(db, "admin2", "AdminPass1234", is_admin=True)
        create_user(db, "plain", "UserPass5678")
        login(client, "plain", "UserPass5678")

        resp = client.get("/api/admin/users")

        assert resp.status_code == 403

    def test_unauthenticated_returns_401(self, client, db):
        """Unauthenticated request gets 401."""
        resp = client.get("/api/admin/users")
        assert resp.status_code == 401

    def test_response_includes_household_info(self, client, db):
        """Users assigned to a household show household_id and can_edit_in_household."""
        admin = create_user(db, "admin3", "AdminPass1234", is_admin=True)
        user = create_user(db, "hh_user", "UserPass5678")
        hh = create_household(db)
        db.add(HouseholdMember(user_id=user.id, household_id=hh.id, can_edit=True))
        db.commit()
        login(client, "admin3", "AdminPass1234")

        resp = client.get("/api/admin/users")
        assert resp.status_code == 200
        user_data = next(u for u in resp.json() if u["username"] == "hh_user")
        assert user_data["household_id"] == hh.id
        assert user_data["can_edit_in_household"] is True


# ---------------------------------------------------------------------------
# Tests: POST /api/admin/users (create user)
# ---------------------------------------------------------------------------

class TestCreateUser:
    def test_admin_can_create_user(self, client, db):
        """Admin can create a new user via POST."""
        create_user(db, "admin", "AdminPass1234", is_admin=True)
        login(client, "admin", "AdminPass1234")

        resp = client.post(
            "/api/admin/users",
            json={"username": "newuser", "password": "NewPassword1", "role": "user"},
        )

        assert resp.status_code == 201
        data = resp.json()
        assert data["username"] == "newuser"
        assert data["role"] == "user"

    def test_create_user_duplicate_username_returns_409(self, client, db):
        """Creating a user with an existing username returns 409."""
        create_user(db, "admin", "AdminPass1234", is_admin=True)
        create_user(db, "existing", "ExistingPass1")
        login(client, "admin", "AdminPass1234")

        resp = client.post(
            "/api/admin/users",
            json={"username": "existing", "password": "AnotherPass1", "role": "user"},
        )

        assert resp.status_code == 409

    def test_regular_user_cannot_create_user(self, client, db):
        """Non-admin cannot create users."""
        create_user(db, "admin", "AdminPass1234", is_admin=True)
        create_user(db, "plain", "UserPass5678")
        login(client, "plain", "UserPass5678")

        resp = client.post(
            "/api/admin/users",
            json={"username": "hacker", "password": "HackerPass1", "role": "user"},
        )

        assert resp.status_code == 403

    def test_admin_can_create_admin_user(self, client, db):
        """Admin can create another admin user."""
        create_user(db, "admin", "AdminPass1234", is_admin=True)
        login(client, "admin", "AdminPass1234")

        resp = client.post(
            "/api/admin/users",
            json={"username": "newadmin", "password": "NewAdminPass1", "role": "admin"},
        )

        assert resp.status_code == 201
        assert resp.json()["role"] == "admin"


# ---------------------------------------------------------------------------
# Tests: PATCH /api/admin/users/{user_id} (update user)
# ---------------------------------------------------------------------------

class TestUpdateUser:
    def test_admin_can_update_username(self, client, db):
        """Admin can rename a user."""
        admin = create_user(db, "admin", "AdminPass1234", is_admin=True)
        user = create_user(db, "oldname", "UserPass5678")
        login(client, "admin", "AdminPass1234")

        resp = client.patch(
            f"/api/admin/users/{user.id}",
            json={"username": "newname"},
        )

        assert resp.status_code == 200
        assert resp.json()["username"] == "newname"

    def test_admin_can_deactivate_user(self, client, db):
        """Admin can deactivate a non-admin user."""
        admin = create_user(db, "admin", "AdminPass1234", is_admin=True)
        user = create_user(db, "activeuser", "UserPass5678")
        login(client, "admin", "AdminPass1234")

        resp = client.patch(
            f"/api/admin/users/{user.id}",
            json={"is_active": False},
        )

        assert resp.status_code == 200
        assert resp.json()["is_active"] is False

    def test_cannot_demote_last_admin(self, client, db):
        """Demoting the last admin to 'user' returns 400."""
        admin = create_user(db, "admin", "AdminPass1234", is_admin=True)
        login(client, "admin", "AdminPass1234")

        resp = client.patch(
            f"/api/admin/users/{admin.id}",
            json={"role": "user"},
        )

        assert resp.status_code == 400

    def test_cannot_disable_last_admin(self, client, db):
        """Disabling the last admin returns 400."""
        admin = create_user(db, "admin", "AdminPass1234", is_admin=True)
        login(client, "admin", "AdminPass1234")

        resp = client.patch(
            f"/api/admin/users/{admin.id}",
            json={"is_active": False},
        )

        assert resp.status_code == 400

    def test_update_nonexistent_user_returns_404(self, client, db):
        """Updating a non-existent user returns 404."""
        create_user(db, "admin", "AdminPass1234", is_admin=True)
        login(client, "admin", "AdminPass1234")

        resp = client.patch("/api/admin/users/99999", json={"username": "ghost"})

        assert resp.status_code == 404

    def test_provisioned_admin_password_cannot_be_changed(self, client, db, monkeypatch):
        """Attempting to change provisioned admin's password returns 403."""
        monkeypatch.setenv("MEALPILOT_ADMIN_USERNAME", "provadmin")
        # Re-evaluate the module-level constant used inside the router.
        import importlib
        import app.routers.admin_users as au_module
        monkeypatch.setattr(au_module, "_PROVISIONED_ADMIN_USERNAME", "provadmin")

        admin = create_user(db, "admin", "AdminPass1234", is_admin=True)
        prov = create_user(db, "provadmin", "ProvAdminPass1", is_admin=True)
        login(client, "admin", "AdminPass1234")

        resp = client.patch(
            f"/api/admin/users/{prov.id}",
            json={"password": "NewPassword12"},
        )

        assert resp.status_code == 403

    def test_regular_user_cannot_update_user(self, client, db):
        """Non-admin cannot update users."""
        create_user(db, "admin", "AdminPass1234", is_admin=True)
        user = create_user(db, "plain", "UserPass5678")
        login(client, "plain", "UserPass5678")

        resp = client.patch(f"/api/admin/users/{user.id}", json={"username": "newname2"})

        assert resp.status_code == 403


# ---------------------------------------------------------------------------
# Tests: DELETE /api/admin/users/{user_id}
# ---------------------------------------------------------------------------

class TestDeleteUser:
    def test_admin_can_delete_user(self, client, db):
        """Admin can delete a regular user."""
        admin = create_user(db, "admin", "AdminPass1234", is_admin=True)
        user = create_user(db, "deleteme", "UserPass5678")
        login(client, "admin", "AdminPass1234")

        resp = client.delete(f"/api/admin/users/{user.id}")

        assert resp.status_code == 204
        # Verify user is gone
        assert db.get(User, user.id) is None

    def test_admin_cannot_delete_self(self, client, db):
        """Admin cannot delete their own account."""
        admin = create_user(db, "admin", "AdminPass1234", is_admin=True)
        login(client, "admin", "AdminPass1234")

        resp = client.delete(f"/api/admin/users/{admin.id}")

        assert resp.status_code == 400

    def test_cannot_delete_last_admin(self, client, db):
        """Cannot delete the only remaining admin."""
        admin = create_user(db, "admin", "AdminPass1234", is_admin=True)
        user = create_user(db, "plain", "UserPass5678")
        login(client, "plain", "UserPass5678")

        # Non-admin can't even reach delete
        resp = client.delete(f"/api/admin/users/{admin.id}")
        assert resp.status_code == 403

    def test_delete_with_two_admins_allows_removing_one(self, client, db):
        """With two admins, one can delete the other."""
        admin1 = create_user(db, "admin1", "AdminPass1234", is_admin=True)
        admin2 = create_user(db, "admin2", "AdminPass5678", is_admin=True)
        login(client, "admin1", "AdminPass1234")

        resp = client.delete(f"/api/admin/users/{admin2.id}")

        assert resp.status_code == 204

    def test_delete_nonexistent_user_returns_404(self, client, db):
        """Deleting a non-existent user returns 404."""
        create_user(db, "admin", "AdminPass1234", is_admin=True)
        login(client, "admin", "AdminPass1234")

        resp = client.delete("/api/admin/users/99999")

        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Tests: PUT /api/admin/users/{user_id}/ai-limits
# ---------------------------------------------------------------------------

class TestUpdateAiLimits:
    def test_admin_can_set_token_limit(self, client, db):
        """Admin can set a monthly token limit on a user."""
        admin = create_user(db, "admin", "AdminPass1234", is_admin=True)
        user = create_user(db, "aiuser", "UserPass5678")
        login(client, "admin", "AdminPass1234")

        resp = client.put(
            f"/api/admin/users/{user.id}/ai-limits",
            json={"ai_monthly_token_limit": 50000},
        )

        assert resp.status_code == 200
        assert resp.json()["ai_monthly_token_limit"] == 50000

    def test_admin_can_set_cost_limit(self, client, db):
        """Admin can set a monthly cost limit (in cents) on a user."""
        admin = create_user(db, "admin", "AdminPass1234", is_admin=True)
        user = create_user(db, "costuser", "UserPass5678")
        login(client, "admin", "AdminPass1234")

        resp = client.put(
            f"/api/admin/users/{user.id}/ai-limits",
            json={"ai_monthly_cost_limit_cents": 200},
        )

        assert resp.status_code == 200
        assert resp.json()["ai_monthly_cost_limit_cents"] == 200

    def test_admin_can_disable_ai_for_user(self, client, db):
        """Admin can set can_use_ai=False, disabling AI for that user."""
        admin = create_user(db, "admin", "AdminPass1234", is_admin=True)
        user = create_user(db, "noaiuser", "UserPass5678")
        login(client, "admin", "AdminPass1234")

        resp = client.put(
            f"/api/admin/users/{user.id}/ai-limits",
            json={"can_use_ai": False},
        )

        assert resp.status_code == 200
        assert resp.json()["can_use_ai"] is False

    def test_admin_can_clear_token_limit(self, client, db):
        """Admin can clear a previously set token limit via clear_token_limit=True."""
        admin = create_user(db, "admin", "AdminPass1234", is_admin=True)
        user = create_user(db, "clearuser", "UserPass5678")
        user.ai_monthly_token_limit = 10000
        db.commit()
        login(client, "admin", "AdminPass1234")

        resp = client.put(
            f"/api/admin/users/{user.id}/ai-limits",
            json={"clear_token_limit": True},
        )

        assert resp.status_code == 200
        assert resp.json()["ai_monthly_token_limit"] is None

    def test_admin_can_clear_cost_limit(self, client, db):
        """Admin can clear a previously set cost limit via clear_cost_limit=True."""
        admin = create_user(db, "admin", "AdminPass1234", is_admin=True)
        user = create_user(db, "clearcostuser", "UserPass5678")
        user.ai_monthly_cost_limit_cents = 500
        db.commit()
        login(client, "admin", "AdminPass1234")

        resp = client.put(
            f"/api/admin/users/{user.id}/ai-limits",
            json={"clear_cost_limit": True},
        )

        assert resp.status_code == 200
        assert resp.json()["ai_monthly_cost_limit_cents"] is None

    def test_regular_user_cannot_set_ai_limits(self, client, db):
        """Non-admin cannot update AI limits."""
        create_user(db, "admin", "AdminPass1234", is_admin=True)
        user = create_user(db, "plain", "UserPass5678")
        login(client, "plain", "UserPass5678")

        resp = client.put(
            f"/api/admin/users/{user.id}/ai-limits",
            json={"can_use_ai": False},
        )

        assert resp.status_code == 403

    def test_ai_limits_nonexistent_user_returns_404(self, client, db):
        """Updating AI limits for a non-existent user returns 404."""
        create_user(db, "admin", "AdminPass1234", is_admin=True)
        login(client, "admin", "AdminPass1234")

        resp = client.put("/api/admin/users/99999/ai-limits", json={"can_use_ai": False})

        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Tests: POST /api/admin/users/{user_id}/ai-usage/reset
# ---------------------------------------------------------------------------

class TestResetAiUsage:
    def test_admin_can_reset_ai_usage(self, client, db):
        """Admin can reset a user's AI usage counters."""
        admin = create_user(db, "admin", "AdminPass1234", is_admin=True)
        user = create_user(db, "usedai", "UserPass5678")
        user.ai_used_tokens_this_month = 12345
        user.ai_used_cost_cents_this_month = 99
        db.commit()
        login(client, "admin", "AdminPass1234")

        resp = client.post(f"/api/admin/users/{user.id}/ai-usage/reset")

        assert resp.status_code == 204
        db.refresh(user)
        assert user.ai_used_tokens_this_month == 0
        assert user.ai_used_cost_cents_this_month == 0

    def test_regular_user_cannot_reset_ai_usage(self, client, db):
        """Non-admin cannot reset AI usage."""
        create_user(db, "admin", "AdminPass1234", is_admin=True)
        user = create_user(db, "plain", "UserPass5678")
        login(client, "plain", "UserPass5678")

        resp = client.post(f"/api/admin/users/{user.id}/ai-usage/reset")

        assert resp.status_code == 403

    def test_reset_nonexistent_user_returns_404(self, client, db):
        """Resetting AI usage for a non-existent user returns 404."""
        create_user(db, "admin", "AdminPass1234", is_admin=True)
        login(client, "admin", "AdminPass1234")

        resp = client.post("/api/admin/users/99999/ai-usage/reset")

        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Tests: PUT /api/admin/users/{user_id}/household
# ---------------------------------------------------------------------------

class TestAssignUserToHousehold:
    def test_admin_can_assign_user_to_household(self, client, db):
        """Admin can assign a user to a household."""
        admin = create_user(db, "admin", "AdminPass1234", is_admin=True)
        user = create_user(db, "assignee", "UserPass5678")
        hh = create_household(db)
        login(client, "admin", "AdminPass1234")

        resp = client.put(
            f"/api/admin/users/{user.id}/household",
            json={"household_id": hh.id, "can_edit": True},
        )

        assert resp.status_code == 200
        data = resp.json()
        assert data["household_id"] == hh.id
        assert data["can_edit_in_household"] is True

    def test_admin_can_remove_user_from_household(self, client, db):
        """Admin can remove a user from a household by passing household_id=null."""
        admin = create_user(db, "admin", "AdminPass1234", is_admin=True)
        user = create_user(db, "removee", "UserPass5678")
        hh = create_household(db)
        db.add(HouseholdMember(user_id=user.id, household_id=hh.id, can_edit=False))
        db.commit()
        login(client, "admin", "AdminPass1234")

        resp = client.put(
            f"/api/admin/users/{user.id}/household",
            json={"household_id": None},
        )

        assert resp.status_code == 200
        assert resp.json()["household_id"] is None

    def test_assign_to_nonexistent_household_returns_404(self, client, db):
        """Assigning a user to a non-existent household returns 404."""
        admin = create_user(db, "admin", "AdminPass1234", is_admin=True)
        user = create_user(db, "orphan", "UserPass5678")
        login(client, "admin", "AdminPass1234")

        resp = client.put(
            f"/api/admin/users/{user.id}/household",
            json={"household_id": 99999},
        )

        assert resp.status_code == 404

    def test_admin_can_reassign_user_to_different_household(self, client, db):
        """Admin can move a user from one household to another."""
        admin = create_user(db, "admin", "AdminPass1234", is_admin=True)
        user = create_user(db, "mover", "UserPass5678")
        hh1 = create_household(db, "Household A")
        hh2 = create_household(db, "Household B")
        db.add(HouseholdMember(user_id=user.id, household_id=hh1.id, can_edit=False))
        db.commit()
        login(client, "admin", "AdminPass1234")

        resp = client.put(
            f"/api/admin/users/{user.id}/household",
            json={"household_id": hh2.id, "can_edit": False},
        )

        assert resp.status_code == 200
        assert resp.json()["household_id"] == hh2.id

    def test_regular_user_cannot_assign_household(self, client, db):
        """Non-admin cannot assign household membership."""
        create_user(db, "admin", "AdminPass1234", is_admin=True)
        user = create_user(db, "plain", "UserPass5678")
        hh = create_household(db)
        login(client, "plain", "UserPass5678")

        resp = client.put(
            f"/api/admin/users/{user.id}/household",
            json={"household_id": hh.id},
        )

        assert resp.status_code == 403


# ---------------------------------------------------------------------------
# Tests: GET /api/admin/households, POST /api/admin/households, etc.
# ---------------------------------------------------------------------------

class TestAdminHouseholds:
    def test_admin_can_list_households(self, client, db):
        """Admin sees a list of all households."""
        admin = create_user(db, "admin", "AdminPass1234", is_admin=True)
        create_household(db, "HH1")
        create_household(db, "HH2")
        login(client, "admin", "AdminPass1234")

        resp = client.get("/api/admin/households")

        assert resp.status_code == 200
        names = {h["name"] for h in resp.json()}
        assert {"HH1", "HH2"} == names

    def test_regular_user_cannot_list_households(self, client, db):
        """Non-admin cannot list households."""
        create_user(db, "admin", "AdminPass1234", is_admin=True)
        create_user(db, "plain", "UserPass5678")
        login(client, "plain", "UserPass5678")

        resp = client.get("/api/admin/households")

        assert resp.status_code == 403

    def test_admin_can_create_household(self, client, db):
        """Admin can create a new household."""
        create_user(db, "admin", "AdminPass1234", is_admin=True)
        login(client, "admin", "AdminPass1234")

        resp = client.post("/api/admin/households", json={"name": "New Household"})

        assert resp.status_code == 201
        data = resp.json()
        assert data["name"] == "New Household"
        assert data["member_count"] == 0

    def test_admin_can_rename_household(self, client, db):
        """Admin can rename a household via PATCH."""
        create_user(db, "admin", "AdminPass1234", is_admin=True)
        hh = create_household(db, "Old Name")
        login(client, "admin", "AdminPass1234")

        resp = client.patch(f"/api/admin/households/{hh.id}", json={"name": "New Name"})

        assert resp.status_code == 200
        assert resp.json()["name"] == "New Name"

    def test_rename_nonexistent_household_returns_404(self, client, db):
        """Renaming a non-existent household returns 404."""
        create_user(db, "admin", "AdminPass1234", is_admin=True)
        login(client, "admin", "AdminPass1234")

        resp = client.patch("/api/admin/households/99999", json={"name": "Ghost"})

        assert resp.status_code == 404

    def test_admin_can_delete_household(self, client, db):
        """Admin can delete a household."""
        create_user(db, "admin", "AdminPass1234", is_admin=True)
        hh = create_household(db, "To Delete")
        login(client, "admin", "AdminPass1234")

        resp = client.delete(f"/api/admin/households/{hh.id}")

        assert resp.status_code == 204
        assert db.get(Household, hh.id) is None

    def test_delete_nonexistent_household_returns_404(self, client, db):
        """Deleting a non-existent household returns 404."""
        create_user(db, "admin", "AdminPass1234", is_admin=True)
        login(client, "admin", "AdminPass1234")

        resp = client.delete("/api/admin/households/99999")

        assert resp.status_code == 404

    def test_admin_can_list_household_members(self, client, db):
        """Admin can view members of a specific household."""
        admin = create_user(db, "admin", "AdminPass1234", is_admin=True)
        user = create_user(db, "member", "UserPass5678")
        hh = create_household(db, "Member HH")
        db.add(HouseholdMember(user_id=user.id, household_id=hh.id, can_edit=True))
        db.commit()
        login(client, "admin", "AdminPass1234")

        resp = client.get(f"/api/admin/households/{hh.id}/members")

        assert resp.status_code == 200
        members = resp.json()
        assert len(members) == 1
        assert members[0]["username"] == "member"
        assert members[0]["can_edit"] is True

    def test_list_members_nonexistent_household_returns_404(self, client, db):
        """Listing members of a non-existent household returns 404."""
        create_user(db, "admin", "AdminPass1234", is_admin=True)
        login(client, "admin", "AdminPass1234")

        resp = client.get("/api/admin/households/99999/members")

        assert resp.status_code == 404

    def test_delete_household_removes_memberships(self, client, db):
        """Deleting a household also removes its member associations."""
        admin = create_user(db, "admin", "AdminPass1234", is_admin=True)
        user = create_user(db, "hhmember", "UserPass5678")
        hh = create_household(db, "Ephemeral HH")
        db.add(HouseholdMember(user_id=user.id, household_id=hh.id, can_edit=False))
        db.commit()
        login(client, "admin", "AdminPass1234")

        client.delete(f"/api/admin/households/{hh.id}")

        membership = db.get(HouseholdMember, user.id)
        assert membership is None


# ---------------------------------------------------------------------------
# Tests: provisioned admin constraints
# ---------------------------------------------------------------------------

class TestProvisionedAdmin:
    def test_provisioned_admin_flag_appears_in_list(self, client, db, monkeypatch):
        """is_provisioned_admin=True appears in list for the matching username."""
        import app.routers.admin_users as au_module
        monkeypatch.setattr(au_module, "_PROVISIONED_ADMIN_USERNAME", "provadmin")

        admin = create_user(db, "admin", "AdminPass1234", is_admin=True)
        create_user(db, "provadmin", "ProvAdminPass1", is_admin=True)
        login(client, "admin", "AdminPass1234")

        resp = client.get("/api/admin/users")
        assert resp.status_code == 200
        prov = next(u for u in resp.json() if u["username"] == "provadmin")
        assert prov["is_provisioned_admin"] is True
        # The non-provisioned admin should not be flagged
        non_prov = next(u for u in resp.json() if u["username"] == "admin")
        assert non_prov["is_provisioned_admin"] is False

    def test_provisioned_admin_cannot_change_password_via_admin_patch(self, client, db, monkeypatch):
        """PATCH /{id} with password for provisioned admin returns 403."""
        import app.routers.admin_users as au_module
        monkeypatch.setattr(au_module, "_PROVISIONED_ADMIN_USERNAME", "provadmin")

        admin = create_user(db, "admin", "AdminPass1234", is_admin=True)
        prov = create_user(db, "provadmin", "ProvAdminPass1", is_admin=True)
        login(client, "admin", "AdminPass1234")

        resp = client.patch(
            f"/api/admin/users/{prov.id}",
            json={"password": "NewPasswordABC1"},
        )

        assert resp.status_code == 403
