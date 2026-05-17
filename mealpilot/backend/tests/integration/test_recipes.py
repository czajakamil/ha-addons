"""Integration tests for /api/recipes endpoints.

Run from the ``backend/`` directory:
    pytest tests/integration/test_recipes.py -v

Dependencies: pytest, httpx, respx
    pip install respx
"""
from __future__ import annotations

import io
import uuid
from unittest.mock import AsyncMock, patch

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.db import Base, get_db
from app.main import app
from app import models
from app.security import hash_password
from app.ratelimit import ai_limiter


# ---------------------------------------------------------------------------
# Fixtures (self-contained — do not rely on a conftest)
# ---------------------------------------------------------------------------

@pytest.fixture()
def db():
    """Fresh SQLite in-memory session per test."""
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
        future=True,
    )

    @event.listens_for(engine, "connect")
    def _pragmas(conn, _record):
        conn.execute("PRAGMA foreign_keys=ON")

    Base.metadata.create_all(bind=engine)
    TestingSession = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
    session = TestingSession()

    app.dependency_overrides[get_db] = lambda: session

    try:
        yield session
    finally:
        app.dependency_overrides.pop(get_db, None)
        session.close()
        Base.metadata.drop_all(bind=engine)
        engine.dispose()


@pytest.fixture()
def client(db):
    """TestClient wired to the in-memory database."""
    with TestClient(app, raise_server_exceptions=True) as c:
        yield c


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def login(client: TestClient, username: str, password: str) -> None:
    """Log in via the API (sets the session cookie on the client)."""
    resp = client.post(
        "/api/auth/login",
        json={"username": username, "password": password},
    )
    assert resp.status_code == 200, f"login failed ({resp.status_code}): {resp.text}"


def make_user(
    db,
    username: str,
    password: str = "TestPass1234",
    role: str = "user",
) -> models.User:
    user = models.User(
        username=username,
        password_hash=hash_password(password),
        role=role,
        is_active=1,
        session_version=0,
    )
    db.add(user)
    db.commit()
    db.refresh(user)
    return user


def make_household(db, name: str = "TestHousehold") -> models.Household:
    hh = models.Household(name=name)
    db.add(hh)
    db.commit()
    db.refresh(hh)
    return hh


def add_member(
    db,
    user: models.User,
    household: models.Household,
    can_edit: bool = False,
) -> models.HouseholdMember:
    m = models.HouseholdMember(
        user_id=user.id,
        household_id=household.id,
        can_edit=can_edit,
    )
    db.add(m)
    db.commit()
    db.refresh(m)
    return m


def recipe_payload(recipe_id: str | None = None, **overrides) -> dict:
    """Return a minimal valid RecipeCreate payload."""
    base = {
        "id": recipe_id or str(uuid.uuid4()),
        "title": "Test Recipe",
        "tags": [],
        "meal_types": [],
        "servings": 2,
        "prep_time": 10,
        "cook_time": 20,
        "kcal": 500.0,
        "p": 30.0,
        "f": 15.0,
        "c": 60.0,
        "hue": 40,
        "ingredients": [{"name": "flour", "qty": 200.0, "unit": "g"}],
        "steps": ["Mix", "Bake"],
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# 1. GET /api/recipes  — list
# ---------------------------------------------------------------------------

class TestListRecipes:
    def test_empty_list_for_new_user(self, client, db):
        make_user(db, "alice")
        login(client, "alice", "TestPass1234")

        resp = client.get("/api/recipes")
        assert resp.status_code == 200
        assert resp.json() == []

    def test_returns_own_recipes_only(self, client, db):
        alice = make_user(db, "alice")
        bob = make_user(db, "bob")

        # Alice creates a recipe (direct DB insert — no HTTP call)
        r = models.Recipe(
            id=str(uuid.uuid4()),
            created_by=alice.id,
            owner_user_id=alice.id,
            title="Alice recipe",
        )
        db.add(r)
        r2 = models.Recipe(
            id=str(uuid.uuid4()),
            created_by=bob.id,
            owner_user_id=bob.id,
            title="Bob recipe",
        )
        db.add(r2)
        db.commit()

        login(client, "alice", "TestPass1234")
        resp = client.get("/api/recipes")
        assert resp.status_code == 200
        titles = [x["title"] for x in resp.json()]
        assert "Alice recipe" in titles
        assert "Bob recipe" not in titles

    def test_household_member_sees_household_recipe(self, client, db):
        alice = make_user(db, "alice")
        bob = make_user(db, "bob")
        hh = make_household(db, "HH1")
        add_member(db, alice, hh, can_edit=True)
        add_member(db, bob, hh, can_edit=True)

        # Bob creates a recipe owned by the household
        r = models.Recipe(
            id=str(uuid.uuid4()),
            created_by=bob.id,
            owner_household_id=hh.id,
            title="Household recipe",
        )
        db.add(r)
        db.commit()

        login(client, "alice", "TestPass1234")
        resp = client.get("/api/recipes")
        assert resp.status_code == 200
        titles = [x["title"] for x in resp.json()]
        assert "Household recipe" in titles

    def test_unauthenticated_returns_401(self, client, db):
        resp = client.get("/api/recipes")
        assert resp.status_code == 401

    def test_filter_by_tag(self, client, db):
        alice = make_user(db, "alice")
        r1 = models.Recipe(
            id=str(uuid.uuid4()),
            created_by=alice.id,
            owner_user_id=alice.id,
            title="Tagged recipe",
            tags=["vegan"],
        )
        r2 = models.Recipe(
            id=str(uuid.uuid4()),
            created_by=alice.id,
            owner_user_id=alice.id,
            title="Untagged recipe",
            tags=[],
        )
        db.add_all([r1, r2])
        db.flush()
        db.add(models.RecipeTag(recipe_id=r1.id, tag="vegan"))
        db.commit()

        login(client, "alice", "TestPass1234")
        resp = client.get("/api/recipes?tags=vegan")
        assert resp.status_code == 200
        titles = [x["title"] for x in resp.json()]
        assert "Tagged recipe" in titles
        assert "Untagged recipe" not in titles

    def test_filter_by_meal_type(self, client, db):
        alice = make_user(db, "alice")
        r1 = models.Recipe(
            id=str(uuid.uuid4()),
            created_by=alice.id,
            owner_user_id=alice.id,
            title="Breakfast recipe",
            meal_types=["breakfast"],
        )
        r2 = models.Recipe(
            id=str(uuid.uuid4()),
            created_by=alice.id,
            owner_user_id=alice.id,
            title="Dinner recipe",
            meal_types=["dinner"],
        )
        db.add_all([r1, r2])
        db.flush()
        db.add(models.RecipeMealType(recipe_id=r1.id, meal_type="breakfast"))
        db.add(models.RecipeMealType(recipe_id=r2.id, meal_type="dinner"))
        db.commit()

        login(client, "alice", "TestPass1234")
        resp = client.get("/api/recipes?meal_types=breakfast")
        assert resp.status_code == 200
        titles = [x["title"] for x in resp.json()]
        assert "Breakfast recipe" in titles
        assert "Dinner recipe" not in titles


# ---------------------------------------------------------------------------
# 2. POST /api/recipes  — create
# ---------------------------------------------------------------------------

class TestCreateRecipe:
    def test_create_recipe_returns_201(self, client, db):
        make_user(db, "alice")
        login(client, "alice", "TestPass1234")

        payload = recipe_payload(tags=["italian"], meal_types=["dinner"])
        resp = client.post("/api/recipes", json=payload)
        assert resp.status_code == 201
        body = resp.json()
        assert body["title"] == "Test Recipe"
        assert body["id"] == payload["id"]

    def test_created_recipe_visible_in_list(self, client, db):
        make_user(db, "alice")
        login(client, "alice", "TestPass1234")

        payload = recipe_payload(title="Unique Pasta")
        client.post("/api/recipes", json=payload)

        resp = client.get("/api/recipes")
        assert any(r["title"] == "Unique Pasta" for r in resp.json())

    def test_tags_persisted_and_visible(self, client, db):
        make_user(db, "alice")
        login(client, "alice", "TestPass1234")

        payload = recipe_payload(tags=["vegan", "quick"])
        resp = client.post("/api/recipes", json=payload)
        assert resp.status_code == 201
        assert set(resp.json()["tags"]) == {"vegan", "quick"}

    def test_meal_types_persisted(self, client, db):
        make_user(db, "alice")
        login(client, "alice", "TestPass1234")

        payload = recipe_payload(meal_types=["lunch", "dinner"])
        resp = client.post("/api/recipes", json=payload)
        assert resp.status_code == 201
        assert set(resp.json()["meal_types"]) == {"lunch", "dinner"}

    def test_duplicate_id_returns_409(self, client, db):
        make_user(db, "alice")
        login(client, "alice", "TestPass1234")

        payload = recipe_payload()
        client.post("/api/recipes", json=payload)
        resp = client.post("/api/recipes", json=payload)
        assert resp.status_code == 409

    def test_unauthenticated_returns_401(self, client, db):
        resp = client.post("/api/recipes", json=recipe_payload())
        assert resp.status_code == 401

    def test_owner_user_id_set_to_current_user(self, client, db):
        alice = make_user(db, "alice")
        login(client, "alice", "TestPass1234")

        payload = recipe_payload()
        resp = client.post("/api/recipes", json=payload)
        assert resp.status_code == 201
        body = resp.json()
        assert body["owner_user_id"] == alice.id
        assert body["owner_household_id"] is None

    def test_created_by_set_correctly(self, client, db):
        alice = make_user(db, "alice")
        login(client, "alice", "TestPass1234")

        payload = recipe_payload()
        resp = client.post("/api/recipes", json=payload)
        assert resp.json()["created_by"] == alice.id


# ---------------------------------------------------------------------------
# 3. GET /api/recipes/{id}  — single recipe
# ---------------------------------------------------------------------------

class TestGetRecipe:
    def test_owner_can_get_own_recipe(self, client, db):
        alice = make_user(db, "alice")
        login(client, "alice", "TestPass1234")

        payload = recipe_payload(title="My Soup")
        client.post("/api/recipes", json=payload)

        resp = client.get(f"/api/recipes/{payload['id']}")
        assert resp.status_code == 200
        assert resp.json()["title"] == "My Soup"

    def test_stranger_gets_404_for_others_recipe(self, client, db):
        alice = make_user(db, "alice")
        make_user(db, "bob")

        r = models.Recipe(
            id=str(uuid.uuid4()),
            created_by=alice.id,
            owner_user_id=alice.id,
            title="Alice secret",
        )
        db.add(r)
        db.commit()

        login(client, "bob", "TestPass1234")
        resp = client.get(f"/api/recipes/{r.id}")
        assert resp.status_code == 404

    def test_nonexistent_id_returns_404(self, client, db):
        make_user(db, "alice")
        login(client, "alice", "TestPass1234")

        resp = client.get("/api/recipes/does-not-exist")
        assert resp.status_code == 404

    def test_household_member_can_see_household_recipe(self, client, db):
        alice = make_user(db, "alice")
        bob = make_user(db, "bob")
        hh = make_household(db)
        add_member(db, alice, hh)
        add_member(db, bob, hh)

        r = models.Recipe(
            id=str(uuid.uuid4()),
            created_by=bob.id,
            owner_household_id=hh.id,
            title="Shared dish",
        )
        db.add(r)
        db.commit()

        login(client, "alice", "TestPass1234")
        resp = client.get(f"/api/recipes/{r.id}")
        assert resp.status_code == 200


# ---------------------------------------------------------------------------
# 4. PUT /api/recipes/{id}  — update
# ---------------------------------------------------------------------------

class TestUpdateRecipe:
    def test_owner_can_update_recipe(self, client, db):
        make_user(db, "alice")
        login(client, "alice", "TestPass1234")

        payload = recipe_payload(title="Original")
        client.post("/api/recipes", json=payload)

        resp = client.put(f"/api/recipes/{payload['id']}", json={"title": "Updated"})
        assert resp.status_code == 200
        assert resp.json()["title"] == "Updated"

    def test_stranger_gets_404_on_update(self, client, db):
        alice = make_user(db, "alice")
        make_user(db, "bob")

        r = models.Recipe(
            id=str(uuid.uuid4()),
            created_by=alice.id,
            owner_user_id=alice.id,
            title="Private",
        )
        db.add(r)
        db.commit()

        login(client, "bob", "TestPass1234")
        resp = client.put(f"/api/recipes/{r.id}", json={"title": "Hijacked"})
        assert resp.status_code == 404

    def test_household_member_without_edit_permission_gets_403(self, client, db):
        alice = make_user(db, "alice")
        bob = make_user(db, "bob")
        hh = make_household(db)
        add_member(db, alice, hh, can_edit=True)   # alice creates recipe
        add_member(db, bob, hh, can_edit=False)     # bob is read-only member

        r = models.Recipe(
            id=str(uuid.uuid4()),
            created_by=alice.id,
            owner_household_id=hh.id,
            title="Household dish",
        )
        db.add(r)
        db.commit()

        login(client, "bob", "TestPass1234")
        resp = client.put(f"/api/recipes/{r.id}", json={"title": "Edited by Bob"})
        assert resp.status_code == 403

    def test_household_member_with_edit_permission_can_update(self, client, db):
        alice = make_user(db, "alice")
        bob = make_user(db, "bob")
        hh = make_household(db)
        add_member(db, alice, hh, can_edit=True)
        add_member(db, bob, hh, can_edit=True)

        r = models.Recipe(
            id=str(uuid.uuid4()),
            created_by=alice.id,
            owner_household_id=hh.id,
            title="Shared dish",
        )
        db.add(r)
        db.commit()

        login(client, "bob", "TestPass1234")
        resp = client.put(f"/api/recipes/{r.id}", json={"title": "Updated by Bob"})
        assert resp.status_code == 200
        assert resp.json()["title"] == "Updated by Bob"

    def test_tags_updated_on_put(self, client, db):
        make_user(db, "alice")
        login(client, "alice", "TestPass1234")

        payload = recipe_payload(tags=["old"])
        client.post("/api/recipes", json=payload)

        resp = client.put(f"/api/recipes/{payload['id']}", json={"tags": ["new1", "new2"]})
        assert resp.status_code == 200
        assert set(resp.json()["tags"]) == {"new1", "new2"}


# ---------------------------------------------------------------------------
# 5. DELETE /api/recipes/{id}
# ---------------------------------------------------------------------------

class TestDeleteRecipe:
    def test_owner_can_delete_recipe(self, client, db):
        make_user(db, "alice")
        login(client, "alice", "TestPass1234")

        payload = recipe_payload()
        client.post("/api/recipes", json=payload)

        resp = client.delete(f"/api/recipes/{payload['id']}")
        assert resp.status_code == 204

        # Should be gone now
        resp = client.get(f"/api/recipes/{payload['id']}")
        assert resp.status_code == 404

    def test_stranger_gets_404_on_delete(self, client, db):
        alice = make_user(db, "alice")
        make_user(db, "bob")

        r = models.Recipe(
            id=str(uuid.uuid4()),
            created_by=alice.id,
            owner_user_id=alice.id,
            title="Private",
        )
        db.add(r)
        db.commit()

        login(client, "bob", "TestPass1234")
        resp = client.delete(f"/api/recipes/{r.id}")
        assert resp.status_code == 404

    def test_delete_cascades_meal_plan_entries(self, client, db):
        alice = make_user(db, "alice")
        r = models.Recipe(
            id=str(uuid.uuid4()),
            created_by=alice.id,
            owner_user_id=alice.id,
            title="Recipe with plan",
        )
        db.add(r)
        db.flush()
        entry = models.MealPlanEntry(
            created_by=alice.id,
            owner_user_id=alice.id,
            week_start="2025-01-06",
            day=0,
            meal="lunch",
            recipe_id=r.id,
            servings=1,
        )
        db.add(entry)
        db.commit()
        entry_id = entry.id

        login(client, "alice", "TestPass1234")
        resp = client.delete(f"/api/recipes/{r.id}")
        assert resp.status_code == 204

        # Meal plan entry should be gone
        remaining = db.get(models.MealPlanEntry, entry_id)
        assert remaining is None

    def test_unauthenticated_returns_401(self, client, db):
        resp = client.delete("/api/recipes/anything")
        assert resp.status_code == 401


# ---------------------------------------------------------------------------
# 6. PUT /api/recipes/{id}/ownership  — transfer
# ---------------------------------------------------------------------------

class TestOwnershipTransfer:
    def test_creator_can_share_with_household(self, client, db):
        alice = make_user(db, "alice")
        hh = make_household(db)
        add_member(db, alice, hh, can_edit=True)
        login(client, "alice", "TestPass1234")

        payload = recipe_payload()
        client.post("/api/recipes", json=payload)

        resp = client.put(
            f"/api/recipes/{payload['id']}/ownership",
            json={"share_with_household": True},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["owner_household_id"] == hh.id
        assert body["owner_user_id"] is None

    def test_creator_can_reclaim_to_personal(self, client, db):
        alice = make_user(db, "alice")
        hh = make_household(db)
        add_member(db, alice, hh, can_edit=True)

        # Create recipe already owned by household
        r = models.Recipe(
            id=str(uuid.uuid4()),
            created_by=alice.id,
            owner_household_id=hh.id,
            title="Shared recipe",
        )
        db.add(r)
        db.commit()

        login(client, "alice", "TestPass1234")
        resp = client.put(
            f"/api/recipes/{r.id}/ownership",
            json={"share_with_household": False},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["owner_user_id"] == alice.id
        assert body["owner_household_id"] is None

    def test_non_creator_cannot_transfer_ownership(self, client, db):
        alice = make_user(db, "alice")
        bob = make_user(db, "bob")
        hh = make_household(db)
        add_member(db, alice, hh, can_edit=True)
        add_member(db, bob, hh, can_edit=True)

        r = models.Recipe(
            id=str(uuid.uuid4()),
            created_by=alice.id,
            owner_household_id=hh.id,
            title="Alice's recipe",
        )
        db.add(r)
        db.commit()

        login(client, "bob", "TestPass1234")
        resp = client.put(
            f"/api/recipes/{r.id}/ownership",
            json={"share_with_household": False},
        )
        assert resp.status_code == 404  # not creator → 404

    def test_share_with_household_when_not_member_returns_400(self, client, db):
        make_user(db, "alice")
        login(client, "alice", "TestPass1234")

        payload = recipe_payload()
        client.post("/api/recipes", json=payload)

        resp = client.put(
            f"/api/recipes/{payload['id']}/ownership",
            json={"share_with_household": True},
        )
        assert resp.status_code == 400


# ---------------------------------------------------------------------------
# 7. Household isolation between two separate households
# ---------------------------------------------------------------------------

class TestHouseholdIsolation:
    def test_users_in_different_households_do_not_see_each_others_recipes(self, client, db):
        alice = make_user(db, "alice")
        bob = make_user(db, "bob")
        hh1 = make_household(db, "HH1")
        hh2 = make_household(db, "HH2")
        add_member(db, alice, hh1)
        add_member(db, bob, hh2)

        r_alice = models.Recipe(
            id=str(uuid.uuid4()),
            created_by=alice.id,
            owner_household_id=hh1.id,
            title="HH1 Recipe",
        )
        r_bob = models.Recipe(
            id=str(uuid.uuid4()),
            created_by=bob.id,
            owner_household_id=hh2.id,
            title="HH2 Recipe",
        )
        db.add_all([r_alice, r_bob])
        db.commit()

        login(client, "alice", "TestPass1234")
        resp = client.get("/api/recipes")
        titles = [x["title"] for x in resp.json()]
        assert "HH1 Recipe" in titles
        assert "HH2 Recipe" not in titles

    def test_readonly_member_can_view_but_not_edit(self, client, db):
        alice = make_user(db, "alice")
        bob = make_user(db, "bob")
        hh = make_household(db)
        add_member(db, alice, hh, can_edit=True)
        add_member(db, bob, hh, can_edit=False)  # read-only

        r = models.Recipe(
            id=str(uuid.uuid4()),
            created_by=alice.id,
            owner_household_id=hh.id,
            title="Shared",
        )
        db.add(r)
        db.commit()

        login(client, "bob", "TestPass1234")

        # Can view
        resp_get = client.get(f"/api/recipes/{r.id}")
        assert resp_get.status_code == 200

        # Cannot edit
        resp_put = client.put(f"/api/recipes/{r.id}", json={"title": "Hijacked"})
        assert resp_put.status_code == 403


# ---------------------------------------------------------------------------
# 8. POST /api/recipes/estimate-macros  — AI endpoint
# ---------------------------------------------------------------------------

class TestEstimateMacros:
    _PAYLOAD = {
        "title": "Oatmeal",
        "servings": 2,
        "ingredients": [
            {"name": "oats", "qty": 100.0, "unit": "g"},
            {"name": "milk", "qty": 200.0, "unit": "ml"},
        ],
    }

    def test_ai_disabled_returns_403(self, client, db):
        alice = make_user(db, "alice")
        alice.can_use_ai = False
        db.commit()

        login(client, "alice", "TestPass1234")
        resp = client.post("/api/recipes/estimate-macros", json=self._PAYLOAD)
        assert resp.status_code == 403

    def test_missing_env_config_returns_424(self, client, db, monkeypatch):
        make_user(db, "alice")
        login(client, "alice", "TestPass1234")

        monkeypatch.delenv("MEALPILOT_AI_API_URL", raising=False)
        monkeypatch.delenv("MEALPILOT_AI_API_KEY", raising=False)

        resp = client.post("/api/recipes/estimate-macros", json=self._PAYLOAD)
        assert resp.status_code == 424

    def test_rate_limiter_blocks_after_limit(self, client, db, monkeypatch):
        """After exhausting the AI rate-limit window the endpoint returns 429."""
        alice = make_user(db, "alice")

        # Exhaust the bucket for this user before making the request
        ai_limiter.reset(str(alice.id))
        for _ in range(ai_limiter.max_attempts):
            ai_limiter.check(str(alice.id))

        login(client, "alice", "TestPass1234")
        resp = client.post("/api/recipes/estimate-macros", json=self._PAYLOAD)
        assert resp.status_code == 429

        # Clean up so other tests are not affected
        ai_limiter.reset(str(alice.id))

    def test_token_quota_exhausted_returns_429(self, client, db, monkeypatch):
        alice = make_user(db, "alice")
        alice.ai_monthly_token_limit = 10
        alice.ai_used_tokens_this_month = 100
        db.commit()

        # Make sure the rate limiter is clear for this user
        ai_limiter.reset(str(alice.id))

        login(client, "alice", "TestPass1234")
        resp = client.post("/api/recipes/estimate-macros", json=self._PAYLOAD)
        assert resp.status_code == 429

        ai_limiter.reset(str(alice.id))

    def test_successful_estimate_calls_llm_and_returns_macros(self, client, db, monkeypatch):
        alice = make_user(db, "alice")
        # Provide an AgentSettings row so the endpoint has a model name
        settings = models.AgentSettings(user_id=alice.id, model="gpt-4o")
        db.add(settings)
        db.commit()

        monkeypatch.setenv("MEALPILOT_AI_API_URL", "https://api.openai.com/v1/chat/completions")
        monkeypatch.setenv("MEALPILOT_AI_API_KEY", "test-key")

        ai_limiter.reset(str(alice.id))

        llm_response = '{"kcal": 350, "p": 15, "f": 8, "c": 55}'

        async def fake_call_llm(endpoint, api_key, model, prompt):
            return llm_response

        login(client, "alice", "TestPass1234")
        with patch("app.routers.recipes._call_llm", side_effect=fake_call_llm):
            resp = client.post("/api/recipes/estimate-macros", json=self._PAYLOAD)

        assert resp.status_code == 200
        body = resp.json()
        assert body["kcal"] == 350.0
        assert body["p"] == 15.0
        assert body["f"] == 8.0
        assert body["c"] == 55.0

        ai_limiter.reset(str(alice.id))

    def test_usage_is_recorded_after_successful_llm_call(self, client, db, monkeypatch):
        alice = make_user(db, "alice")
        settings = models.AgentSettings(user_id=alice.id, model="gpt-4o")
        db.add(settings)
        db.commit()

        monkeypatch.setenv("MEALPILOT_AI_API_URL", "https://api.openai.com/v1/chat/completions")
        monkeypatch.setenv("MEALPILOT_AI_API_KEY", "test-key")

        ai_limiter.reset(str(alice.id))
        initial_tokens = alice.ai_used_tokens_this_month

        async def fake_call_llm(endpoint, api_key, model, prompt):
            return '{"kcal": 100, "p": 5, "f": 2, "c": 20}'

        login(client, "alice", "TestPass1234")
        with patch("app.routers.recipes._call_llm", side_effect=fake_call_llm):
            resp = client.post("/api/recipes/estimate-macros", json=self._PAYLOAD)

        assert resp.status_code == 200
        db.refresh(alice)
        assert alice.ai_used_tokens_this_month > initial_tokens

        ai_limiter.reset(str(alice.id))


# ---------------------------------------------------------------------------
# 9. Image upload/download  — POST & GET /api/recipes/{id}/image
# ---------------------------------------------------------------------------

class TestRecipeImage:
    _JPEG_MAGIC = b"\xff\xd8\xff\xe0" + b"\x00" * 100
    _PNG_MAGIC = b"\x89PNG\r\n\x1a\n" + b"\x00" * 100
    _INVALID_BYTES = b"THIS IS NOT AN IMAGE" * 20

    def _create_recipe(self, client):
        payload = recipe_payload()
        r = client.post("/api/recipes", json=payload)
        assert r.status_code == 201
        return payload["id"]

    def test_upload_jpeg_returns_200(self, client, db, tmp_path, monkeypatch):
        monkeypatch.setattr("app.images.IMAGES_DIR", tmp_path)
        monkeypatch.setattr("app.routers.recipes.IMAGES_DIR", tmp_path)

        make_user(db, "alice")
        login(client, "alice", "TestPass1234")
        rid = self._create_recipe(client)

        resp = client.post(
            f"/api/recipes/{rid}/image",
            files={"file": ("photo.jpg", io.BytesIO(self._JPEG_MAGIC), "image/jpeg")},
        )
        assert resp.status_code == 200
        assert resp.json()["image_filename"] is not None

    def test_upload_png_returns_200(self, client, db, tmp_path, monkeypatch):
        monkeypatch.setattr("app.images.IMAGES_DIR", tmp_path)
        monkeypatch.setattr("app.routers.recipes.IMAGES_DIR", tmp_path)

        make_user(db, "alice")
        login(client, "alice", "TestPass1234")
        rid = self._create_recipe(client)

        resp = client.post(
            f"/api/recipes/{rid}/image",
            files={"file": ("photo.png", io.BytesIO(self._PNG_MAGIC), "image/png")},
        )
        assert resp.status_code == 200

    def test_invalid_magic_bytes_returns_415(self, client, db, tmp_path, monkeypatch):
        monkeypatch.setattr("app.images.IMAGES_DIR", tmp_path)
        monkeypatch.setattr("app.routers.recipes.IMAGES_DIR", tmp_path)

        make_user(db, "alice")
        login(client, "alice", "TestPass1234")
        rid = self._create_recipe(client)

        resp = client.post(
            f"/api/recipes/{rid}/image",
            files={"file": ("evil.txt", io.BytesIO(self._INVALID_BYTES), "image/jpeg")},
        )
        assert resp.status_code == 415

    def test_file_exceeds_size_limit_returns_413(self, client, db, tmp_path, monkeypatch):
        from app.images import MAX_IMAGE_BYTES

        monkeypatch.setattr("app.images.IMAGES_DIR", tmp_path)
        monkeypatch.setattr("app.routers.recipes.IMAGES_DIR", tmp_path)

        make_user(db, "alice")
        login(client, "alice", "TestPass1234")
        rid = self._create_recipe(client)

        # JPEG magic + enough bytes to exceed the limit
        oversized = self._JPEG_MAGIC + b"\x00" * MAX_IMAGE_BYTES

        resp = client.post(
            f"/api/recipes/{rid}/image",
            files={"file": ("big.jpg", io.BytesIO(oversized), "image/jpeg")},
        )
        assert resp.status_code == 413

    def test_stranger_cannot_upload_image_to_others_recipe(self, client, db, tmp_path, monkeypatch):
        monkeypatch.setattr("app.images.IMAGES_DIR", tmp_path)
        monkeypatch.setattr("app.routers.recipes.IMAGES_DIR", tmp_path)

        alice = make_user(db, "alice")
        make_user(db, "bob")

        r = models.Recipe(
            id=str(uuid.uuid4()),
            created_by=alice.id,
            owner_user_id=alice.id,
            title="Private",
        )
        db.add(r)
        db.commit()

        login(client, "bob", "TestPass1234")
        resp = client.post(
            f"/api/recipes/{r.id}/image",
            files={"file": ("photo.jpg", io.BytesIO(self._JPEG_MAGIC), "image/jpeg")},
        )
        assert resp.status_code == 404

    def test_delete_image_clears_filename(self, client, db, tmp_path, monkeypatch):
        monkeypatch.setattr("app.images.IMAGES_DIR", tmp_path)
        monkeypatch.setattr("app.routers.recipes.IMAGES_DIR", tmp_path)

        make_user(db, "alice")
        login(client, "alice", "TestPass1234")
        rid = self._create_recipe(client)

        # Upload first
        client.post(
            f"/api/recipes/{rid}/image",
            files={"file": ("photo.jpg", io.BytesIO(self._JPEG_MAGIC), "image/jpeg")},
        )

        # Delete
        resp = client.delete(f"/api/recipes/{rid}/image")
        assert resp.status_code == 200
        assert resp.json()["image_filename"] is None


# ---------------------------------------------------------------------------
# 10. Meta endpoints — /api/recipes/meta/tags  and  /api/recipes/meta/meal_types
# ---------------------------------------------------------------------------

class TestMetaEndpoints:
    def test_tags_meta_returns_visible_tags(self, client, db):
        alice = make_user(db, "alice")
        r = models.Recipe(
            id=str(uuid.uuid4()),
            created_by=alice.id,
            owner_user_id=alice.id,
            title="Tagged",
            tags=["vegan"],
        )
        db.add(r)
        db.flush()
        db.add(models.RecipeTag(recipe_id=r.id, tag="vegan"))
        db.commit()

        login(client, "alice", "TestPass1234")
        resp = client.get("/api/recipes/meta/tags")
        assert resp.status_code == 200
        assert "vegan" in resp.json()["tags"]

    def test_tags_meta_does_not_return_other_users_tags(self, client, db):
        alice = make_user(db, "alice")
        bob = make_user(db, "bob")

        r_bob = models.Recipe(
            id=str(uuid.uuid4()),
            created_by=bob.id,
            owner_user_id=bob.id,
            title="Bob's",
            tags=["carnivore"],
        )
        db.add(r_bob)
        db.flush()
        db.add(models.RecipeTag(recipe_id=r_bob.id, tag="carnivore"))
        db.commit()

        login(client, "alice", "TestPass1234")
        resp = client.get("/api/recipes/meta/tags")
        assert resp.status_code == 200
        assert "carnivore" not in resp.json()["tags"]

    def test_meal_types_meta_returns_visible_types(self, client, db):
        alice = make_user(db, "alice")
        r = models.Recipe(
            id=str(uuid.uuid4()),
            created_by=alice.id,
            owner_user_id=alice.id,
            title="Lunch dish",
            meal_types=["lunch"],
        )
        db.add(r)
        db.flush()
        db.add(models.RecipeMealType(recipe_id=r.id, meal_type="lunch"))
        db.commit()

        login(client, "alice", "TestPass1234")
        resp = client.get("/api/recipes/meta/meal_types")
        assert resp.status_code == 200
        assert "lunch" in resp.json()["meal_types"]
