"""Shared pytest fixtures for MealPilot backend tests.

Run from the `backend/` directory:
    pytest
"""
from __future__ import annotations

import os

# Must be set before app imports — app/main.py reads these at module level.
os.environ.setdefault("MEALPILOT_SECRET", "test-secret-not-for-production")
os.environ.setdefault("MEALPILOT_DB", ":memory:")

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.db import Base, get_db
from app.main import app
from app.models import User
from app.ratelimit import login_limiter, setup_limiter, ai_limiter
from app.security import hash_password


# ---------------------------------------------------------------------------
# Rate-limiter reset (module-level singletons persist across tests)
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def reset_rate_limiters():
    for limiter in (login_limiter, setup_limiter, ai_limiter):
        limiter._buckets.clear()
    yield


# ---------------------------------------------------------------------------
# Database fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def db():
    """Provide a fresh SQLite in-memory session for each test."""
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
        future=True,
    )

    # Enable FK enforcement so integrity constraints fire like on SQLite prod.
    @event.listens_for(engine, "connect")
    def _set_pragmas(conn, _record):
        conn.execute("PRAGMA foreign_keys=ON")

    Base.metadata.create_all(bind=engine)
    TestingSession = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)

    session = TestingSession()
    try:
        yield session
    finally:
        session.close()
        Base.metadata.drop_all(bind=engine)
        engine.dispose()


# ---------------------------------------------------------------------------
# HTTP client fixture
# ---------------------------------------------------------------------------

@pytest.fixture()
def client(db):
    """TestClient with the real FastAPI app wired to the in-memory database."""

    def _override_get_db():
        try:
            yield db
        finally:
            pass  # session lifecycle managed by `db` fixture

    app.dependency_overrides[get_db] = _override_get_db
    with TestClient(app, raise_server_exceptions=True) as c:
        yield c
    app.dependency_overrides.pop(get_db, None)


# ---------------------------------------------------------------------------
# User fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def admin_user(db):
    """Create and persist an admin user, return the ORM object."""
    user = User(
        username="admin",
        password_hash=hash_password("AdminPass1234"),
        role="admin",
        is_active=1,
        session_version=0,
    )
    db.add(user)
    db.commit()
    db.refresh(user)
    return user


@pytest.fixture()
def regular_user(db):
    """Create and persist a regular (non-admin) user, return the ORM object."""
    user = User(
        username="regularuser",
        password_hash=hash_password("UserPass5678"),
        role="user",
        is_active=1,
        session_version=0,
    )
    db.add(user)
    db.commit()
    db.refresh(user)
    return user


# ---------------------------------------------------------------------------
# Auth helpers
# ---------------------------------------------------------------------------

def login(client: TestClient, username: str, password: str) -> dict:
    """Log in via the API and return the session cookies dict."""
    resp = client.post(
        "/api/auth/login",
        json={"username": username, "password": password},
    )
    assert resp.status_code == 200, f"login failed: {resp.text}"
    return dict(client.cookies)


@pytest.fixture()
def admin_client(client, admin_user):
    """A TestClient whose session is already authenticated as the admin user."""
    login(client, "admin", "AdminPass1234")
    return client
