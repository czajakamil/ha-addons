"""MCP SSE transport: bramka autoryzacji + regresja naive datetime (commit 256f1e8)."""

from datetime import datetime, timedelta

import pytest
from fastapi import HTTPException

import app.routers.mcp_sse as mcp_sse
from app import models

pytestmark = pytest.mark.integration


def test_post_requires_token(admin_client):
    assert admin_client.post("/mcp/messages").status_code == 401


def test_sse_requires_token(admin_client):
    assert admin_client.get("/mcp/sse").status_code == 401


def test_invalid_token_rejected(admin_client):
    r = admin_client.post("/mcp/messages", headers={"X-MealPilot-Token": "mp_zle"})
    assert r.status_code == 401


def test_resolve_user_with_valid_key(make_user, db_session):
    client, uid = make_user("mcp_ok")
    raw = client.post("/api/auth/api-keys", json={"name": "mcp"}).json()["key"]
    user = mcp_sse._resolve_user(raw)
    # _resolve_user zwraca instancję po zamknięciu sesji — sprawdzamy tylko że
    # rozwiązała poprawny rekord (bez dotykania expirowanych atrybutów).
    assert isinstance(user, models.User)
    key = db_session.query(models.ApiKey).filter(models.ApiKey.user_id == uid).one()
    assert key is not None


def test_resolve_user_handles_naive_last_used(make_user, db_session):
    """Regresja: SQLite zwraca naive datetime; nie może wysadzić odejmowania od aware now."""
    client, uid = make_user("mcp_naive")
    raw = client.post("/api/auth/api-keys", json={"name": "mcp"}).json()["key"]

    key = db_session.query(models.ApiKey).filter(models.ApiKey.user_id == uid).one()
    key.last_used_at = datetime.utcnow() - timedelta(days=1)  # naive, w przeszłości
    db_session.commit()

    user = mcp_sse._resolve_user(raw)  # nie może rzucić TypeError
    assert isinstance(user, models.User)


def test_resolve_user_invalid_key_raises():
    with pytest.raises(HTTPException) as exc:
        mcp_sse._resolve_user("mp_nieistnieje")
    assert exc.value.status_code == 401


def test_resolve_user_inactive_user_rejected(admin_client, make_user, db_session):
    client, uid = make_user("mcp_inactive")
    raw = client.post("/api/auth/api-keys", json={"name": "mcp"}).json()["key"]
    # Admin dezaktywuje konto.
    admin_client.patch(f"/api/admin/users/{uid}", json={"is_active": False})
    with pytest.raises(HTTPException) as exc:
        mcp_sse._resolve_user(raw)
    assert exc.value.status_code == 401
