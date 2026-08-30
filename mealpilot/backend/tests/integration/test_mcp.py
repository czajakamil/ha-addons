"""MCP SSE transport: bramka autoryzacji, wiązanie sesji, zakresy kluczy i rate limit."""

import asyncio
from datetime import datetime, timedelta
from uuid import uuid4

import pytest
from fastapi import HTTPException

import app.routers.mcp_sse as mcp_sse
from app import models
from app.mcpserver.server import Principal, current_principal
from app.mcpserver.server import call_tool as mcp_call_tool
from app.ratelimit import mcp_auth_limiter

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


# --------------------------------------------------------------------------- #
# Wiązanie POST-a z sesją SSE
# --------------------------------------------------------------------------- #


def _api_key(client, name="mcp", scope="write"):
    r = client.post("/api/auth/api-keys", json={"name": name, "scope": scope})
    assert r.status_code == 201, r.text
    return r.json()["key"]


def test_post_with_unknown_session_id_is_not_dispatched(make_user):
    client, _ = make_user("mcp_unknown_session")
    raw = _api_key(client)
    r = client.post(
        "/mcp/messages",
        params={"session_id": uuid4().hex},
        headers={"X-MealPilot-Token": raw},
        json={"jsonrpc": "2.0", "id": 1, "method": "tools/list"},
    )
    assert r.status_code == 404


def test_post_without_session_id_is_not_dispatched(make_user):
    client, _ = make_user("mcp_no_session")
    raw = _api_key(client)
    r = client.post("/mcp/messages", headers={"X-MealPilot-Token": raw}, json={})
    assert r.status_code == 404


def test_token_cannot_drive_another_users_session(make_user):
    """Klucz Boba nie może sterować sesją SSE otwartą przez Alicję."""
    _alice, alice_id = make_user("mcp_owner")
    bob, _bob_id = make_user("mcp_intruder")
    bob_key = _api_key(bob)

    session_id = uuid4().hex
    mcp_sse._session_owner[session_id] = alice_id
    try:
        r = bob.post(
            "/mcp/messages",
            params={"session_id": session_id},
            headers={"X-MealPilot-Token": bob_key},
            json={"jsonrpc": "2.0", "id": 1, "method": "tools/list"},
        )
        assert r.status_code == 403
    finally:
        mcp_sse._session_owner.pop(session_id, None)


# --------------------------------------------------------------------------- #
# Zakres klucza API
# --------------------------------------------------------------------------- #


def test_read_scope_key_allows_get_but_rejects_unsafe_methods(make_user, client):
    owner, _ = make_user("mcp_read_scope")
    rid = owner.post("/api/recipes", json={"title": "RO", "servings": 1, "ingredients": [], "steps": []}).json()["id"]
    raw = _api_key(owner, name="tylko-odczyt", scope="read")
    headers = {"X-MealPilot-Token": raw}

    assert client.get("/api/recipes", headers=headers).status_code == 200
    assert client.get(f"/api/recipes/{rid}", headers=headers).status_code == 200

    payload = {"title": "RW", "servings": 1, "ingredients": [], "steps": []}
    assert client.post("/api/recipes", json=payload, headers=headers).status_code == 403
    assert client.put(f"/api/recipes/{rid}", json={"title": "x"}, headers=headers).status_code == 403
    assert client.delete(f"/api/recipes/{rid}", headers=headers).status_code == 403


def test_write_scope_key_may_write(make_user, client):
    owner, _ = make_user("mcp_write_scope")
    raw = _api_key(owner, name="zapis", scope="write")
    payload = {"title": "Zapis", "servings": 1, "ingredients": [], "steps": []}
    r = client.post("/api/recipes", json=payload, headers={"X-MealPilot-Token": raw})
    assert r.status_code == 201


def test_resolve_principal_carries_the_key_scope(make_user):
    client, uid = make_user("mcp_principal")
    raw = _api_key(client, name="ro", scope="read")
    principal = mcp_sse._resolve_principal(raw, "testclient")
    assert principal.user_id == uid
    assert principal.scope == "read"


# --------------------------------------------------------------------------- #
# Egzekwowanie zakresu w samym serwerze MCP
# --------------------------------------------------------------------------- #


def _as_principal(user_id, scope, name, args):
    token = current_principal.set(Principal(user_id=user_id, scope=scope))
    try:
        return asyncio.run(mcp_call_tool(name, args))
    finally:
        current_principal.reset(token)


def test_read_scope_principal_cannot_call_a_write_tool(make_user, db_session):
    """Zakres klucza jest egzekwowany w serwerze MCP, zanim narzędzie dotknie bazy."""
    _client, uid = make_user("mcp_scope_write_tool")
    # RuntimeError, bo to na nim SDK buduje CallToolResult(isError=True); liczy się,
    # że wywołanie się wywala, mówi dlaczego, a przepis nie powstaje.
    with pytest.raises(RuntimeError) as exc:
        _as_principal(uid, "read", "create_recipe", {"title": "Nie powinno powstać"})
    assert "tylko-do-odczytu" in str(exc.value)
    assert db_session.query(models.Recipe).filter(models.Recipe.created_by == uid).count() == 0


def test_read_scope_principal_may_call_a_read_only_tool(make_user):
    client, uid = make_user("mcp_scope_read_tool")
    rid = client.post("/api/recipes", json={"title": "RO2", "servings": 1, "ingredients": [], "steps": []}).json()["id"]
    out = _as_principal(uid, "read", "list_recipes", {"limit": 5})
    assert [i["id"] for i in out["items"]] == [rid]
    assert out["total"] == 1


def test_write_scope_principal_may_call_a_write_tool(make_user):
    _client, uid = make_user("mcp_scope_write_ok")
    out = _as_principal(uid, "write", "create_recipe", {"title": "Z MCP", "servings": 1})
    assert isinstance(out["id"], int)
    assert out["title"] == "Z MCP"


def test_call_tool_without_a_principal_is_rejected():
    with pytest.raises(RuntimeError):
        asyncio.run(mcp_call_tool("list_recipes", {}))


def test_service_errors_surface_as_runtime_errors(make_user):
    _client, uid = make_user("mcp_service_error")
    with pytest.raises(RuntimeError) as exc:
        _as_principal(uid, "write", "get_recipe", {"recipe_id": 999999})
    assert "not_found" in str(exc.value)


def test_failed_call_does_not_poison_the_next_one(make_user):
    """Handler pracuje w wątku roboczym, `db.rollback()` leci potem w pętli zdarzeń.

    Sesja SQLAlchemy nie jest thread-safe, ale przechodzi między wątkami
    sekwencyjnie, nigdy równolegle — kolejne wywołanie musi normalnie zapisać.
    """
    _client, uid = make_user("mcp_rollback_ok")
    with pytest.raises(RuntimeError):
        _as_principal(uid, "write", "update_recipe", {"recipe_id": 999999, "title": "X"})
    out = _as_principal(uid, "write", "create_recipe", {"title": "Po rollbacku", "servings": 1})
    assert out["title"] == "Po rollbacku"


# --------------------------------------------------------------------------- #
# Rate limiting
# --------------------------------------------------------------------------- #


def test_repeated_bad_tokens_are_rate_limited(client):
    limiter = mcp_auth_limiter
    try:
        statuses = [
            client.post("/mcp/messages", headers={"X-MealPilot-Token": f"mp_zle_{n}"}).status_code
            for n in range(limiter.max_attempts + 1)
        ]
        assert set(statuses[: limiter.max_attempts]) == {401}
        assert statuses[-1] == 429
    finally:
        limiter._buckets.clear()


def test_a_valid_token_is_not_charged_to_the_auth_limiter(make_user):
    """Udane uwierzytelnienie nie zużywa budżetu prób — inaczej normalny ruch by się blokował."""
    owner, _ = make_user("mcp_limiter_ok")
    raw = _api_key(owner)
    for _ in range(mcp_auth_limiter.max_attempts + 5):
        assert owner.post("/mcp/messages", headers={"X-MealPilot-Token": raw}).status_code == 404
