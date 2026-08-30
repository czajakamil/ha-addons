"""Higiena zasobów agenta: kasowanie rozmów i sesje DB w streamingu.

Dwa osobne wycieki:
* `DELETE /api/agent/conversations/{id}` kasował samą rozmowę — wiadomości
  i tool_use'y zostawały w bazie na zawsze.
* Generator strumienia tworzył `SessionLocal()` poza `try/finally`, więc każde
  wczesne wyjście (brak konfiguracji AI) i każde rozłączenie klienta w połowie
  strumienia zostawiało otwartą sesję. Kilkanaście przerwanych rozmów
  wyczerpywało pulę połączeń.
"""

import asyncio

import pytest

import app.routers.agent as agent_router
from app import models

pytestmark = pytest.mark.integration


def _conversation_with_tool_use(client):
    conv_id = client.post("/api/agent/conversations", json={"title": "Rozmowa"}).json()["id"]
    msg = client.post(
        f"/api/agent/conversations/{conv_id}/messages",
        json={
            "role": "user",
            "content": "hej",
            "tool_uses": [{"tool_use_id": "tu1", "tool_name": "list_recipes", "input": {}}],
        },
    )
    assert msg.status_code == 200, msg.text
    assert msg.json()["tool_uses"]
    return conv_id, msg.json()["id"]


def test_deleting_conversation_removes_messages_and_tool_uses(admin_client, db_session):
    conv_id, msg_id = _conversation_with_tool_use(admin_client)

    assert admin_client.delete(f"/api/agent/conversations/{conv_id}").status_code == 204
    db_session.expire_all()

    assert db_session.query(models.AgentMessage).filter(models.AgentMessage.conversation_id == conv_id).count() == 0
    assert db_session.query(models.AgentToolUse).filter(models.AgentToolUse.message_id == msg_id).count() == 0


# --------------------------------------------------------------------------- #
# Sesje DB w streamingu
# --------------------------------------------------------------------------- #


def _track_stream_sessions(monkeypatch) -> list:
    """Podmienia `SessionLocal` w routerze agenta na fabrykę licząca `close()`."""
    created: list = []
    real_factory = agent_router.SessionLocal

    def factory():
        session = real_factory()
        original_close = session.close
        session.close_calls = 0

        def counting_close():
            session.close_calls += 1
            original_close()

        session.close = counting_close
        created.append(session)
        return session

    monkeypatch.setattr(agent_router, "SessionLocal", factory)
    return created


def test_stream_closes_its_session_when_ai_is_unconfigured(admin_client, monkeypatch):
    created = _track_stream_sessions(monkeypatch)
    monkeypatch.delenv("MEALPILOT_AI_API_URL", raising=False)
    monkeypatch.delenv("MEALPILOT_AI_API_KEY", raising=False)

    conv_id = admin_client.post("/api/agent/conversations", json={}).json()["id"]
    admin_client.post(f"/api/agent/conversations/{conv_id}/messages", json={"role": "user", "content": "x"})

    r = admin_client.post(f"/api/agent/conversations/{conv_id}/stream")
    assert r.status_code == 200
    assert "MEALPILOT_AI_API_URL" in r.text

    assert len(created) == 1
    assert created[0].close_calls == 1


def test_stream_closes_its_session_when_provider_explodes(admin_client, monkeypatch):
    created = _track_stream_sessions(monkeypatch)

    async def exploding_stream(*_args, **_kwargs):
        raise RuntimeError("dostawca sie wysypal")
        yield  # pragma: no cover - czyni z funkcji async generator

    monkeypatch.setattr(agent_router, "is_anthropic", lambda url: False)
    monkeypatch.setattr(agent_router, "stream_openai", exploding_stream)
    monkeypatch.setenv("MEALPILOT_AI_API_URL", "https://example.test/v1/chat")
    monkeypatch.setenv("MEALPILOT_AI_API_KEY", "sk-test")

    conv_id = admin_client.post("/api/agent/conversations", json={}).json()["id"]
    admin_client.post(f"/api/agent/conversations/{conv_id}/messages", json={"role": "user", "content": "x"})

    r = admin_client.post(f"/api/agent/conversations/{conv_id}/stream")
    assert r.status_code == 200
    assert "dostawca sie wysypal" in r.text

    assert len(created) == 1
    assert created[0].close_calls == 1


def test_stream_closes_its_session_when_client_disconnects(admin_client, monkeypatch, db_session):
    """Rozłączenie w połowie strumienia = GeneratorExit w generatorze."""
    created = _track_stream_sessions(monkeypatch)

    async def endless_stream(*_args, **_kwargs):
        while True:
            yield {"type": "text_delta", "text": "…"}

    monkeypatch.setattr(agent_router, "is_anthropic", lambda url: False)
    monkeypatch.setattr(agent_router, "stream_openai", endless_stream)
    monkeypatch.setenv("MEALPILOT_AI_API_URL", "https://example.test/v1/chat")
    monkeypatch.setenv("MEALPILOT_AI_API_KEY", "sk-test")

    conv_id = admin_client.post("/api/agent/conversations", json={}).json()["id"]
    admin_client.post(f"/api/agent/conversations/{conv_id}/messages", json={"role": "user", "content": "x"})
    user_id = admin_client.get("/api/auth/me").json()["id"]

    async def drive():
        user = db_session.get(models.User, user_id)
        response = await agent_router.stream_conversation(conv_id=conv_id, user=user, db=db_session)
        chunks = response.body_iterator
        first = await chunks.__anext__()
        assert "text_delta" in first
        # Starlette robi dokładnie to, gdy klient znika w trakcie strumienia.
        await chunks.aclose()

    asyncio.run(drive())

    assert len(created) == 1
    assert created[0].close_calls == 1


def test_successful_stream_still_closes_exactly_once(admin_client, monkeypatch):
    created = _track_stream_sessions(monkeypatch)

    async def fake_stream(*_args, **_kwargs):
        yield {"type": "text_delta", "text": "Gotowe"}

    monkeypatch.setattr(agent_router, "is_anthropic", lambda url: False)
    monkeypatch.setattr(agent_router, "stream_openai", fake_stream)
    monkeypatch.setenv("MEALPILOT_AI_API_URL", "https://example.test/v1/chat")
    monkeypatch.setenv("MEALPILOT_AI_API_KEY", "sk-test")

    conv_id = admin_client.post("/api/agent/conversations", json={}).json()["id"]
    admin_client.post(f"/api/agent/conversations/{conv_id}/messages", json={"role": "user", "content": "x"})

    r = admin_client.post(f"/api/agent/conversations/{conv_id}/stream")
    assert r.status_code == 200
    assert '"type": "done"' in r.text

    assert len(created) == 1
    assert created[0].close_calls == 1
