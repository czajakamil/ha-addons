"""Testy agenta AI. LLM jest zamockowany — nie wykonujemy realnych wywołań sieci."""
import pytest

import app.routers.agent as agent_router

pytestmark = pytest.mark.integration


def test_usage_status_endpoint(admin_client):
    r = admin_client.get("/api/agent/usage")
    assert r.status_code == 200
    body = r.json()
    assert body["can_use_ai"] is True
    assert body["ai_used_tokens_this_month"] == 0


def test_usage_check_ok_by_default(admin_client):
    assert admin_client.post("/api/agent/usage/check").status_code == 204


def test_conversation_crud(admin_client):
    r = admin_client.post("/api/agent/conversations", json={"title": "Rozmowa", "model": "gpt"})
    assert r.status_code == 200
    cid = r.json()["id"]

    assert cid in {c["id"] for c in admin_client.get("/api/agent/conversations").json()}

    r = admin_client.patch(f"/api/agent/conversations/{cid}", json={"title": "Nowy tytuł"})
    assert r.json()["title"] == "Nowy tytuł"

    assert admin_client.get(f"/api/agent/conversations/{cid}").status_code == 200
    assert admin_client.delete(f"/api/agent/conversations/{cid}").status_code == 204
    assert admin_client.get(f"/api/agent/conversations/{cid}").status_code == 404


def test_conversation_isolated_per_user(make_user):
    alice, _ = make_user("ag_a")
    bob, _ = make_user("ag_b")
    cid = alice.post("/api/agent/conversations", json={"title": "A"}).json()["id"]
    assert bob.get(f"/api/agent/conversations/{cid}").status_code == 404


def test_run_persists_reply_and_tool_events(admin_client, monkeypatch):
    async def fake_run_agent(db, user, settings, history):
        return {
            "reply": "Dodałem przepis.",
            "tool_events": [{
                "tool_use_id": "tu1", "name": "create_recipe",
                "input": {"title": "X"}, "output": {"ok": True},
            }],
            "changed": ["recipes"],
        }

    async def fake_title(model, user_text, assistant_text):
        return "Wygenerowany tytuł"

    monkeypatch.setattr(agent_router, "run_agent", fake_run_agent)
    monkeypatch.setattr(agent_router, "generate_conversation_title", fake_title)

    cid = admin_client.post("/api/agent/conversations", json={}).json()["id"]
    admin_client.post(f"/api/agent/conversations/{cid}/messages",
                      json={"role": "user", "content": "dodaj przepis"})

    r = admin_client.post(f"/api/agent/conversations/{cid}/run")
    assert r.status_code == 200
    body = r.json()
    assert body["reply"] == "Dodałem przepis."
    assert body["changed"] == ["recipes"]
    assert body["tool_events"][0]["name"] == "create_recipe"
    assert body["title"] == "Wygenerowany tytuł"

    # Wiadomość asystenta utrwalona + tytuł zapisany na konwersacji.
    detail = admin_client.get(f"/api/agent/conversations/{cid}").json()
    assert detail["title"] == "Wygenerowany tytuł"
    roles = [m["role"] for m in detail["messages"]]
    assert roles == ["user", "assistant"]
    assert detail["messages"][1]["tool_uses"][0]["tool_name"] == "create_recipe"

    # Zużycie tokenów zostało doliczone.
    assert admin_client.get("/api/agent/usage").json()["ai_used_tokens_this_month"] > 0


def test_run_blocked_when_quota_exhausted(admin_client, monkeypatch):
    # Wyczerpany limit tokenów → 429 zanim dojdzie do LLM.
    me = admin_client.get("/api/auth/me").json()
    admin_client.put(f"/api/admin/users/{me['id']}/ai-limits", json={"ai_monthly_token_limit": 10})
    admin_client.post("/api/agent/usage/report", json={"tokens": 10})

    cid = admin_client.post("/api/agent/conversations", json={}).json()["id"]
    admin_client.post(f"/api/agent/conversations/{cid}/messages",
                      json={"role": "user", "content": "hej"})
    r = admin_client.post(f"/api/agent/conversations/{cid}/run")
    assert r.status_code == 429


def test_stream_emits_text_then_done(admin_client, monkeypatch):
    async def fake_stream(*args, **kwargs):
        yield {"type": "text_delta", "text": "Cześć"}
        yield {"type": "text_delta", "text": " świat"}

    monkeypatch.setattr(agent_router, "is_anthropic", lambda url: False)
    monkeypatch.setattr(agent_router, "stream_openai", fake_stream)
    monkeypatch.setenv("MEALPILOT_AI_API_URL", "https://example.test/v1/chat")
    monkeypatch.setenv("MEALPILOT_AI_API_KEY", "sk-test")

    cid = admin_client.post("/api/agent/conversations", json={}).json()["id"]
    admin_client.post(f"/api/agent/conversations/{cid}/messages",
                      json={"role": "user", "content": "powiedz cześć"})

    r = admin_client.post(f"/api/agent/conversations/{cid}/stream")
    assert r.status_code == 200
    body = r.text
    assert "text_delta" in body
    assert '"type": "done"' in body

    # Odpowiedź asystenta złożona z deltów i utrwalona.
    detail = admin_client.get(f"/api/agent/conversations/{cid}").json()
    assistant = [m for m in detail["messages"] if m["role"] == "assistant"]
    assert assistant and assistant[0]["content"] == "Cześć świat"


def test_stream_errors_without_ai_config(admin_client, monkeypatch):
    monkeypatch.delenv("MEALPILOT_AI_API_URL", raising=False)
    monkeypatch.delenv("MEALPILOT_AI_API_KEY", raising=False)
    cid = admin_client.post("/api/agent/conversations", json={}).json()["id"]
    admin_client.post(f"/api/agent/conversations/{cid}/messages",
                      json={"role": "user", "content": "x"})
    r = admin_client.post(f"/api/agent/conversations/{cid}/stream")
    assert r.status_code == 200
    assert "error" in r.text and "MEALPILOT_AI_API_URL" in r.text
