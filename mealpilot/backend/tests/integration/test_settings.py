"""Ustawienia agenta i preferencje UI (/api/settings)."""

import pytest

from app.agent.prompts import DEFAULT_SYSTEM_PROMPT

pytestmark = pytest.mark.integration


def test_agent_settings_require_auth(client):
    assert client.get("/api/settings/agent").status_code == 401
    assert client.put("/api/settings/agent", json={"model": "x"}).status_code == 401


def test_agent_settings_expose_backend_default_prompt(admin_client):
    r = admin_client.get("/api/settings/agent")
    assert r.status_code == 200, r.text
    body = r.json()
    # Domyślnie brak nadpisania — UI ma pokazać `default_system_prompt`.
    assert body["system_prompt"] == ""
    assert body["default_system_prompt"] == DEFAULT_SYSTEM_PROMPT


def test_agent_settings_have_no_endpoint_or_api_key_fields(admin_client):
    """Endpoint i klucz pochodzą z env dodatku — nie z API ustawień."""
    body = admin_client.get("/api/settings/agent").json()
    assert "endpoint" not in body
    assert "api_key" not in body


def test_agent_settings_ignore_endpoint_and_api_key_on_write(admin_client):
    r = admin_client.put(
        "/api/settings/agent",
        json={
            "model": "claude-haiku-4-5-20251001",
            "system_prompt": "",
            "endpoint": "https://evil.example/v1",
            "api_key": "sk-nope",
        },
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["model"] == "claude-haiku-4-5-20251001"
    assert "endpoint" not in body
    assert "api_key" not in body

    body = admin_client.get("/api/settings/agent").json()
    assert "endpoint" not in body
    assert "api_key" not in body


def test_saving_model_alone_does_not_pin_a_prompt_copy(admin_client):
    """Regresja: UI zapisujące sam model nie może przypiąć kopii promptu."""
    r = admin_client.put(
        "/api/settings/agent",
        json={"model": "some-model", "system_prompt": ""},
    )
    assert r.status_code == 200, r.text
    assert r.json()["system_prompt"] == ""
    assert r.json()["default_system_prompt"] == DEFAULT_SYSTEM_PROMPT

    stored = admin_client.get("/api/settings/agent").json()
    assert stored["model"] == "some-model"
    assert stored["system_prompt"] == ""
    # Zmiana promptu w backendzie od razu obowiązuje — nic go nie nadpisuje.
    assert stored["default_system_prompt"] == DEFAULT_SYSTEM_PROMPT


def test_custom_prompt_round_trips_and_can_be_reset(admin_client):
    custom = "Mów wyłącznie wierszem."
    r = admin_client.put("/api/settings/agent", json={"model": "m", "system_prompt": custom})
    assert r.status_code == 200, r.text
    assert r.json()["system_prompt"] == custom
    assert admin_client.get("/api/settings/agent").json()["system_prompt"] == custom

    # "Przywróć domyślny" = zapis pustego stringa.
    r = admin_client.put("/api/settings/agent", json={"model": "m", "system_prompt": ""})
    assert r.status_code == 200, r.text
    assert r.json()["system_prompt"] == ""
    assert admin_client.get("/api/settings/agent").json()["system_prompt"] == ""


def test_agent_settings_are_per_user(admin_client, make_user):
    alice, _ = make_user("alice")
    admin_client.put("/api/settings/agent", json={"model": "admin-model", "system_prompt": "A"})
    alice.put("/api/settings/agent", json={"model": "alice-model", "system_prompt": "B"})

    assert admin_client.get("/api/settings/agent").json()["model"] == "admin-model"
    assert alice.get("/api/settings/agent").json()["model"] == "alice-model"
    assert alice.get("/api/settings/agent").json()["system_prompt"] == "B"
