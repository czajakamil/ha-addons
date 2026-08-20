"""Cloudflare Access middleware. _verify jest mockowany, by nie wykonywać sieci do JWKS."""

import pytest
from starlette.applications import Starlette
from starlette.responses import PlainTextResponse
from starlette.routing import Route
from starlette.testclient import TestClient

from app.middleware import CloudflareAccessMiddleware

pytestmark = pytest.mark.integration


def _build_app(monkeypatch, enabled=True):
    monkeypatch.setenv("MEALPILOT_REQUIRE_CF_ACCESS", "1" if enabled else "0")
    monkeypatch.setenv("MEALPILOT_CF_ACCESS_TEAM_DOMAIN", "team.cloudflareaccess.com")
    monkeypatch.setenv("MEALPILOT_CF_ACCESS_AUD", "aud-tag")

    async def ok(request):
        return PlainTextResponse("ok")

    app = Starlette(routes=[Route("/api/x", ok), Route("/healthz", ok)])
    app.add_middleware(CloudflareAccessMiddleware)
    return app


def test_missing_assertion_rejected(monkeypatch):
    app = _build_app(monkeypatch)
    with TestClient(app) as c:
        assert c.get("/api/x").status_code == 401


def test_bypass_path_allowed_without_token(monkeypatch):
    app = _build_app(monkeypatch)
    with TestClient(app) as c:
        assert c.get("/healthz").status_code == 200


def test_invalid_token_rejected(monkeypatch):
    monkeypatch.setattr(CloudflareAccessMiddleware, "_verify", lambda self, token: None)
    app = _build_app(monkeypatch)
    with TestClient(app) as c:
        r = c.get("/api/x", headers={"cf-access-jwt-assertion": "bad.token"})
        assert r.status_code == 401


def test_valid_token_passes(monkeypatch):
    monkeypatch.setattr(CloudflareAccessMiddleware, "_verify", lambda self, token: {"sub": "user@example.com"})
    app = _build_app(monkeypatch)
    with TestClient(app) as c:
        r = c.get("/api/x", headers={"cf-access-jwt-assertion": "dobry.token"})
        assert r.status_code == 200
        assert r.text == "ok"


def test_disabled_passes_everything(monkeypatch):
    app = _build_app(monkeypatch, enabled=False)
    with TestClient(app) as c:
        assert c.get("/api/x").status_code == 200


def test_enabled_without_config_raises(monkeypatch):
    monkeypatch.setenv("MEALPILOT_REQUIRE_CF_ACCESS", "1")
    monkeypatch.setenv("MEALPILOT_CF_ACCESS_TEAM_DOMAIN", "")
    monkeypatch.setenv("MEALPILOT_CF_ACCESS_AUD", "")
    with pytest.raises(RuntimeError):
        CloudflareAccessMiddleware(app=lambda *a: None)
