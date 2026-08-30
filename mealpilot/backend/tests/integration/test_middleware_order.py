"""Kolejność middleware z `app.main`.

`add_middleware` doda warstwę na zewnątrz poprzednich, więc ostatnia dodana jest
najbardziej zewnętrzna. CORS musi być NA ZEWNĄTRZ Cloudflare Access: przeglądarka
wysyła preflight `OPTIONS` bez nagłówka `cf-access-jwt-assertion`, więc przy CF na
zewnątrz każdy preflight kończył się 401 i właściwe żądanie nigdy nie wychodziło.
"""

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.main import _install_middleware

pytestmark = pytest.mark.integration

ORIGIN = "https://app.example.com"


@pytest.fixture
def cf_app(monkeypatch):
    """Aplikacja z pełnym stosem middleware i włączonym CF Access."""
    monkeypatch.setenv("MEALPILOT_REQUIRE_CF_ACCESS", "1")
    monkeypatch.setenv("MEALPILOT_CF_ACCESS_TEAM_DOMAIN", "team.cloudflareaccess.com")
    monkeypatch.setenv("MEALPILOT_CF_ACCESS_AUD", "aud-tag")
    monkeypatch.setenv("MEALPILOT_CORS_ORIGINS", ORIGIN)

    app_ = FastAPI()

    @app_.get("/api/x")
    def _x():
        return {"ok": True}

    _install_middleware(app_)
    return app_


def test_preflight_passes_without_cf_token(cf_app):
    with TestClient(cf_app) as c:
        r = c.options(
            "/api/x",
            headers={"Origin": ORIGIN, "Access-Control-Request-Method": "GET"},
        )
    assert r.status_code == 200, r.text
    assert r.headers["access-control-allow-origin"] == ORIGIN


def test_real_request_without_cf_token_still_rejected(cf_app):
    with TestClient(cf_app) as c:
        r = c.get("/api/x", headers={"Origin": ORIGIN})
    assert r.status_code == 401
    # 401 też musi nieść nagłówki CORS, inaczej przeglądarka pokaże błąd CORS
    # zamiast prawdziwego statusu.
    assert r.headers["access-control-allow-origin"] == ORIGIN


def test_valid_cf_token_reaches_the_route(cf_app, monkeypatch):
    from app.middleware import CloudflareAccessMiddleware

    monkeypatch.setattr(CloudflareAccessMiddleware, "_verify", lambda self, token: {"sub": "ala@example.com"})
    with TestClient(cf_app) as c:
        r = c.get("/api/x", headers={"Origin": ORIGIN, "cf-access-jwt-assertion": "dobry.token"})
    assert r.status_code == 200
    assert r.json() == {"ok": True}
