"""Smoke / release-blockers: minimalny zestaw, który MUSI przejść przed tagiem."""

import pytest

from tests.conftest import ADMIN_PASSWORD, ADMIN_USERNAME

pytestmark = [pytest.mark.integration, pytest.mark.smoke]


def test_healthz(client):
    r = client.get("/healthz")
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}


def test_setup_required_then_setup(client):
    r = client.get("/api/auth/setup-required")
    assert r.status_code == 200
    assert r.json()["setup_required"] is True

    r = client.post(
        "/api/auth/setup",
        json={"username": ADMIN_USERNAME, "password": ADMIN_PASSWORD},
    )
    assert r.status_code == 201
    body = r.json()
    assert body["username"] == ADMIN_USERNAME
    assert body["role"] == "admin"

    # Po setupie nie jest już wymagany.
    r = client.get("/api/auth/setup-required")
    assert r.json()["setup_required"] is False


def test_admin_can_login_and_me(admin_client):
    r = admin_client.get("/api/auth/me")
    assert r.status_code == 200
    assert r.json()["username"] == ADMIN_USERNAME


def test_recipe_crud_round_trip(admin_client):
    payload = {
        "title": "Testowy przepis",
        "servings": 2,
        "ingredients": [{"name": "jajko", "qty": 2, "unit": "szt"}],
        "steps": ["Krok pierwszy"],
    }
    r = admin_client.post("/api/recipes", json=payload)
    assert r.status_code == 201, r.text
    assert r.json()["title"] == "Testowy przepis"
    rid = r.json()["id"]

    r = admin_client.get(f"/api/recipes/{rid}")
    assert r.status_code == 200
    assert r.json()["steps"][0]["text"] == "Krok pierwszy"

    r = admin_client.put(f"/api/recipes/{rid}", json={"title": "Zmieniony"})
    assert r.status_code == 200
    assert r.json()["title"] == "Zmieniony"
    # Zmiana tytułu nie rusza id — powiązania planu i listy zakupów przeżywają.
    assert r.json()["id"] == rid

    r = admin_client.delete(f"/api/recipes/{rid}")
    assert r.status_code == 204

    r = admin_client.get(f"/api/recipes/{rid}")
    assert r.status_code == 404


def test_plan_then_generate_shopping(admin_client):
    # Setup zasiał demo-przepisy. Pobierz istniejący przepis.
    recipes = admin_client.get("/api/recipes").json()
    assert recipes, "setup powinien zasiać demo-przepisy"
    rid = recipes[0]["id"]

    week = "2026-06-01"
    entries = [
        {"day": 0, "meal": "Obiad", "recipe_id": rid, "servings": 2},
        {"day": 1, "meal": "Obiad", "recipe_id": rid, "servings": 2},
    ]
    r = admin_client.put(f"/api/plan/{week}", json=entries)
    assert r.status_code == 200, r.text
    assert len(r.json()["entries"]) == 2

    r = admin_client.post(f"/api/shopping/{week}/generate")
    assert r.status_code == 200, r.text
    items = r.json()
    assert items, "lista zakupów powinna mieć pozycje z przepisu"
    # Pozycje skonsolidowane: ten sam przepis 2x → ilości zsumowane, brak duplikatów (name, unit).
    keys = [(i["name"].lower(), i["unit"]) for i in items]
    assert len(keys) == len(set(keys))
