"""Wspólne fixtury dla testów MealPilot.

Kluczowe: zmienne środowiskowe MUSZĄ być ustawione zanim zaimportujemy `app`,
bo `app.db` tworzy engine na podstawie MEALPILOT_DB w czasie importu. conftest.py
jest ładowany przez pytest przed modułami testowymi, więc ustawiamy je tutaj na
samej górze.
"""

import os
import tempfile
from pathlib import Path

# --- Środowisko testowe (ustawiane PRZED importem aplikacji) ---------------
_TMP = Path(tempfile.mkdtemp(prefix="mealpilot-tests-"))
os.environ["MEALPILOT_DB"] = str(_TMP / "test.db")
os.environ["MEALPILOT_IMAGES_DIR"] = str(_TMP / "images")
os.environ["MEALPILOT_SECRET"] = "test-session-secret-fixed-value"
# Wyzeruj konfigurację, która zmienia zachowanie startu / autoryzacji.
for _k in (
    "MEALPILOT_ADMIN_USERNAME",
    "MEALPILOT_ADMIN_PASSWORD",
    "MEALPILOT_AI_API_URL",
    "MEALPILOT_AI_API_KEY",
    "MEALPILOT_SETUP_TOKEN",
    "MEALPILOT_REQUIRE_CF_ACCESS",
    "MEALPILOT_CORS_ORIGINS",
    "MEALPILOT_COOKIE_SECURE",
):
    os.environ.pop(_k, None)

import pytest  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

from app.db import Base, SessionLocal, engine  # noqa: E402
from app.main import app  # noqa: E402

# --- Stałe pomocnicze ------------------------------------------------------
ADMIN_USERNAME = "admin"
ADMIN_PASSWORD = "AdminPass1234"
DEFAULT_PASSWORD = "UserPass1234"


def _reset_schema() -> None:
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)


@pytest.fixture(autouse=True)
def clean_state():
    """Świeży schemat bazy + wyczyszczony rate limiter przed każdym testem."""
    _reset_schema()
    from app.ratelimit import login_limiter

    login_limiter._buckets.clear()
    # Posprzątaj obrazy między testami.
    images_dir = Path(os.environ["MEALPILOT_IMAGES_DIR"])
    if images_dir.exists():
        for f in images_dir.glob("*"):
            if f.is_file():
                f.unlink()
    yield


@pytest.fixture
def db_session():
    """Bezpośrednia sesja do bazy (do aranżacji/asercji na poziomie ORM)."""
    s = SessionLocal()
    try:
        yield s
    finally:
        s.close()


@pytest.fixture
def client():
    """Surowy klient HTTP bez zalogowanego użytkownika (uruchamia lifespan)."""
    with TestClient(app) as c:
        yield c


def _do_setup(c: TestClient, username: str = ADMIN_USERNAME, password: str = ADMIN_PASSWORD):
    r = c.post("/api/auth/setup", json={"username": username, "password": password})
    assert r.status_code == 201, r.text
    return r


@pytest.fixture
def admin_client():
    """Klient zalogowany jako bootstrapowany admin (po /setup)."""
    with TestClient(app) as c:
        _do_setup(c)
        yield c


@pytest.fixture
def new_client():
    """Fabryka świeżych klientów HTTP (osobny cookie-jar = osobna sesja)."""
    created = []

    def _factory() -> TestClient:
        c = TestClient(app)
        c.__enter__()
        created.append(c)
        return c

    yield _factory
    for c in created:
        c.__exit__(None, None, None)


@pytest.fixture
def make_user(admin_client, new_client):
    """Fabryka: tworzy użytkownika przez API admina i zwraca zalogowanego klienta.

    Zwraca (client, user_id).
    """

    def _make(
        username: str, password: str = DEFAULT_PASSWORD, role: str = "user", login: bool = True
    ):
        r = admin_client.post(
            "/api/admin/users",
            json={"username": username, "password": password, "role": role},
        )
        assert r.status_code == 201, r.text
        user_id = r.json()["id"]
        c = new_client()
        if login:
            lr = c.post("/api/auth/login", json={"username": username, "password": password})
            assert lr.status_code == 200, lr.text
        return c, user_id

    return _make
