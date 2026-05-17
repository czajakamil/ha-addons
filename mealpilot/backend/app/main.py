import os
import secrets
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles  # used for frontend only
from starlette.middleware.sessions import SessionMiddleware

from .db import DB_PATH
from .middleware import CloudflareAccessMiddleware, SecurityHeadersMiddleware
from .routers import admin_households, admin_users, agent, auth, households as households_router, plan, recipes, settings as settings_router, shopping, templates as templates_router


SECRET_FILE = Path(DB_PATH).parent / ".session_secret"


def _load_session_secret() -> str:
    env = os.environ.get("MEALPILOT_SECRET")
    if env:
        return env
    if SECRET_FILE.exists():
        return SECRET_FILE.read_text().strip()
    SECRET_FILE.parent.mkdir(parents=True, exist_ok=True)
    secret = secrets.token_urlsafe(64)
    SECRET_FILE.write_text(secret)
    try:
        os.chmod(SECRET_FILE, 0o600)
    except OSError:
        pass
    return secret


_debug = os.environ.get("MEALPILOT_DEBUG", "0") == "1"
app = FastAPI(
    title="MealPilot API",
    version="0.2.0",
    docs_url="/docs" if _debug else None,
    redoc_url="/redoc" if _debug else None,
    openapi_url="/openapi.json" if _debug else None,
)

_origins = os.environ.get("MEALPILOT_CORS_ORIGINS", "*").split(",")
_allow_credentials = "*" not in _origins
app.add_middleware(
    CORSMiddleware,
    allow_origins=_origins,
    allow_credentials=_allow_credentials,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(
    SessionMiddleware,
    secret_key=_load_session_secret(),
    session_cookie="mealpilot_session",
    same_site="lax",
    https_only=os.environ.get("MEALPILOT_COOKIE_SECURE", "0") == "1",
    max_age=60 * 60 * 24 * 30,
)
app.add_middleware(CloudflareAccessMiddleware)
app.add_middleware(
    SecurityHeadersMiddleware,
    https_only=os.environ.get("MEALPILOT_COOKIE_SECURE", "0") == "1",
)

app.include_router(auth.router)
app.include_router(admin_users.router)
app.include_router(admin_households.router)
app.include_router(households_router.router)
app.include_router(recipes.router)
app.include_router(plan.router)
app.include_router(shopping.router)
app.include_router(settings_router.router)
app.include_router(agent.router)
app.include_router(templates_router.router)

@app.get("/healthz")
def healthz():
    return {"status": "ok"}


_static = os.environ.get("MEALPILOT_STATIC_DIR")
if _static:
    static_dir = Path(_static).resolve()
    if static_dir.is_dir():
        app.mount("/", StaticFiles(directory=static_dir, html=True), name="frontend")
