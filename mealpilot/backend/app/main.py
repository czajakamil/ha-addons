import os
import secrets
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from sqlalchemy import inspect, text
from starlette.middleware.sessions import SessionMiddleware

from .db import Base, engine, DB_PATH, SessionLocal
from .images import IMAGES_DIR
from .middleware import CloudflareAccessMiddleware
from .routers import admin_users, agent, auth, plan, recipes, settings as settings_router, shopping, templates as templates_router
from . import models
from .security import hash_password, verify_password
from .seed import seed_for_user


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


def _migrate(engine_) -> None:
    inspector = inspect(engine_)
    tables = set(inspector.get_table_names())

    with engine_.begin() as conn:
        if "recipes" in tables:
            cols = {c["name"] for c in inspector.get_columns("recipes")}
            if "image_filename" not in cols:
                conn.execute(text("ALTER TABLE recipes ADD COLUMN image_filename VARCHAR"))
            if "user_id" not in cols:
                conn.execute(text("ALTER TABLE recipes ADD COLUMN user_id INTEGER"))
            if "meal_types" not in cols:
                conn.execute(
                    text("ALTER TABLE recipes ADD COLUMN meal_types JSON NOT NULL DEFAULT '[]'")
                )

        if "meal_plan_entries" in tables:
            cols = {c["name"] for c in inspector.get_columns("meal_plan_entries")}
            if "user_id" not in cols:
                conn.execute(text("ALTER TABLE meal_plan_entries ADD COLUMN user_id INTEGER"))

        if "shopping_items" in tables:
            cols = {c["name"] for c in inspector.get_columns("shopping_items")}
            if "user_id" not in cols:
                conn.execute(text("ALTER TABLE shopping_items ADD COLUMN user_id INTEGER"))
            if "is_custom" not in cols:
                conn.execute(
                    text("ALTER TABLE shopping_items ADD COLUMN is_custom INTEGER NOT NULL DEFAULT 0")
                )

        if "agent_settings" in tables:
            cols = {c["name"] for c in inspector.get_columns("agent_settings")}
            if "ui_prefs" not in cols:
                conn.execute(text("ALTER TABLE agent_settings ADD COLUMN ui_prefs JSON NOT NULL DEFAULT '{}'"))


def _provision_admin() -> None:
    username = os.environ.get("MEALPILOT_ADMIN_USERNAME", "").strip()
    password = os.environ.get("MEALPILOT_ADMIN_PASSWORD", "").strip()
    if not username or not password:
        return

    db = SessionLocal()
    try:
        user = db.query(models.User).filter(models.User.username == username).one_or_none()
        if user is None:
            user = models.User(
                username=username,
                password_hash=hash_password(password),
                role="admin",
                is_active=1,
            )
            db.add(user)
            db.commit()
            db.refresh(user)
            seed_for_user(db, user.id)
        else:
            if not verify_password(password, user.password_hash):
                user.password_hash = hash_password(password)
                db.commit()

    finally:
        db.close()


@asynccontextmanager
async def lifespan(app: FastAPI):
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    IMAGES_DIR.mkdir(parents=True, exist_ok=True)
    Base.metadata.create_all(bind=engine)
    _migrate(engine)
    _provision_admin()
    yield


app = FastAPI(title="MealPilot API", version="0.2.0", lifespan=lifespan)

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

app.include_router(auth.router)
app.include_router(admin_users.router)
app.include_router(recipes.router)
app.include_router(plan.router)
app.include_router(shopping.router)
app.include_router(settings_router.router)
app.include_router(agent.router)
app.include_router(templates_router.router)

IMAGES_DIR.mkdir(parents=True, exist_ok=True)
app.mount("/images", StaticFiles(directory=str(IMAGES_DIR)), name="images")


@app.get("/healthz")
def healthz():
    return {"status": "ok"}


_static = os.environ.get("MEALPILOT_STATIC_DIR")
if _static:
    static_dir = Path(_static).resolve()
    if static_dir.is_dir():
        app.mount("/", StaticFiles(directory=static_dir, html=True), name="frontend")
