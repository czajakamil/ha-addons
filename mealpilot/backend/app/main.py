import os
import secrets
from contextlib import asynccontextmanager, suppress
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy import inspect, text
from starlette.middleware.sessions import SessionMiddleware

from . import models
from .db import DB_PATH, Base, SessionLocal, engine
from .images import IMAGES_DIR
from .middleware import CloudflareAccessMiddleware
from .routers import admin_households, admin_users, agent, auth, mcp_sse, plan, recipes, shopping
from .routers import settings as settings_router
from .routers import templates as templates_router
from .security import hash_password, verify_password
from .seed import seed_for_user
from .services.errors import ServiceError

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
    with suppress(OSError):
        os.chmod(SECRET_FILE, 0o600)
    return secret


def _migrate(engine_) -> None:
    inspector = inspect(engine_)
    tables = set(inspector.get_table_names())

    with engine_.begin() as conn:
        if "users" in tables:
            cols = {c["name"] for c in inspector.get_columns("users")}
            if "session_version" not in cols:
                conn.execute(text("ALTER TABLE users ADD COLUMN session_version INTEGER NOT NULL DEFAULT 0"))
            if "can_use_ai" not in cols:
                conn.execute(text("ALTER TABLE users ADD COLUMN can_use_ai BOOLEAN NOT NULL DEFAULT 1"))
            if "ai_monthly_token_limit" not in cols:
                conn.execute(text("ALTER TABLE users ADD COLUMN ai_monthly_token_limit INTEGER"))
            if "ai_monthly_cost_limit_cents" not in cols:
                conn.execute(text("ALTER TABLE users ADD COLUMN ai_monthly_cost_limit_cents INTEGER"))
            if "ai_used_tokens_this_month" not in cols:
                conn.execute(text("ALTER TABLE users ADD COLUMN ai_used_tokens_this_month INTEGER NOT NULL DEFAULT 0"))
            if "ai_used_cost_cents_this_month" not in cols:
                conn.execute(
                    text("ALTER TABLE users ADD COLUMN ai_used_cost_cents_this_month INTEGER NOT NULL DEFAULT 0")
                )
            if "ai_usage_period_start" not in cols:
                conn.execute(text("ALTER TABLE users ADD COLUMN ai_usage_period_start DATETIME"))
                conn.execute(
                    text(
                        "UPDATE users SET ai_usage_period_start = CURRENT_TIMESTAMP WHERE ai_usage_period_start IS NULL"
                    )
                )

        # Add owner columns + backfill for resource tables
        for tname in ("recipes", "meal_plan_entries", "week_templates", "shopping_items"):
            if tname not in tables:
                continue
            cols = {c["name"] for c in inspector.get_columns(tname)}
            if "owner_user_id" not in cols:
                conn.execute(text(f"ALTER TABLE {tname} ADD COLUMN owner_user_id INTEGER"))
            if "owner_household_id" not in cols:
                conn.execute(text(f"ALTER TABLE {tname} ADD COLUMN owner_household_id INTEGER"))
            # Backfill: rows without any owner -> owned by creator (user_id)
            conn.execute(
                text(
                    f"UPDATE {tname} SET owner_user_id = user_id "
                    f"WHERE owner_user_id IS NULL AND owner_household_id IS NULL"
                )
            )

        if "recipes" in tables:
            cols = {c["name"] for c in inspector.get_columns("recipes")}
            if "image_filename" not in cols:
                conn.execute(text("ALTER TABLE recipes ADD COLUMN image_filename VARCHAR"))
            if "user_id" not in cols:
                conn.execute(text("ALTER TABLE recipes ADD COLUMN user_id INTEGER"))
            if "meal_types" not in cols:
                conn.execute(text("ALTER TABLE recipes ADD COLUMN meal_types JSON NOT NULL DEFAULT '[]'"))
            if "is_meal_prep" not in cols:
                conn.execute(text("ALTER TABLE recipes ADD COLUMN is_meal_prep BOOLEAN NOT NULL DEFAULT 0"))
            if "meal_prep_days" not in cols:
                conn.execute(text("ALTER TABLE recipes ADD COLUMN meal_prep_days INTEGER"))
            if "meal_prep_steps" not in cols:
                conn.execute(text("ALTER TABLE recipes ADD COLUMN meal_prep_steps JSON NOT NULL DEFAULT '[]'"))

        if "meal_plan_entries" in tables:
            cols = {c["name"] for c in inspector.get_columns("meal_plan_entries")}
            if "user_id" not in cols:
                conn.execute(text("ALTER TABLE meal_plan_entries ADD COLUMN user_id INTEGER"))

        if "shopping_items" in tables:
            cols = {c["name"] for c in inspector.get_columns("shopping_items")}
            if "user_id" not in cols:
                conn.execute(text("ALTER TABLE shopping_items ADD COLUMN user_id INTEGER"))
            if "is_custom" not in cols:
                conn.execute(text("ALTER TABLE shopping_items ADD COLUMN is_custom INTEGER NOT NULL DEFAULT 0"))

        if "api_keys" in tables:
            cols = {c["name"] for c in inspector.get_columns("api_keys")}
            if "scope" not in cols:
                conn.execute(text("ALTER TABLE api_keys ADD COLUMN scope VARCHAR NOT NULL DEFAULT 'write'"))

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


@app.exception_handler(ServiceError)
async def _service_error_handler(_request: Request, exc: ServiceError) -> JSONResponse:
    """Domain errors carry their own status code and a machine-readable `error`."""
    return JSONResponse(status_code=exc.status_code, content={"detail": str(exc), "error": exc.code})


app.include_router(auth.router)
app.include_router(admin_users.router)
app.include_router(admin_households.router)
app.include_router(recipes.router)
app.include_router(plan.router)
app.include_router(shopping.router)
app.include_router(settings_router.router)
app.include_router(agent.router)
app.include_router(templates_router.router)
app.include_router(mcp_sse.router)

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
