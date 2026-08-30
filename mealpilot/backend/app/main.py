import json
import logging
import os
import secrets
import sqlite3
from contextlib import asynccontextmanager, suppress
from datetime import datetime
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy import JSON, DateTime, inspect, text
from starlette.middleware.sessions import SessionMiddleware

from . import migrator, models
from .db import DB_PATH, Base, SessionLocal, engine
from .images import IMAGES_DIR
from .middleware import CloudflareAccessMiddleware
from .routers import admin_households, admin_users, agent, auth, mcp_http, mcp_sse, plan, recipes, shopping
from .routers import settings as settings_router
from .routers import templates as templates_router
from .security import hash_password, verify_password
from .seed import seed_for_user
from .services.errors import ServiceError

logger = logging.getLogger(__name__)

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


# --------------------------------------------------------------------------- #
# Recipe ids: text (slug / UUID) -> integer surrogate key
# --------------------------------------------------------------------------- #

# Rebuilt together, because every one of them keys on ``recipes.id``.
_RECIPE_ID_TABLES = (
    "recipes",
    "meal_plan_entries",
    "recipe_ratings",
    "recipe_notes",
    "shopping_item_recipes",
)


def _recipe_id_is_text(inspector) -> bool:
    """True while ``recipes.id`` still holds the old title-derived string ids."""
    for col in inspector.get_columns("recipes"):
        if col["name"] == "id":
            return not str(col["type"]).upper().startswith("INT")
    return False


def _dump_table(conn, table) -> list[dict]:
    """Every row of `table` as plain dicts, ready to be handed back to ``insert()``.

    Read with ``SELECT *`` rather than through the model, because the on-disk
    table may still be missing columns the model has gained — which also means
    the values arrive as raw SQLite scalars and have to be decoded by hand.
    """
    decoders = {}
    for col in table.columns:
        if isinstance(col.type, JSON):
            decoders[col.name] = json.loads
        elif isinstance(col.type, DateTime):
            decoders[col.name] = datetime.fromisoformat

    rows = []
    for row in conn.execute(text(f"SELECT * FROM {table.name} ORDER BY rowid")).mappings().all():
        d = dict(row)
        for name, decode in decoders.items():
            if isinstance(d.get(name), str):
                with suppress(json.JSONDecodeError, ValueError):
                    d[name] = decode(d[name])
        rows.append(d)
    return rows


def _insertable(table, row: dict, **overrides) -> dict:
    """Row narrowed to `table`'s columns; ``None`` is dropped so model defaults apply."""
    values = {c.name: row[c.name] for c in table.columns if row.get(c.name) is not None}
    values.update(overrides)
    return values


def _backup_db(engine_, suffix: str = "pre-int-ids") -> None:
    """Snapshot the database next to it as ``<db>.<suffix>.bak``.

    Used before the irreversible recipe-id rebuild and before any Alembic
    upgrade that actually has work to do.

    A plain ``shutil.copy2`` of the ``.db`` file stopped being a valid backup the
    moment db.py turned on WAL: freshly committed pages live in ``<db>-wal``
    until a checkpoint folds them back, and this very engine has already written
    to the database by the time we get here — so the copy could be missing the
    newest rows, which is exactly what a pre-migration backup must not do.

    SQLite's own online-backup API is used rather than a checkpoint-then-copy or
    a copy of the ``-wal``/``-shm`` sidecars: it reads through this engine's
    connection (so it sees the WAL), it is atomic against a concurrent writer
    instead of racing one, and it writes a single self-contained file with no
    sidecars that must be kept together to be restorable.
    """
    path = engine_.url.database
    if not path or path == ":memory:" or not Path(path).exists():
        return
    backup = Path(f"{path}.{suffix}.bak")
    try:
        with engine_.connect() as conn:
            dest = sqlite3.connect(backup)
            try:
                conn.connection.driver_connection.backup(dest)
            finally:
                dest.close()
    except (OSError, sqlite3.Error) as exc:
        # Never block the upgrade on a failed backup — but never lose it quietly.
        logger.error("Nie udało się zapisać kopii bazy sprzed migracji (%s): %s", suffix, exc)
        return
    logger.warning("Kopia bazy sprzed migracji: %s", backup)


def _migrate_recipe_ids(engine_, conn) -> None:
    """Retype ``recipes.id`` from text to an autoincrement integer.

    Ids used to be slugs derived from the title, so they went stale the moment a
    recipe was renamed — and a rename could not fix them without breaking every
    row pointing at them. SQLite cannot retype a primary key in place, so the
    keyed tables are read out, dropped, recreated from the models and written
    back under the new ids. It all runs in the caller's transaction: a failure
    rolls the whole thing back and leaves the old database untouched.
    """
    tables = [models.Base.metadata.tables[name] for name in _RECIPE_ID_TABLES]
    dumped = {t.name: _dump_table(conn, t) for t in tables}

    # A creator is mandatory now; pre-multi-user rows may predate the column.
    fallback_user = conn.execute(text("SELECT MIN(id) FROM users")).scalar()

    new_id: dict[str, int] = {}
    recipes: list[dict] = []
    for row in dumped["recipes"]:
        created_by = row.get("user_id") or fallback_user
        if created_by is None:
            continue  # no users at all: nothing could ever see this recipe
        new_id[row["id"]] = len(new_id) + 1
        owner_user = row.get("owner_user_id")
        owner_household = row.get("owner_household_id")
        if owner_user is None and owner_household is None:
            owner_user = created_by
        recipes.append(
            _insertable(
                models.Recipe.__table__,
                row,
                id=new_id[row["id"]],
                user_id=created_by,
                owner_user_id=owner_user,
                owner_household_id=owner_household,
            )
        )

    for table in reversed(tables):
        table.drop(conn)
    for table in tables:
        table.create(conn)

    if recipes:
        conn.execute(models.Recipe.__table__.insert(), recipes)

    dropped = 0
    for table in tables[1:]:
        rows = []
        for row in dumped[table.name]:
            mapped = new_id.get(row.get("recipe_id"))
            if mapped is None:
                dropped += 1
                continue
            rows.append(_insertable(table, row, recipe_id=mapped))
        if rows:
            conn.execute(table.insert(), rows)

    _remap_json_recipe_ids(conn, new_id)
    logger.warning(
        "Zmigrowano id przepisów na liczby: %d przepisów, pominięto %d osieroconych powiązań.",
        len(recipes),
        dropped,
    )


def _remap_json_recipe_ids(conn, new_id: dict[str, int]) -> None:
    """Recipe ids also live inside two JSON blobs, which no rebuild would touch."""
    for tpl_id, raw in conn.execute(text("SELECT id, entries FROM week_templates")).all():
        entries = json.loads(raw) if isinstance(raw, str) else (raw or [])
        kept = [{**e, "recipe_id": new_id[e["recipe_id"]]} for e in entries if e.get("recipe_id") in new_id]
        conn.execute(
            text("UPDATE week_templates SET entries = :entries WHERE id = :id"),
            {"entries": json.dumps(kept, ensure_ascii=False), "id": tpl_id},
        )

    for user_id, raw in conn.execute(text("SELECT user_id, ui_prefs FROM agent_settings")).all():
        prefs = json.loads(raw) if isinstance(raw, str) else (raw or {})
        if "favorite_recipe_ids" not in prefs:
            continue
        prefs["favorite_recipe_ids"] = [new_id[r] for r in prefs["favorite_recipe_ids"] if r in new_id]
        conn.execute(
            text("UPDATE agent_settings SET ui_prefs = :prefs WHERE user_id = :user_id"),
            {"prefs": json.dumps(prefs, ensure_ascii=False), "user_id": user_id},
        )


def _columns(conn, table: str) -> set[str]:
    """Column names of `table` as of *right now*, read inside `conn`'s transaction.

    Deliberately builds a fresh ``Inspector`` on every call. ``Inspector``
    memoises ``get_columns()`` in ``info_cache``, so a single long-lived one
    keeps handing back the pre-``ALTER TABLE`` picture — a trap that stayed
    harmless only because the two reads of ``recipes`` happened to check
    different columns. Do not fold this back into one shared instance.

    It is bound to `conn`, not to the engine: the ALTERs below are still
    uncommitted, and any other connection would read the old schema (or block).
    """
    return {c["name"] for c in inspect(conn).get_columns(table)}


def _migrate(engine_) -> None:
    """ZAMROŻONE. Jednorazowy pomost dla baz sprzed wdrożenia Alembica.

    NIE ROZSZERZAJ TEJ FUNKCJI. Każda nowa zmiana schematu to rewizja Alembica
    w ``app/migrations/versions/`` — instrukcja w nagłówku ``app/migrator.py``.

    Ta funkcja jest wołana wyłącznie z jednej gałęzi ``_run_migrations()``: gdy
    baza ma tabele, ale nie ma jeszcze ``alembic_version``. Jej jedynym zadaniem
    jest dowieźć taką bazę do stanu rewizji ``0001_baseline``, która zaraz potem
    zostaje ostemplowana. Bazy zaadoptowane raz nigdy tu nie wracają, więc
    dopisany tu ``ALTER TABLE`` nie wykonałby się u nikogo poza świeżo
    aktualizowanym add-onem — czyli byłby cichym błędem.
    """
    Base.metadata.create_all(bind=engine_)
    tables = set(inspect(engine_).get_table_names())

    with engine_.begin() as conn:
        if "users" in tables:
            cols = _columns(conn, "users")
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
            cols = _columns(conn, tname)
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
            cols = _columns(conn, "recipes")
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
            cols = _columns(conn, "meal_plan_entries")
            if "user_id" not in cols:
                conn.execute(text("ALTER TABLE meal_plan_entries ADD COLUMN user_id INTEGER"))

        if "shopping_items" in tables:
            cols = _columns(conn, "shopping_items")
            if "user_id" not in cols:
                conn.execute(text("ALTER TABLE shopping_items ADD COLUMN user_id INTEGER"))
            if "is_custom" not in cols:
                conn.execute(text("ALTER TABLE shopping_items ADD COLUMN is_custom INTEGER NOT NULL DEFAULT 0"))

        if "api_keys" in tables:
            cols = _columns(conn, "api_keys")
            if "scope" not in cols:
                conn.execute(text("ALTER TABLE api_keys ADD COLUMN scope VARCHAR NOT NULL DEFAULT 'write'"))

        if "agent_settings" in tables:
            cols = _columns(conn, "agent_settings")
            if "ui_prefs" not in cols:
                conn.execute(text("ALTER TABLE agent_settings ADD COLUMN ui_prefs JSON NOT NULL DEFAULT '{}'"))

    # Last, and in its own transaction: it rebuilds tables and depends on every
    # ADD COLUMN above having landed.
    if _recipe_id_is_text(inspect(engine_)):
        _backup_db(engine_)
        with engine_.begin() as conn:
            _migrate_recipe_ids(engine_, conn)


def _run_migrations(engine_) -> None:
    """Doprowadza bazę do najnowszej rewizji Alembica. Trzy stany wejściowe.

    1. **Pusta baza / brak pliku** — sam ``upgrade head``. Świadomie bez
       ``create_all()``: dzięki temu każda świeża instalacja przechodzi realnym
       łańcuchem migracji, więc rozjazd między rewizjami a modelami wychodzi od
       razu, a nie dopiero u kogoś, kto aktualizuje add-on.
    2. **Są tabele, nie ma ``alembic_version``** — czyli każda instalacja
       sprzed wdrożenia Alembica. Stary ``_migrate()`` dowozi schemat do stanu
       bazowego, ``stamp`` zapisuje rewizję bazową, a ``upgrade head`` dokłada
       wszystko, co powstało po niej.
    3. **Jest ``alembic_version``** — sam ``upgrade head``; ``_migrate()`` nie
       jest wtedy w ogóle wołany.

    Kopia bazy jest robiona tylko wtedy, gdy ``upgrade head`` ma faktycznie coś
    do zrobienia — w add-onie nikt nie robi backupów sam, a migracja w trybie
    batch przebudowuje całe tabele.
    """
    tables = set(inspect(engine_).get_table_names())

    if not tables:
        logger.info("Pusta baza — budowanie schematu przez migracje Alembica.")
        migrator.upgrade_head(engine_)
        return

    if migrator.VERSION_TABLE not in tables:
        logger.warning("Baza sprzed Alembica — jednorazowa adopcja przez _migrate() + stamp.")
        _migrate(engine_)
        migrator.stamp(engine_, migrator.base_revision())

    if migrator.has_pending(engine_):
        _backup_db(engine_, "pre-upgrade")
        migrator.upgrade_head(engine_)


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
    _run_migrations(engine)
    _provision_admin()
    # The Streamable HTTP manager dispatches requests inside its own task group,
    # which only exists for the duration of this context.
    async with mcp_http.session_manager_lifespan():
        yield


def _install_middleware(app_: FastAPI) -> None:
    """Wrap `app_` in the middleware stack. Order is load-bearing.

    ``add_middleware`` prepends, so the *last* one added ends up outermost. The
    resulting stack is CORS -> Cloudflare Access -> Session -> routes.

    CORS has to sit outside Cloudflare Access. A preflight ``OPTIONS`` is sent
    by the browser without any of our headers — ``cf-access-jwt-assertion``
    included — so with CF Access outermost every preflight was answered 401 and
    the real request never happened; error responses likewise came back without
    CORS headers, so the browser showed a CORS failure instead of the actual
    status. Letting CORSMiddleware answer preflights ahead of CF Access gives
    nothing away: a preflight carries no credentials and the reply is only our
    own CORS policy. Every request that can read or change data still passes
    through CloudflareAccessMiddleware.
    """
    origins = os.environ.get("MEALPILOT_CORS_ORIGINS", "*").split(",")
    app_.add_middleware(
        SessionMiddleware,
        secret_key=_load_session_secret(),
        session_cookie="mealpilot_session",
        same_site="lax",
        https_only=os.environ.get("MEALPILOT_COOKIE_SECURE", "0") == "1",
        max_age=60 * 60 * 24 * 30,
    )
    app_.add_middleware(CloudflareAccessMiddleware)
    app_.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_credentials="*" not in origins,
        allow_methods=["*"],
        allow_headers=["*"],
    )


app = FastAPI(title="MealPilot API", version="0.2.0", lifespan=lifespan)

_install_middleware(app)


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
app.include_router(mcp_http.router)

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
