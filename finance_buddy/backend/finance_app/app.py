"""FastAPI application exposing the finance backend."""

from __future__ import annotations

import asyncio
import contextlib
import datetime as dt
import logging
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, status
from fastapi.concurrency import run_in_threadpool
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from sqlalchemy import select

from .config import PROJECT_ROOT, settings
from .db import SessionLocal, init_db
from .dependencies import (
    ATTACHMENTS_DIR,
    _assign_existing_data_to_user,
    _get_bearer_token,
    _load_valid_session,
)
from .helpers import _split_csv
from .logging_config import setup_logging
from .models import User
from .routers import (
    accounts,
    auth,
    bank_import,
    budget,
    categories,
    demo,
    home,
    product_matching,
    products,
    receipts,
    reports,
    stores,
    system,
    transactions,
    ynab,
)
from .services import backups

setup_logging(
    log_level=settings.log_level,
    log_file=settings.log_file,
    project_root=PROJECT_ROOT,
    log_to_console=settings.log_to_console,
)
log = logging.getLogger("finance_app.api")


def log_startup():
    log.info(
        "Finance API started (db=%s, attachments=%s, log_level=%s)",
        settings.db_path,
        ATTACHMENTS_DIR,
        settings.log_level.upper(),
    )
    with SessionLocal() as db:
        users = db.scalars(select(User).order_by(User.created_at.asc())).all()
        if len(users) == 1:
            if not users[0].is_admin:
                log.info("Promoting existing user %s to admin.", users[0].username)
                users[0].is_admin = True
            _assign_existing_data_to_user(db, users[0].id)
        elif users and not any(user.is_admin for user in users):
            log.warning("No admin user found; promote an admin before inviting new users.")


@asynccontextmanager
async def lifespan(_: FastAPI):
    # Bring the schema up to date (Alembic) before touching the DB. Done here,
    # not at import time, so that importing this package has no side effects
    # (importing `backend.finance_app.*` must not create tables — otherwise
    # Alembic's own env.py would find a populated DB and its migrations fail).
    init_db()
    log_startup()
    scheduler = _start_backup_scheduler()
    try:
        yield
    finally:
        if scheduler is not None:
            scheduler.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await scheduler


def _start_backup_scheduler() -> asyncio.Task[None] | None:
    if not settings.backup_enabled or settings.db_path == ":memory:":
        log.info("Automatic backups disabled.")
        return None
    config = backups.BackupConfig.from_settings()
    interval = dt.timedelta(hours=settings.backup_interval_hours)
    log.info(
        "Automatic backups every %g h into %s", settings.backup_interval_hours, config.backup_dir
    )
    return asyncio.create_task(backups.run_scheduler(config, interval), name="backup-scheduler")


app = FastAPI(title="Finance API", lifespan=lifespan)
allowed_origins = _split_csv(settings.allowed_origins) or ["http://localhost:5173"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def log_requests(request: Request, call_next):
    start = time.perf_counter()
    try:
        response = await call_next(request)
    except Exception:
        duration_ms = (time.perf_counter() - start) * 1000
        log.exception(
            "Unhandled error for %s %s (%.1f ms)",
            request.method,
            request.url.path,
            duration_ms,
        )
        raise

    duration_ms = (time.perf_counter() - start) * 1000
    log.info(
        "%s %s -> %s (%.1f ms)",
        request.method,
        request.url.path,
        response.status_code,
        duration_ms,
    )
    return response


@app.middleware("http")
async def require_authentication(request: Request, call_next):
    if request.method == "OPTIONS":
        return await call_next(request)

    path = request.url.path
    if path.startswith("/auth/") or path in {"/health", "/docs", "/openapi.json", "/redoc"}:
        return await call_next(request)

    token = _get_bearer_token(request)
    if not token:
        return JSONResponse(
            status_code=status.HTTP_401_UNAUTHORIZED, content={"detail": "Not authenticated."}
        )

    # The lookup is blocking SQLite I/O; run it off the event loop.
    user_id = await run_in_threadpool(_session_user_id, token)
    if user_id is None:
        return JSONResponse(
            status_code=status.HTTP_401_UNAUTHORIZED,
            content={"detail": "Session expired."},
        )
    request.state.user_id = user_id

    return await call_next(request)


def _session_user_id(token: str) -> str | None:
    with SessionLocal() as db:
        session = _load_valid_session(db, token)
        return session.user_id if session else None


app.include_router(auth.router)
app.include_router(accounts.router)
app.include_router(stores.router)
app.include_router(products.router)
app.include_router(product_matching.router)
app.include_router(categories.router)
app.include_router(budget.router)
app.include_router(home.router)
app.include_router(transactions.router)
app.include_router(receipts.router)
app.include_router(bank_import.router)
app.include_router(reports.router)
app.include_router(ynab.router)
app.include_router(system.router)
app.include_router(demo.router)


def create_app() -> FastAPI:
    """Provide an application factory for ASGI servers/tests."""
    return app


__all__ = ["app", "create_app"]
