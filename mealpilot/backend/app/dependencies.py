import hashlib
from datetime import UTC, datetime, timedelta

from fastapi import Depends, Header, HTTPException, Request, status
from sqlalchemy.orm import Session

from . import models
from .db import get_db

API_KEY_HEADER = "X-MealPilot-Token"
SAFE_METHODS = frozenset({"GET", "HEAD", "OPTIONS"})
_LAST_USED_THROTTLE = timedelta(seconds=60)
_AUTH_ERROR = "Not authenticated"

# An API key is a *data* credential handed to an external client (Claude
# Desktop, a script). It must never be able to escalate itself into full account
# control, so it only reaches the domain surface — never account management
# (/api/auth/*: minting more keys, changing the password) and never
# administration (/api/admin/*). Anything not listed here is denied by default,
# so a new router does not silently become key-reachable.
API_KEY_PATH_PREFIXES = (
    "/api/recipes",
    "/api/plan",
    "/api/shopping",
    "/api/templates",
    "/api/settings",
    "/mcp",
)
# /api/auth/me is the one exception: it is a read-only identity echo, used by
# clients as a "is this key still valid, and who am I?" health check. It exposes
# nothing an API-key holder does not already have, and grants no control.
API_KEY_EXACT_PATHS = frozenset({"/api/auth/me"})


def _api_key_path_allowed(path: str) -> bool:
    if path in API_KEY_EXACT_PATHS:
        return True
    return any(path == p or path.startswith(p + "/") for p in API_KEY_PATH_PREFIXES)


def _hash_api_key(key: str) -> str:
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


def _unauthorized() -> HTTPException:
    return HTTPException(status.HTTP_401_UNAUTHORIZED, _AUTH_ERROR)


def get_current_user(
    request: Request,
    db: Session = Depends(get_db),
    x_mealpilot_token: str | None = Header(default=None, alias=API_KEY_HEADER),
) -> models.User:
    token = x_mealpilot_token.strip() if x_mealpilot_token else ""
    if token:
        digest = _hash_api_key(token)
        api_key = db.query(models.ApiKey).filter(models.ApiKey.key_hash == digest).one_or_none()
        if not api_key:
            raise _unauthorized()
        user = db.get(models.User, api_key.user_id)
        if not user or not user.is_active:
            raise _unauthorized()
        now = datetime.now(UTC)
        last_used = api_key.last_used_at
        if last_used is not None and last_used.tzinfo is None:
            # SQLite zwraca naive datetime — traktuj jako UTC, żeby móc odjąć od aware `now`.
            last_used = last_used.replace(tzinfo=UTC)
        if last_used is None or (now - last_used) > _LAST_USED_THROTTLE:
            api_key.last_used_at = now
            db.commit()
        if (api_key.scope or "write") == "read" and request.method not in SAFE_METHODS:
            raise HTTPException(
                status.HTTP_403_FORBIDDEN,
                "Ten klucz API ma zakres tylko-do-odczytu.",
            )
        root_path = request.scope.get("root_path") or ""
        path = request.url.path
        if root_path and path.startswith(root_path):
            path = path[len(root_path) :] or "/"
        if not _api_key_path_allowed(path):
            raise HTTPException(
                status.HTTP_403_FORBIDDEN,
                "Klucz API nie ma dostępu do tej ścieżki — zarządzanie kontem "
                "i panel administratora wymagają zalogowanej sesji.",
            )
        return user

    user_id = request.session.get("user_id")
    if not user_id:
        raise _unauthorized()
    user = db.get(models.User, user_id)
    if not user or not user.is_active:
        request.session.pop("user_id", None)
        raise _unauthorized()
    if request.session.get("session_version") != user.session_version:
        request.session.clear()
        raise _unauthorized()
    return user


def get_current_admin(user: models.User = Depends(get_current_user)) -> models.User:
    if user.role != "admin":
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Admin only")
    return user
