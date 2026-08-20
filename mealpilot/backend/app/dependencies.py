import hashlib
from datetime import UTC, datetime, timedelta

from fastapi import Depends, Header, HTTPException, Request, status
from sqlalchemy.orm import Session

from . import models
from .db import get_db

API_KEY_HEADER = "X-MealPilot-Token"
_LAST_USED_THROTTLE = timedelta(seconds=60)
_AUTH_ERROR = "Not authenticated"


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
