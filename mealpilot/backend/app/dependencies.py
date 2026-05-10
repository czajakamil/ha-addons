import hashlib
from datetime import datetime, timezone

from fastapi import Depends, Header, HTTPException, Request, status
from sqlalchemy.orm import Session

from . import models
from .db import get_db


API_KEY_HEADER = "X-MealPilot-Token"


def _hash_api_key(key: str) -> str:
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


def get_current_user(
    request: Request,
    db: Session = Depends(get_db),
    x_mealpilot_token: str | None = Header(default=None, alias=API_KEY_HEADER),
) -> models.User:
    if x_mealpilot_token:
        digest = _hash_api_key(x_mealpilot_token.strip())
        api_key = (
            db.query(models.ApiKey).filter(models.ApiKey.key_hash == digest).one_or_none()
        )
        if not api_key:
            raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid API key")
        user = db.get(models.User, api_key.user_id)
        if not user or not user.is_active:
            raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Not authenticated")
        api_key.last_used_at = datetime.now(timezone.utc)
        db.commit()
        return user

    user_id = request.session.get("user_id")
    if not user_id:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Not authenticated")
    user = db.get(models.User, user_id)
    if not user or not user.is_active:
        request.session.pop("user_id", None)
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Not authenticated")
    return user


def get_current_admin(user: models.User = Depends(get_current_user)) -> models.User:
    if user.role != "admin":
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Admin only")
    return user
