import hmac
import os
import secrets
from typing import List

from fastapi import APIRouter, Depends, HTTPException, Request, status
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from sqlalchemy import update

from .. import models, schemas
from ..db import get_db
from ..dependencies import _hash_api_key, get_current_user
from ..ratelimit import login_limiter, setup_limiter
from ..security import (
    dummy_verify,
    hash_password,
    password_needs_rehash,
    verify_password,
)
from ..seed import seed_for_user

API_KEY_PREFIX = "mp_"

router = APIRouter(prefix="/api/auth", tags=["auth"])


def _setup_required(db: Session) -> bool:
    return db.query(models.User).count() == 0


def _client_ip(request: Request) -> str:
    # Cloudflare sets CF-Connecting-IP. Fall back to request.client.
    cf = request.headers.get("cf-connecting-ip")
    if cf:
        return cf.strip()
    fwd = request.headers.get("x-forwarded-for")
    if fwd:
        return fwd.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


def _bump_session(request: Request, user: models.User) -> None:
    request.session.clear()
    request.session["user_id"] = user.id
    request.session["session_version"] = user.session_version


@router.get("/setup-required", response_model=schemas.SetupStatus)
def setup_required(db: Session = Depends(get_db)):
    return schemas.SetupStatus(setup_required=_setup_required(db))


@router.post("/setup", response_model=schemas.UserOut, status_code=status.HTTP_201_CREATED)
def setup(payload: schemas.SetupRequest, request: Request, db: Session = Depends(get_db)):
    rate_key = _client_ip(request)
    allowed, retry_after = setup_limiter.check(rate_key)
    if not allowed:
        raise HTTPException(
            status.HTTP_429_TOO_MANY_REQUESTS,
            "Too many setup attempts. Try again later.",
            headers={"Retry-After": str(int(retry_after))},
        )

    if not _setup_required(db):
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Setup already completed")

    # If the operator set MEALPILOT_SETUP_TOKEN, require it in the body to
    # prevent a race condition on a fresh deploy (attacker vs. owner).
    expected_token = os.environ.get("MEALPILOT_SETUP_TOKEN", "")
    if expected_token:
        provided = payload.setup_token or ""
        if not hmac.compare_digest(expected_token, provided):
            raise HTTPException(status.HTTP_403_FORBIDDEN, "Invalid setup token")

    user = models.User(
        username=payload.username.strip(),
        password_hash=hash_password(payload.password),
        role="admin",
        is_active=1,
    )
    db.add(user)
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        raise HTTPException(status.HTTP_409_CONFLICT, "User already exists")
    db.refresh(user)

    # Claim rows from the single-user era (user_id IS NULL) and seed demo data
    # if the DB is empty. The setup token above guards against the race condition.
    db.execute(
        update(models.Recipe)
        .where(models.Recipe.created_by.is_(None))
        .values(created_by=user.id, owner_user_id=user.id)
    )
    db.execute(
        update(models.MealPlanEntry)
        .where(models.MealPlanEntry.created_by.is_(None))
        .values(created_by=user.id, owner_user_id=user.id)
    )
    db.execute(
        update(models.ShoppingItem)
        .where(models.ShoppingItem.created_by.is_(None))
        .values(created_by=user.id, owner_user_id=user.id)
    )
    db.commit()
    seed_for_user(db, user.id)

    _bump_session(request, user)
    return user


@router.post("/login", response_model=schemas.UserOut)
def login(payload: schemas.LoginRequest, request: Request, db: Session = Depends(get_db)):
    username = payload.username.strip()
    rate_key = f"{_client_ip(request)}::{username.lower()}"
    allowed, retry_after = login_limiter.check(rate_key)
    if not allowed:
        raise HTTPException(
            status.HTTP_429_TOO_MANY_REQUESTS,
            "Too many login attempts. Try again later.",
            headers={"Retry-After": str(int(retry_after))},
        )

    user = (
        db.query(models.User)
        .filter(models.User.username == username)
        .one_or_none()
    )
    # Always run Argon2 (against real or dummy hash) to equalize response time.
    if not user or not user.is_active:
        dummy_verify(payload.password)
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid credentials")
    if not verify_password(payload.password, user.password_hash):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid credentials")

    # Rehash on login if the Argon2 parameters have changed.
    if password_needs_rehash(user.password_hash):
        user.password_hash = hash_password(payload.password)
        db.commit()

    login_limiter.reset(rate_key)
    _bump_session(request, user)
    return user


@router.post("/logout", status_code=status.HTTP_204_NO_CONTENT)
def logout(request: Request):
    request.session.clear()
    return None


@router.get("/me", response_model=schemas.UserOut)
def me(user: models.User = Depends(get_current_user)):
    return user


_PROVISIONED_ADMIN_USERNAME = os.environ.get("MEALPILOT_ADMIN_USERNAME", "").strip()


@router.post("/change-password", status_code=status.HTTP_204_NO_CONTENT)
def change_password(
    payload: schemas.ChangePasswordRequest,
    request: Request,
    user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    if _PROVISIONED_ADMIN_USERNAME and user.username == _PROVISIONED_ADMIN_USERNAME:
        raise HTTPException(
            status.HTTP_403_FORBIDDEN,
            "Admin password is managed via Home Assistant add-on options.",
        )
    if not verify_password(payload.old_password, user.password_hash):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Old password is incorrect")
    user.password_hash = hash_password(payload.new_password)
    # Bumping session_version invalidates all existing sessions.
    user.session_version = (user.session_version or 0) + 1
    # Also revoke all API keys — after a password rotation we assume old tokens
    # may have leaked.
    db.query(models.ApiKey).filter(models.ApiKey.user_id == user.id).delete()
    db.commit()
    # Keep the current session alive for the caller.
    _bump_session(request, user)
    return None


@router.get("/api-keys", response_model=List[schemas.ApiKeyOut])
def list_api_keys(
    user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    return (
        db.query(models.ApiKey)
        .filter(models.ApiKey.user_id == user.id)
        .order_by(models.ApiKey.created_at.desc())
        .all()
    )


@router.post(
    "/api-keys",
    response_model=schemas.ApiKeyCreatedOut,
    status_code=status.HTTP_201_CREATED,
)
def create_api_key(
    payload: schemas.ApiKeyCreateRequest,
    user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    raw = API_KEY_PREFIX + secrets.token_urlsafe(32)
    key = models.ApiKey(
        user_id=user.id,
        name=payload.name.strip(),
        prefix=raw[:12],
        key_hash=_hash_api_key(raw),
    )
    db.add(key)
    db.commit()
    db.refresh(key)
    return schemas.ApiKeyCreatedOut(
        id=key.id,
        name=key.name,
        prefix=key.prefix,
        created_at=key.created_at,
        last_used_at=key.last_used_at,
        key=raw,
    )


@router.delete("/api-keys/{key_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_api_key(
    key_id: int,
    user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    row = (
        db.query(models.ApiKey)
        .filter(models.ApiKey.id == key_id, models.ApiKey.user_id == user.id)
        .one_or_none()
    )
    if not row:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "API key not found")
    db.delete(row)
    db.commit()
    return None
