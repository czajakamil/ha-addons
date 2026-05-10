import secrets
from typing import List

from fastapi import APIRouter, Depends, HTTPException, Request, status
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from sqlalchemy import update

from .. import models, schemas
from ..db import get_db
from ..dependencies import _hash_api_key, get_current_user
from ..security import hash_password, verify_password
from ..seed import seed_for_user

API_KEY_PREFIX = "mp_"

router = APIRouter(prefix="/api/auth", tags=["auth"])


def _setup_required(db: Session) -> bool:
    return db.query(models.User).count() == 0


@router.get("/setup-required", response_model=schemas.SetupStatus)
def setup_required(db: Session = Depends(get_db)):
    return schemas.SetupStatus(setup_required=_setup_required(db))


@router.post("/setup", response_model=schemas.UserOut, status_code=status.HTTP_201_CREATED)
def setup(payload: schemas.SetupRequest, request: Request, db: Session = Depends(get_db)):
    if not _setup_required(db):
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Setup already completed")
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

    # Adopt any pre-existing rows from the single-user era and seed demo data
    # if the database is empty.
    db.execute(
        update(models.Recipe)
        .where(models.Recipe.user_id.is_(None))
        .values(user_id=user.id)
    )
    db.execute(
        update(models.MealPlanEntry)
        .where(models.MealPlanEntry.user_id.is_(None))
        .values(user_id=user.id)
    )
    db.execute(
        update(models.ShoppingItem)
        .where(models.ShoppingItem.user_id.is_(None))
        .values(user_id=user.id)
    )
    db.commit()
    seed_for_user(db, user.id)

    request.session["user_id"] = user.id
    return user


@router.post("/login", response_model=schemas.UserOut)
def login(payload: schemas.LoginRequest, request: Request, db: Session = Depends(get_db)):
    user = (
        db.query(models.User)
        .filter(models.User.username == payload.username.strip())
        .one_or_none()
    )
    if not user or not user.is_active or not verify_password(payload.password, user.password_hash):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid credentials")
    request.session.clear()
    request.session["user_id"] = user.id
    return user


@router.post("/logout", status_code=status.HTTP_204_NO_CONTENT)
def logout(request: Request):
    request.session.clear()
    return None


@router.get("/me", response_model=schemas.UserOut)
def me(user: models.User = Depends(get_current_user)):
    return user


@router.post("/change-password", status_code=status.HTTP_204_NO_CONTENT)
def change_password(
    payload: schemas.ChangePasswordRequest,
    user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    if not verify_password(payload.old_password, user.password_hash):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Old password is incorrect")
    user.password_hash = hash_password(payload.new_password)
    db.commit()
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
