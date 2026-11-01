"""Shared FastAPI dependencies, auth guards, and cross-router helpers."""

from __future__ import annotations

import datetime as dt
from pathlib import Path

from fastapi import Depends, HTTPException, Request, UploadFile, status
from sqlalchemy import Select
from sqlalchemy.orm import Session

from .config import PROJECT_ROOT, settings
from .db import get_db_session
from .models import (
    Account,
    AuthSession,
    Category,
    Product,
    Store,
    Transaction,
    User,
)

__all__ = [
    "ATTACHMENTS_DIR",
    "apply_pagination",
    "get_current_user",
    "get_db_session",
]


ATTACHMENTS_DIR = Path(settings.attachments_dir)
if not ATTACHMENTS_DIR.is_absolute():
    ATTACHMENTS_DIR = (PROJECT_ROOT / ATTACHMENTS_DIR).resolve()
ATTACHMENTS_DIR.mkdir(parents=True, exist_ok=True)


MAX_UPLOAD_BYTES = settings.max_upload_mb * 1024 * 1024


def _upload_too_large(filename: str | None) -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_413_CONTENT_TOO_LARGE,
        detail=f"Plik {filename or ''} przekracza limit {settings.max_upload_mb} MB.",
    )


def read_upload_capped(upload: UploadFile, limit: int = MAX_UPLOAD_BYTES) -> bytes:
    """Read an upload into memory, refusing anything over ``limit`` bytes."""
    content = upload.file.read(limit + 1)
    if len(content) > limit:
        raise _upload_too_large(upload.filename)
    return content


def copy_upload_capped(upload: UploadFile, target: Path, limit: int = MAX_UPLOAD_BYTES) -> int:
    """Stream an upload to ``target``; on overflow remove the partial file and raise 413."""
    written = 0
    with target.open("wb") as buffer:
        while chunk := upload.file.read(1024 * 1024):
            written += len(chunk)
            if written > limit:
                break
            buffer.write(chunk)
    if written > limit:
        target.unlink(missing_ok=True)
        raise _upload_too_large(upload.filename)
    return written


def apply_pagination(stmt: Select, offset: int, limit: int) -> Select:
    return stmt.offset(offset).limit(limit)


def _get_bearer_token(request: Request) -> str | None:
    header = request.headers.get("authorization")
    if not header:
        return None
    scheme, _, value = header.partition(" ")
    if scheme.lower() != "bearer" or not value:
        return None
    return value.strip()


def _load_valid_session(db: Session, token: str) -> AuthSession | None:
    """Return the live session for ``token``; expired ones are deleted on sight."""
    session = db.get(AuthSession, token)
    if session and session.expires_at < dt.datetime.now(dt.UTC):
        db.delete(session)
        db.commit()
        return None
    return session


def _require_session(request: Request, db: Session) -> User:
    token = _get_bearer_token(request)
    if not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing session token."
        )

    session = _load_valid_session(db, token)
    if not session:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Session expired.")
    return session.user


def _get_current_user(request: Request, db: Session) -> User:
    user_id = getattr(request.state, "user_id", None)
    if not user_id:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Not authenticated.")
    user = db.get(User, user_id)
    if not user:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found.")
    return user


def _require_admin(user: User) -> None:
    if not user.is_admin:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Admin access required.")


def get_current_user(request: Request, db: Session = Depends(get_db_session)) -> User:
    return _get_current_user(request, db)


def _get_owned_or_404(db: Session, model, entity_id: str, user_id: str, label: str):
    entity = db.get(model, entity_id)
    if not entity or getattr(entity, "user_id", None) != user_id:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"{label} not found.")
    return entity


def _assign_existing_data_to_user(db: Session, user_id: str) -> None:
    db.query(Account).filter(Account.user_id.is_(None)).update({Account.user_id: user_id})
    db.query(Store).filter(Store.user_id.is_(None)).update({Store.user_id: user_id})
    db.query(Category).filter(Category.user_id.is_(None)).update({Category.user_id: user_id})
    db.query(Product).filter(Product.user_id.is_(None)).update({Product.user_id: user_id})
    db.query(Transaction).filter(Transaction.user_id.is_(None)).update(
        {Transaction.user_id: user_id}
    )
    db.commit()
