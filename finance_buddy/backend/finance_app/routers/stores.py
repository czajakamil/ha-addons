"""Store endpoints."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Query, status
from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session

from ..db import get_db_session
from ..dependencies import (
    _get_owned_or_404,
    apply_pagination,
    get_current_user,
)
from ..models import Store, User
from ..schemas import StoreCreate, StoreRead, StoreUpdate

router = APIRouter()


@router.get("/stores", response_model=list[StoreRead])
def list_stores(
    search: str | None = None,
    offset: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    stmt = (
        select(Store)
        .where(Store.user_id == current_user.id)
        .order_by(
            Store.is_deleted.asc(),
            func.lower(func.coalesce(Store.group_name, Store.name)),
            Store.name,
        )
    )
    if search:
        pattern = f"%{search}%"
        stmt = stmt.where(
            or_(
                Store.name.ilike(pattern),
                Store.group_name.ilike(pattern),
            )
        )
    stmt = apply_pagination(stmt, offset, limit)
    return db.scalars(stmt).all()


@router.post("/stores", response_model=StoreRead, status_code=status.HTTP_201_CREATED)
def create_store(
    payload: StoreCreate,
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    store = Store(**payload.model_dump(), user_id=current_user.id)
    db.add(store)
    db.commit()
    db.refresh(store)
    return store


@router.put("/stores/{store_id}", response_model=StoreRead)
@router.patch("/stores/{store_id}", response_model=StoreRead)
def update_store(
    store_id: str,
    payload: StoreUpdate,
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    store = _get_owned_or_404(db, Store, store_id, current_user.id, "Store")

    data = payload.model_dump(exclude_unset=True)
    for field, value in data.items():
        setattr(store, field, value)

    db.add(store)
    db.commit()
    db.refresh(store)
    return store


@router.delete("/stores/{store_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_store(
    store_id: str,
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    store = _get_owned_or_404(db, Store, store_id, current_user.id, "Store")

    db.delete(store)
    db.commit()
    return
