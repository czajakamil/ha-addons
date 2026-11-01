"""Store endpoints."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session

from ..db import get_db_session
from ..dependencies import (
    _get_owned_or_404,
    apply_pagination,
    get_current_user,
)
from ..models import Store, StoreAlias, Transaction, User
from ..schemas import (
    StoreAliasCreate,
    StoreAliasPreview,
    StoreAliasPreviewRequest,
    StoreAliasRead,
    StoreCreate,
    StoreMergePreview,
    StoreMergeRead,
    StoreMergeRequest,
    StoreMergeResult,
    StoreMergeUndoResult,
    StoreMonthlySpend,
    StoreRead,
    StoreStats,
    StoreUpdate,
)
from ..services import store_aliases as aliases_service
from ..services import store_merges as merges_service
from ..services import store_stats as store_stats_service

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
        .where(Store.user_id == current_user.id, Store.merged_into_id.is_(None))
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


@router.get("/stores/stats", response_model=list[StoreStats])
def get_store_stats(
    store_ids: Annotated[list[str] | None, Query()] = None,
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    return store_stats_service.store_stats(db, current_user.id, store_ids)


@router.get("/stores/stats/monthly", response_model=list[StoreMonthlySpend])
def get_store_monthly_spend(
    store_ids: Annotated[list[str], Query(min_length=1)],
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    return store_stats_service.store_monthly_spend(db, current_user.id, store_ids)


@router.post("/stores", response_model=StoreRead, status_code=status.HTTP_201_CREATED)
def create_store(
    payload: StoreCreate,
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    store = Store(**payload.model_dump(), user_id=current_user.id)
    db.add(store)
    aliases_service.register_store_name(db, store)
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
    if "name" in data:
        aliases_service.register_store_name(db, store)
    if "is_deleted" in data:
        names = aliases_service.exact_names_of(db, store)
        aliases_service.resolve_names(db, store.user_id, names)
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

    count = db.scalar(select(func.count(Transaction.id)).where(Transaction.store_id == store_id))
    if count:
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            f"Sklep ma transakcje ({count}), więc nie można go usunąć. Najpierw go scal.",
        )
    names = aliases_service.exact_names_of(db, store)
    db.delete(store)
    aliases_service.resolve_names(db, current_user.id, names)
    db.commit()
    return


@router.get("/stores/aliases", response_model=list[StoreAliasRead])
def list_store_aliases(
    store_ids: Annotated[list[str] | None, Query()] = None,
    ambiguous: bool = False,
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    return aliases_service.list_aliases(db, current_user.id, store_ids, ambiguous_only=ambiguous)


@router.post("/stores/aliases/preview", response_model=StoreAliasPreview)
def preview_store_alias(
    payload: StoreAliasPreviewRequest,
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    return aliases_service.preview_alias(db, current_user.id, payload.pattern, payload.store_ids)


@router.post(
    "/stores/{store_id}/aliases",
    response_model=StoreAliasRead,
    status_code=status.HTTP_201_CREATED,
)
def add_store_alias(
    store_id: str,
    payload: StoreAliasCreate,
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    store = _get_owned_or_404(db, Store, store_id, current_user.id, "Store")
    return aliases_service.add_alias(db, current_user.id, store, payload.pattern)


@router.delete("/stores/aliases/{alias_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_store_alias(
    alias_id: str,
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    alias = _get_owned_or_404(db, StoreAlias, alias_id, current_user.id, "Store alias")
    aliases_service.remove_alias(db, alias)


@router.post("/stores/merge", response_model=StoreMergeResult | StoreMergePreview)
def merge_stores(
    payload: StoreMergeRequest,
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    if payload.dry_run:
        return merges_service.preview_merge(
            db, current_user.id, payload.source_ids, payload.target_id
        )
    return merges_service.merge_stores(
        db,
        current_user.id,
        payload.source_ids,
        payload.target_id,
        name=payload.name,
        category_id=payload.category_id,
    )


@router.get("/stores/merges", response_model=list[StoreMergeRead])
def list_store_merges(
    store_ids: Annotated[list[str], Query(min_length=1)],
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    return merges_service.list_merges(db, current_user.id, store_ids)


@router.post("/stores/merges/{merge_id}/undo", response_model=StoreMergeUndoResult)
def undo_store_merge(
    merge_id: str,
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    return merges_service.undo_merge(db, current_user.id, merge_id)
