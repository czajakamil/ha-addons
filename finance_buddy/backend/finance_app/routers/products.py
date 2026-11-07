"""Product (item) endpoints."""

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
from ..models import Category, Product, ProductAlias, Store, TransactionItem, User
from ..schemas import (
    ItemAliasRead,
    ItemCreate,
    ItemMergeRequest,
    ItemMergeResult,
    ItemMonthlySpend,
    ItemRead,
    ItemStats,
    ItemUpdate,
    ItemUsageRead,
)
from ..services import product_merges, product_stats

router = APIRouter()


@router.get("/items", response_model=list[ItemRead])
def list_items(
    search: str | None = None,
    offset: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=500),
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    stmt = (
        select(Product)
        .where(Product.user_id == current_user.id)
        .order_by(
            func.lower(func.coalesce(Product.group_name, Product.name)),
            Product.name,
        )
    )
    if search:
        pattern = f"%{search}%"
        stmt = stmt.where(
            or_(
                Product.name.ilike(pattern),
                Product.group_name.ilike(pattern),
                Product.manufacturer.ilike(pattern),
            )
        )
    stmt = apply_pagination(stmt, offset, limit)
    return db.scalars(stmt).all()


@router.get("/items/usage", response_model=list[ItemUsageRead])
def list_item_usage(
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """Ile pozycji transakcji i aliasów ma każdy produkt — do listy i wyboru celu scalenia."""
    item_counts = dict(
        db.execute(
            select(TransactionItem.product_id, func.count())
            .join(Product, Product.id == TransactionItem.product_id)
            .where(Product.user_id == current_user.id)
            .group_by(TransactionItem.product_id)
        ).all()
    )
    alias_counts = dict(
        db.execute(
            select(ProductAlias.product_id, func.count())
            .join(Product, Product.id == ProductAlias.product_id)
            .where(Product.user_id == current_user.id)
            .group_by(ProductAlias.product_id)
        ).all()
    )
    ids = db.scalars(select(Product.id).where(Product.user_id == current_user.id))
    return [
        ItemUsageRead(
            item_id=item_id,
            transaction_items=item_counts.get(item_id, 0),
            aliases=alias_counts.get(item_id, 0),
        )
        for item_id in ids
    ]


@router.get("/items/stats", response_model=list[ItemStats])
def get_item_stats(
    item_ids: Annotated[list[str] | None, Query()] = None,
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """Zakupy przedmiotów; przedmiot nigdy niekupiony nie ma wiersza."""
    return product_stats.item_stats(db, current_user.id, item_ids)


@router.get("/items/stats/monthly", response_model=list[ItemMonthlySpend])
def get_item_monthly_spend(
    item_ids: Annotated[list[str], Query(min_length=1)],
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    return product_stats.item_monthly_spend(db, current_user.id, item_ids)


@router.post("/items/merge", response_model=ItemMergeResult)
def merge_items(
    payload: ItemMergeRequest,
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """Scala produkty w jeden: pozycje i aliasy przechodzą do celu, źródła znikają."""
    outcome = product_merges.merge_products(
        db, current_user.id, payload.source_ids, payload.target_id, preview=payload.dry_run
    )
    if not payload.dry_run:
        db.commit()
    return ItemMergeResult(**vars(outcome), dry_run=payload.dry_run)


@router.get("/items/{item_id}/aliases", response_model=list[ItemAliasRead])
def list_item_aliases(
    item_id: str,
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """Nazwy z paragonów, które rozpoznajemy jako ten produkt."""
    _get_owned_or_404(db, Product, item_id, current_user.id, "Item")
    rows = db.execute(
        select(ProductAlias, Store.name)
        .outerjoin(Store, Store.id == ProductAlias.store_id)
        .where(ProductAlias.product_id == item_id)
        .order_by(Store.name.is_not(None), Store.name, ProductAlias.alias)
    ).all()
    return [
        ItemAliasRead(id=alias.id, alias=alias.alias, store_id=alias.store_id, store_name=name)
        for alias, name in rows
    ]


@router.delete("/items/{item_id}/aliases/{alias_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_item_alias(
    item_id: str,
    alias_id: str,
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """Odpina nazwę z paragonu od produktu. Zapisane pozycje transakcji się nie zmieniają."""
    _get_owned_or_404(db, Product, item_id, current_user.id, "Item")
    alias = db.get(ProductAlias, alias_id)
    if alias is None or alias.product_id != item_id:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Alias not found.")
    db.delete(alias)
    db.commit()


@router.post("/items", response_model=ItemRead, status_code=status.HTTP_201_CREATED)
def create_item(
    payload: ItemCreate,
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    if payload.category_id:
        _get_owned_or_404(db, Category, payload.category_id, current_user.id, "Category")
    item = Product(**payload.model_dump(), user_id=current_user.id)
    db.add(item)
    db.commit()
    db.refresh(item)
    return item


@router.put("/items/{item_id}", response_model=ItemRead)
@router.patch("/items/{item_id}", response_model=ItemRead)
def update_item(
    item_id: str,
    payload: ItemUpdate,
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    item = _get_owned_or_404(db, Product, item_id, current_user.id, "Item")
    if payload.category_id:
        _get_owned_or_404(db, Category, payload.category_id, current_user.id, "Category")

    data = payload.model_dump(exclude_unset=True)
    for field, value in data.items():
        setattr(item, field, value)

    db.add(item)
    db.commit()
    db.refresh(item)
    return item


@router.delete("/items/{item_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_item(
    item_id: str,
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """Usuwa przedmiot ze słownika (aliasy paragonów kasują się kaskadowo).

    Blokujemy usunięcie, gdy przedmiot występuje w pozycjach transakcji — klucz
    obcy ``TransactionItem.product_id`` ma ``ondelete=RESTRICT``, więc zamiast
    dopuścić do błędu bazy zwracamy czytelny komunikat 409.
    """
    item = _get_owned_or_404(db, Product, item_id, current_user.id, "Item")

    used = db.scalar(
        select(func.count())
        .select_from(TransactionItem)
        .where(TransactionItem.product_id == item_id)
    )
    if used:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=(f"Nie można usunąć przedmiotu — występuje w pozycjach transakcji ({used})."),
        )

    db.delete(item)
    db.commit()
