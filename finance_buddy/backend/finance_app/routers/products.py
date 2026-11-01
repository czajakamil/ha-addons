"""Product (item) endpoints."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session

from ..db import get_db_session
from ..dependencies import (
    _get_owned_or_404,
    apply_pagination,
    get_current_user,
)
from ..models import Category, Product, TransactionItem, User
from ..schemas import ItemCreate, ItemRead, ItemUpdate

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
