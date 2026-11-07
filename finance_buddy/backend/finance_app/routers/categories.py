"""Category endpoints.

Kategorie zachowują się jak zagnieżdżone tagi (docs/design/zagniezdzone-tagi.md):
każda ma wyliczaną ścieżkę, tworzenie po ścieżce dokłada brakujących przodków,
a usunięcie zabiera całe pod-drzewo (FK CASCADE po parent_id).
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from ..db import get_db_session
from ..dependencies import (
    _get_owned_or_404,
    apply_pagination,
    get_current_user,
)
from ..models import Category, Product, Transaction, User
from ..schemas import CategoryCreate, CategoryDeletePreview, CategoryRead, CategoryUpdate
from ..services.categories import (
    build_path_map,
    descendant_ids,
    ensure_path,
    find_child,
    is_descendant_or_self,
    sanitize_segment,
)

router = APIRouter()


def _to_read(category: Category, paths: dict[str, str]) -> CategoryRead:
    return CategoryRead(
        id=category.id,
        name=category.name,
        parent_id=category.parent_id,
        is_for_accounts=category.is_for_accounts,
        color=category.color,
        path=paths.get(category.id, category.name),
    )


@router.get("/categories", response_model=list[CategoryRead])
def list_categories(
    parent_id: str | None = None,
    for_accounts: bool | None = Query(None),
    path_prefix: str | None = Query(
        None, description="Zwraca kategorię o tej ścieżce i całe jej pod-drzewo."
    ),
    offset: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=500),
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    stmt = select(Category).where(Category.user_id == current_user.id).order_by(Category.name)
    if parent_id is not None:
        stmt = stmt.where(Category.parent_id == parent_id)
    if for_accounts is not None:
        stmt = stmt.where(Category.is_for_accounts == for_accounts)
    stmt = apply_pagination(stmt, offset, limit)
    categories = db.scalars(stmt).all()

    paths = build_path_map(db, current_user.id)
    result = [_to_read(category, paths) for category in categories]
    if path_prefix:
        prefix = path_prefix.strip().lower().rstrip("/")
        result = [
            item
            for item in result
            if item.path.lower() == prefix or item.path.lower().startswith(f"{prefix}/")
        ]
    return result


@router.post("/categories", response_model=CategoryRead, status_code=status.HTTP_201_CREATED)
def create_category(
    payload: CategoryCreate,
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    if payload.path:
        # Tworzenie po ścieżce: "dom/remont" dokłada brakujących przodków.
        try:
            category = ensure_path(
                db, current_user.id, payload.path, is_for_accounts=payload.is_for_accounts
            )
        except ValueError as exc:
            raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc)) from exc
        # Kolor ustawiamy tylko na liściu ścieżki — przodkowie zostają na dziedziczeniu.
        if payload.color is not None:
            category.color = payload.color
    else:
        if not payload.name or not payload.name.strip():
            raise HTTPException(
                status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Podaj name albo path."
            )
        if payload.parent_id:
            _get_owned_or_404(db, Category, payload.parent_id, current_user.id, "Category")
        existing = find_child(db, current_user.id, payload.parent_id, payload.name)
        if existing:
            raise HTTPException(
                status.HTTP_409_CONFLICT,
                detail="Kategoria o tej nazwie już istnieje na tym poziomie.",
            )
        category = Category(
            name=sanitize_segment(payload.name),
            parent_id=payload.parent_id,
            is_for_accounts=payload.is_for_accounts,
            color=payload.color,
            user_id=current_user.id,
        )
        db.add(category)

    db.commit()
    db.refresh(category)
    return _to_read(category, build_path_map(db, current_user.id))


@router.patch("/categories/{category_id}", response_model=CategoryRead)
def update_category(
    category_id: str,
    payload: CategoryUpdate,
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    category = _get_owned_or_404(db, Category, category_id, current_user.id, "Category")

    new_name = sanitize_segment(payload.name) if payload.name is not None else category.name
    new_parent_id = (
        payload.parent_id if "parent_id" in payload.model_fields_set else category.parent_id
    )

    if not new_name:
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Nazwa nie może być pusta."
        )

    if new_parent_id:
        _get_owned_or_404(db, Category, new_parent_id, current_user.id, "Category")
        # Przeniesienie gałęzi w jej własne pod-drzewo utworzyłoby cykl.
        if is_descendant_or_self(db, current_user.id, new_parent_id, category.id):
            raise HTTPException(
                status.HTTP_409_CONFLICT,
                detail="Nie można przenieść kategorii do jej własnego pod-drzewa.",
            )

    sibling = find_child(db, current_user.id, new_parent_id, new_name)
    if sibling and sibling.id != category.id:
        raise HTTPException(
            status.HTTP_409_CONFLICT, detail="Kategoria o tej nazwie już istnieje na tym poziomie."
        )

    category.name = new_name
    category.parent_id = new_parent_id
    if payload.is_for_accounts is not None:
        category.is_for_accounts = payload.is_for_accounts
    # None jest znaczące (wyczyszczenie koloru = dziedzicz), więc patrzymy na
    # obecność pola w żądaniu, nie na wartość.
    if "color" in payload.model_fields_set:
        category.color = payload.color

    db.commit()
    db.refresh(category)
    return _to_read(category, build_path_map(db, current_user.id))


@router.get("/categories/{category_id}/delete-preview", response_model=CategoryDeletePreview)
def delete_preview(
    category_id: str,
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """Ile tagów zniknie i ile przypięć zostanie odpiętych — dane do potwierdzenia w UI."""
    _get_owned_or_404(db, Category, category_id, current_user.id, "Category")
    affected_ids = [category_id, *descendant_ids(db, current_user.id, category_id)]

    product_count = db.scalar(
        select(func.count()).select_from(Product).where(Product.category_id.in_(affected_ids))
    )
    transaction_count = db.scalar(
        select(func.count())
        .select_from(Transaction)
        .where(Transaction.category_id.in_(affected_ids))
    )
    return CategoryDeletePreview(
        category_count=len(affected_ids),
        product_count=product_count,
        transaction_count=transaction_count,
    )


@router.delete("/categories/{category_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_category(
    category_id: str,
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """Usuwa tag z całym pod-drzewem; produkty/transakcje tracą kategorię (SET NULL)."""
    category = _get_owned_or_404(db, Category, category_id, current_user.id, "Category")
    db.delete(category)
    db.commit()
