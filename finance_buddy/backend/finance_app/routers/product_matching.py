"""Dopasowanie pozycji paragonu do produktów + zapis aliasów (Etap 3 OCR)."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from ..db import get_db_session
from ..dependencies import _get_owned_or_404, get_current_user
from ..models import Store, User
from ..schemas import (
    ProductAliasCreate,
    ProductAliasRead,
    ProductMatchRequest,
    ProductMatchResponse,
)
from ..services import product_matching

router = APIRouter()


@router.post("/product-matches", response_model=ProductMatchResponse)
def match_products(
    payload: ProductMatchRequest,
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """Dopasowuje nazwy pozycji paragonu do produktów użytkownika (kaskada aliasów/fuzzy)."""
    if payload.store_id:
        _get_owned_or_404(db, Store, payload.store_id, current_user.id, "Store")

    matches = product_matching.match_items(
        db,
        current_user.id,
        payload.raw_names,
        store_id=payload.store_id,
    )
    return ProductMatchResponse(matches=matches)


@router.post(
    "/product-aliases",
    response_model=ProductAliasRead,
    status_code=status.HTTP_201_CREATED,
)
def create_product_alias(
    payload: ProductAliasCreate,
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """Zapisuje alias z potwierdzenia użytkownika (pętla uczenia się z paragonu)."""
    alias = product_matching.save_alias(
        db,
        current_user.id,
        payload.product_id,
        payload.raw_name,
        store_id=payload.store_id,
    )
    if alias is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Nie udało się zapisać aliasu (pusta nazwa albo produkt/sklep spoza konta).",
        )
    db.commit()
    db.refresh(alias)
    return alias
