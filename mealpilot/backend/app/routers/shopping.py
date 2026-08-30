from fastapi import APIRouter, Depends, status
from sqlalchemy.orm import Session

from .. import models, schemas
from ..db import get_db
from ..dependencies import get_current_user
from ..services import shopping as shopping_svc

router = APIRouter(prefix="/api/shopping", tags=["shopping"])


@router.get("/{week_start}", response_model=list[schemas.ShoppingItemOut])
def get_shopping_list(
    week_start: str,
    db: Session = Depends(get_db),
    user: models.User = Depends(get_current_user),
):
    return shopping_svc.get_shopping_list(db, user, {"week_start": week_start})["items"]


@router.post("/{week_start}/generate", response_model=list[schemas.ShoppingItemOut])
def generate_shopping_list(
    week_start: str,
    db: Session = Depends(get_db),
    user: models.User = Depends(get_current_user),
):
    return shopping_svc.generate_shopping_list(db, user, {"week_start": week_start})["items"]


@router.patch("/items/{item_id}", response_model=schemas.ShoppingItemOut)
def patch_shopping_item_by_id(
    item_id: int,
    payload: schemas.ShoppingItemPatch,
    db: Session = Depends(get_db),
    user: models.User = Depends(get_current_user),
):
    return shopping_svc.check_shopping_item(db, user, {"item_id": item_id, "checked": payload.checked})


@router.patch("/{week_start}/items/{item_id}", response_model=schemas.ShoppingItemOut)
def patch_shopping_item(
    week_start: str,
    item_id: int,
    payload: schemas.ShoppingItemPatch,
    db: Session = Depends(get_db),
    user: models.User = Depends(get_current_user),
):
    """Week-scoped variant (the one the frontend calls); 404 if the item is elsewhere."""
    return shopping_svc.check_shopping_item(
        db,
        user,
        {"week_start": week_start, "item_id": item_id, "checked": payload.checked},
    )


@router.post("/{week_start}/items", response_model=schemas.ShoppingItemOut)
def add_shopping_item(
    week_start: str,
    payload: schemas.ShoppingItemCreate,
    db: Session = Depends(get_db),
    user: models.User = Depends(get_current_user),
):
    return shopping_svc.add_shopping_item(db, user, {"week_start": week_start, **payload.model_dump()})


@router.delete("/items/{item_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_shopping_item_by_id(
    item_id: int,
    db: Session = Depends(get_db),
    user: models.User = Depends(get_current_user),
):
    shopping_svc.delete_shopping_item(db, user, {"item_id": item_id})
    return None


@router.delete("/{week_start}/items/{item_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_shopping_item(
    week_start: str,
    item_id: int,
    db: Session = Depends(get_db),
    user: models.User = Depends(get_current_user),
):
    """Week-scoped variant (the one the frontend calls); 404 if the item is elsewhere."""
    shopping_svc.delete_shopping_item(db, user, {"week_start": week_start, "item_id": item_id})
    return None


@router.delete("/{week_start}", status_code=status.HTTP_204_NO_CONTENT)
def clear_shopping_list(
    week_start: str,
    db: Session = Depends(get_db),
    user: models.User = Depends(get_current_user),
):
    shopping_svc.clear_shopping_list(db, user, {"week_start": week_start})
    return None
