from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile, status
from sqlalchemy.orm import Session

from .. import models, schemas
from ..db import get_db
from ..dependencies import get_current_user
from ..images import ALLOWED_CONTENT_TYPES, IMAGES_DIR, MAX_IMAGE_BYTES
from ..ownership import get_household_id
from ..services import macros as macros_svc
from ..services import recipes as recipes_svc
from ..services.common import require_editable, require_visible

router = APIRouter(prefix="/api/recipes", tags=["recipes"])


def _with_ratings(db: Session, user_id: int, recipes: list) -> list:
    """Annotate recipe ORM objects with avg_rating, rating_count, my_rating, my_note."""
    maps = recipes_svc.rating_maps(db, user_id, [r.id for r in recipes])
    result = []
    for r in recipes:
        d = schemas.Recipe.model_validate(r).model_dump()
        d["avg_rating"] = maps["avg"].get(r.id)
        d["rating_count"] = maps["count"].get(r.id, 0)
        d["my_rating"] = maps["mine"].get(r.id)
        d["my_note"] = maps["note"].get(r.id)
        result.append(d)
    return result


def _visible_recipe(db: Session, user: models.User, recipe_id: int) -> models.Recipe:
    return require_visible(db, user, models.Recipe, recipe_id)


def _editable_recipe(db: Session, user: models.User, recipe_id: int) -> models.Recipe:
    return require_editable(db, user, models.Recipe, recipe_id)


def _split_csv(value: str | None) -> list[str]:
    if not value:
        return []
    return [x.strip() for x in value.split(",") if x.strip()]


@router.get("", response_model=list[schemas.Recipe])
def list_recipes(
    tags: str | None = Query(default=None),
    meal_types: str | None = Query(default=None),
    max_kcal: float | None = Query(default=None),
    min_protein: float | None = Query(default=None),
    max_total_time: float | None = Query(default=None),
    is_meal_prep: bool | None = Query(default=None),
    min_my_rating: int | None = Query(default=None, ge=1, le=5),
    min_avg_rating: float | None = Query(default=None, ge=1.0, le=5.0),
    q: str | None = Query(default=None, description="Wyszukiwanie tekstowe (tytuł, tagi, składniki)."),
    db: Session = Depends(get_db),
    user: models.User = Depends(get_current_user),
):
    filters = {
        "tags": _split_csv(tags),
        "meal_types": _split_csv(meal_types),
        "max_kcal": max_kcal,
        "min_protein": min_protein,
        "max_total_time": max_total_time,
        "is_meal_prep": is_meal_prep,
        "min_my_rating": min_my_rating,
        "min_avg_rating": min_avg_rating,
    }
    # `search_rows` already applies `filters` and returns the ORM rows ranked by
    # relevance, so the library is scanned once — it used to be walked twice, and
    # the rating maps built twice, just to get from the search hits back to rows.
    if q and q.strip():
        rows = recipes_svc.search_rows(db, user, {**filters, "query": q})
    else:
        rows = recipes_svc.filtered_rows(db, user, filters)
    return _with_ratings(db, user.id, rows)


@router.get("/meta/tags")
def list_tags_meta(
    db: Session = Depends(get_db),
    user: models.User = Depends(get_current_user),
):
    return recipes_svc.list_tags(db, user, {})


@router.get("/meta/meal_types")
def list_meal_types_meta(
    db: Session = Depends(get_db),
    user: models.User = Depends(get_current_user),
):
    return recipes_svc.list_meal_types(db, user, {})


@router.post("/estimate-macros", response_model=schemas.MacroEstimateOut)
async def estimate_macros(
    payload: schemas.MacroEstimateRequest,
    db: Session = Depends(get_db),
    user: models.User = Depends(get_current_user),
):
    out = await macros_svc.estimate_recipe_macros(db, user, payload.model_dump())
    return schemas.MacroEstimateOut(kcal=out["kcal"], p=out["p"], f=out["f"], c=out["c"])


@router.get("/{recipe_id}", response_model=schemas.Recipe)
def get_recipe(
    recipe_id: int,
    db: Session = Depends(get_db),
    user: models.User = Depends(get_current_user),
):
    r = _visible_recipe(db, user, recipe_id)
    return _with_ratings(db, user.id, [r])[0]


def _pop_explicit_nulls(updates: dict) -> list[str]:
    """Take the deliberate `null`s out of `updates` and return the ones to clear.

    The service layer reads ``None`` as "field not supplied" — it has to, since
    the tool schemas cannot tell the two apart. REST can: ``exclude_unset``
    already established that the client sent the key. So the two meanings are
    separated here, before the payload reaches the service:

      * ``null`` on a NOT NULL column is a client mistake — it used to reach
        SQLAlchemy and surface as a 500; it is a 422 now.
      * ``null`` on a nullable column means "clear it", which the recipe editor
        relies on for `meal_prep_days`, so it is applied after the service call.

    Nullability is read off the model, so the two lists cannot drift.
    """
    columns = models.Recipe.__table__.columns
    nulls = sorted(k for k, v in updates.items() if v is None and k in columns)
    bad = [k for k in nulls if not columns[k].nullable]
    if bad:
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_ENTITY,
            f"Pola nie mogą mieć wartości null: {', '.join(bad)}",
        )
    for key in nulls:
        updates.pop(key)
    return nulls


@router.post("", response_model=schemas.Recipe, status_code=status.HTTP_201_CREATED)
def create_recipe(
    payload: schemas.RecipeCreate,
    db: Session = Depends(get_db),
    user: models.User = Depends(get_current_user),
):
    # Through the service layer, so REST, the agent and MCP share one
    # normalisation path; the ORM row is re-read afterwards because the service
    # returns the tool-facing shape and this endpoint's contract is the ORM one.
    created = recipes_svc.create_recipe(db, user, payload.model_dump())
    return db.get(models.Recipe, created["id"])


@router.put("/{recipe_id}", response_model=schemas.Recipe)
def update_recipe(
    recipe_id: int,
    payload: schemas.RecipeUpdate,
    db: Session = Depends(get_db),
    user: models.User = Depends(get_current_user),
):
    updates = payload.model_dump(exclude_unset=True)
    cleared = _pop_explicit_nulls(updates)
    # The service runs first: it owns the permission check, so an uneditable
    # recipe is rejected before anything is written.
    recipes_svc.update_recipe(db, user, {**updates, "recipe_id": recipe_id})
    r = db.get(models.Recipe, recipe_id)
    if cleared:
        for key in cleared:
            setattr(r, key, None)
        db.commit()
        db.refresh(r)
    return r


@router.delete("/{recipe_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_recipe(
    recipe_id: int,
    db: Session = Depends(get_db),
    user: models.User = Depends(get_current_user),
):
    recipes_svc.delete_recipe(db, user, {"recipe_id": recipe_id})
    return None


@router.put("/{recipe_id}/rating", response_model=schemas.RatingOut)
def upsert_rating(
    recipe_id: int,
    payload: schemas.RatingUpsert,
    db: Session = Depends(get_db),
    user: models.User = Depends(get_current_user),
):
    _visible_recipe(db, user, recipe_id)
    existing = (
        db.query(models.RecipeRating)
        .filter(models.RecipeRating.recipe_id == recipe_id, models.RecipeRating.user_id == user.id)
        .one_or_none()
    )
    if existing:
        existing.rating = payload.rating
    else:
        existing = models.RecipeRating(recipe_id=recipe_id, user_id=user.id, rating=payload.rating)
        db.add(existing)
    db.commit()
    db.refresh(existing)
    return existing


@router.delete("/{recipe_id}/rating", status_code=status.HTTP_204_NO_CONTENT)
def delete_rating(
    recipe_id: int,
    db: Session = Depends(get_db),
    user: models.User = Depends(get_current_user),
):
    _visible_recipe(db, user, recipe_id)
    db.query(models.RecipeRating).filter(
        models.RecipeRating.recipe_id == recipe_id,
        models.RecipeRating.user_id == user.id,
    ).delete(synchronize_session=False)
    db.commit()
    return None


@router.post("/{recipe_id}/image", response_model=schemas.Recipe)
async def upload_recipe_image(
    recipe_id: int,
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
    user: models.User = Depends(get_current_user),
):
    r = _editable_recipe(db, user, recipe_id)
    ext = ALLOWED_CONTENT_TYPES.get((file.content_type or "").lower())
    if not ext:
        raise HTTPException(415, "Unsupported image type. Use JPEG, PNG, or WebP.")

    data = await file.read(MAX_IMAGE_BYTES + 1)
    if len(data) > MAX_IMAGE_BYTES:
        raise HTTPException(413, "Image exceeds 10 MB limit")

    IMAGES_DIR.mkdir(parents=True, exist_ok=True)
    filename = f"{recipe_id}.{ext}"
    # Drop whatever this recipe pointed at before — including images stored under
    # the pre-integer-id naming scheme, which no longer matches `{recipe_id}.{ext}`.
    if r.image_filename and r.image_filename != filename:
        (IMAGES_DIR / r.image_filename).unlink(missing_ok=True)
    for old_ext in set(ALLOWED_CONTENT_TYPES.values()) - {ext}:
        (IMAGES_DIR / f"{recipe_id}.{old_ext}").unlink(missing_ok=True)

    (IMAGES_DIR / filename).write_bytes(data)

    r.image_filename = filename
    db.commit()
    db.refresh(r)
    return r


@router.put("/{recipe_id}/ownership", response_model=schemas.Recipe)
def update_recipe_ownership(
    recipe_id: int,
    payload: schemas.OwnershipPatch,
    db: Session = Depends(get_db),
    user: models.User = Depends(get_current_user),
):
    r = db.get(models.Recipe, recipe_id)
    if not r or r.created_by != user.id:
        raise HTTPException(404, "Recipe not found")
    if payload.share_with_household:
        hh = get_household_id(db, user.id)
        if hh is None:
            raise HTTPException(400, "Nie należysz do żadnego household")
        r.owner_user_id = None
        r.owner_household_id = hh
    else:
        r.owner_user_id = user.id
        r.owner_household_id = None
    db.commit()
    db.refresh(r)
    return r


@router.put("/{recipe_id}/note", response_model=schemas.RecipeNoteOut)
def upsert_note(
    recipe_id: int,
    payload: schemas.RecipeNoteUpsert,
    db: Session = Depends(get_db),
    user: models.User = Depends(get_current_user),
):
    _visible_recipe(db, user, recipe_id)
    existing = (
        db.query(models.RecipeNote)
        .filter(models.RecipeNote.recipe_id == recipe_id, models.RecipeNote.user_id == user.id)
        .one_or_none()
    )
    if existing:
        existing.note = payload.note
    else:
        existing = models.RecipeNote(recipe_id=recipe_id, user_id=user.id, note=payload.note)
        db.add(existing)
    db.commit()
    db.refresh(existing)
    return existing


@router.delete("/{recipe_id}/note", status_code=status.HTTP_204_NO_CONTENT)
def delete_note(
    recipe_id: int,
    db: Session = Depends(get_db),
    user: models.User = Depends(get_current_user),
):
    _visible_recipe(db, user, recipe_id)
    db.query(models.RecipeNote).filter(
        models.RecipeNote.recipe_id == recipe_id,
        models.RecipeNote.user_id == user.id,
    ).delete(synchronize_session=False)
    db.commit()
    return None


@router.delete("/{recipe_id}/image", response_model=schemas.Recipe)
def delete_recipe_image(
    recipe_id: int,
    db: Session = Depends(get_db),
    user: models.User = Depends(get_current_user),
):
    r = _editable_recipe(db, user, recipe_id)
    if r.image_filename:
        path = IMAGES_DIR / r.image_filename
        if path.exists():
            path.unlink()
        r.image_filename = None
        db.commit()
        db.refresh(r)
    return r
