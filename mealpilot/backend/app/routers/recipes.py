import json
import os
import re
from typing import List, Optional

import httpx
from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile, status
from sqlalchemy.orm import Session

from .. import models, schemas
from ..db import get_db
from ..dependencies import get_current_user
from ..images import ALLOWED_CONTENT_TYPES, IMAGES_DIR, MAX_IMAGE_BYTES
from ..ai_usage import check_quota, record_usage
from ..ownership import (
    can_edit,
    can_view,
    default_owner_kwargs,
    get_household_id,
    get_membership,
    visible_filter,
)

router = APIRouter(prefix="/api/recipes", tags=["recipes"])


def _visible_recipe(db: Session, user: models.User, recipe_id: str) -> models.Recipe:
    r = db.get(models.Recipe, recipe_id)
    if not r:
        raise HTTPException(404, "Recipe not found")
    if not can_view(r, user, get_household_id(db, user.id)):
        raise HTTPException(404, "Recipe not found")
    return r


def _editable_recipe(db: Session, user: models.User, recipe_id: str) -> models.Recipe:
    r = db.get(models.Recipe, recipe_id)
    if not r:
        raise HTTPException(404, "Recipe not found")
    member = get_membership(db, user.id)
    hh = member.household_id if member else None
    if not can_view(r, user, hh):
        raise HTTPException(404, "Recipe not found")
    if not can_edit(r, user, member):
        raise HTTPException(403, "Brak uprawnień do edycji tego przepisu")
    return r


def _split_csv(value: Optional[str]) -> List[str]:
    if not value:
        return []
    return [x.strip() for x in value.split(",") if x.strip()]


@router.get("", response_model=List[schemas.Recipe])
def list_recipes(
    tags: Optional[str] = Query(default=None),
    meal_types: Optional[str] = Query(default=None),
    max_kcal: Optional[float] = Query(default=None),
    min_protein: Optional[float] = Query(default=None),
    db: Session = Depends(get_db),
    user: models.User = Depends(get_current_user),
):
    hh = get_household_id(db, user.id)
    rows = db.query(models.Recipe).filter(visible_filter(models.Recipe, user, hh)).all()

    tag_filter = _split_csv(tags)
    mt_filter = _split_csv(meal_types)

    def matches(r: models.Recipe) -> bool:
        r_tags = list(r.tags or [])
        r_mts = list(r.meal_types or [])
        if tag_filter and not all(t in r_tags for t in tag_filter):
            return False
        if mt_filter and not any(m in r_mts for m in mt_filter):
            return False
        if max_kcal is not None and (r.kcal or 0) > max_kcal:
            return False
        if min_protein is not None and (r.p or 0) < min_protein:
            return False
        return True

    return [r for r in rows if matches(r)]


@router.get("/meta/tags")
def list_tags_meta(
    db: Session = Depends(get_db),
    user: models.User = Depends(get_current_user),
):
    hh = get_household_id(db, user.id)
    rows = db.query(models.Recipe).filter(visible_filter(models.Recipe, user, hh)).all()
    out: set[str] = set()
    for r in rows:
        for t in (r.tags or []):
            if isinstance(t, str) and t:
                out.add(t)
    return {"tags": sorted(out)}


@router.get("/meta/meal_types")
def list_meal_types_meta(
    db: Session = Depends(get_db),
    user: models.User = Depends(get_current_user),
):
    hh = get_household_id(db, user.id)
    rows = db.query(models.Recipe).filter(visible_filter(models.Recipe, user, hh)).all()
    out: set[str] = set()
    for r in rows:
        for m in (r.meal_types or []):
            if isinstance(m, str) and m:
                out.add(m)
    return {"meal_types": sorted(out)}


def _is_anthropic(endpoint: str) -> bool:
    return "anthropic.com" in endpoint or "/v1/messages" in endpoint


async def _call_llm(endpoint: str, api_key: str, model: str, prompt: str, json_mode: bool = False, system_prompt: str | None = None) -> str:
    if _is_anthropic(endpoint):
        headers = {
            "Content-Type": "application/json",
            "x-api-key": api_key,
            "anthropic-version": "2023-06-01",
        }
        body: dict = {
            "model": model,
            "max_tokens": 256,
            "messages": [{"role": "user", "content": prompt}],
        }
        if system_prompt:
            body["system"] = system_prompt
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(endpoint, headers=headers, json=body)
        resp.raise_for_status()
        data = resp.json()
        return data["content"][0]["text"]
    else:
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
        }
        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})
        body = {
            "model": model,
            "max_tokens": 256,
            "messages": messages,
        }
        if json_mode:
            body["response_format"] = {"type": "json_object"}
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(endpoint, headers=headers, json=body)
        resp.raise_for_status()
        data = resp.json()
        return data["choices"][0]["message"]["content"]


@router.post("/estimate-macros", response_model=schemas.MacroEstimateOut)
async def estimate_macros(
    payload: schemas.MacroEstimateRequest,
    db: Session = Depends(get_db),
    user: models.User = Depends(get_current_user),
):
    check_quota(db, user)
    endpoint = os.environ.get("MEALPILOT_AI_API_URL", "").strip()
    api_key = os.environ.get("MEALPILOT_AI_API_KEY", "").strip()
    settings = db.get(models.AgentSettings, user.id)
    model = (settings.model if settings else "") or ""
    if not endpoint or not api_key:
        raise HTTPException(424, "Brak konfiguracji MEALPILOT_AI_API_URL / MEALPILOT_AI_API_KEY w ustawieniach Home Assistant.")
    if not model:
        raise HTTPException(424, "Skonfiguruj model w Ustawieniach agenta.")

    ing_lines = "\n".join(
        f"- {i.name}: {i.qty} {i.unit}"
        for i in payload.ingredients
    ) or "(brak składników)"

    prompt = (
        f"Przepis: {payload.title}\n"
        f"Liczba porcji: {payload.servings}\n"
        f"Składniki:\n{ing_lines}\n\n"
        "Oszacuj makroskładniki dla CAŁEGO przepisu (wszystkich porcji łącznie).\n"
        "Zwróć WYŁĄCZNIE obiekt JSON w tej formie (same liczby, bez jednostek, bez opisu):\n"
        '{"kcal": 0, "p": 0, "f": 0, "c": 0}'
    )

    try:
        text = await _call_llm(
            endpoint, api_key, model, prompt,
            json_mode=True,
            system_prompt="You are a nutrition data API. Always respond with raw JSON only, no markdown, no explanation.",
        )
    except httpx.HTTPStatusError as e:
        raise HTTPException(502, f"Błąd LLM: {e.response.status_code} {e.response.text[:200]}")
    except Exception as e:
        raise HTTPException(502, f"Błąd LLM: {e}")
    # Rough estimate (server-side LLM call): ~ prompt + response chars / 4
    approx_tokens = max(1, (len(prompt) + len(text)) // 4)
    record_usage(db, user, tokens=approx_tokens)
    db.commit()

    match = re.search(r'\{[^}]+\}', text, re.DOTALL)
    if not match:
        raise HTTPException(502, f"LLM nie zwrócił JSON: {text[:200]}")
    try:
        data = json.loads(match.group())
        return schemas.MacroEstimateOut(
            kcal=float(data["kcal"]),
            p=float(data["p"]),
            f=float(data["f"]),
            c=float(data["c"]),
        )
    except (KeyError, ValueError, json.JSONDecodeError) as e:
        raise HTTPException(502, f"Nie można sparsować odpowiedzi LLM: {e}")


@router.get("/{recipe_id}", response_model=schemas.Recipe)
def get_recipe(
    recipe_id: str,
    db: Session = Depends(get_db),
    user: models.User = Depends(get_current_user),
):
    return _visible_recipe(db, user, recipe_id)


@router.post("", response_model=schemas.Recipe, status_code=status.HTTP_201_CREATED)
def create_recipe(
    payload: schemas.RecipeCreate,
    db: Session = Depends(get_db),
    user: models.User = Depends(get_current_user),
):
    if db.get(models.Recipe, payload.id):
        raise HTTPException(409, "Recipe with this id already exists")
    data = payload.model_dump()
    data["ingredients"] = [i if isinstance(i, dict) else i.model_dump() for i in data["ingredients"]]
    r = models.Recipe(created_by=user.id, **default_owner_kwargs(user), **data)
    db.add(r)
    db.commit()
    db.refresh(r)
    return r


@router.put("/{recipe_id}", response_model=schemas.Recipe)
def update_recipe(
    recipe_id: str,
    payload: schemas.RecipeUpdate,
    db: Session = Depends(get_db),
    user: models.User = Depends(get_current_user),
):
    r = _editable_recipe(db, user, recipe_id)
    updates = payload.model_dump(exclude_unset=True)
    if "ingredients" in updates and updates["ingredients"] is not None:
        updates["ingredients"] = [
            i if isinstance(i, dict) else i.model_dump() for i in updates["ingredients"]
        ]
    for k, v in updates.items():
        setattr(r, k, v)
    db.commit()
    db.refresh(r)
    return r


@router.delete("/{recipe_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_recipe(
    recipe_id: str,
    db: Session = Depends(get_db),
    user: models.User = Depends(get_current_user),
):
    r = _editable_recipe(db, user, recipe_id)
    affected_weeks = {
        w for (w,) in db.query(models.MealPlanEntry.week_start)
        .filter(models.MealPlanEntry.recipe_id == recipe_id)
        .distinct().all()
    }
    db.query(models.MealPlanEntry).filter(
        models.MealPlanEntry.recipe_id == recipe_id
    ).delete(synchronize_session=False)
    db.delete(r)
    db.commit()
    if affected_weeks:
        from ..agent.tools import regenerate_auto_shopping
        for week_start in affected_weeks:
            regenerate_auto_shopping(db, user, week_start)
    return None


@router.post("/{recipe_id}/image", response_model=schemas.Recipe)
async def upload_recipe_image(
    recipe_id: str,
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
    for old_ext in set(ALLOWED_CONTENT_TYPES.values()):
        old = IMAGES_DIR / f"{recipe_id}.{old_ext}"
        if old.exists() and old_ext != ext:
            old.unlink()

    filename = f"{recipe_id}.{ext}"
    (IMAGES_DIR / filename).write_bytes(data)

    r.image_filename = filename
    db.commit()
    db.refresh(r)
    return r


@router.put("/{recipe_id}/ownership", response_model=schemas.Recipe)
def update_recipe_ownership(
    recipe_id: str,
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


@router.delete("/{recipe_id}/image", response_model=schemas.Recipe)
def delete_recipe_image(
    recipe_id: str,
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
