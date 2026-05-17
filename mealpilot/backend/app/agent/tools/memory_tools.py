from __future__ import annotations

from typing import Any, Dict

from sqlalchemy.orm import Session

from ... import models
from ..registry import tool


@tool(
    name="update_my_memory",
    description=(
        "Zapisuje preferencje użytkownika w pamięci trwałej. "
        "Wywołaj gdy użytkownik wspomni o ograniczeniach dietetycznych, nielubianych składnikach, "
        "ulubionych kuchniach, celach kalorycznych/makro, zwyczajach kulinarnych lub wielkości gospodarstwa. "
        "Nie pytaj o potwierdzenie — po prostu zapisz."
    ),
    input_schema={
        "type": "object",
        "properties": {
            "dietary": {
                "type": "object",
                "description": "Preferencje żywieniowe użytkownika.",
                "properties": {
                    "restrictions": {"type": "array", "items": {"type": "string"}, "description": "Diety/ograniczenia, np. wegetarianin, bez-laktozy"},
                    "dislikes": {"type": "array", "items": {"type": "string"}, "description": "Niepolubione składniki lub dania"},
                    "likes": {"type": "array", "items": {"type": "string"}, "description": "Ulubione kuchnie, smaki, tagi"},
                    "allergies": {"type": "array", "items": {"type": "string"}, "description": "Alergie pokarmowe"},
                },
            },
            "goals": {
                "type": "object",
                "description": "Cele żywieniowe.",
                "properties": {
                    "kcal": {"type": "integer", "description": "Dzienny cel kaloryczny"},
                    "p": {"type": "integer", "description": "Cel białka [g/dzień]"},
                    "f": {"type": "integer", "description": "Cel tłuszczu [g/dzień]"},
                    "c": {"type": "integer", "description": "Cel węglowodanów [g/dzień]"},
                    "notes": {"type": "string", "description": "Kontekst, np. budowanie masy, redukcja"},
                },
            },
            "habits": {
                "type": "object",
                "description": "Zwyczaje kulinarne.",
                "properties": {
                    "breakfast_max_prep_min": {"type": "integer", "description": "Max czas przygotowania śniadania [min]"},
                    "batch_cook_day": {"type": "integer", "description": "Dzień batch-cookingu (0=pon, 6=niedz)"},
                    "shopping_day": {"type": "integer", "description": "Dzień zakupów (0=pon, 6=niedz)"},
                },
            },
            "household_size": {"type": "integer", "description": "Liczba osób w gospodarstwie domowym"},
        },
    },
)
def tool_update_my_memory(db: Session, user: models.User, args: Dict[str, Any]) -> Any:
    row = db.get(models.AgentSettings, user.id)
    if row is None:
        row = models.AgentSettings(user_id=user.id)
        db.add(row)
        db.flush()
    mem = dict(row.memory or {})

    if "dietary" in args:
        mem["dietary"] = args["dietary"]
    if "goals" in args:
        mem.setdefault("goals", {}).update({k: v for k, v in args["goals"].items() if v is not None})
    if "habits" in args:
        mem.setdefault("habits", {}).update({k: v for k, v in args["habits"].items() if v is not None})
    if "household_size" in args and args["household_size"] is not None:
        mem["household_size"] = args["household_size"]

    row.memory = mem
    db.commit()
    return {"saved": True, "memory": mem}


@tool(
    name="update_household_memory",
    description=(
        "Zapisuje wspólne preferencje całego household w pamięci trwałej. "
        "Wywołaj gdy rozmowa dotyczy wspólnych ograniczeń, planowania dla całej rodziny lub "
        "domyślnej liczby porcji. Wymaga uprawnień can_edit w household."
    ),
    input_schema={
        "type": "object",
        "properties": {
            "household_id": {"type": "integer", "description": "ID household"},
            "shared_restrictions": {
                "type": "array",
                "items": {"type": "string"},
                "description": "Ograniczenia dietetyczne całego household, np. bez orzechów",
            },
            "shared_dislikes": {
                "type": "array",
                "items": {"type": "string"},
                "description": "Składniki/dania nieakceptowane przez któregokolwiek członka",
            },
            "planning_notes": {"type": "string", "description": "Notatki o strukturze household, np. 2 dorosłych + 1 dziecko"},
            "servings_default": {"type": "integer", "description": "Domyślna liczba porcji przy planowaniu"},
        },
        "required": ["household_id"],
    },
)
def tool_update_household_memory(db: Session, user: models.User, args: Dict[str, Any]) -> Any:
    household_id = args.get("household_id")
    if not household_id:
        raise ValueError("Brak household_id")

    member = (
        db.query(models.HouseholdMember)
        .filter(
            models.HouseholdMember.user_id == user.id,
            models.HouseholdMember.household_id == household_id,
        )
        .one_or_none()
    )
    if member is None:
        raise ValueError(f"Nie jesteś członkiem household {household_id}")
    if not member.can_edit:
        raise ValueError("Nie masz uprawnień do edycji ustawień tego household")

    row = db.get(models.HouseholdSettings, household_id)
    if row is None:
        row = models.HouseholdSettings(household_id=household_id)
        db.add(row)
        db.flush()
    mem = dict(row.memory or {})

    if "shared_restrictions" in args:
        mem["shared_restrictions"] = args["shared_restrictions"]
    if "shared_dislikes" in args:
        mem["shared_dislikes"] = args["shared_dislikes"]
    if "planning_notes" in args and args["planning_notes"] is not None:
        mem["planning_notes"] = args["planning_notes"]
    if "servings_default" in args and args["servings_default"] is not None:
        mem["servings_default"] = args["servings_default"]

    row.memory = mem
    db.commit()
    return {"saved": True, "memory": mem}
