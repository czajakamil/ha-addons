from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy.orm import Session

from .. import models


def _load_memory_context(
    db: Session,
    user: models.User,
) -> Tuple[Dict[str, Any], Optional[int], Optional[Dict[str, Any]], Optional[List[Tuple[str, Dict[str, Any]]]]]:
    """Load user memory, household id, household memory, and co-member memories."""
    agent_row = db.get(models.AgentSettings, user.id)
    user_memory: Dict[str, Any] = (agent_row.memory or {}) if agent_row else {}

    membership = (
        db.query(models.HouseholdMember)
        .filter(models.HouseholdMember.user_id == user.id)
        .one_or_none()
    )
    if membership is None:
        return user_memory, None, None, None

    household_id = membership.household_id
    hh_settings = db.get(models.HouseholdSettings, household_id)
    household_memory: Dict[str, Any] = (hh_settings.memory or {}) if hh_settings else {}

    other_members = (
        db.query(models.HouseholdMember, models.User, models.AgentSettings)
        .join(models.User, models.User.id == models.HouseholdMember.user_id)
        .outerjoin(models.AgentSettings, models.AgentSettings.user_id == models.HouseholdMember.user_id)
        .filter(
            models.HouseholdMember.household_id == household_id,
            models.HouseholdMember.user_id != user.id,
        )
        .all()
    )
    member_memories: List[Tuple[str, Dict[str, Any]]] = [
        (u.username, (s.memory or {}) if s else {})
        for _, u, s in other_members
    ]

    return user_memory, household_id, household_memory, member_memories


def _build_system_prompt(
    base_prompt: str,
    user: models.User,
    user_memory: Dict[str, Any],
    household_id: Optional[int],
    household_memory: Optional[Dict[str, Any]],
    member_memories: Optional[List[Tuple[str, Dict[str, Any]]]],
) -> str:
    rating_note = (
        "## Oceny przepisów\n"
        "Każdy przepis może mieć ocenę 1-5 (pole `rating`, null = nieoceniony). "
        "Przy planowaniu posiłków preferuj przepisy z wyższą oceną (4-5). "
        "Jeśli użytkownik mówi że coś mu smakowało lub nie, zaktualizuj ocenę przez update_recipe. "
        "Do planowania tylko ulubionych użyj filter_recipes z min_rating=4."
    )
    sections = [base_prompt] if base_prompt else []
    sections.append(rating_note)

    def _fmt_user_mem(username: str, mem: Dict[str, Any]) -> Optional[str]:
        lines = []
        d = mem.get("dietary") or {}
        if d.get("allergies"):
            lines.append(f"  - Alergie: {', '.join(d['allergies'])}")
        if d.get("restrictions"):
            lines.append(f"  - Ograniczenia: {', '.join(d['restrictions'])}")
        if d.get("dislikes"):
            lines.append(f"  - Nie lubi: {', '.join(d['dislikes'])}")
        if d.get("likes"):
            lines.append(f"  - Preferuje: {', '.join(d['likes'])}")
        g = mem.get("goals") or {}
        goal_parts = []
        if g.get("kcal"):
            goal_parts.append(f"{g['kcal']} kcal")
        if g.get("p"):
            goal_parts.append(f"białko {g['p']}g")
        if goal_parts:
            lines.append(f"  - Cel: {', '.join(goal_parts)}" + (f" ({g['notes']})" if g.get("notes") else ""))
        h = mem.get("habits") or {}
        if h.get("breakfast_max_prep_min"):
            lines.append(f"  - Śniadanie max {h['breakfast_max_prep_min']} min")
        if mem.get("household_size"):
            lines.append(f"  - Liczba osób w domu: {mem['household_size']}")
        if not lines:
            return None
        return f"## Profil: {username}\n" + "\n".join(lines)

    user_block = _fmt_user_mem(user.username, user_memory)
    if user_block:
        sections.append(user_block)

    if household_id and household_memory:
        hh_lines = []
        if household_memory.get("shared_restrictions"):
            hh_lines.append(f"  - Ograniczenia household: {', '.join(household_memory['shared_restrictions'])}")
        if household_memory.get("shared_dislikes"):
            hh_lines.append(f"  - Nieakceptowane przez household: {', '.join(household_memory['shared_dislikes'])}")
        if household_memory.get("planning_notes"):
            hh_lines.append(f"  - Skład: {household_memory['planning_notes']}")
        if household_memory.get("servings_default"):
            hh_lines.append(f"  - Domyślne porcje: {household_memory['servings_default']}")
        if hh_lines:
            sections.append("## Household\n" + "\n".join(hh_lines))

    if member_memories:
        for username, mem in member_memories:
            if username != user.username:
                block = _fmt_user_mem(username, mem)
                if block:
                    sections.append(block)

    return "\n\n".join(sections)
