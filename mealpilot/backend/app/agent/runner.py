from __future__ import annotations

import os
from typing import Any, Dict, List

import httpx
from sqlalchemy.orm import Session

from .. import models
from .memory import _build_system_prompt, _load_memory_context
from .providers.anthropic import _run_anthropic, _stream_anthropic
from .providers.openai import _run_openai, _stream_openai
from .registry import _is_anthropic

# Importing tools package triggers @tool registration for all tool modules.
from . import tools  # noqa: F401


async def run_agent(
    db: Session,
    user: models.User,
    settings: models.AgentSettings,
    history: List[Dict[str, Any]],
) -> Dict[str, Any]:
    endpoint = os.environ.get("MEALPILOT_AI_API_URL", "").strip()
    api_key = os.environ.get("MEALPILOT_AI_API_KEY", "").strip()

    if not endpoint:
        return {
            "reply": "❗ Błąd: Brak konfiguracji MEALPILOT_AI_API_URL w ustawieniach Home Assistant.",
            "tool_events": [],
            "changed": [],
        }
    if not api_key:
        return {
            "reply": "❗ Błąd: Brak konfiguracji MEALPILOT_AI_API_KEY w ustawieniach Home Assistant.",
            "tool_events": [],
            "changed": [],
        }

    model = settings.model or ""
    base_prompt = settings.system_prompt or ""

    user_memory, household_id, household_memory, member_memories = _load_memory_context(db, user)
    system_prompt = _build_system_prompt(
        base_prompt, user, user_memory, household_id, household_memory, member_memories
    )

    tool_events: List[Dict[str, Any]] = []
    changed_set: set = set()
    tokens_used = 0

    try:
        if _is_anthropic(endpoint):
            reply, tokens_used = await _run_anthropic(
                endpoint, api_key, model, system_prompt,
                history, db, user, tool_events, changed_set,
            )
        else:
            reply, tokens_used = await _run_openai(
                endpoint, api_key, model, system_prompt,
                history, db, user, tool_events, changed_set,
            )
    except httpx.HTTPStatusError as exc:
        reply = f"❗ Błąd: {exc.response.status_code} {exc.response.text[:300]}"
    except Exception as exc:
        reply = f"❗ Błąd: {exc}"

    return {
        "reply": reply,
        "tool_events": tool_events,
        "changed": sorted(changed_set),
        "tokens_used": tokens_used,
    }


async def stream_agent(
    db: Session,
    user: models.User,
    settings: models.AgentSettings,
    history: List[Dict[str, Any]],
):
    """Async generator yielding SSE-ready event dicts for the streaming endpoint."""
    endpoint = os.environ.get("MEALPILOT_AI_API_URL", "").strip()
    api_key = os.environ.get("MEALPILOT_AI_API_KEY", "").strip()

    if not endpoint:
        yield {"type": "error", "detail": "Brak konfiguracji MEALPILOT_AI_API_URL w ustawieniach Home Assistant."}
        return
    if not api_key:
        yield {"type": "error", "detail": "Brak konfiguracji MEALPILOT_AI_API_KEY w ustawieniach Home Assistant."}
        return

    model = settings.model or ""
    base_prompt = settings.system_prompt or ""
    user_memory, household_id, household_memory, member_memories = _load_memory_context(db, user)
    system_prompt = _build_system_prompt(
        base_prompt, user, user_memory, household_id, household_memory, member_memories
    )

    changed_set: set = set()
    try:
        if _is_anthropic(endpoint):
            provider_gen = _stream_anthropic(
                endpoint, api_key, model, system_prompt, history, db, user, changed_set
            )
        else:
            provider_gen = _stream_openai(
                endpoint, api_key, model, system_prompt, history, db, user, changed_set
            )

        async for event in provider_gen:
            if event["type"] == "final":
                yield {**event, "changed": sorted(changed_set)}
            else:
                yield event
    except httpx.HTTPStatusError as exc:
        yield {"type": "error", "detail": f"{exc.response.status_code} {exc.response.text[:300]}"}
    except Exception as exc:
        yield {"type": "error", "detail": str(exc)}
