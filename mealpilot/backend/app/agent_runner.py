"""
Backend agent runner — orchestrates the agent loop using Anthropic or
OpenAI-compatible APIs. Tool implementations and provider loops live in
the `agent/` package.
"""

from __future__ import annotations

import os
import re
from typing import Any

import httpx
from sqlalchemy.orm import Session

from . import models
from .agent.prompts import DEFAULT_SYSTEM_PROMPT, TITLE_SYSTEM_PROMPT
from .agent.providers import is_anthropic, run_anthropic, run_openai

__all__ = ["generate_conversation_title", "run_agent"]


async def run_agent(
    db: Session,
    user: models.User,
    settings: models.AgentSettings,
    history: list[dict[str, Any]],
) -> dict[str, Any]:
    """
    Run the full agent loop.

    Args:
        db: SQLAlchemy session.
        user: The authenticated user.
        settings: AgentSettings row (for model + system_prompt).
        history: List of messages in provider format: [{"role": ..., "content": ...}].

    Returns:
        {"reply": str, "tool_events": list, "changed": list}
    """
    endpoint = os.environ.get("MEALPILOT_AI_API_URL", "").strip()
    api_key = os.environ.get("MEALPILOT_AI_API_KEY", "").strip()

    if not endpoint:
        return {
            "reply": ("❗ Błąd: Brak konfiguracji MEALPILOT_AI_API_URL w ustawieniach Home Assistant."),
            "tool_events": [],
            "changed": [],
        }
    if not api_key:
        return {
            "reply": ("❗ Błąd: Brak konfiguracji MEALPILOT_AI_API_KEY w ustawieniach Home Assistant."),
            "tool_events": [],
            "changed": [],
        }

    model = settings.model or ""
    system_prompt = settings.system_prompt or DEFAULT_SYSTEM_PROMPT

    tool_events: list[dict[str, Any]] = []
    changed_set: set = set()

    try:
        if is_anthropic(endpoint):
            reply = await run_anthropic(
                endpoint,
                api_key,
                model,
                system_prompt,
                history,
                db,
                user,
                tool_events,
                changed_set,
            )
        else:
            reply = await run_openai(
                endpoint,
                api_key,
                model,
                system_prompt,
                history,
                db,
                user,
                tool_events,
                changed_set,
            )
    except httpx.HTTPStatusError as exc:
        reply = f"❗ Błąd: {exc.response.status_code} {exc.response.text[:300]}"
    except Exception as exc:
        reply = f"❗ Błąd: {exc}"

    return {
        "reply": reply,
        "tool_events": tool_events,
        "changed": sorted(changed_set),
    }


# ---------------------------------------------------------------------------
# Conversation title generation
# ---------------------------------------------------------------------------


def _clean_title(text: str) -> str:
    t = (text or "").strip()
    while t and t[0] in "\"'`„«»" and t[-1] in "\"'`„«»":
        t = t[1:-1].strip()
    t = re.sub(r"^(tytuł|temat|title)\s*[:\-–—]\s*", "", t, flags=re.IGNORECASE)
    t = re.sub(r"\s+", " ", t).strip()
    t = t.rstrip(".!?,;: ")
    if len(t) > 60:
        t = t[:60].rstrip()
    return t


async def generate_conversation_title(
    model: str,
    user_text: str,
    assistant_text: str,
) -> str:
    """
    Ask the LLM for a 3-5 word Polish title summarizing this exchange.
    Returns a cleaned title or "" on failure.
    """
    endpoint = os.environ.get("MEALPILOT_AI_API_URL", "").strip()
    api_key = os.environ.get("MEALPILOT_AI_API_KEY", "").strip()
    if not endpoint or not api_key or not model:
        return ""

    u = (user_text or "").strip()[:500]
    a = (assistant_text or "").strip()[:500]
    prompt = f"Wiadomość użytkownika:\n{u}\n\nOdpowiedź asystenta:\n{a}\n\nZwróć krótki, opisowy tytuł tej rozmowy."

    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            if is_anthropic(endpoint):
                resp = await client.post(
                    endpoint,
                    headers={
                        "Content-Type": "application/json",
                        "x-api-key": api_key,
                        "anthropic-version": "2023-06-01",
                    },
                    json={
                        "model": model,
                        "max_tokens": 120,
                        "system": TITLE_SYSTEM_PROMPT,
                        "messages": [{"role": "user", "content": prompt}],
                    },
                )
                resp.raise_for_status()
                data = resp.json()
                blocks = data.get("content") or []
                texts = [b.get("text", "") for b in blocks if b.get("type") == "text"]
                return _clean_title(" ".join(texts))
            else:
                resp = await client.post(
                    endpoint,
                    headers={
                        "Content-Type": "application/json",
                        "Authorization": f"Bearer {api_key}",
                    },
                    json={
                        "model": model,
                        "max_tokens": 120,
                        "messages": [
                            {"role": "system", "content": TITLE_SYSTEM_PROMPT},
                            {"role": "user", "content": prompt},
                        ],
                    },
                )
                resp.raise_for_status()
                data = resp.json()
                choices = data.get("choices") or []
                if not choices:
                    return ""
                return _clean_title(choices[0].get("message", {}).get("content", ""))
    except Exception:
        return ""
