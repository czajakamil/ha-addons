"""LLM-backed macro estimation, shared by REST, the in-app agent and MCP."""

from __future__ import annotations

import json
import os
import re
from typing import Any

import httpx
from sqlalchemy.orm import Session

from .. import models
from ..ai_usage import check_quota, record_usage
from .errors import Invalid, Unavailable, Upstream

_SYSTEM_PROMPT = "You are a nutrition data API. Always respond with raw JSON only, no markdown, no explanation."


def is_anthropic_endpoint(endpoint: str) -> bool:
    return "anthropic.com" in endpoint or "/v1/messages" in endpoint


async def call_llm(
    endpoint: str,
    api_key: str,
    model: str,
    prompt: str,
    json_mode: bool = False,
    system_prompt: str | None = None,
) -> str:
    if is_anthropic_endpoint(endpoint):
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
        return resp.json()["content"][0]["text"]

    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {api_key}"}
    messages = []
    if system_prompt:
        messages.append({"role": "system", "content": system_prompt})
    messages.append({"role": "user", "content": prompt})
    body = {"model": model, "max_tokens": 256, "messages": messages}
    if json_mode:
        body["response_format"] = {"type": "json_object"}
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(endpoint, headers=headers, json=body)
    resp.raise_for_status()
    return resp.json()["choices"][0]["message"]["content"]


async def estimate_recipe_macros(db: Session, user: models.User, args: dict[str, Any]) -> dict[str, Any]:
    """Ask the configured LLM for whole-recipe macros. Counts against the AI quota."""
    # Raises HTTPException(403/429); REST returns it verbatim and MCP turns it
    # into an isError result, so there is nothing useful to translate here.
    check_quota(db, user)

    endpoint = os.environ.get("MEALPILOT_AI_API_URL", "").strip()
    api_key = os.environ.get("MEALPILOT_AI_API_KEY", "").strip()
    settings = db.get(models.AgentSettings, user.id)
    model = (settings.model if settings else "") or ""
    if not endpoint or not api_key:
        raise Unavailable(
            "Brak konfiguracji MEALPILOT_AI_API_URL / MEALPILOT_AI_API_KEY w ustawieniach Home Assistant."
        )
    if not model:
        raise Unavailable("Skonfiguruj model w Ustawieniach agenta.")

    title = str(args.get("title", "")).strip()
    if not title:
        raise Invalid("title jest wymagane.")
    try:
        servings = int(args.get("servings") or 1)
    except (TypeError, ValueError) as exc:
        raise Invalid("servings musi być liczbą całkowitą.") from exc
    ingredients = [i for i in (args.get("ingredients") or []) if isinstance(i, dict)]

    ing_lines = (
        "\n".join(f"- {i.get('name')}: {i.get('qty')} {i.get('unit')}" for i in ingredients) or "(brak składników)"
    )
    prompt = (
        f"Przepis: {title}\n"
        f"Liczba porcji: {servings}\n"
        f"Składniki:\n{ing_lines}\n\n"
        "Oszacuj makroskładniki dla CAŁEGO przepisu (wszystkich porcji łącznie).\n"
        "Zwróć WYŁĄCZNIE obiekt JSON w tej formie (same liczby, bez jednostek, bez opisu):\n"
        '{"kcal": 0, "p": 0, "f": 0, "c": 0}'
    )

    try:
        text = await call_llm(endpoint, api_key, model, prompt, json_mode=True, system_prompt=_SYSTEM_PROMPT)
    except httpx.HTTPStatusError as exc:
        raise Upstream(f"Błąd LLM: {exc.response.status_code} {exc.response.text[:200]}") from exc
    except Exception as exc:
        raise Upstream(f"Błąd LLM: {exc}") from exc

    # Rough estimate (server-side LLM call): ~ prompt + response chars / 4
    record_usage(db, user, tokens=max(1, (len(prompt) + len(text)) // 4))
    db.commit()

    match = re.search(r"\{[^}]+\}", text, re.DOTALL)
    if not match:
        raise Upstream(f"LLM nie zwrócił JSON: {text[:200]}")
    try:
        data = json.loads(match.group())
        return {
            "kcal": float(data["kcal"]),
            "p": float(data["p"]),
            "f": float(data["f"]),
            "c": float(data["c"]),
            "note": "Wartości szacunkowe dla CAŁEGO przepisu (wszystkich porcji łącznie).",
        }
    except (KeyError, ValueError, json.JSONDecodeError) as exc:
        raise Upstream(f"Nie można sparsować odpowiedzi LLM: {exc}") from exc
