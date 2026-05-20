"""Anthropic agent loop."""
from __future__ import annotations

import uuid
from typing import Any, Dict, List

import httpx
from sqlalchemy.orm import Session

from ... import models
from .. import MAX_STEPS
from ..helpers import json_str, safe_json_parse
from ..tools import TOOL_DEFS, call_tool


def is_anthropic(endpoint: str) -> bool:
    return "anthropic.com" in endpoint or "/v1/messages" in endpoint


async def run_anthropic(
    endpoint: str,
    api_key: str,
    model: str,
    system_prompt: str,
    history: List[Dict[str, Any]],
    db: Session,
    user: models.User,
    tool_events: List[Dict[str, Any]],
    changed_set: set,
) -> str:
    messages = list(history)
    headers = {
        "Content-Type": "application/json",
        "x-api-key": api_key,
        "anthropic-version": "2023-06-01",
    }
    final_text = ""

    async with httpx.AsyncClient(timeout=120.0) as client:
        for _step in range(MAX_STEPS):
            body = {
                "model": model,
                "max_tokens": 4096,
                "system": system_prompt,
                "messages": messages,
                "tools": TOOL_DEFS,
            }
            resp = await client.post(endpoint, headers=headers, json=body)
            resp.raise_for_status()
            data = resp.json()

            if not data.get("content"):
                err = data.get("error", {})
                err_msg = err.get("message", json_str(data)) if isinstance(err, dict) else str(err)
                raise RuntimeError(f"Anthropic: brak pola 'content' — {err_msg}")

            blocks = data["content"]
            text_blocks = [b["text"] for b in blocks if b.get("type") == "text" and b.get("text")]
            if text_blocks:
                final_text = "\n".join(text_blocks).strip()

            tool_uses = [b for b in blocks if b.get("type") == "tool_use"]
            if data.get("stop_reason") != "tool_use" or not tool_uses:
                break

            messages.append({"role": "assistant", "content": blocks})
            tool_results = []
            for tu in tool_uses:
                tu_id = tu.get("id", str(uuid.uuid4()))
                name = tu.get("name", "")
                input_args = tu.get("input") or {}
                result_text, is_error = call_tool(db, user, name, input_args, changed_set)
                tool_events.append({
                    "tool_use_id": tu_id,
                    "name": name,
                    "input": input_args,
                    "output": None if is_error else safe_json_parse(result_text),
                    "error": result_text if is_error else None,
                })
                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": tu_id,
                    "content": result_text,
                    "is_error": is_error,
                })
            messages.append({"role": "user", "content": tool_results})

    return final_text or "(agent przekroczył limit kroków)"
