"""OpenAI-compatible agent loop."""
from __future__ import annotations

import json
import uuid
from typing import Any, Dict, List

import httpx
from sqlalchemy.orm import Session

from ... import models
from .. import MAX_STEPS
from ..helpers import safe_json_parse
from ..tools import TOOL_DEFS_OPENAI, call_tool


async def run_openai(
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
    messages: List[Dict[str, Any]] = [
        {"role": "system", "content": system_prompt},
        *history,
    ]
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}",
    }
    final_text = ""

    async with httpx.AsyncClient(timeout=120.0) as client:
        for _step in range(MAX_STEPS):
            body = {
                "model": model,
                "messages": messages,
                "tools": TOOL_DEFS_OPENAI,
                "tool_choice": "auto",
            }
            resp = await client.post(endpoint, headers=headers, json=body)
            resp.raise_for_status()
            data = resp.json()

            choices = data.get("choices") or []
            if not choices:
                err = data.get("error", {})
                if isinstance(err, dict):
                    parts = [err.get("message", "")]
                    meta = err.get("metadata") or {}
                    if isinstance(meta, dict) and meta.get("raw"):
                        parts.append(f"raw: {meta['raw']}")
                    elif meta:
                        parts.append(f"metadata: {json.dumps(meta)}")
                    err_msg = " | ".join(p for p in parts if p) or json.dumps(data)
                else:
                    err_msg = str(err) if err else json.dumps(data)
                raise RuntimeError(f"OpenAI: brak pola 'choices' — {err_msg}")

            msg = choices[0]["message"]
            if msg.get("content"):
                final_text = msg["content"].strip()

            tool_calls = msg.get("tool_calls") or []
            if not tool_calls:
                break

            # Strip extra fields (refusal, audio, …) that newer models return
            # but the API rejects when echoed back in the request.
            clean_msg: Dict[str, Any] = {"role": msg["role"], "content": msg.get("content")}
            if tool_calls:
                clean_msg["tool_calls"] = tool_calls
            messages.append(clean_msg)
            for call in tool_calls:
                call_id = call.get("id", str(uuid.uuid4()))
                name = call.get("function", {}).get("name", "")
                try:
                    input_args = json.loads(call.get("function", {}).get("arguments", "{}") or "{}")
                except json.JSONDecodeError:
                    input_args = {}

                result_text, is_error = call_tool(db, user, name, input_args, changed_set)
                tool_events.append({
                    "tool_use_id": call_id,
                    "name": name,
                    "input": input_args,
                    "output": None if is_error else safe_json_parse(result_text),
                    "error": result_text if is_error else None,
                })
                messages.append({
                    "role": "tool",
                    "tool_call_id": call_id,
                    "content": result_text,
                })

    return final_text or "(agent przekroczył limit kroków)"
