from __future__ import annotations

import json
import uuid
from typing import Any, AsyncGenerator, Dict, List, Tuple

import httpx
from sqlalchemy.orm import Session

from ... import models
from ..registry import _REGISTRY, _call_tool, _safe_json_parse

MAX_STEPS = 10


async def _run_anthropic(
    endpoint: str,
    api_key: str,
    model: str,
    system_prompt: str,
    history: List[Dict[str, Any]],
    db: Session,
    user: models.User,
    tool_events: List[Dict[str, Any]],
    changed_set: set,
) -> Tuple[str, int]:
    messages = list(history)
    headers = {
        "Content-Type": "application/json",
        "x-api-key": api_key,
        "anthropic-version": "2023-06-01",
    }
    tool_defs = [
        {"name": r.name, "description": r.description, "input_schema": r.input_schema}
        for r in _REGISTRY
    ]
    final_text = ""
    total_tokens = 0

    async with httpx.AsyncClient(timeout=120.0) as client:
        for _step in range(MAX_STEPS):
            body = {
                "model": model,
                "max_tokens": 4096,
                "system": system_prompt,
                "messages": messages,
                "tools": tool_defs,
            }
            resp = await client.post(endpoint, headers=headers, json=body)
            resp.raise_for_status()
            data = resp.json()

            usage = data.get("usage") or {}
            total_tokens += (usage.get("input_tokens") or 0) + (usage.get("output_tokens") or 0)

            if not data.get("content"):
                err = data.get("error", {})
                err_msg = err.get("message", str(data)) if isinstance(err, dict) else str(err)
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
                result_text, is_error = _call_tool(db, user, name, input_args, [], changed_set)
                tool_events.append({
                    "tool_use_id": tu_id,
                    "name": name,
                    "input": input_args,
                    "output": None if is_error else _safe_json_parse(result_text),
                    "error": result_text if is_error else None,
                })
                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": tu_id,
                    "content": result_text,
                    "is_error": is_error,
                })
            messages.append({"role": "user", "content": tool_results})

    return final_text or "(agent przekroczył limit kroków)", total_tokens


async def _stream_anthropic(
    endpoint: str,
    api_key: str,
    model: str,
    system_prompt: str,
    history: List[Dict[str, Any]],
    db: Session,
    user: models.User,
    changed_set: set,
) -> AsyncGenerator[Dict[str, Any], None]:
    """Async generator yielding token/tool/final events for the streaming SSE endpoint."""
    messages = list(history)
    headers = {
        "Content-Type": "application/json",
        "x-api-key": api_key,
        "anthropic-version": "2023-06-01",
    }
    tool_defs = [
        {"name": r.name, "description": r.description, "input_schema": r.input_schema}
        for r in _REGISTRY
    ]
    final_text_parts: List[str] = []
    all_tool_events: List[Dict[str, Any]] = []
    total_tokens = 0

    async with httpx.AsyncClient(timeout=120.0) as client:
        for _step in range(MAX_STEPS):
            body = {
                "model": model,
                "max_tokens": 4096,
                "system": system_prompt,
                "messages": messages,
                "tools": tool_defs,
                "stream": True,
            }

            # idx -> {type, id?, name?, text, input_json}
            current_blocks: Dict[int, Dict[str, Any]] = {}
            stop_reason = None
            step_text = ""

            async with client.stream("POST", endpoint, headers=headers, json=body) as resp:
                resp.raise_for_status()
                async for line in resp.aiter_lines():
                    if not line.startswith("data: "):
                        continue
                    raw = line[6:]
                    if raw.strip() == "[DONE]":
                        break
                    try:
                        ev = json.loads(raw)
                    except json.JSONDecodeError:
                        continue

                    etype = ev.get("type")

                    if etype == "message_start":
                        usage = ev.get("message", {}).get("usage", {})
                        total_tokens += usage.get("input_tokens") or 0

                    elif etype == "content_block_start":
                        idx = ev["index"]
                        block = ev["content_block"]
                        current_blocks[idx] = {"type": block["type"], "text": "", "input_json": ""}
                        if block["type"] == "tool_use":
                            current_blocks[idx]["id"] = block["id"]
                            current_blocks[idx]["name"] = block["name"]

                    elif etype == "content_block_delta":
                        idx = ev["index"]
                        delta = ev.get("delta", {})
                        block = current_blocks.get(idx)
                        if block is None:
                            continue
                        if delta.get("type") == "text_delta":
                            text = delta["text"]
                            step_text += text
                            block["text"] += text
                            yield {"type": "token", "text": text}
                        elif delta.get("type") == "input_json_delta":
                            block["input_json"] += delta.get("partial_json", "")

                    elif etype == "message_delta":
                        stop_reason = ev.get("delta", {}).get("stop_reason")
                        total_tokens += ev.get("usage", {}).get("output_tokens") or 0

            if step_text:
                final_text_parts.append(step_text)

            tool_uses_raw = [b for b in current_blocks.values() if b.get("type") == "tool_use"]

            if stop_reason != "tool_use" or not tool_uses_raw:
                break

            # Reconstruct the assistant content block list for history
            assistant_content = []
            for idx in sorted(current_blocks.keys()):
                b = current_blocks[idx]
                if b["type"] == "text" and b["text"]:
                    assistant_content.append({"type": "text", "text": b["text"]})
                elif b["type"] == "tool_use":
                    try:
                        input_args = json.loads(b["input_json"] or "{}")
                    except json.JSONDecodeError:
                        input_args = {}
                    assistant_content.append({
                        "type": "tool_use",
                        "id": b["id"],
                        "name": b["name"],
                        "input": input_args,
                    })
            messages.append({"role": "assistant", "content": assistant_content})

            tool_results = []
            for b in tool_uses_raw:
                tu_id = b["id"]
                name = b["name"]
                try:
                    input_args = json.loads(b["input_json"] or "{}")
                except json.JSONDecodeError:
                    input_args = {}

                yield {"type": "tool_call", "name": name, "input": input_args}

                result_text, is_error = _call_tool(db, user, name, input_args, [], changed_set)

                tool_event = {
                    "tool_use_id": tu_id,
                    "name": name,
                    "input": input_args,
                    "output": None if is_error else _safe_json_parse(result_text),
                    "error": result_text if is_error else None,
                }
                all_tool_events.append(tool_event)
                yield {"type": "tool_done", "name": name, "ok": not is_error, "tool_event": tool_event}

                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": tu_id,
                    "content": result_text,
                    "is_error": is_error,
                })
            messages.append({"role": "user", "content": tool_results})

    final_text = "\n".join(final_text_parts).strip() or "(agent przekroczył limit kroków)"
    yield {
        "type": "final",
        "reply": final_text,
        "tool_events": all_tool_events,
        "tokens_used": total_tokens,
    }
