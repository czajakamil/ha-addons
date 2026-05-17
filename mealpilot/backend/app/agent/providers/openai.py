from __future__ import annotations

import json
import uuid
from typing import Any, AsyncGenerator, Dict, List, Tuple

import httpx
from sqlalchemy.orm import Session

from ... import models
from ..registry import _REGISTRY, _call_tool, _safe_json_parse

MAX_STEPS = 10


async def _run_openai(
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
    messages: List[Dict[str, Any]] = [
        {"role": "system", "content": system_prompt},
        *history,
    ]
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}",
    }
    tool_defs = [
        {"type": "function", "function": {"name": r.name, "description": r.description, "parameters": r.input_schema}}
        for r in _REGISTRY
    ]
    final_text = ""
    total_tokens = 0

    async with httpx.AsyncClient(timeout=120.0) as client:
        for _step in range(MAX_STEPS):
            body = {
                "model": model,
                "messages": messages,
                "tools": tool_defs,
            }
            resp = await client.post(endpoint, headers=headers, json=body)
            resp.raise_for_status()
            data = resp.json()

            usage = data.get("usage") or {}
            total_tokens += usage.get("total_tokens") or (
                (usage.get("prompt_tokens") or 0) + (usage.get("completion_tokens") or 0)
            )

            choices = data.get("choices") or []
            if not choices:
                err = data.get("error", {})
                err_msg = err.get("message", json.dumps(data)) if isinstance(err, dict) else str(err)
                raise RuntimeError(f"OpenAI: brak pola 'choices' — {err_msg}")

            msg = choices[0]["message"]
            if msg.get("content"):
                final_text = msg["content"].strip()

            tool_calls = msg.get("tool_calls") or []
            if not tool_calls:
                break

            messages.append(msg)
            for call in tool_calls:
                call_id = call.get("id", str(uuid.uuid4()))
                name = call.get("function", {}).get("name", "")
                try:
                    input_args = json.loads(call.get("function", {}).get("arguments", "{}") or "{}")
                except json.JSONDecodeError:
                    input_args = {}

                result_text, is_error = _call_tool(db, user, name, input_args, [], changed_set)
                tool_events.append({
                    "tool_use_id": call_id,
                    "name": name,
                    "input": input_args,
                    "output": None if is_error else _safe_json_parse(result_text),
                    "error": result_text if is_error else None,
                })
                messages.append({
                    "role": "tool",
                    "tool_call_id": call_id,
                    "content": result_text,
                })

    return final_text or "(agent przekroczył limit kroków)", total_tokens


async def _stream_openai(
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
    messages: List[Dict[str, Any]] = [
        {"role": "system", "content": system_prompt},
        *history,
    ]
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}",
    }
    tool_defs = [
        {"type": "function", "function": {"name": r.name, "description": r.description, "parameters": r.input_schema}}
        for r in _REGISTRY
    ]
    final_text_parts: List[str] = []
    all_tool_events: List[Dict[str, Any]] = []
    total_tokens = 0

    async with httpx.AsyncClient(timeout=120.0) as client:
        for _step in range(MAX_STEPS):
            body = {
                "model": model,
                "messages": messages,
                "tools": tool_defs,
                "stream": True,
                "stream_options": {"include_usage": True},
            }

            # idx -> {id, name, args_json}
            tool_call_acc: Dict[int, Dict[str, Any]] = {}
            step_text = ""
            finish_reason = None

            async with client.stream("POST", endpoint, headers=headers, json=body) as resp:
                resp.raise_for_status()
                async for line in resp.aiter_lines():
                    if not line.startswith("data: "):
                        continue
                    raw = line[6:]
                    if raw.strip() == "[DONE]":
                        break
                    try:
                        chunk = json.loads(raw)
                    except json.JSONDecodeError:
                        continue

                    # Usage arrives in the final chunk (with stream_options)
                    if chunk.get("usage"):
                        usage = chunk["usage"]
                        total_tokens += usage.get("total_tokens") or (
                            (usage.get("prompt_tokens") or 0) + (usage.get("completion_tokens") or 0)
                        )

                    choices = chunk.get("choices") or []
                    if not choices:
                        continue
                    choice = choices[0]
                    delta = choice.get("delta", {})

                    if delta.get("content"):
                        text = delta["content"]
                        step_text += text
                        yield {"type": "token", "text": text}

                    for tc in delta.get("tool_calls") or []:
                        idx = tc["index"]
                        if idx not in tool_call_acc:
                            tool_call_acc[idx] = {"id": "", "name": "", "args_json": ""}
                        if tc.get("id"):
                            tool_call_acc[idx]["id"] = tc["id"]
                        fn = tc.get("function", {})
                        if fn.get("name"):
                            tool_call_acc[idx]["name"] = fn["name"]
                        if fn.get("arguments"):
                            tool_call_acc[idx]["args_json"] += fn["arguments"]

                    if choice.get("finish_reason"):
                        finish_reason = choice["finish_reason"]

            if step_text:
                final_text_parts.append(step_text)

            tool_calls = list(tool_call_acc.values())
            if finish_reason != "tool_calls" or not tool_calls:
                break

            # Build assistant message for history
            assistant_msg: Dict[str, Any] = {"role": "assistant", "content": step_text or None}
            assistant_msg["tool_calls"] = [
                {
                    "id": tc["id"] or str(uuid.uuid4()),
                    "type": "function",
                    "function": {"name": tc["name"], "arguments": tc["args_json"]},
                }
                for tc in tool_calls
            ]
            messages.append(assistant_msg)

            for tc in tool_calls:
                name = tc["name"]
                call_id = tc["id"] or str(uuid.uuid4())
                try:
                    input_args = json.loads(tc["args_json"] or "{}")
                except json.JSONDecodeError:
                    input_args = {}

                yield {"type": "tool_call", "name": name, "input": input_args}

                result_text, is_error = _call_tool(db, user, name, input_args, [], changed_set)

                tool_event = {
                    "tool_use_id": call_id,
                    "name": name,
                    "input": input_args,
                    "output": None if is_error else _safe_json_parse(result_text),
                    "error": result_text if is_error else None,
                }
                all_tool_events.append(tool_event)
                yield {"type": "tool_done", "name": name, "ok": not is_error, "tool_event": tool_event}

                messages.append({
                    "role": "tool",
                    "tool_call_id": call_id,
                    "content": result_text,
                })

    final_text = "\n".join(final_text_parts).strip() or "(agent przekroczył limit kroków)"
    yield {
        "type": "final",
        "reply": final_text,
        "tool_events": all_tool_events,
        "tokens_used": total_tokens,
    }
