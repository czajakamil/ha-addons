"""OpenAI-compatible agent loop."""
from __future__ import annotations

import json
import uuid
from typing import Any, AsyncGenerator, Dict, List, Optional

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


async def stream_openai(
    endpoint: str,
    api_key: str,
    model: str,
    system_prompt: str,
    history: List[Dict[str, Any]],
    db: Session,
    user: models.User,
    tool_events: List[Dict[str, Any]],
    changed_set: set,
) -> AsyncGenerator[Dict[str, Any], None]:
    messages: List[Dict[str, Any]] = [
        {"role": "system", "content": system_prompt},
        *history,
    ]
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}",
    }

    async with httpx.AsyncClient(timeout=120.0) as client:
        for _step in range(MAX_STEPS):
            body = {
                "model": model,
                "messages": messages,
                "tools": TOOL_DEFS_OPENAI,
                "tool_choice": "auto",
                "stream": True,
            }

            # index → {id, name, arguments_str}
            pending_calls: Dict[int, Dict[str, str]] = {}
            assistant_content: Optional[str] = None
            finish_reason: Optional[str] = None

            async with client.stream("POST", endpoint, headers=headers, json=body) as resp:
                resp.raise_for_status()
                async for raw_line in resp.aiter_lines():
                    if not raw_line.startswith("data: "):
                        continue
                    data_str = raw_line[6:].strip()
                    if data_str == "[DONE]":
                        break
                    try:
                        chunk = json.loads(data_str)
                    except json.JSONDecodeError:
                        continue

                    choices = chunk.get("choices") or []
                    if not choices:
                        continue
                    choice = choices[0]
                    delta = choice.get("delta", {})

                    if delta.get("content"):
                        t = delta["content"]
                        if assistant_content is None:
                            assistant_content = ""
                        assistant_content += t
                        yield {"type": "text_delta", "text": t}

                    for tc in delta.get("tool_calls") or []:
                        idx = tc.get("index", 0)
                        if idx not in pending_calls:
                            pending_calls[idx] = {"id": "", "name": "", "arguments": ""}
                        if tc.get("id"):
                            pending_calls[idx]["id"] = tc["id"]
                        fn = tc.get("function") or {}
                        if fn.get("name"):
                            pending_calls[idx]["name"] = fn["name"]
                        if fn.get("arguments"):
                            pending_calls[idx]["arguments"] += fn["arguments"]

                    fr = choice.get("finish_reason")
                    if fr:
                        finish_reason = fr

            if not pending_calls:
                break

            clean_tool_calls = [
                {
                    "id": pending_calls[i]["id"],
                    "type": "function",
                    "function": {
                        "name": pending_calls[i]["name"],
                        "arguments": pending_calls[i]["arguments"],
                    },
                }
                for i in sorted(pending_calls.keys())
            ]
            clean_msg: Dict[str, Any] = {"role": "assistant", "content": assistant_content}
            clean_msg["tool_calls"] = clean_tool_calls
            messages.append(clean_msg)

            for i in sorted(pending_calls.keys()):
                tc = pending_calls[i]
                call_id = tc["id"] or str(uuid.uuid4())
                name = tc["name"]
                try:
                    input_args = json.loads(tc["arguments"] or "{}")
                except json.JSONDecodeError:
                    input_args = {}

                yield {"type": "tool_start", "tool_use_id": call_id, "name": name, "input": input_args}
                result_text, is_error = call_tool(db, user, name, input_args, changed_set)
                tool_events.append({
                    "tool_use_id": call_id,
                    "name": name,
                    "input": input_args,
                    "output": None if is_error else safe_json_parse(result_text),
                    "error": result_text if is_error else None,
                })
                yield {
                    "type": "tool_result",
                    "tool_use_id": call_id,
                    "output": None if is_error else safe_json_parse(result_text),
                    "is_error": is_error,
                }
                messages.append({
                    "role": "tool",
                    "tool_call_id": call_id,
                    "content": result_text,
                })
