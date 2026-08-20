"""Anthropic agent loop."""

from __future__ import annotations

import json
import uuid
from collections.abc import AsyncGenerator
from typing import Any

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
    history: list[dict[str, Any]],
    db: Session,
    user: models.User,
    tool_events: list[dict[str, Any]],
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
                tool_events.append(
                    {
                        "tool_use_id": tu_id,
                        "name": name,
                        "input": input_args,
                        "output": None if is_error else safe_json_parse(result_text),
                        "error": result_text if is_error else None,
                    }
                )
                tool_results.append(
                    {
                        "type": "tool_result",
                        "tool_use_id": tu_id,
                        "content": result_text,
                        "is_error": is_error,
                    }
                )
            messages.append({"role": "user", "content": tool_results})

    return final_text or "(agent przekroczył limit kroków)"


async def stream_anthropic(
    endpoint: str,
    api_key: str,
    model: str,
    system_prompt: str,
    history: list[dict[str, Any]],
    db: Session,
    user: models.User,
    tool_events: list[dict[str, Any]],
    changed_set: set,
) -> AsyncGenerator[dict[str, Any], None]:
    messages = list(history)
    headers = {
        "Content-Type": "application/json",
        "x-api-key": api_key,
        "anthropic-version": "2023-06-01",
    }

    async with httpx.AsyncClient(timeout=120.0) as client:
        for _step in range(MAX_STEPS):
            body = {
                "model": model,
                "max_tokens": 4096,
                "system": system_prompt,
                "messages": messages,
                "tools": TOOL_DEFS,
                "stream": True,
            }

            blocks: dict[int, dict[str, Any]] = {}
            stop_reason: str | None = None

            async with client.stream("POST", endpoint, headers=headers, json=body) as resp:
                resp.raise_for_status()
                async for raw_line in resp.aiter_lines():
                    if not raw_line.startswith("data: "):
                        continue
                    payload = json.loads(raw_line[6:])
                    ev_type = payload.get("type")

                    if ev_type == "content_block_start":
                        idx = payload["index"]
                        cb = payload["content_block"]
                        blocks[idx] = {"type": cb["type"]}
                        if cb["type"] == "text":
                            blocks[idx]["text"] = cb.get("text", "")
                        elif cb["type"] == "tool_use":
                            blocks[idx].update(
                                {
                                    "id": cb.get("id", str(uuid.uuid4())),
                                    "name": cb.get("name", ""),
                                    "input_json": "",
                                }
                            )

                    elif ev_type == "content_block_delta":
                        idx = payload["index"]
                        delta = payload["delta"]
                        if delta["type"] == "text_delta":
                            t = delta.get("text", "")
                            blocks[idx]["text"] = blocks[idx].get("text", "") + t
                            yield {"type": "text_delta", "text": t}
                        elif delta["type"] == "input_json_delta":
                            blocks[idx]["input_json"] = blocks[idx].get("input_json", "") + delta.get(
                                "partial_json", ""
                            )

                    elif ev_type == "content_block_stop":
                        idx = payload["index"]
                        block = blocks.get(idx, {})
                        if block.get("type") == "tool_use":
                            try:
                                input_args = json.loads(block.get("input_json") or "{}")
                            except json.JSONDecodeError:
                                input_args = {}
                            tu_id = block["id"]
                            name = block["name"]
                            block["_parsed_input"] = input_args

                            yield {
                                "type": "tool_start",
                                "tool_use_id": tu_id,
                                "name": name,
                                "input": input_args,
                            }
                            result_text, is_error = call_tool(db, user, name, input_args, changed_set)
                            block["_result_text"] = result_text
                            block["_is_error"] = is_error
                            tool_events.append(
                                {
                                    "tool_use_id": tu_id,
                                    "name": name,
                                    "input": input_args,
                                    "output": None if is_error else safe_json_parse(result_text),
                                    "error": result_text if is_error else None,
                                }
                            )
                            yield {
                                "type": "tool_result",
                                "tool_use_id": tu_id,
                                "output": None if is_error else safe_json_parse(result_text),
                                "is_error": is_error,
                            }

                    elif ev_type == "message_delta":
                        stop_reason = payload.get("delta", {}).get("stop_reason")

            if stop_reason != "tool_use":
                break

            api_blocks = []
            for idx in sorted(blocks.keys()):
                b = blocks[idx]
                if b["type"] == "text":
                    api_blocks.append({"type": "text", "text": b.get("text", "")})
                elif b["type"] == "tool_use":
                    api_blocks.append(
                        {
                            "type": "tool_use",
                            "id": b["id"],
                            "name": b["name"],
                            "input": b.get("_parsed_input", {}),
                        }
                    )
            messages.append({"role": "assistant", "content": api_blocks})

            tool_results = []
            for idx in sorted(blocks.keys()):
                b = blocks[idx]
                if b["type"] == "tool_use":
                    tool_results.append(
                        {
                            "type": "tool_result",
                            "tool_use_id": b["id"],
                            "content": b.get("_result_text", ""),
                            "is_error": b.get("_is_error", False),
                        }
                    )
            messages.append({"role": "user", "content": tool_results})
