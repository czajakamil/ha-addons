#!/usr/bin/env python3
"""
CatPrint MCP Server
===================
Exposes the CatPrint thermal printer as MCP tools for AI agents
(Claude Desktop, Claude Code, custom agents, etc.).

Requires the CatPrint Flask server to be running.
Default URL: http://localhost:5123  (override via CATPRINT_URL env var)

Claude Desktop config  (~/.config/claude/claude_desktop_config.json on Linux,
                        ~/Library/Application Support/Claude/claude_desktop_config.json on macOS):

  {
    "mcpServers": {
      "catprint": {
        "command": "/path/to/files/.venv/bin/python",
        "args": ["/path/to/files/mcp_server.py"],
        "env": { "CATPRINT_URL": "http://localhost:5123" }
      }
    }
  }

Available tools (9):
  get_printer_status     — status BLE, liczniki, flagi
  scan_printer           — skanowanie BLE i połączenie
  print_text             — druk tekstu (font_size, align)
  print_shopping_list    — lista zakupów z checkboxami
  print_notification     — karta powiadomienia
  feed_paper             — przesunięcie papieru
  list_templates         — lista szablonów z SQLite
  print_from_template    — druk szablonu po ID
  get_print_history      — historia wydruków z SQLite
"""

import os
from typing import Any

import httpx
from mcp.server.fastmcp import FastMCP

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

BASE_URL = os.environ.get("CATPRINT_URL", "http://localhost:5123").rstrip("/")
TIMEOUT = 60.0  # BLE operations can take up to ~30 s

# ---------------------------------------------------------------------------
# FastMCP server instance
# ---------------------------------------------------------------------------

mcp = FastMCP(
    "CatPrint HA",
    instructions=(
        "You control a GOTOOGO C15 / MXW01 BLE thermal printer through the CatPrint service. "
        "Workflow: (1) call get_printer_status — if status is 'disconnected', call scan_printer first. "
        "(2) Choose the right print tool for the job. "
        "(3) Optionally call feed_paper(lines=3) after printing to create a tear-off margin. "
        "Text longer than ~400 chars should be split across multiple print_text calls. "
        "Prefer plain ASCII/Polish text — complex unicode emoji may not render correctly on the thermal head."
    ),
)

# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def _request(method: str, path: str, payload: dict | None = None, **params) -> Any:
    url = f"{BASE_URL}{path}"
    try:
        if method == "GET":
            r = httpx.get(url, params=params or None, timeout=TIMEOUT)
        elif method == "POST":
            r = httpx.post(url, json=payload, timeout=TIMEOUT)
        elif method == "DELETE":
            r = httpx.delete(url, timeout=TIMEOUT)
        else:
            raise ValueError(f"Unknown method: {method}")
        r.raise_for_status()
        return r.json()
    except httpx.ConnectError:
        raise RuntimeError(
            f"Cannot reach CatPrint server at {BASE_URL}. "
            "Make sure app.py is running (e.g. python app.py)."
        )
    except httpx.HTTPStatusError as exc:
        body = exc.response.text[:300]
        raise RuntimeError(f"CatPrint API error {exc.response.status_code}: {body}")


def _get(path: str, **params) -> Any:
    return _request("GET", path, **params)


def _post(path: str, payload: dict | None = None) -> Any:
    return _request("POST", path, payload)


def _delete(path: str) -> Any:
    return _request("DELETE", path)


# ---------------------------------------------------------------------------
# Tools — Printer control
# ---------------------------------------------------------------------------

@mcp.tool()
def get_printer_status() -> dict:
    """
    Return the current status of the thermal printer and service capabilities.

    Response fields:
      printer.status    — "idle" | "disconnected" | "printing" | "connecting" | "error"
      printer.name      — BLE device name (e.g. "MXW01"), empty if not connected
      printer.address   — BLE MAC address, empty if not connected
      printer.print_count — prints performed since server start
      printer.last_print  — ISO timestamp of last successful print
      printer.error     — last error message (if status == "error")
      capabilities.*    — has_pil, has_bleak feature flags
    """
    return _get("/api/status")


@mcp.tool()
def scan_printer() -> dict:
    """
    Scan for nearby BLE thermal printers and connect to the first one found.

    Call this when get_printer_status() returns status="disconnected".
    Scanning takes up to 10 seconds.

    Returns {"status": "found", "printer": {"name": ..., "address": ...}}
    or {"status": "not_found"} with HTTP 404.
    """
    return _post("/api/scan")


@mcp.tool()
def print_text(text: str, font_size: int = 24, align: str = "left") -> dict:
    """
    Print plain text on the thermal printer.

    The text is rendered to a bitmap using PIL/DejaVu, dithered to 1-bit,
    and streamed to the printer over BLE.

    Args:
        text      : Text to print. Use \\n for line breaks. Max ~500 chars.
                    Long paragraphs are auto-wrapped to fit the 384 px paper width.
        font_size : Font size in pixels. Range 8–48. Default 24.
                    12–16  → small, dense output
                    22–28  → normal reading size (recommended default)
                    36–48  → headlines / large labels
        align     : Text alignment — "left" | "center" | "right". Default "left".

    Returns:
        {"status": "ok", "rows_printed": <int>}
    """
    return _post("/api/print/text", {
        "text": text,
        "font_size": int(font_size),
        "align": align,
    })




@mcp.tool()
def print_shopping_list(items: list[str], title: str = "Lista zakupów") -> dict:
    """
    Print a formatted checklist / shopping list with checkbox squares.

    Each item receives a □ checkbox. Separator lines and a timestamp are
    added automatically at the bottom.

    Args:
        items : List of item strings. 1–50 items recommended.
                Example: ["Mleko", "Chleb", "Masło", "Jajka"]
        title : Title printed at the top. Default: "Lista zakupów".

    Returns:
        {"status": "ok", "items_count": <int>}
    """
    return _post("/api/print/ha-shopping-list", {"items": items, "title": title})


@mcp.tool()
def print_notification(title: str, message: str, source: str = "Agent") -> dict:
    """
    Print a notification card on the thermal printer.

    Formatted with a bold title banner (━ separator), body text, and a
    footer with source name and current time.

    Args:
        title   : Short notification title, e.g. "Meeting in 10 min".
        message : Body text. Supports \\n line breaks.
        source  : Sender label shown in the footer. Default: "Agent".

    Returns:
        {"status": "ok"}
    """
    return _post("/api/print/ha-notification", {
        "title": title,
        "message": message,
        "source": source,
    })


@mcp.tool()
def feed_paper(lines: int = 3) -> dict:
    """
    Feed blank paper through the printer to add spacing or a tear-off margin.

    Call this after a print job so the printed area clears the tear bar.

    Args:
        lines : Number of blank lines to feed. Clamped to 1–20. Default: 3.

    Returns:
        {"status": "ok"}
    """
    return _post("/api/feed", {"lines": max(1, min(20, lines))})


# ---------------------------------------------------------------------------
# Tools — Templates & History
# ---------------------------------------------------------------------------

@mcp.tool()
def list_templates() -> list:
    """
    Return all saved text templates from the SQLite database.

    Each entry: {id, name, text, font_size, created_at}.
    Use print_from_template(id) to print a template, or read .text / .font_size
    and call print_text() directly to customise before printing.
    """
    return _get("/api/templates")


@mcp.tool()
def print_from_template(template_id: int) -> dict:
    """
    Print a saved text template by its numeric database ID.

    Fetches the stored text and font_size, then sends it to the printer.
    Use list_templates() first to discover available IDs and names.

    Args:
        template_id : Numeric ID of the template (from list_templates).

    Returns:
        {"status": "ok", "rows_printed": <int>}

    Raises:
        ValueError if the template ID does not exist.
    """
    templates = _get("/api/templates")
    tmpl = next((t for t in templates if t["id"] == template_id), None)
    if tmpl is None:
        available = [f"{t['id']} ({t['name']})" for t in templates]
        raise ValueError(
            f"Template {template_id} not found. "
            f"Available: {available if available else 'none — create one in the web UI'}"
        )
    return _post("/api/print/text", {"text": tmpl["text"], "font_size": tmpl["font_size"]})


@mcp.tool()
def get_print_history(limit: int = 10) -> list:
    """
    Return recent print history entries from the SQLite database.

    Each entry: {id, type, summary, rows, font_size, created_at}.
    Ordered newest first.

    Types: "text" | "image" | "shopping_list" | "notification"

    Args:
        limit : Maximum entries to return. Range 1–100. Default: 10.
    """
    return _get("/api/history", limit=max(1, min(100, limit)))


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    mcp.run()
