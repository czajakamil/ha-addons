"""Strumień SSE providera Anthropic — na sztucznym strumieniu, bez sieci.

`httpx.AsyncClient` jest podmieniany w przestrzeni nazw modułu providera, więc
testujemy dokładnie tę pętlę, która czyta `data: ` z odpowiedzi.
"""

import asyncio
import json

import pytest

from app.agent.providers import anthropic as provider

pytestmark = pytest.mark.integration


class _FakeResponse:
    def __init__(self, lines: list[str]) -> None:
        self._lines = lines

    def raise_for_status(self) -> None:
        return None

    async def aiter_lines(self):
        for line in self._lines:
            yield line


class _FakeStream:
    def __init__(self, lines: list[str]) -> None:
        self._lines = lines

    async def __aenter__(self):
        return _FakeResponse(self._lines)

    async def __aexit__(self, *_exc):
        return False


def _fake_client_factory(lines: list[str]):
    class _FakeClient:
        def __init__(self, *_a, **_kw):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_exc):
            return False

        def stream(self, *_a, **_kw):
            return _FakeStream(lines)

    return _FakeClient


def _data(payload: dict) -> str:
    return "data: " + json.dumps(payload, ensure_ascii=False)


def _collect(monkeypatch, lines: list[str]) -> list[dict]:
    monkeypatch.setattr(provider.httpx, "AsyncClient", _fake_client_factory(lines))

    async def _run():
        out = []
        gen = provider.stream_anthropic(
            "https://api.anthropic.com/v1/messages",
            "k",
            "model",
            "system",
            [{"role": "user", "content": "cześć"}],
            None,  # db — nietykane, dopóki nie ma tool_use
            None,  # user
            [],
            set(),
        )
        async for event in gen:
            out.append(event)
        return out

    return asyncio.run(_run())


_TEXT_START = _data({"type": "content_block_start", "index": 0, "content_block": {"type": "text", "text": ""}})
_STOP = _data({"type": "message_delta", "delta": {"stop_reason": "end_turn"}})


def _delta(text: str) -> str:
    return _data({"type": "content_block_delta", "index": 0, "delta": {"type": "text_delta", "text": text}})


def test_unparsable_line_does_not_kill_the_stream(monkeypatch):
    events = _collect(
        monkeypatch,
        [
            _TEXT_START,
            _delta("Cześć"),
            "data: {to nie jest json",  # ucięta ramka w środku strumienia
            _delta(", jak mogę pomóc?"),
            _data({"type": "content_block_stop", "index": 0}),
            _STOP,
        ],
    )
    assert [e["text"] for e in events if e["type"] == "text_delta"] == ["Cześć", ", jak mogę pomóc?"]


def test_keepalive_and_event_lines_are_ignored(monkeypatch):
    events = _collect(
        monkeypatch,
        ["event: content_block_start", _TEXT_START, "", ": ping", _delta("ok"), _STOP],
    )
    assert [e["text"] for e in events if e["type"] == "text_delta"] == ["ok"]


def test_error_event_surfaces_to_the_caller(monkeypatch):
    """Bez tego strumień kończył się cicho i użytkownik dostawał „(brak odpowiedzi)"."""
    lines = [
        _TEXT_START,
        _delta("Zaczynam"),
        _data({"type": "error", "error": {"type": "overloaded_error", "message": "Overloaded"}}),
        _delta("tego już nie zobaczysz"),
        _STOP,
    ]
    with pytest.raises(RuntimeError) as exc:
        _collect(monkeypatch, lines)
    assert "overloaded_error" in str(exc.value)
    assert "Overloaded" in str(exc.value)


def test_error_event_without_details_still_raises(monkeypatch):
    with pytest.raises(RuntimeError):
        _collect(monkeypatch, [_TEXT_START, _data({"type": "error"}), _STOP])
