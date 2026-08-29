import json
import os
from datetime import UTC, datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse
from sqlalchemy import select
from sqlalchemy.orm import Session

from .. import models, schemas
from ..agent.prompts import DEFAULT_SYSTEM_PROMPT
from ..agent.providers import is_anthropic, stream_anthropic, stream_openai
from ..agent_runner import generate_conversation_title, run_agent
from ..ai_usage import check_quota, record_usage
from ..db import SessionLocal, get_db
from ..dependencies import get_current_user
from ..services import registry as tools_registry

router = APIRouter(prefix="/api/agent", tags=["agent"])


@router.get("/tools")
def list_agent_tools(_user: models.User = Depends(get_current_user)):
    """The agent's real tool surface, straight from the shared registry.

    The UI renders this; it used to render a hand-maintained TypeScript copy
    that had already drifted (it advertised tools the agent did not have).
    """
    return {
        "groups": [{"label": label, "icon": icon} for label, icon in tools_registry.GROUP_ORDER],
        "tools": [tools_registry.describe(spec) for spec in tools_registry.TOOL_SPECS],
    }


def _usage_status(user: models.User) -> schemas.AiUsageStatus:
    return schemas.AiUsageStatus(
        can_use_ai=bool(user.can_use_ai),
        ai_monthly_token_limit=user.ai_monthly_token_limit,
        ai_monthly_cost_limit_cents=user.ai_monthly_cost_limit_cents,
        ai_used_tokens_this_month=user.ai_used_tokens_this_month or 0,
        ai_used_cost_cents_this_month=user.ai_used_cost_cents_this_month or 0,
    )


@router.get("/usage", response_model=schemas.AiUsageStatus)
def get_ai_usage(
    user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    from ..ai_usage import _ensure_period

    _ensure_period(user)
    db.commit()
    return _usage_status(user)


@router.post("/usage/check", status_code=204)
def check_ai_quota(
    user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Frontend calls this before invoking the LLM; raises 403/429 if blocked."""
    check_quota(db, user)
    return None


@router.post("/usage/report", response_model=schemas.AiUsageStatus)
def report_ai_usage(
    payload: schemas.AiUsageReport,
    user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    record_usage(db, user, tokens=payload.tokens, cost_cents=payload.cost_cents)
    db.commit()
    return _usage_status(user)


def _get_conversation(db: Session, conv_id: int, user_id: int) -> models.AgentConversation:
    conv = db.get(models.AgentConversation, conv_id)
    if conv is None or conv.user_id != user_id:
        raise HTTPException(status_code=404, detail="Conversation not found")
    return conv


def _get_tool_use(db: Session, tool_use_id: int, user_id: int) -> models.AgentToolUse:
    tu = db.get(models.AgentToolUse, tool_use_id)
    if tu is None:
        raise HTTPException(status_code=404, detail="Tool use not found")
    msg = db.get(models.AgentMessage, tu.message_id)
    if msg is None:
        raise HTTPException(status_code=404, detail="Tool use not found")
    conv = db.get(models.AgentConversation, msg.conversation_id)
    if conv is None or conv.user_id != user_id:
        raise HTTPException(status_code=404, detail="Tool use not found")
    return tu


def _serialize_message(msg: models.AgentMessage, db: Session) -> schemas.AgentMessageOut:
    tool_uses = (
        db.execute(
            select(models.AgentToolUse).where(models.AgentToolUse.message_id == msg.id).order_by(models.AgentToolUse.id)
        )
        .scalars()
        .all()
    )
    return schemas.AgentMessageOut(
        id=msg.id,
        role=msg.role,
        content=msg.content,
        created_at=msg.created_at,
        tool_uses=[
            schemas.AgentToolUseOut(
                id=tu.id,
                tool_use_id=tu.tool_use_id,
                tool_name=tu.tool_name,
                input=tu.input or {},
                output=tu.output,
                is_error=bool(tu.is_error),
                started_at=tu.started_at,
                finished_at=tu.finished_at,
            )
            for tu in tool_uses
        ],
    )


@router.get("/conversations", response_model=list[schemas.AgentConversationOut])
def list_conversations(
    user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    rows = (
        db.execute(
            select(models.AgentConversation)
            .where(models.AgentConversation.user_id == user.id)
            .order_by(models.AgentConversation.updated_at.desc())
        )
        .scalars()
        .all()
    )
    return rows


@router.post("/conversations", response_model=schemas.AgentConversationOut)
def create_conversation(
    payload: schemas.AgentConversationCreate,
    user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    conv = models.AgentConversation(
        user_id=user.id,
        title=payload.title,
        model=payload.model,
    )
    db.add(conv)
    db.commit()
    db.refresh(conv)
    return conv


@router.get("/conversations/{conv_id}", response_model=schemas.AgentConversationDetail)
def get_conversation(
    conv_id: int,
    user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    conv = _get_conversation(db, conv_id, user.id)
    msgs = (
        db.execute(
            select(models.AgentMessage)
            .where(models.AgentMessage.conversation_id == conv.id)
            .order_by(models.AgentMessage.created_at, models.AgentMessage.id)
        )
        .scalars()
        .all()
    )
    return schemas.AgentConversationDetail(
        id=conv.id,
        title=conv.title,
        model=conv.model,
        created_at=conv.created_at,
        updated_at=conv.updated_at,
        messages=[_serialize_message(m, db) for m in msgs],
    )


@router.patch("/conversations/{conv_id}", response_model=schemas.AgentConversationOut)
def update_conversation(
    conv_id: int,
    payload: schemas.AgentConversationPatch,
    user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    conv = _get_conversation(db, conv_id, user.id)
    if payload.title is not None:
        conv.title = payload.title
    db.commit()
    db.refresh(conv)
    return conv


@router.delete("/conversations/{conv_id}", status_code=204)
def delete_conversation(
    conv_id: int,
    user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    conv = _get_conversation(db, conv_id, user.id)
    db.delete(conv)
    db.commit()
    return None


@router.post("/conversations/{conv_id}/run", response_model=schemas.AgentRunResponse)
async def run_conversation(
    conv_id: int,
    user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    conv = _get_conversation(db, conv_id, user.id)
    check_quota(db, user)

    # Get or create agent settings for this user
    settings = db.get(models.AgentSettings, user.id)
    if settings is None:
        settings = models.AgentSettings(user_id=user.id)
        db.add(settings)
        db.commit()
        db.refresh(settings)

    # Build message history in provider-neutral format (role + content)
    msgs = (
        db.execute(
            select(models.AgentMessage)
            .where(models.AgentMessage.conversation_id == conv.id)
            .order_by(models.AgentMessage.created_at, models.AgentMessage.id)
        )
        .scalars()
        .all()
    )
    history = [{"role": m.role, "content": m.content} for m in msgs]

    # Run the agent loop
    result = await run_agent(db, user, settings, history)

    reply = result["reply"] or "(brak odpowiedzi)"
    tool_events = result["tool_events"]
    changed = result["changed"]

    # Rough token estimate: char-count / 4 over prompt + reply + tool I/O.
    # agent_runner doesn't surface real usage; this keeps quota accounting
    # honest enough to prevent runaway spend.
    char_count = sum(len(m.get("content") or "") for m in history) + len(reply)
    for ev in tool_events:
        char_count += len(str(ev.get("input") or "")) + len(str(ev.get("output") or ""))
    approx_tokens = max(1, char_count // 4)
    record_usage(db, user, tokens=approx_tokens)

    # Persist the assistant message
    now = datetime.now(UTC)
    assistant_msg = models.AgentMessage(
        conversation_id=conv.id,
        role="assistant",
        content=reply,
        created_at=now,
    )
    db.add(assistant_msg)
    db.flush()

    for ev in tool_events:
        db.add(
            models.AgentToolUse(
                message_id=assistant_msg.id,
                tool_use_id=ev["tool_use_id"],
                tool_name=ev["name"],
                input=ev["input"] or {},
                output=ev.get("error") if ev.get("error") else ev["output"],
                is_error=1 if ev.get("error") else 0,
                started_at=now,
                finished_at=now,
            )
        )

    conv.updated_at = now

    # Generate LLM-based conversation title on the first user→assistant turn
    # (history at run-start was just [user-message]). Skips on agent errors.
    generated_title: str | None = None
    is_first_turn = len(history) == 1 and history[0].get("role") == "user"
    if is_first_turn and reply and not reply.startswith("❗"):
        first_user_text = str(history[0].get("content") or "")
        try:
            generated_title = await generate_conversation_title(settings.model or "", first_user_text, reply)
        except Exception:
            generated_title = None
        if generated_title:
            conv.title = generated_title

    db.commit()
    db.refresh(assistant_msg)

    return schemas.AgentRunResponse(
        reply=reply,
        tool_events=[
            schemas.AgentToolEventOut(
                tool_use_id=ev["tool_use_id"],
                name=ev["name"],
                input=ev["input"] or {},
                output=ev.get("output"),
                error=ev.get("error"),
            )
            for ev in tool_events
        ],
        changed=changed,
        message_id=assistant_msg.id,
        title=generated_title or None,
    )


@router.post("/conversations/{conv_id}/stream")
async def stream_conversation(
    conv_id: int,
    user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    conv = _get_conversation(db, conv_id, user.id)
    check_quota(db, user)

    settings = db.get(models.AgentSettings, user.id)
    if settings is None:
        settings = models.AgentSettings(user_id=user.id)
        db.add(settings)
        db.commit()
        db.refresh(settings)

    msgs = (
        db.execute(
            select(models.AgentMessage)
            .where(models.AgentMessage.conversation_id == conv.id)
            .order_by(models.AgentMessage.created_at, models.AgentMessage.id)
        )
        .scalars()
        .all()
    )
    history = [{"role": m.role, "content": m.content} for m in msgs]
    model = settings.model or ""
    system_prompt = settings.system_prompt or DEFAULT_SYSTEM_PROMPT

    # Capture ids for use inside the generator (session-independent)
    captured_conv_id = conv.id
    captured_user_id = user.id
    is_first_turn = len(history) == 1 and (history[0].get("role") == "user" if history else False)
    first_user_text = str(history[0].get("content") or "") if is_first_turn else ""

    def _sse(data: dict[str, Any]) -> str:
        return f"data: {json.dumps(data)}\n\n"

    async def generate():
        stream_db = SessionLocal()
        tool_events: list[dict[str, Any]] = []
        changed_set: set = set()
        text_parts: list[str] = []

        try:
            endpoint_url = os.environ.get("MEALPILOT_AI_API_URL", "").strip()
            api_key = os.environ.get("MEALPILOT_AI_API_KEY", "").strip()

            if not endpoint_url:
                yield _sse({"type": "error", "message": "Brak konfiguracji MEALPILOT_AI_API_URL"})
                return
            if not api_key:
                yield _sse({"type": "error", "message": "Brak konfiguracji MEALPILOT_AI_API_KEY"})
                return

            stream_user = stream_db.get(models.User, captured_user_id)
            if stream_user is None:
                yield _sse({"type": "error", "message": "Użytkownik nie znaleziony"})
                return

            if is_anthropic(endpoint_url):
                provider = stream_anthropic(
                    endpoint_url,
                    api_key,
                    model,
                    system_prompt,
                    history,
                    stream_db,
                    stream_user,
                    tool_events,
                    changed_set,
                )
            else:
                provider = stream_openai(
                    endpoint_url,
                    api_key,
                    model,
                    system_prompt,
                    history,
                    stream_db,
                    stream_user,
                    tool_events,
                    changed_set,
                )

            async for event in provider:
                if event["type"] == "text_delta":
                    text_parts.append(event["text"])
                yield _sse(event)

        except Exception as exc:
            yield _sse({"type": "error", "message": str(exc)})
            stream_db.close()
            return

        # Persist assistant message
        reply = "".join(text_parts) or "(brak odpowiedzi)"
        now = datetime.now(UTC)

        char_count = sum(len(m.get("content") or "") for m in history) + len(reply)
        for ev in tool_events:
            char_count += len(str(ev.get("input") or "")) + len(str(ev.get("output") or ""))
        approx_tokens = max(1, char_count // 4)

        stream_conv = stream_db.get(models.AgentConversation, captured_conv_id)
        if stream_conv is None:
            stream_db.close()
            return

        record_usage(stream_db, stream_user, tokens=approx_tokens)

        assistant_msg = models.AgentMessage(
            conversation_id=captured_conv_id,
            role="assistant",
            content=reply,
            created_at=now,
        )
        stream_db.add(assistant_msg)
        stream_db.flush()

        for ev in tool_events:
            stream_db.add(
                models.AgentToolUse(
                    message_id=assistant_msg.id,
                    tool_use_id=ev["tool_use_id"],
                    tool_name=ev["name"],
                    input=ev["input"] or {},
                    output=ev.get("error") if ev.get("error") else ev["output"],
                    is_error=1 if ev.get("error") else 0,
                    started_at=now,
                    finished_at=now,
                )
            )

        stream_conv.updated_at = now

        generated_title: str | None = None
        if is_first_turn and reply and not reply.startswith("❗"):
            try:
                generated_title = await generate_conversation_title(model, first_user_text, reply)
            except Exception:
                generated_title = None
            if generated_title:
                stream_conv.title = generated_title

        stream_db.commit()
        stream_db.refresh(assistant_msg)
        stream_db.close()

        yield _sse(
            {
                "type": "done",
                "message_id": assistant_msg.id,
                "changed": sorted(changed_set),
                "title": generated_title,
            }
        )

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.post(
    "/conversations/{conv_id}/messages",
    response_model=schemas.AgentMessageOut,
)
def append_message(
    conv_id: int,
    payload: schemas.AgentMessageCreate,
    user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    conv = _get_conversation(db, conv_id, user.id)
    msg = models.AgentMessage(
        conversation_id=conv.id,
        role=payload.role,
        content=payload.content,
    )
    db.add(msg)
    db.flush()

    for tu in payload.tool_uses:
        db.add(
            models.AgentToolUse(
                message_id=msg.id,
                tool_use_id=tu.tool_use_id,
                tool_name=tu.tool_name,
                input=tu.input,
                output=tu.output,
                is_error=1 if tu.is_error else 0,
                finished_at=tu.finished_at,
            )
        )

    # bump conversation updated_at
    conv.updated_at = msg.created_at
    db.commit()
    db.refresh(msg)
    return _serialize_message(msg, db)


@router.patch(
    "/messages/{msg_id}",
    response_model=schemas.AgentConversationDetail,
)
def edit_message(
    msg_id: int,
    payload: schemas.AgentMessageEdit,
    user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    msg = db.get(models.AgentMessage, msg_id)
    if msg is None:
        raise HTTPException(status_code=404, detail="Message not found")
    conv = db.get(models.AgentConversation, msg.conversation_id)
    if conv is None or conv.user_id != user.id:
        raise HTTPException(status_code=404, detail="Message not found")
    if msg.role != "user":
        raise HTTPException(status_code=400, detail="Only user messages can be edited")

    # Delete every message that comes after this one in the conversation.
    later = (
        db.execute(
            select(models.AgentMessage)
            .where(models.AgentMessage.conversation_id == conv.id)
            .where(
                (models.AgentMessage.created_at > msg.created_at)
                | ((models.AgentMessage.created_at == msg.created_at) & (models.AgentMessage.id > msg.id))
            )
        )
        .scalars()
        .all()
    )
    for m in later:
        db.execute(models.AgentToolUse.__table__.delete().where(models.AgentToolUse.message_id == m.id))
        db.delete(m)

    msg.content = payload.content
    conv.updated_at = msg.created_at
    db.commit()

    msgs = (
        db.execute(
            select(models.AgentMessage)
            .where(models.AgentMessage.conversation_id == conv.id)
            .order_by(models.AgentMessage.created_at, models.AgentMessage.id)
        )
        .scalars()
        .all()
    )
    return schemas.AgentConversationDetail(
        id=conv.id,
        title=conv.title,
        model=conv.model,
        created_at=conv.created_at,
        updated_at=conv.updated_at,
        messages=[_serialize_message(m, db) for m in msgs],
    )


@router.patch(
    "/tool_uses/{tool_use_id}",
    response_model=schemas.AgentToolUseOut,
)
def update_tool_use(
    tool_use_id: int,
    payload: schemas.AgentToolUsePatch,
    user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    tu = _get_tool_use(db, tool_use_id, user.id)
    if payload.output is not None:
        tu.output = payload.output
    if payload.is_error is not None:
        tu.is_error = 1 if payload.is_error else 0
    if payload.finished_at is not None:
        tu.finished_at = payload.finished_at
    db.commit()
    db.refresh(tu)
    return schemas.AgentToolUseOut(
        id=tu.id,
        tool_use_id=tu.tool_use_id,
        tool_name=tu.tool_name,
        input=tu.input or {},
        output=tu.output,
        is_error=bool(tu.is_error),
        started_at=tu.started_at,
        finished_at=tu.finished_at,
    )
