import json
from datetime import datetime, timezone
from typing import List

from fastapi import APIRouter, Depends, Header, HTTPException
from fastapi.responses import JSONResponse, StreamingResponse
from sqlalchemy import select
from sqlalchemy.orm import Session

from .. import models, schemas
from ..agent_runner import run_agent, stream_agent
from ..ai_usage import check_quota, record_usage
from ..db import get_db
from ..dependencies import get_current_user
from ..ratelimit import ai_limiter, conv_inflight, idempotency_cache


def _sse(event: str, data: dict) -> str:
    return f"event: {event}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"

router = APIRouter(prefix="/api/agent", tags=["agent"])


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


def _serialize_message(
    msg: models.AgentMessage,
    tool_uses_by_msg: dict[int, list[models.AgentToolUse]],
) -> schemas.AgentMessageOut:
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
            for tu in tool_uses_by_msg.get(msg.id, [])
        ],
    )


@router.get("/conversations", response_model=List[schemas.AgentConversationOut])
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
    msg_ids = [m.id for m in msgs]
    tool_uses_by_msg: dict[int, list[models.AgentToolUse]] = {mid: [] for mid in msg_ids}
    if msg_ids:
        for tu in (
            db.execute(
                select(models.AgentToolUse)
                .where(models.AgentToolUse.message_id.in_(msg_ids))
                .order_by(models.AgentToolUse.id)
            )
            .scalars()
            .all()
        ):
            tool_uses_by_msg[tu.message_id].append(tu)
    return schemas.AgentConversationDetail(
        id=conv.id,
        title=conv.title,
        model=conv.model,
        created_at=conv.created_at,
        updated_at=conv.updated_at,
        messages=[_serialize_message(m, tool_uses_by_msg) for m in msgs],
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
    idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
    user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    conv = _get_conversation(db, conv_id, user.id)

    # Replay cached result when the client retries with the same key.
    if idempotency_key:
        cached = idempotency_cache.get(f"{user.id}:{idempotency_key}")
        if cached is not None:
            return cached

    # Reject concurrent runs on the same conversation (double-click guard).
    inflight_key = f"{user.id}:{conv_id}"
    if not conv_inflight.acquire(inflight_key):
        raise HTTPException(status_code=409, detail="Run already in progress for this conversation")

    try:
        return await _do_run(conv, conv_id, user, db, idempotency_key)
    finally:
        conv_inflight.release(inflight_key)


async def _do_run(
    conv: models.AgentConversation,
    conv_id: int,
    user: models.User,
    db: Session,
    idempotency_key: str | None,
) -> schemas.AgentRunResponse:
    allowed, retry_after = ai_limiter.check(str(user.id))
    if not allowed:
        return JSONResponse(
            status_code=429,
            content={"detail": "Zbyt wiele żądań AI. Poczekaj chwilę."},
            headers={"Retry-After": str(int(retry_after))},
        )

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

    tokens_used = result.get("tokens_used") or 0
    if not tokens_used:
        # Fallback: provider didn't return usage (e.g. proxy without passthrough).
        char_count = sum(len(m.get("content") or "") for m in history) + len(reply)
        for ev in tool_events:
            char_count += len(str(ev.get("input") or "")) + len(str(ev.get("output") or ""))
        tokens_used = max(1, char_count // 4)
    record_usage(db, user, tokens=tokens_used)

    # Persist the assistant message
    now = datetime.now(timezone.utc)
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
                output=ev["output"],
                is_error=1 if ev.get("error") else 0,
                started_at=now,
                finished_at=now,
            )
        )

    conv.updated_at = now
    db.commit()
    db.refresh(assistant_msg)

    response = schemas.AgentRunResponse(
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
    )

    if idempotency_key:
        idempotency_cache.set(f"{user.id}:{idempotency_key}", response)

    return response


@router.post("/conversations/{conv_id}/stream")
async def stream_conversation(
    conv_id: int,
    user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    conv = _get_conversation(db, conv_id, user.id)

    inflight_key = f"{user.id}:{conv_id}"
    if not conv_inflight.acquire(inflight_key):
        async def _busy():
            yield _sse("error", {"detail": "Run already in progress for this conversation"})
        return StreamingResponse(_busy(), media_type="text/event-stream",
                                 headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

    async def generate():
        try:
            allowed, retry_after = ai_limiter.check(str(user.id))
            if not allowed:
                yield _sse("error", {"detail": "Zbyt wiele żądań AI. Poczekaj chwilę.", "retry_after": int(retry_after)})
                return

            try:
                check_quota(db, user)
            except HTTPException as exc:
                yield _sse("error", {"detail": exc.detail, "status": exc.status_code})
                return

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

            reply = "(brak odpowiedzi)"
            tool_events: list = []
            tokens_used = 0
            changed: list = []

            async for event in stream_agent(db, user, settings, history):
                etype = event["type"]
                if etype == "token":
                    yield _sse("token", {"text": event["text"]})
                elif etype == "tool_call":
                    yield _sse("tool_call", {"name": event["name"], "input": event["input"]})
                elif etype == "tool_done":
                    tool_events.append(event["tool_event"])
                    yield _sse("tool_done", {"name": event["name"], "ok": event["ok"]})
                elif etype == "final":
                    reply = event["reply"] or "(brak odpowiedzi)"
                    tokens_used = event.get("tokens_used") or 0
                    changed = event.get("changed") or []
                elif etype == "error":
                    yield _sse("error", {"detail": event["detail"]})
                    return

            if not tokens_used:
                char_count = sum(len(m.get("content") or "") for m in history) + len(reply)
                tokens_used = max(1, char_count // 4)
            record_usage(db, user, tokens=tokens_used)

            now = datetime.now(timezone.utc)
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
                        output=ev["output"],
                        is_error=1 if ev.get("error") else 0,
                        started_at=now,
                        finished_at=now,
                    )
                )

            conv.updated_at = now
            db.commit()
            db.refresh(assistant_msg)

            yield _sse("done", {
                "message_id": assistant_msg.id,
                "changed": changed,
                "tokens_used": tokens_used,
                "reply": reply,
            })
        except Exception as exc:
            yield _sse("error", {"detail": str(exc)})
        finally:
            conv_inflight.release(inflight_key)

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
                | (
                    (models.AgentMessage.created_at == msg.created_at)
                    & (models.AgentMessage.id > msg.id)
                )
            )
        )
        .scalars()
        .all()
    )
    for m in later:
        db.execute(
            models.AgentToolUse.__table__.delete().where(
                models.AgentToolUse.message_id == m.id
            )
        )
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
