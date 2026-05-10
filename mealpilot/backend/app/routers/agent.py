from typing import List

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.orm import Session

from .. import models, schemas
from ..db import get_db
from ..dependencies import get_current_user

router = APIRouter(prefix="/api/agent", tags=["agent"])


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
            select(models.AgentToolUse)
            .where(models.AgentToolUse.message_id == msg.id)
            .order_by(models.AgentToolUse.id)
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
