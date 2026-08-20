"""AI usage tracking with per-user monthly token + cost limits.

Limits are stored on `User` so they work for users without a household too.
Reset is lazy: on every check/record call, if the calendar month flipped
since `ai_usage_period_start`, counters are zeroed and the period rolls
forward. Admin can also force-reset via the admin endpoint.
"""

from __future__ import annotations

from datetime import UTC, datetime

from fastapi import HTTPException, status
from sqlalchemy.orm import Session

from . import models


def _ensure_period(user: models.User) -> None:
    """Roll month if needed. Mutates user in-place (caller commits)."""
    now = datetime.now(UTC)
    start = user.ai_usage_period_start
    if start is None:
        user.ai_usage_period_start = now
        user.ai_used_tokens_this_month = 0
        user.ai_used_cost_cents_this_month = 0
        return
    if start.tzinfo is None:
        start = start.replace(tzinfo=UTC)
    if (start.year, start.month) != (now.year, now.month):
        user.ai_used_tokens_this_month = 0
        user.ai_used_cost_cents_this_month = 0
        user.ai_usage_period_start = now


def check_quota(db: Session, user: models.User) -> None:
    """Raise if user cannot use AI right now. Commits any period roll."""
    _ensure_period(user)
    if not user.can_use_ai:
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Brak uprawnień do korzystania z AI")
    if user.ai_monthly_token_limit is not None and (user.ai_used_tokens_this_month or 0) >= user.ai_monthly_token_limit:
        raise HTTPException(
            status.HTTP_429_TOO_MANY_REQUESTS,
            "Miesięczny limit tokenów AI wyczerpany",
        )
    if (
        user.ai_monthly_cost_limit_cents is not None
        and (user.ai_used_cost_cents_this_month or 0) >= user.ai_monthly_cost_limit_cents
    ):
        raise HTTPException(
            status.HTTP_429_TOO_MANY_REQUESTS,
            "Miesięczny limit kosztów AI wyczerpany",
        )
    db.commit()


def record_usage(
    db: Session,
    user: models.User,
    tokens: int = 0,
    cost_cents: int = 0,
) -> None:
    """Increment counters. Caller commits."""
    _ensure_period(user)
    if tokens > 0:
        user.ai_used_tokens_this_month = (user.ai_used_tokens_this_month or 0) + tokens
    if cost_cents > 0:
        user.ai_used_cost_cents_this_month = (user.ai_used_cost_cents_this_month or 0) + cost_cents
