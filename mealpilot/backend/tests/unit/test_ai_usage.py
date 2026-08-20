from datetime import UTC, datetime, timedelta

import pytest
from fastapi import HTTPException

from app import models
from app.ai_usage import _ensure_period, check_quota, record_usage

pytestmark = pytest.mark.unit


def _user(**kw):
    u = models.User(username="x", password_hash="h")
    u.can_use_ai = True
    u.ai_monthly_token_limit = None
    u.ai_monthly_cost_limit_cents = None
    u.ai_used_tokens_this_month = 0
    u.ai_used_cost_cents_this_month = 0
    u.ai_usage_period_start = datetime.now(UTC)
    for k, v in kw.items():
        setattr(u, k, v)
    return u


def test_ensure_period_rolls_over_on_new_month():
    last_month = datetime.now(UTC) - timedelta(days=40)
    u = _user(
        ai_usage_period_start=last_month,
        ai_used_tokens_this_month=999,
        ai_used_cost_cents_this_month=500,
    )
    _ensure_period(u)
    assert u.ai_used_tokens_this_month == 0
    assert u.ai_used_cost_cents_this_month == 0
    assert (u.ai_usage_period_start.year, u.ai_usage_period_start.month) == (
        datetime.now(UTC).year,
        datetime.now(UTC).month,
    )


def test_ensure_period_keeps_counters_in_same_month():
    u = _user(ai_used_tokens_this_month=123)
    _ensure_period(u)
    assert u.ai_used_tokens_this_month == 123


def test_check_quota_blocks_when_ai_disabled(db_session):
    u = _user(can_use_ai=False)
    with pytest.raises(HTTPException) as exc:
        check_quota(db_session, u)
    assert exc.value.status_code == 403


def test_check_quota_blocks_on_token_limit(db_session):
    u = _user(ai_monthly_token_limit=100, ai_used_tokens_this_month=100)
    with pytest.raises(HTTPException) as exc:
        check_quota(db_session, u)
    assert exc.value.status_code == 429


def test_check_quota_blocks_on_cost_limit(db_session):
    u = _user(ai_monthly_cost_limit_cents=50, ai_used_cost_cents_this_month=50)
    with pytest.raises(HTTPException) as exc:
        check_quota(db_session, u)
    assert exc.value.status_code == 429


def test_check_quota_passes_under_limits(db_session):
    u = _user(ai_monthly_token_limit=100, ai_used_tokens_this_month=99)
    check_quota(db_session, u)  # nie podnosi wyjątku


def test_record_usage_increments():
    u = _user()
    record_usage(None, u, tokens=10, cost_cents=5)  # caller commituje; tu db nieużywany
    record_usage(None, u, tokens=3)
    assert u.ai_used_tokens_this_month == 13
    assert u.ai_used_cost_cents_this_month == 5
