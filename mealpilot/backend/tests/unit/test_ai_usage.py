"""Unit tests for backend/app/ai_usage.py.

Uses SQLite in-memory databases so no external services are needed.
freezegun controls wall-clock time so that _ensure_period() sees the
dates we intend.
"""
from __future__ import annotations

from datetime import datetime, timezone

import pytest
from freezegun import freeze_time
from fastapi import HTTPException
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker, Session

from app.db import Base
from app.models import User
from app.ai_usage import check_quota, record_usage, _ensure_period


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def db() -> Session:
    """Provide an SQLite in-memory session with the full schema."""
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        future=True,
    )

    @event.listens_for(engine, "connect")
    def _fk_on(conn, _rec):
        conn.execute("PRAGMA foreign_keys=ON")

    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
    session = SessionLocal()
    yield session
    session.close()
    engine.dispose()


def make_user(
    db: Session,
    username: str = "testuser",
    *,
    can_use_ai: bool = True,
    ai_monthly_token_limit: int | None = None,
    ai_monthly_cost_limit_cents: int | None = None,
    ai_used_tokens_this_month: int = 0,
    ai_used_cost_cents_this_month: int = 0,
    ai_usage_period_start: datetime | None = None,
) -> User:
    """Create and persist a User with sensible AI defaults."""
    if ai_usage_period_start is None:
        ai_usage_period_start = datetime.now(timezone.utc)
    user = User(
        username=username,
        password_hash="hashed",
        can_use_ai=can_use_ai,
        ai_monthly_token_limit=ai_monthly_token_limit,
        ai_monthly_cost_limit_cents=ai_monthly_cost_limit_cents,
        ai_used_tokens_this_month=ai_used_tokens_this_month,
        ai_used_cost_cents_this_month=ai_used_cost_cents_this_month,
        ai_usage_period_start=ai_usage_period_start,
    )
    db.add(user)
    db.commit()
    db.refresh(user)
    return user


# ---------------------------------------------------------------------------
# _ensure_period
# ---------------------------------------------------------------------------

class TestEnsurePeriod:
    def test_same_month_does_not_reset_counters(self, db):
        with freeze_time("2024-03-15"):
            user = make_user(
                db,
                ai_used_tokens_this_month=500,
                ai_used_cost_cents_this_month=10,
                ai_usage_period_start=datetime(2024, 3, 1, tzinfo=timezone.utc),
            )
            _ensure_period(user)
        assert user.ai_used_tokens_this_month == 500
        assert user.ai_used_cost_cents_this_month == 10

    def test_new_month_resets_counters(self, db):
        with freeze_time("2024-04-01"):
            user = make_user(
                db,
                ai_used_tokens_this_month=1000,
                ai_used_cost_cents_this_month=50,
                ai_usage_period_start=datetime(2024, 3, 15, tzinfo=timezone.utc),
            )
            _ensure_period(user)
        assert user.ai_used_tokens_this_month == 0
        assert user.ai_used_cost_cents_this_month == 0

    def test_new_month_updates_period_start(self, db):
        with freeze_time("2024-04-01 12:00:00"):
            user = make_user(
                db,
                ai_usage_period_start=datetime(2024, 3, 1, tzinfo=timezone.utc),
            )
            _ensure_period(user)
        assert user.ai_usage_period_start.year == 2024
        assert user.ai_usage_period_start.month == 4

    def test_none_period_start_initialises(self, db):
        with freeze_time("2024-06-15"):
            user = make_user(db)
            user.ai_usage_period_start = None  # type: ignore[assignment]
            _ensure_period(user)
        assert user.ai_usage_period_start is not None
        assert user.ai_used_tokens_this_month == 0


# ---------------------------------------------------------------------------
# check_quota
# ---------------------------------------------------------------------------

class TestCheckQuota:
    def test_no_limits_does_not_raise(self, db):
        with freeze_time("2024-01-10"):
            user = make_user(db)
            check_quota(db, user)  # must not raise

    def test_ai_disabled_raises_403(self, db):
        with freeze_time("2024-01-10"):
            user = make_user(db, can_use_ai=False)
            with pytest.raises(HTTPException) as exc_info:
                check_quota(db, user)
        assert exc_info.value.status_code == 403

    def test_token_limit_exceeded_raises_429(self, db):
        with freeze_time("2024-01-10"):
            user = make_user(
                db,
                ai_monthly_token_limit=100,
                ai_used_tokens_this_month=100,
            )
            with pytest.raises(HTTPException) as exc_info:
                check_quota(db, user)
        assert exc_info.value.status_code == 429

    def test_cost_limit_exceeded_raises_429(self, db):
        with freeze_time("2024-01-10"):
            user = make_user(
                db,
                ai_monthly_cost_limit_cents=500,
                ai_used_cost_cents_this_month=500,
            )
            with pytest.raises(HTTPException) as exc_info:
                check_quota(db, user)
        assert exc_info.value.status_code == 429

    def test_just_under_token_limit_does_not_raise(self, db):
        with freeze_time("2024-01-10"):
            user = make_user(
                db,
                ai_monthly_token_limit=100,
                ai_used_tokens_this_month=99,
            )
            check_quota(db, user)  # must not raise

    def test_just_under_cost_limit_does_not_raise(self, db):
        with freeze_time("2024-01-10"):
            user = make_user(
                db,
                ai_monthly_cost_limit_cents=500,
                ai_used_cost_cents_this_month=499,
            )
            check_quota(db, user)  # must not raise

    def test_no_token_limit_set_does_not_raise_regardless_of_usage(self, db):
        with freeze_time("2024-01-10"):
            user = make_user(
                db,
                ai_monthly_token_limit=None,
                ai_used_tokens_this_month=9_999_999,
            )
            check_quota(db, user)  # must not raise

    def test_no_cost_limit_set_does_not_raise_regardless_of_usage(self, db):
        with freeze_time("2024-01-10"):
            user = make_user(
                db,
                ai_monthly_cost_limit_cents=None,
                ai_used_cost_cents_this_month=9_999_999,
            )
            check_quota(db, user)  # must not raise


# ---------------------------------------------------------------------------
# record_usage
# ---------------------------------------------------------------------------

class TestRecordUsage:
    def test_increments_tokens(self, db):
        with freeze_time("2024-01-10"):
            user = make_user(db, ai_used_tokens_this_month=0)
            record_usage(db, user, tokens=200)
        assert user.ai_used_tokens_this_month == 200

    def test_increments_cost_cents(self, db):
        with freeze_time("2024-01-10"):
            user = make_user(db, ai_used_cost_cents_this_month=0)
            record_usage(db, user, cost_cents=75)
        assert user.ai_used_cost_cents_this_month == 75

    def test_accumulates_multiple_calls(self, db):
        with freeze_time("2024-01-10"):
            user = make_user(db)
            record_usage(db, user, tokens=100, cost_cents=10)
            record_usage(db, user, tokens=50, cost_cents=5)
            record_usage(db, user, tokens=25, cost_cents=3)
        assert user.ai_used_tokens_this_month == 175
        assert user.ai_used_cost_cents_this_month == 18

    def test_zero_tokens_does_not_change_count(self, db):
        with freeze_time("2024-01-10"):
            user = make_user(db, ai_used_tokens_this_month=100)
            record_usage(db, user, tokens=0)
        assert user.ai_used_tokens_this_month == 100

    def test_zero_cost_does_not_change_count(self, db):
        with freeze_time("2024-01-10"):
            user = make_user(db, ai_used_cost_cents_this_month=50)
            record_usage(db, user, cost_cents=0)
        assert user.ai_used_cost_cents_this_month == 50

    def test_accumulates_on_existing_nonzero_base(self, db):
        with freeze_time("2024-01-10"):
            user = make_user(
                db,
                ai_used_tokens_this_month=300,
                ai_used_cost_cents_this_month=20,
            )
            record_usage(db, user, tokens=100, cost_cents=5)
        assert user.ai_used_tokens_this_month == 400
        assert user.ai_used_cost_cents_this_month == 25


# ---------------------------------------------------------------------------
# Monthly rollover integration (check_quota + record_usage together)
# ---------------------------------------------------------------------------

class TestMonthlyRollover:
    def test_new_month_resets_usage_before_check(self, db):
        """Usage from January must not affect February's check."""
        with freeze_time("2024-01-31"):
            user = make_user(
                db,
                ai_monthly_token_limit=100,
                ai_used_tokens_this_month=0,
                ai_usage_period_start=datetime(2024, 1, 1, tzinfo=timezone.utc),
            )
            record_usage(db, user, tokens=100)

        # Sanity: in January the quota is now exhausted
        with freeze_time("2024-01-31"):
            with pytest.raises(HTTPException) as exc_info:
                check_quota(db, user)
            assert exc_info.value.status_code == 429

        # After month boundary counters roll over → quota is free again
        with freeze_time("2024-02-01"):
            check_quota(db, user)  # must not raise

    def test_same_month_accumulates(self, db):
        with freeze_time("2024-03-10"):
            user = make_user(
                db,
                ai_monthly_token_limit=300,
                ai_used_tokens_this_month=0,
                ai_usage_period_start=datetime(2024, 3, 1, tzinfo=timezone.utc),
            )
            record_usage(db, user, tokens=100)

        with freeze_time("2024-03-20"):
            record_usage(db, user, tokens=100)

        assert user.ai_used_tokens_this_month == 200

    def test_old_month_values_dont_carry_over(self, db):
        """After a month flip, previous counters are gone."""
        with freeze_time("2024-05-31"):
            user = make_user(
                db,
                ai_monthly_token_limit=50,
                ai_used_tokens_this_month=0,
                ai_usage_period_start=datetime(2024, 5, 1, tzinfo=timezone.utc),
            )
            record_usage(db, user, tokens=50)
            assert user.ai_used_tokens_this_month == 50

        with freeze_time("2024-06-01"):
            _ensure_period(user)
            assert user.ai_used_tokens_this_month == 0
