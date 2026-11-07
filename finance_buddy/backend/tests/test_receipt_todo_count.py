from __future__ import annotations

from collections.abc import Iterator

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from backend.finance_app.models import Base, ReceiptDraft
from backend.finance_app.services.receipt_drafts import count_todo

USER = "user-1"


@pytest.fixture
def db() -> Iterator[Session]:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        yield session


def test_counts_only_own_todo_drafts(db: Session) -> None:
    for user, status in [
        (USER, ReceiptDraft.STATUS_TODO),
        (USER, ReceiptDraft.STATUS_TODO),
        (USER, ReceiptDraft.STATUS_SKIPPED),
        (USER, ReceiptDraft.STATUS_COMMITTED),
        ("user-2", ReceiptDraft.STATUS_TODO),
    ]:
        db.add(ReceiptDraft(user_id=user, status=status, source_name="scan.pdf", payload={}))
    db.flush()

    assert count_todo(db, USER) == 2
    assert count_todo(db, "nobody") == 0
