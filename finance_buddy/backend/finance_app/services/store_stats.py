"""Statystyki sklepów liczone w bazie.

Liczba i ostatnia transakcja obejmują całą historię, a wydatki 12 miesięcy
kalendarzowych razem z bieżącym. Wydatki pomijają to samo co raporty (przelewy,
korekty salda) i inne waluty niż PLN, bo bez kursów nie da się ich uczciwie
zsumować. Zwrot pomniejsza sumę.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Sequence

from sqlalchemy import ColumnElement, Select, and_, case, func, select
from sqlalchemy.orm import Session

from ..helpers import _is_internal_report_transaction
from ..models import Category, Store, Transaction

MONTHS_IN_WINDOW = 12
MAIN_CURRENCY = "PLN"


def _month_start(today: dt.date, months_back: int) -> dt.date:
    index = today.year * 12 + today.month - 1 - months_back
    return dt.date(index // 12, index % 12 + 1, 1)


def spend_window(today: dt.date) -> tuple[dt.date, dt.date]:
    next_month = _month_start(today, -1)
    return _month_start(today, MONTHS_IN_WINDOW - 1), next_month - dt.timedelta(days=1)


def _counts_as_spending(today: dt.date) -> ColumnElement[bool]:
    start, end = spend_window(today)
    return and_(
        Transaction.date.between(start, end),
        Transaction.currency == MAIN_CURRENCY,
        ~_is_internal_report_transaction(),
    )


def _scoped(stmt: Select, user_id: str, store_ids: Sequence[str] | None) -> Select:
    # Joiny dla warunków z raportów, które patrzą na nazwy sklepu i kategorii.
    stmt = (
        stmt.outerjoin(Store, Transaction.store_id == Store.id)
        .outerjoin(Category, Transaction.category_id == Category.id)
        .where(Transaction.user_id == user_id, Transaction.store_id.is_not(None))
    )
    return stmt.where(Transaction.store_id.in_(store_ids)) if store_ids else stmt


def store_stats(
    db: Session,
    user_id: str,
    store_ids: Sequence[str] | None = None,
    today: dt.date | None = None,
) -> list[dict[str, object]]:
    spent = func.sum(
        case(
            (
                _counts_as_spending(today or dt.datetime.now(dt.UTC).date()),
                -Transaction.total_cents,
            ),
            else_=0,
        )
    )
    stmt = _scoped(
        select(
            Transaction.store_id,
            func.count(Transaction.id),
            func.coalesce(spent, 0),
            func.max(Transaction.date),
        ),
        user_id,
        store_ids,
    ).group_by(Transaction.store_id)
    return [
        {"store_id": store_id, "count": count, "spent_cents_12m": cents, "last_date": last}
        for store_id, count, cents, last in db.execute(stmt)
    ]


def store_monthly_spend(
    db: Session,
    user_id: str,
    store_ids: Sequence[str],
    today: dt.date | None = None,
) -> list[dict[str, object]]:
    """Wydatki grupy w 12 miesiącach, od najstarszego; miesiąc bez wydatków to 0."""
    today = today or dt.datetime.now(dt.UTC).date()
    month = func.strftime("%Y-%m", Transaction.date)
    stmt = _scoped(
        select(month, func.sum(-Transaction.total_cents)).where(_counts_as_spending(today)),
        user_id,
        store_ids,
    ).group_by(month)
    totals = dict(db.execute(stmt).tuples().all())
    months = [
        _month_start(today, back).strftime("%Y-%m") for back in range(MONTHS_IN_WINDOW - 1, -1, -1)
    ]
    return [{"month": entry, "cents": totals.get(entry, 0)} for entry in months]


# Podpowiedź pada tylko przy wyraźnej przewadze: inaczej sklep zostaje do ręcznego wyboru.
MIN_SUGGESTION_SHARE = 0.6


def category_suggestions(
    db: Session,
    user_id: str,
    store_ids: Sequence[str] | None = None,
    min_share: float = MIN_SUGGESTION_SHARE,
) -> list[dict[str, object]]:
    """Najczęstsza kategoria transakcji sklepu, gdy ma co najmniej `min_share` z nich.

    Mianownik to wszystkie transakcje sklepu poza przelewami i korektami salda, także
    te bez kategorii: sklep z dwiema skategoryzowanymi transakcjami na dziesięć nie
    dostaje podpowiedzi. Kategorie kont (spłaty kart) się nie liczą.
    """
    stmt = _scoped(
        select(
            Transaction.store_id, Transaction.category_id, Category.is_for_accounts, func.count()
        ),
        user_id,
        store_ids,
    ).where(~_is_internal_report_transaction())
    stmt = stmt.group_by(Transaction.store_id, Transaction.category_id, Category.is_for_accounts)

    totals: dict[str, int] = {}
    best: dict[str, tuple[str, int]] = {}
    for store_id, category_id, for_accounts, count in db.execute(stmt):
        totals[store_id] = totals.get(store_id, 0) + count
        if category_id is None or for_accounts:
            continue
        current = best.get(store_id)
        # Remis rozstrzyga id, żeby wynik nie zależał od kolejności wierszy z bazy
        if current is None or (count, category_id) > (current[1], current[0]):
            best[store_id] = (category_id, count)

    return [
        {
            "store_id": store_id,
            "category_id": category_id,
            "count": count,
            "total": totals[store_id],
        }
        for store_id, (category_id, count) in sorted(best.items())
        if count >= min_share * totals[store_id]
    ]


def apply_category_suggestions(db: Session, user_id: str) -> int:
    """Ustawia podpowiedziane kategorie aktywnym sklepom, które nie mają żadnej.

    Zwraca liczbę uzupełnionych sklepów. Niczego nie nadpisuje.
    """
    suggestions = {row["store_id"]: row["category_id"] for row in category_suggestions(db, user_id)}
    if not suggestions:
        return 0
    stores = db.scalars(
        select(Store).where(
            Store.user_id == user_id,
            Store.id.in_(suggestions),
            Store.automatic_category_id.is_(None),
            Store.is_deleted.is_(False),
        )
    ).all()
    for store in stores:
        store.automatic_category_id = str(suggestions[store.id])
        # Domyślna kategoria działa przy imporcie tylko razem z tą flagą
        store.automatically_categorize_payee = True
    db.flush()
    return len(stores)
