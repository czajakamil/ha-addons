"""Ekran startowy: „co wymaga uwagi" zebrane w jednym zapytaniu.

Nic tu nie liczy się od nowa — licznik kolejki, koperty i saldo pochodzą
z tych samych funkcji co ich własne strony, żeby liczba na ekranie startowym
zawsze zgadzała się z tą, którą użytkownik zobaczy po kliknięciu.

Nowe są tylko dwa sygnały:

* świeżość danych — konto, z którego importujemy wyciągi, jest „nieaktualne",
  gdy ostatni import pokrywa dni sprzed więcej niż `STALE_AFTER_DAYS`;
  dodatkowo data ostatniej transakcji w ogóle, bo dopóki żadne konto nie ma
  historii importów, to jedyny znak, że dane przestały spływać,
* wydatki od początku miesiąca wobec tego samego odcinka poprzedniego
  miesiąca — porównanie z całym poprzednim miesiącem w połowie bieżącego
  zawsze wyglądałoby dobrze.
"""

from __future__ import annotations

import calendar
import datetime as dt
from collections import defaultdict
from typing import NamedTuple

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from ..helpers import (
    _is_internal_report_transaction,
    _is_reconciliation_adjustment,
    _month_floor,
    _month_key,
    _shift_month,
)
from ..models import (
    Account,
    BankImportBatch,
    Category,
    ReceiptDraft,
    Store,
    Transaction,
)
from ..schemas import (
    BudgetCategoryRow,
    HomeEnvelope,
    HomePace,
    HomeRecentTransaction,
    HomeStaleAccount,
    HomeSummary,
    HomeTopCategory,
)
from . import budget as budget_service
from . import tagging
from .categories import build_path_map
from .category_split import load_item_shares, split_lines

STALE_AFTER_DAYS = 7
# Koperta trafia na listę „blisko limitu", gdy wydano z niej co najmniej tyle.
NEAR_LIMIT_RATIO = 0.8
ENVELOPES_LIMIT = 5
RECENT_LIMIT = 8
# Z ilu poprzednich pełnych miesięcy liczymy średnie tempo wydatków.
PACE_MONTHS = 6
TOP_CATEGORIES_LIMIT = 5


def _same_day_previous_month(today: dt.date) -> tuple[dt.date, dt.date]:
    """Odcinek poprzedniego miesiąca od 1. do dnia odpowiadającego dzisiejszemu."""
    start = _shift_month(_month_floor(today), -1)
    last_day = calendar.monthrange(start.year, start.month)[1]
    return start, start.replace(day=min(today.day, last_day))


class _Outflow(NamedTuple):
    id: str
    date: dt.date
    total_cents: int
    category_id: str | None


def _outflows(db: Session, user_id: str, start: dt.date, end: dt.date) -> list[_Outflow]:
    """Wydatki z kont budżetowych — ta sama definicja co „Wydatki" w Raportach.

    Przelewy i korekty salda pomijamy, zwrotów nie odejmujemy (Raporty liczą
    je jako przychód), więc obie liczby da się bezpośrednio porównać.
    Wydatki bez kategorii też się liczą — pieniądze już wyszły z konta.
    Z tej jednej listy liczymy „Wydane", porównanie, tempo i top kategorii,
    więc te liczby zawsze do siebie pasują.
    """
    rows = db.execute(
        select(Transaction.id, Transaction.date, Transaction.total_cents, Transaction.category_id)
        .select_from(Transaction)
        .join(Account, Transaction.account_id == Account.id)
        .outerjoin(Store, Transaction.store_id == Store.id)
        .outerjoin(Category, Transaction.category_id == Category.id)
        .where(
            Transaction.user_id == user_id,
            Account.is_on_budget.is_(True),
            Transaction.total_cents < 0,
            Transaction.date >= start,
            Transaction.date <= end,
            ~_is_internal_report_transaction(),
        )
    ).all()
    return [_Outflow(row[0], row[1], int(row[2]), row[3]) for row in rows]


def _spent(outflows: list[_Outflow], start: dt.date, end: dt.date) -> int:
    return -sum(tx.total_cents for tx in outflows if start <= tx.date <= end)


def _cumulative(daily: dict[int, int], days: int) -> list[int]:
    """Narastająco dla dni 1..days z wydatków dziennych {dzień: grosze}."""
    total = 0
    out: list[int] = []
    for day in range(1, days + 1):
        total += daily.get(day, 0)
        out.append(total)
    return out


def _pace(outflows: list[_Outflow], today: dt.date, first_data: dt.date | None) -> HomePace:
    """Tempo wydatków: bieżący miesiąc narastająco wobec średniej z poprzednich.

    Średnia bierze tylko pełne miesiące, w których były już jakiekolwiek dane —
    miesiące sprzed pierwszej transakcji zaniżałyby ją zerami. Krótszy miesiąc
    po swoim ostatnim dniu trzyma sumę całego miesiąca.
    """
    month_start = _month_floor(today)
    days_in_month = calendar.monthrange(today.year, today.month)[1]

    by_month: dict[dt.date, dict[int, int]] = defaultdict(lambda: defaultdict(int))
    for tx in outflows:
        by_month[_month_floor(tx.date)][tx.date.day] -= tx.total_cents

    current = _cumulative(by_month.get(month_start, {}), today.day)

    curves: list[list[int]] = []
    for offset in range(1, PACE_MONTHS + 1):
        start = _shift_month(month_start, -offset)
        if first_data is None or _month_floor(first_data) > start:
            break
        length = calendar.monthrange(start.year, start.month)[1]
        curve = _cumulative(by_month.get(start, {}), length)
        curves.append([curve[min(day, length) - 1] for day in range(1, days_in_month + 1)])

    average = (
        [round(sum(values) / len(curves)) for values in zip(*curves, strict=True)] if curves else []
    )
    return HomePace(
        days_in_month=days_in_month,
        current_cents=current,
        average_cents=average,
        average_months=len(curves),
    )


def _top_categories(
    db: Session, user_id: str, outflows: list[_Outflow]
) -> tuple[list[HomeTopCategory], int]:
    """Najwięcej wydane w kategoriach-liściach; pozycje paragonu idą do kategorii produktów.

    Zwraca też kwotę bez kategorii, żeby top + reszta dało się zestawić z „Wydanymi".
    """
    shares = load_item_shares(db, [tx.id for tx in outflows])
    per_category: dict[str | None, int] = defaultdict(int)
    for tx in outflows:
        for category_id, cents in split_lines(tx.category_id, tx.total_cents, shares.get(tx.id)):
            per_category[category_id] -= cents

    uncategorized = per_category.pop(None, 0)
    paths = build_path_map(db, user_id)
    ranked = sorted(
        ((cid, cents) for cid, cents in per_category.items() if cents > 0 and cid in paths),
        key=lambda item: item[1],
        reverse=True,
    )
    top = [
        HomeTopCategory(category_id=cid, path=paths[cid], spent_cents=cents)
        for cid, cents in ranked[:TOP_CATEGORIES_LIMIT]
    ]
    return top, uncategorized


def _leaf_rows(rows: list[BudgetCategoryRow]) -> list[BudgetCategoryRow]:
    """Koperty-liście, bez kopert spłaty karty — tak jak liczy je strona Budżetu."""
    parent_ids = {row.parent_id for row in rows if row.parent_id}
    return [row for row in rows if row.category_id not in parent_ids and not row.is_credit_payment]


def spendable_cents(rows: list[BudgetCategoryRow], rta_cents: int) -> int:
    """Ile zostało do wydania w tym miesiącu według budżetu.

    Suma „Dostępnych" ze wszystkich kopert poza kopertami spłaty karty
    (to pieniądze odłożone na dług, nie na zakupy). Przekroczenia wchodzą
    na minusie — wydane ponad limit pieniądze trzeba skądś wziąć. Gdy RTA
    jest ujemne, przypisano więcej, niż jest na kontach: tej nadwyżki nie
    da się wydać, więc ją odejmujemy. Dodatniego RTA nie dodajemy — to
    pieniądze jeszcze bez przeznaczenia, pokazywane osobno.

    Z tożsamości RTA = saldo - Σ dostępnych wynika, że przy RTA < 0 wynik
    to dokładnie saldo kont budżetowych minus koperty spłaty karty.
    """
    envelopes = sum(row.available_cents for row in rows if not row.is_credit_payment)
    return envelopes + min(0, rta_cents)


def _envelope(row: BudgetCategoryRow) -> HomeEnvelope:
    spent = max(0, -row.activity_cents)
    return HomeEnvelope(
        category_id=row.category_id,
        category_name=row.category_name,
        available_cents=row.available_cents,
        spent_cents=spent,
        funds_cents=row.available_cents + spent,
    )


def _envelopes_near_limit(rows: list[BudgetCategoryRow]) -> list[HomeEnvelope]:
    """Najpierw koperty na minusie (od największego), potem najbardziej wydrenowane."""
    overspent = sorted(
        (_envelope(row) for row in rows if row.available_cents < 0),
        key=lambda env: env.available_cents,
    )
    # Koperta wydana co do grosza (opłacony rachunek, abonament) to plan
    # wykonany, a nie ostrzeżenie — „blisko limitu" wymaga, żeby coś zostało.
    near = [
        env
        for env in (_envelope(row) for row in rows if row.available_cents > 0)
        if env.funds_cents > 0 and env.spent_cents >= env.funds_cents * NEAR_LIMIT_RATIO
    ]
    near.sort(key=lambda env: env.spent_cents / env.funds_cents, reverse=True)
    return (overspent + near)[:ENVELOPES_LIMIT]


def _stale_accounts(db: Session, user_id: str, today: dt.date) -> list[HomeStaleAccount]:
    """Konta z historią importów, których wyciągi kończą się za dawno.

    Liczy się dzień, do którego sięga wyciąg (`date_to`), a nie dzień importu:
    wczorajszy import wyciągu sprzed miesiąca nie czyni danych świeżymi.
    """
    rows = db.execute(
        select(
            BankImportBatch.account_id,
            Account.name,
            BankImportBatch.date_to,
            BankImportBatch.created_at,
        )
        .join(Account, BankImportBatch.account_id == Account.id)
        .where(
            BankImportBatch.user_id == user_id,
            Account.is_closed.is_(False),
            Account.is_deleted.is_(False),
        )
    ).all()

    latest: dict[str, tuple[str, dt.date]] = {}
    for account_id, name, date_to, created_at in rows:
        covered = date_to or created_at.date()
        known = latest.get(account_id)
        if known is None or covered > known[1]:
            latest[account_id] = (name, covered)

    stale = [
        HomeStaleAccount(
            account_id=account_id,
            account_name=name,
            last_data_date=covered,
            days_since=(today - covered).days,
        )
        for account_id, (name, covered) in latest.items()
        if (today - covered).days > STALE_AFTER_DAYS
    ]
    stale.sort(key=lambda acc: acc.days_since, reverse=True)
    return stale


def _last_transaction_date(db: Session, user_id: str, today: dt.date) -> dt.date | None:
    # Transakcje z przyszłą datą (zaplanowane) nie świadczą o świeżości danych.
    return db.scalar(
        select(func.max(Transaction.date))
        .join(Account, Transaction.account_id == Account.id)
        .where(
            Transaction.user_id == user_id,
            Transaction.date <= today,
            Account.is_closed.is_(False),
            Account.is_deleted.is_(False),
        )
    )


def _recent(db: Session, user_id: str) -> list[HomeRecentTransaction]:
    # Korekty salda to ślad po uzgadnianiu konta, a nie coś, co się kupiło —
    # zostają na liście Transakcji, tu zasłaniałyby prawdziwe wydatki.
    rows = db.execute(
        select(Transaction, Account.name, Store.name)
        .join(Account, Transaction.account_id == Account.id)
        .outerjoin(Store, Transaction.store_id == Store.id)
        .outerjoin(Category, Transaction.category_id == Category.id)
        .where(
            Transaction.user_id == user_id,
            Account.is_deleted.is_(False),
            ~_is_reconciliation_adjustment(),
        )
        .order_by(Transaction.date.desc(), Transaction.recorded_at.desc())
        .limit(RECENT_LIMIT)
    ).all()
    needs_tag = tagging.untagged_among(db, user_id, [tx.id for tx, _, _ in rows])
    return [
        HomeRecentTransaction(
            id=tx.id,
            date=tx.date,
            total_cents=tx.total_cents,
            currency=tx.currency,
            account_name=account_name,
            store_name=store_name,
            comment=tx.comment,
            category_id=tx.category_id,
            is_transfer=bool(tx.transfer_account_id or tx.transfer_transaction_id),
            needs_tag=tx.id in needs_tag,
        )
        for tx, account_name, store_name in rows
    ]


def get_home_summary(db: Session, user_id: str) -> HomeSummary:
    # Ten sam „dziś" co w budżecie — inaczej na przełomie miesiąca ekran
    # startowy i Budżet pokazywałyby różne miesiące.
    today = dt.datetime.now(dt.UTC).date()
    month_start = _month_floor(today)
    month = _month_key(month_start)

    budget = budget_service.get_budget_month(db, month, user_id)
    leaves = _leaf_rows(budget.categories)
    previous_start, previous_end = _same_day_previous_month(today)
    outflows = _outflows(db, user_id, _shift_month(month_start, -PACE_MONTHS), today)
    this_month = [tx for tx in outflows if tx.date >= month_start]
    top_categories, uncategorized_spent = _top_categories(db, user_id, this_month)
    first_data = db.scalar(
        select(func.min(Transaction.date))
        .join(Account, Transaction.account_id == Account.id)
        .where(Transaction.user_id == user_id, Account.is_on_budget.is_(True))
    )

    receipts_todo = db.scalar(
        select(func.count(ReceiptDraft.id)).where(
            ReceiptDraft.user_id == user_id,
            ReceiptDraft.status == ReceiptDraft.STATUS_TODO,
        )
    )

    on_budget_accounts = db.scalar(
        select(func.count(Account.id)).where(
            Account.user_id == user_id,
            Account.is_on_budget.is_(True),
            Account.is_closed.is_(False),
            Account.is_deleted.is_(False),
        )
    )

    return HomeSummary(
        today=today,
        month=month,
        untagged_count=tagging.count_untagged(db, user_id),
        receipts_todo=receipts_todo or 0,
        ready_to_assign_cents=budget.summary.rta_cents,
        spendable_cents=spendable_cents(budget.categories, budget.summary.rta_cents),
        on_budget_account_count=on_budget_accounts or 0,
        overspent_count=sum(1 for row in leaves if row.available_cents < 0),
        envelopes=_envelopes_near_limit(leaves),
        spent_month_cents=_spent(outflows, month_start, today),
        spent_previous_month_cents=_spent(outflows, previous_start, previous_end),
        on_budget_balance_cents=budget.summary.on_budget_balance_cents,
        last_transaction_date=_last_transaction_date(db, user_id, today),
        stale_threshold_days=STALE_AFTER_DAYS,
        stale_accounts=_stale_accounts(db, user_id, today),
        recent=_recent(db, user_id),
        pace=_pace(outflows, today, first_data),
        top_categories=top_categories,
        uncategorized_spent_cents=uncategorized_spent,
    )
