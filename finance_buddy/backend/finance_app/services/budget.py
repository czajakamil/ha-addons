"""Business logic for budgeting (monthly envelope computation + assignments).

The router in ``routers/budget.py`` validates the request shape and returns the
response; the credit-card payment handling, carry-over math, and persistence
live here.
"""

from __future__ import annotations

import datetime as dt
from collections import defaultdict

from fastapi import HTTPException, status
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from ..dependencies import _get_owned_or_404
from ..helpers import (
    _iter_months,
    _month_end,
    _month_floor,
    _month_key,
    _month_label,
    _normalize_account_kind,
    _parse_month_key,
    _shift_month,
)
from ..models import Account, Budget, Category, Transaction
from ..schemas import (
    BudgetAssignmentUpdate,
    BudgetCategoryRow,
    BudgetMonthResponse,
    BudgetMonthSummary,
)
from .category_split import load_item_shares, split_lines


def _allowed_budget_months() -> set[str]:
    today = dt.datetime.now(dt.UTC).date()
    current = _month_floor(today)
    next_month = _shift_month(current, 1)
    return {_month_key(current), _month_key(next_month)}


def _ensure_allowed_month(month: str) -> None:
    if month not in _allowed_budget_months():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Budget month must be current or next month.",
        )


def _is_credit_account_kind(value: str | None) -> bool:
    return _normalize_account_kind(value) == "creditcard"


def _ensure_credit_payment_category(db: Session, account: Account, user_id: str) -> Category:
    if account.credit_payment_category_id:
        existing = db.get(Category, account.credit_payment_category_id)
        if existing:
            return existing

    parent = db.scalar(
        select(Category).where(
            Category.user_id == user_id,
            Category.parent_id.is_(None),
            Category.is_for_accounts.is_(False),
            func.lower(Category.name) == "credit card payment",
        )
    )
    if not parent:
        parent = Category(
            name="Credit Card Payment",
            parent_id=None,
            is_for_accounts=False,
            user_id=user_id,
        )
        db.add(parent)
        db.flush()

    child = db.scalar(
        select(Category).where(
            Category.user_id == user_id,
            Category.parent_id == parent.id,
            Category.is_for_accounts.is_(False),
            func.lower(Category.name) == account.name.lower(),
        )
    )
    if not child:
        child = Category(
            name=account.name,
            parent_id=parent.id,
            is_for_accounts=False,
            user_id=user_id,
        )
        db.add(child)
        db.flush()

    account.credit_payment_category_id = child.id
    db.add(account)
    db.commit()
    db.refresh(account)
    return child


def _on_budget_balance_cents(db: Session, user_id: str) -> int:
    balance_stmt = (
        select(func.coalesce(func.sum(Transaction.total_cents), 0))
        .select_from(Transaction)
        .join(Account, Transaction.account_id == Account.id)
        .where(
            Transaction.user_id == user_id,
            Account.is_on_budget.is_(True),
        )
    )
    return int(db.scalar(balance_stmt) or 0)


def _card_balances_by_month(
    db: Session,
    user_id: str,
    card_ids: list[str],
    month_keys: list[str],
    end_date: dt.date,
) -> dict[str, dict[str, int]]:
    """Saldo każdej karty na koniec każdego z miesięcy: month -> card_id -> grosze.

    Liczone ze wszystkich transakcji karty, także bez kategorii (saldo
    otwarcia, korekty), bo chodzi o prawdziwy dług, a nie o to, co widzi budżet.
    """
    if not card_ids or not month_keys:
        return {}
    rows = db.execute(
        select(Transaction.account_id, Transaction.date, Transaction.total_cents).where(
            Transaction.user_id == user_id,
            Transaction.account_id.in_(card_ids),
            Transaction.date <= end_date,
        )
    ).all()
    first_month = month_keys[0]
    running: dict[str, int] = defaultdict(int)
    deltas: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for account_id, tx_date, total_cents in rows:
        key = _month_key(tx_date)
        # Klucze „RRRR-MM" porównują się jak daty.
        if key < first_month:
            running[account_id] += int(total_cents)
        else:
            deltas[key][account_id] += int(total_cents)

    balances: dict[str, dict[str, int]] = {}
    for key in month_keys:
        for account_id, cents in deltas.get(key, {}).items():
            running[account_id] += cents
        balances[key] = dict(running)
    return balances


def _allocate_by_weights(weights: dict[str, int], total: int) -> dict[str, int]:
    if total <= 0 or not weights:
        return {}
    total_weight = sum(weights.values())
    if total_weight <= 0:
        return {}
    allocations: dict[str, int] = {}
    remainder = total
    for key, weight in weights.items():
        share = (total * weight) // total_weight
        allocations[key] = share
        remainder -= share
    if remainder > 0:
        for key in sorted(weights.keys(), key=lambda k: weights[k], reverse=True):
            if remainder <= 0:
                break
            allocations[key] += 1
            remainder -= 1
    return allocations


def get_budget_month(db: Session, month: str, user_id: str) -> BudgetMonthResponse:
    _ensure_allowed_month(month)

    target_month = _parse_month_key(month)
    earliest_tx_date = db.scalar(
        select(func.min(Transaction.date))
        .select_from(Transaction)
        .join(Account, Transaction.account_id == Account.id)
        .where(
            Transaction.user_id == user_id,
            Account.is_on_budget.is_(True),
            Transaction.category_id.isnot(None),
        )
    )
    earliest_assignment = db.scalar(
        select(func.min(Budget.month)).where(
            Budget.entry_type == "assignment",
            Budget.user_id == user_id,
        )
    )
    start_month = target_month
    if earliest_tx_date:
        start_month = min(start_month, _month_floor(earliest_tx_date))
    if earliest_assignment:
        start_month = min(start_month, _parse_month_key(earliest_assignment))

    month_sequence = list(_iter_months(start_month, target_month))
    month_keys = [_month_key(m) for m in month_sequence]

    # `list(...)`, bo niżej dokładamy do tej listy kategorie płatności kartą,
    # a `.all()` zwraca niemodyfikowalną w sensie typu `Sequence`.
    # Tag przychodu (is_ready_to_assign) nie jest kopertą: jego transakcje są
    # w saldzie kont, a nie w żadnym „Dostępnym", więc same trafiają do RTA.
    categories = list(
        db.scalars(
            select(Category)
            .where(
                Category.user_id == user_id,
                Category.is_for_accounts.is_(False),
                Category.is_ready_to_assign.is_(False),
            )
            .order_by(Category.name)
        ).all()
    )
    ready_to_assign_ids = set(
        db.scalars(
            select(Category.id).where(
                Category.user_id == user_id, Category.is_ready_to_assign.is_(True)
            )
        ).all()
    )
    category_lookup = {category.id: category for category in categories}

    accounts = db.scalars(select(Account).where(Account.user_id == user_id)).all()
    on_budget_accounts = {account.id for account in accounts if account.is_on_budget}
    credit_accounts = [
        account
        for account in accounts
        if account.is_on_budget and _is_credit_account_kind(account.account_kind)
    ]
    credit_payment_map: dict[str, str] = {}
    credit_payment_category_ids: set[str] = set()
    if credit_accounts:
        for account in credit_accounts:
            payment_category = _ensure_credit_payment_category(db, account, user_id)
            credit_payment_map[account.id] = payment_category.id
            credit_payment_category_ids.add(payment_category.id)
            if payment_category.id not in category_lookup:
                categories.append(payment_category)
                category_lookup[payment_category.id] = payment_category

    assignments = db.scalars(
        select(Budget).where(
            Budget.entry_type == "assignment",
            Budget.user_id == user_id,
            Budget.month.in_(month_keys),
        )
    ).all()
    assigned_by_month: dict[str, dict[str, int]] = defaultdict(dict)
    for row in assignments:
        # `Budget.month` jest w modelu nullowalne, ale filtr `.in_(month_keys)`
        # wyżej odsiewa NULL-e — do pętli trafiają wyłącznie wiersze z miesiącem.
        assigned_by_month[row.month][row.category_id] = (  # ty: ignore[invalid-argument-type]
            int(row.assigned_cents)
        )

    activity_cash: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    activity_credit: dict[str, dict[str, dict[str, int]]] = defaultdict(
        lambda: defaultdict(lambda: defaultdict(int))
    )
    activity_total: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    # Spłaty kart (przelew z konta budżetowego na kartę) zdejmują pieniądze
    # z koperty spłaty tej karty: month -> card_id -> grosze (dodatnie).
    card_payments: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    if month_keys and on_budget_accounts:
        start_date = _parse_month_key(month_keys[0])
        end_date = _month_end(target_month)
        # Bez filtra na `category_id`: transakcja bez głównej kategorii może mieć
        # pozycje z kategoriami produktów, a te liczą się do swoich kopert.
        tx_rows = db.execute(
            select(
                Transaction.id,
                Transaction.category_id,
                Transaction.account_id,
                Transaction.transfer_account_id,
                Transaction.date,
                Transaction.total_cents,
            ).where(
                Transaction.user_id == user_id,
                Transaction.date >= start_date,
                Transaction.date <= end_date,
            )
        ).all()
        item_shares = load_item_shares(db, [row[0] for row in tx_rows])
        credit_account_ids = {account.id for account in credit_accounts}
        for tx_id, tx_category_id, account_id, transfer_account_id, tx_date, tx_total in tx_rows:
            if account_id not in on_budget_accounts:
                continue
            month_key = _month_key(tx_date)
            if month_key not in month_keys:
                continue
            # Przelew między kontami budżetowymi nie zmienia budżetu — z jednym
            # wyjątkiem, jak w YNAB-ie: wpływ na kartę to spłata. Wypłata
            # gotówki z karty kopert nie rusza, tylko powiększa dług. Przelew na
            # konto spoza budżetu ma kategorię i liczy się jak wydatek (niżej).
            if transfer_account_id in on_budget_accounts:
                if account_id in credit_account_ids and tx_total > 0:
                    card_payments[month_key][account_id] += int(tx_total)
                continue
            for category_id, total_cents in split_lines(
                tx_category_id, int(tx_total), item_shares.get(tx_id)
            ):
                if category_id is None or category_id in ready_to_assign_ids:
                    continue
                if account_id in credit_account_ids:
                    activity_credit[month_key][category_id][account_id] += int(total_cents)
                else:
                    activity_cash[month_key][category_id] += int(total_cents)
                activity_total[month_key][category_id] += int(total_cents)

    card_balances = _card_balances_by_month(
        db, user_id, list(credit_payment_map), month_keys, _month_end(target_month)
    )

    category_ids = {category.id for category in categories}
    target_rows: dict[str, BudgetCategoryRow] = {}
    target_status: dict[str, str | None] = {}
    available_carryover = dict.fromkeys(category_ids, 0)

    for month_key in month_keys:
        available = dict(available_carryover)
        assigned_map = assigned_by_month.get(month_key, {})
        for category_id, assigned_cents in assigned_map.items():
            available[category_id] = available.get(category_id, 0) + assigned_cents

        for category_id, cents in activity_cash.get(month_key, {}).items():
            available[category_id] = available.get(category_id, 0) + cents

        available_after_cash = dict(available)
        payment_moves: dict[str, int] = defaultdict(int)
        credit_overspend: dict[str, int] = defaultdict(int)

        for category_id, card_map in activity_credit.get(month_key, {}).items():
            total_credit = sum(card_map.values())
            credit_outflow_weights = {
                card_id: -min(0, cents) for card_id, cents in card_map.items() if cents < 0
            }
            total_credit_outflow = sum(credit_outflow_weights.values())
            if total_credit_outflow > 0:
                available_before = available_after_cash.get(category_id, 0)
                covered = min(max(available_before, 0), total_credit_outflow)
                if covered > 0:
                    covered_moves = _allocate_by_weights(credit_outflow_weights, covered)
                    for card_id, amount in covered_moves.items():
                        payment_moves[card_id] += amount
                overspend_total = total_credit_outflow - covered
                if overspend_total > 0:
                    overspend_moves = _allocate_by_weights(credit_outflow_weights, overspend_total)
                    for card_id, amount in overspend_moves.items():
                        credit_overspend[card_id] += amount
            available[category_id] = available.get(category_id, 0) + total_credit

        for card_id, amount in payment_moves.items():
            payment_category_id = credit_payment_map.get(card_id)
            if payment_category_id:
                available[payment_category_id] = available.get(payment_category_id, 0) + amount

        for card_id, amount in card_payments.get(month_key, {}).items():
            payment_category_id = credit_payment_map.get(card_id)
            if payment_category_id:
                available[payment_category_id] = available.get(payment_category_id, 0) - amount

        # Minus w kopercie spłaty to dług karty bez pokrycia — nie może być
        # większy niż sam dług na koniec miesiąca. Nadwyżka to spłata ponad
        # odłożone pieniądze (albo nadpłata karty, która długu nie ma): jak
        # wydatek bez pokrycia obciąża RTA, a nie wisi w kopercie. Bez tego
        # para „wpłata na kartę i przelew z powrotem" podbijała RTA o całą
        # kwotę na zawsze, a spłata niezabudżetowanego długu — o jego dwukrotność.
        payment_debt: dict[str, int] = defaultdict(int)
        for card_id, payment_category_id in credit_payment_map.items():
            balance = card_balances.get(month_key, {}).get(card_id, 0)
            payment_debt[payment_category_id] += max(0, -balance)
        for payment_category_id, debt in payment_debt.items():
            prior = available.get(payment_category_id, 0)
            available[payment_category_id] = max(prior, -debt)

        if month_key == month:
            for category_id in category_ids:
                category = category_lookup.get(category_id)
                if not category:
                    continue
                assigned_value = assigned_map.get(category_id, 0)
                activity_value = activity_total.get(month_key, {}).get(category_id, 0)
                if category_id in credit_payment_category_ids:
                    # Spłata to aktywność koperty spłaty, ale nie wydatek miesiąca —
                    # wydatek kartą liczymy już w kopercie, z której poszedł.
                    activity_value -= sum(
                        amount
                        for card_id, amount in card_payments.get(month_key, {}).items()
                        if credit_payment_map.get(card_id) == category_id
                    )
                available_value = available.get(category_id, 0)
                row_status = None
                if available_value < 0:
                    cash_overspend = max(0, -available_after_cash.get(category_id, 0))
                    credit_overspend_value = max(0, -available_value - cash_overspend)
                    if cash_overspend > 0:
                        row_status = "overspent_cash"
                    elif credit_overspend_value > 0:
                        row_status = "overspent_credit"
                target_status[category_id] = row_status
                target_rows[category_id] = BudgetCategoryRow(
                    category_id=category.id,
                    category_name=category.name,
                    parent_id=category.parent_id,
                    assigned_cents=assigned_value,
                    activity_cents=activity_value,
                    available_cents=available_value,
                    is_credit_payment=category_id in credit_payment_category_ids,
                    status=row_status,
                )

        next_carryover: dict[str, int] = {}
        for category_id in category_ids:
            value = available.get(category_id, 0)
            if category_id in credit_payment_category_ids:
                next_carryover[category_id] = value
            else:
                next_carryover[category_id] = max(0, value)
        for card_id, amount in credit_overspend.items():
            payment_category_id = credit_payment_map.get(card_id)
            if payment_category_id:
                prior = next_carryover.get(payment_category_id, 0)
                next_carryover[payment_category_id] = prior - amount
        # Niepokryte przekroczenie kartą z tego miesiąca też nie może przekroczyć długu.
        for payment_category_id, debt in payment_debt.items():
            prior = next_carryover.get(payment_category_id, 0)
            next_carryover[payment_category_id] = max(prior, -debt)
        available_carryover = next_carryover

    assigned_total = sum(assigned_by_month.get(month, {}).values())
    activity_total_value = sum(activity_total.get(month, {}).values())
    available_total = sum(row.available_cents for row in target_rows.values())
    on_budget_balance = _on_budget_balance_cents(db, user_id)
    rta_value = on_budget_balance - available_total
    summary = BudgetMonthSummary(
        month=month,
        label=_month_label(target_month),
        rta_cents=rta_value,
        assigned_cents=assigned_total,
        activity_cents=activity_total_value,
        available_cents=available_total,
        on_budget_balance_cents=on_budget_balance,
    )

    return BudgetMonthResponse(
        month=month,
        label=_month_label(target_month),
        summary=summary,
        categories=[target_rows[cid] for cid in sorted(target_rows.keys())],
    )


def upsert_budget_assignment(
    db: Session, payload: BudgetAssignmentUpdate, user_id: str
) -> BudgetAssignmentUpdate:
    _ensure_allowed_month(payload.month)

    category = _get_owned_or_404(db, Category, payload.category_id, user_id, "Category")
    if category.is_for_accounts:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot budget for account categories.",
        )
    if category.is_ready_to_assign:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot budget for the Ready to Assign income tag.",
        )

    assignment = db.scalar(
        select(Budget).where(
            Budget.entry_type == "assignment",
            Budget.user_id == user_id,
            Budget.category_id == payload.category_id,
            Budget.month == payload.month,
        )
    )
    now = dt.datetime.now(dt.UTC)
    if assignment is None:
        assignment = Budget(
            entry_type="assignment",
            user_id=user_id,
            category_id=payload.category_id,
            month=payload.month,
            assigned_cents=payload.assigned_cents,
            created_at=now,
            updated_at=now,
        )
        db.add(assignment)
    else:
        assignment.assigned_cents = payload.assigned_cents
        assignment.updated_at = now
        db.add(assignment)
    db.commit()
    return payload
