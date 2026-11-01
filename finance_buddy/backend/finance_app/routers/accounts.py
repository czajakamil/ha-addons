"""Account endpoints."""

from __future__ import annotations

import datetime as dt

from fastapi import APIRouter, Depends, Query, status
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from ..db import get_db_session
from ..dependencies import (
    _get_owned_or_404,
    apply_pagination,
    get_current_user,
)
from ..models import (
    Account,
    AccountDebtTerms,
    Category,
    Transaction,
    TransactionAttachment,
    User,
)
from ..schemas import (
    AccountBalanceAdjustment,
    AccountCreate,
    AccountDebtRead,
    AccountDebtTermsRead,
    AccountRead,
    AccountsWithYnabResponse,
    AccountUpdate,
    DebtForecast,
    DebtPaymentSplit,
)
from ..services import debt
from ..services.transactions import attachment_file_path
from ..services.ynab import build_ynab_budget_snapshot

router = APIRouter()


def _account_balance_cents(db: Session, account_id: str, user_id: str) -> int:
    balance = db.scalar(
        select(func.coalesce(func.sum(Transaction.total_cents), 0)).where(
            Transaction.account_id == account_id,
            Transaction.user_id == user_id,
        )
    )
    return int(balance or 0)


@router.post("/accounts", response_model=AccountRead, status_code=status.HTTP_201_CREATED)
def create_account(
    payload: AccountCreate,
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    if payload.category_id:
        _get_owned_or_404(db, Category, payload.category_id, current_user.id, "Category")
    account = Account(**payload.model_dump(), user_id=current_user.id)
    db.add(account)
    db.commit()
    db.refresh(account)
    balance_cents = _account_balance_cents(db, account.id, current_user.id)
    return AccountRead(
        id=account.id,
        name=account.name,
        currency=account.currency,
        comment=account.comment,
        is_closed=account.is_closed,
        is_deleted=account.is_deleted,
        is_on_budget=account.is_on_budget,
        last_reconciled_at=account.last_reconciled_at,
        account_kind=account.account_kind,
        icon=account.icon,
        color=account.color,
        category_id=account.category_id,
        balance_cents=balance_cents,
    )


@router.get("/accounts", response_model=AccountsWithYnabResponse)
def list_accounts(
    offset: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    balances_subq = (
        select(
            Transaction.account_id.label("account_id"),
            func.coalesce(func.sum(Transaction.total_cents), 0).label("balance_cents"),
        )
        .where(Transaction.user_id == current_user.id)
        .group_by(Transaction.account_id)
        .subquery()
    )

    stmt = (
        select(
            Account.id,
            Account.name,
            Account.currency,
            Account.comment,
            Account.is_closed,
            Account.is_deleted,
            Account.is_on_budget,
            Account.last_reconciled_at,
            Account.account_kind,
            Account.icon,
            Account.color,
            Account.category_id,
            func.coalesce(balances_subq.c.balance_cents, 0).label("balance_cents"),
        )
        .outerjoin(balances_subq, balances_subq.c.account_id == Account.id)
        .where(Account.user_id == current_user.id)
        # Usunięte w YNAB-ie na koniec listy — tak samo jak sklepy.
        .order_by(Account.is_deleted.asc(), Account.name)
    )
    stmt = apply_pagination(stmt, offset, limit)
    rows = db.execute(stmt).all()

    accounts = [
        AccountRead(
            id=row.id,
            name=row.name,
            currency=row.currency,
            comment=row.comment,
            is_closed=row.is_closed,
            is_deleted=row.is_deleted,
            is_on_budget=row.is_on_budget,
            last_reconciled_at=row.last_reconciled_at,
            account_kind=row.account_kind,
            icon=row.icon,
            color=row.color,
            category_id=row.category_id,
            balance_cents=int(row.balance_cents or 0),
        )
        for row in rows
    ]

    ynab_budget = build_ynab_budget_snapshot(db, current_user)
    return AccountsWithYnabResponse(accounts=accounts, ynab_budget=ynab_budget)


@router.put("/accounts/{account_id}", response_model=AccountRead)
@router.patch("/accounts/{account_id}", response_model=AccountRead)
def update_account(
    account_id: str,
    payload: AccountUpdate,
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    account = _get_owned_or_404(db, Account, account_id, current_user.id, "Account")
    if payload.category_id:
        _get_owned_or_404(db, Category, payload.category_id, current_user.id, "Category")

    data = payload.model_dump(exclude_unset=True)
    for field, value in data.items():
        setattr(account, field, value)

    db.add(account)
    db.commit()
    db.refresh(account)
    balance_cents = _account_balance_cents(db, account.id, current_user.id)
    return AccountRead(
        id=account.id,
        name=account.name,
        currency=account.currency,
        comment=account.comment,
        is_closed=account.is_closed,
        is_deleted=account.is_deleted,
        is_on_budget=account.is_on_budget,
        last_reconciled_at=account.last_reconciled_at,
        account_kind=account.account_kind,
        icon=account.icon,
        color=account.color,
        category_id=account.category_id,
        balance_cents=balance_cents,
    )


@router.delete("/accounts/{account_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_account(
    account_id: str,
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """Trwale usuwa konto wraz ze WSZYSTKIMI powiązanymi transakcjami.

    To operacja nieodwracalna — inaczej niż archiwizacja (``is_closed``), która
    jedynie chowa konto na liście. Kasowanie konta pociąga za sobą kaskadowe
    usunięcie transakcji (oraz ich pozycji i załączników) przez relację ORM.
    """
    account = _get_owned_or_404(db, Account, account_id, current_user.id, "Account")

    # Kaskada ORM skasuje rekordy załączników, ale nie pliki na dysku — usuwamy je
    # ręcznie (tak jak przy kasowaniu pojedynczej transakcji).
    attachments = db.scalars(
        select(TransactionAttachment)
        .join(Transaction, TransactionAttachment.transaction_id == Transaction.id)
        .where(Transaction.account_id == account_id)
    ).all()
    for attachment in attachments:
        path = attachment_file_path(attachment)
        if path.exists():
            path.unlink(missing_ok=True)

    db.delete(account)
    db.commit()


@router.post("/accounts/{account_id}/adjust-balance", response_model=AccountRead)
def adjust_account_balance(
    account_id: str,
    payload: AccountBalanceAdjustment,
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    account = _get_owned_or_404(db, Account, account_id, current_user.id, "Account")

    target_balance = int(payload.target_balance_cents)
    current_balance = _account_balance_cents(db, account_id, current_user.id)
    difference = target_balance - current_balance

    if difference != 0:
        tx = Transaction(
            user_id=current_user.id,
            account_id=account.id,
            total_cents=difference,
            currency=account.currency,
            comment="Wyrównanie salda konta",
            recorded_at=dt.datetime.now(dt.UTC),
            date=dt.datetime.now(dt.UTC).date(),
        )
        db.add(tx)
        db.commit()

    balance_cents = _account_balance_cents(db, account.id, current_user.id)
    return AccountRead(
        id=account.id,
        name=account.name,
        currency=account.currency,
        comment=account.comment,
        is_closed=account.is_closed,
        is_deleted=account.is_deleted,
        is_on_budget=account.is_on_budget,
        last_reconciled_at=account.last_reconciled_at,
        account_kind=account.account_kind,
        icon=account.icon,
        color=account.color,
        category_id=account.category_id,
        balance_cents=balance_cents,
    )


@router.get("/accounts/{account_id}/debt", response_model=AccountDebtRead)
def get_account_debt(
    account_id: str,
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """Warunki kredytu i prognoza spłaty dla konta.

    Endpoint działa dla KAŻDEGO konta, nie tylko `liability`: konto bez
    warunków dostaje pustą historię i `forecast.status == "missing_terms"`.
    Odpowiedź 404 zarezerwowana jest dla konta, którego nie ma — brak
    warunków długu to poprawny stan, a nie błąd.

    Prognoza jest liczona tu i teraz, bo YNAB harmonogramu przez API nie
    wystawia (patrz services/debt.py).
    """
    account = _get_owned_or_404(db, Account, account_id, current_user.id, "Account")
    balance_cents = _account_balance_cents(db, account.id, current_user.id)

    rows = db.scalars(
        select(AccountDebtTerms)
        .where(
            AccountDebtTerms.account_id == account.id,
            AccountDebtTerms.user_id == current_user.id,
        )
        .order_by(AccountDebtTerms.month)
    ).all()

    terms = [
        debt.DebtTerms(
            month=row.month,
            interest_rate_bps=row.interest_rate_bps,
            minimum_payment_cents=row.minimum_payment_cents,
            escrow_cents=row.escrow_cents,
        )
        for row in rows
    ]

    as_of_month = dt.datetime.now(dt.UTC).strftime("%Y-%m")
    active, forecast = debt.build_overview(terms, balance_cents, as_of_month)

    return AccountDebtRead(
        account_id=account.id,
        balance_cents=balance_cents,
        outstanding_cents=debt.outstanding_from_balance(balance_cents),
        current_terms=None if active is None else AccountDebtTermsRead(**vars(active)),
        terms=[AccountDebtTermsRead.model_validate(row) for row in rows],
        forecast=DebtForecast(
            status=forecast.status.value,
            months=forecast.months,
            payoff_month=forecast.payoff_month,
            total_interest_cents=forecast.total_interest_cents,
        ),
        schedule=[DebtPaymentSplit(**vars(step)) for step in forecast.schedule],
    )
