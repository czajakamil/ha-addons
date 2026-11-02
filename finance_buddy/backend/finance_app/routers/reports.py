"""Reporting endpoints (monthly category, dashboard)."""

from __future__ import annotations

import datetime as dt
from collections import defaultdict
from decimal import Decimal
from typing import NamedTuple

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import and_, func, or_, select
from sqlalchemy.orm import Session

from ..db import get_db_session
from ..dependencies import get_current_user
from ..helpers import (
    _is_internal_report_transaction,
    _is_internal_transfer,
    _iter_months,
    _month_end,
    _month_floor,
    _month_key,
    _month_label,
    _normalize_account_kind,
    _shift_month,
)
from ..models import Account, Category, Store, Transaction, User
from ..schemas import (
    AgeOfMoneyPoint,
    AgeOfMoneyReport,
    IncomeExpenseCategoryRow,
    IncomeExpenseMonthRow,
    IncomeExpenseReport,
    MonthlyCategorySummary,
    MonthlySpendingPoint,
    NetWorthAccountBalance,
    NetWorthPoint,
    NetWorthReport,
    ReportsDashboardResponse,
    SpendingBreakdownEntry,
    SpendingBreakdownReport,
    SpendingOutflowInfo,
    SpendingTrendsReport,
)

router = APIRouter()


def _cents_to_amount(value: int) -> float:
    return float(Decimal(value) / Decimal(100))


def _build_spending_breakdown(rows: list[dict]) -> SpendingBreakdownReport:
    totals: dict[str, dict] = {}
    total_spending = 0
    largest_row: dict | None = None

    for row in rows:
        amount = int(row["total_cents"])
        if amount >= 0:
            continue
        cents = abs(amount)
        key = row["category_id"] or "uncategorized"
        category_name = row["category_name"] or "Uncategorized"
        entry = totals.setdefault(
            key,
            {
                "category_id": row["category_id"],
                "category_name": category_name,
                "total_cents": 0,
                "transaction_count": 0,
            },
        )
        entry["total_cents"] += cents
        entry["transaction_count"] += 1
        total_spending += cents
        if largest_row is None or cents > abs(int(largest_row["total_cents"])):
            largest_row = row

    entries = [
        SpendingBreakdownEntry(
            category_id=entry["category_id"],
            category_name=entry["category_name"],
            total_cents=entry["total_cents"],
            total_amount=_cents_to_amount(entry["total_cents"]),
            percentage=(entry["total_cents"] / total_spending) if total_spending else 0.0,
            transaction_count=entry["transaction_count"],
        )
        for entry in totals.values()
    ]
    entries.sort(key=lambda e: e.total_cents, reverse=True)

    largest_outflow = None
    if largest_row is not None:
        largest_cents = abs(int(largest_row["total_cents"]))
        largest_outflow = SpendingOutflowInfo(
            store_name=largest_row.get("store_name"),
            comment=largest_row.get("comment"),
            category_name=largest_row.get("category_name"),
            date=largest_row["date"],
            total_cents=largest_cents,
            total_amount=_cents_to_amount(largest_cents),
        )

    return SpendingBreakdownReport(
        total_spending_cents=total_spending,
        total_spending_amount=_cents_to_amount(total_spending),
        entries=entries,
        largest_outflow=largest_outflow,
    )


def _build_spending_trends(
    rows: list[dict],
    month_starts: list[dt.date],
) -> SpendingTrendsReport:
    monthly_totals: dict[dt.date, int] = defaultdict(int)
    total_spending = 0

    for row in rows:
        amount = int(row["total_cents"])
        if amount >= 0:
            continue
        cents = abs(amount)
        month = dt.date(row["date"].year, row["date"].month, 1)
        monthly_totals[month] += cents
        total_spending += cents

    points = [
        MonthlySpendingPoint(
            month=_month_key(month),
            label=_month_label(month),
            total_spending_cents=monthly_totals.get(month, 0),
            total_spending_amount=_cents_to_amount(monthly_totals.get(month, 0)),
        )
        for month in month_starts
    ]

    avg = int(total_spending / len(month_starts)) if month_starts else 0

    return SpendingTrendsReport(
        total_spending_cents=total_spending,
        average_monthly_spending_cents=avg,
        months=points,
    )


def _build_income_expense(
    rows: list[dict],
    month_starts: list[dt.date],
) -> IncomeExpenseReport:
    months = list(month_starts)
    month_data: dict[dt.date, dict] = {
        month: {
            "income_cents": 0,
            "expense_cents": 0,
            "income_categories": {},
            "expense_categories": {},
        }
        for month in months
    }
    total_income = 0
    total_expense = 0

    for row in rows:
        month = dt.date(row["date"].year, row["date"].month, 1)
        if month not in month_data:
            # include rows even if they fall slightly outside requested months
            month_data[month] = {
                "income_cents": 0,
                "expense_cents": 0,
                "income_categories": {},
                "expense_categories": {},
            }
            months.append(month)
        data = month_data[month]
        amount = int(row["total_cents"])
        key = row["category_id"] or "uncategorized"
        category_name = row["category_name"] or "Uncategorized"

        if amount >= 0:
            data["income_cents"] += amount
            total_income += amount
            cat_entry = data["income_categories"].setdefault(
                key,
                {
                    "category_id": row["category_id"],
                    "category_name": category_name,
                    "total_cents": 0,
                },
            )
            cat_entry["total_cents"] += amount
        else:
            cents = abs(amount)
            data["expense_cents"] += cents
            total_expense += cents
            cat_entry = data["expense_categories"].setdefault(
                key,
                {
                    "category_id": row["category_id"],
                    "category_name": category_name,
                    "total_cents": 0,
                },
            )
            cat_entry["total_cents"] += cents

    # ensure chronological order
    month_starts_sorted = sorted(set(months))
    month_rows: list[IncomeExpenseMonthRow] = []
    for month in month_starts_sorted:
        data = month_data.get(
            month,
            {
                "income_cents": 0,
                "expense_cents": 0,
                "income_categories": {},
                "expense_categories": {},
            },
        )
        income_categories = [
            IncomeExpenseCategoryRow(
                category_id=cat["category_id"],
                category_name=cat["category_name"],
                total_cents=cat["total_cents"],
                total_amount=_cents_to_amount(cat["total_cents"]),
            )
            for cat in data["income_categories"].values()
        ]
        income_categories.sort(key=lambda c: c.total_cents, reverse=True)
        expense_categories = [
            IncomeExpenseCategoryRow(
                category_id=cat["category_id"],
                category_name=cat["category_name"],
                total_cents=cat["total_cents"],
                total_amount=_cents_to_amount(cat["total_cents"]),
            )
            for cat in data["expense_categories"].values()
        ]
        expense_categories.sort(key=lambda c: c.total_cents, reverse=True)

        income_cents = data["income_cents"]
        expense_cents = data["expense_cents"]
        month_rows.append(
            IncomeExpenseMonthRow(
                month=_month_key(month),
                label=_month_label(month),
                income_cents=income_cents,
                expense_cents=expense_cents,
                net_cents=income_cents - expense_cents,
                income_categories=income_categories,
                expense_categories=expense_categories,
            )
        )

    return IncomeExpenseReport(
        total_income_cents=total_income,
        total_expense_cents=total_expense,
        months=month_rows,
    )


class _BalanceRow(NamedTuple):
    """Transakcja sprowadzona do trzech pól potrzebnych do salda kont."""

    date: dt.date
    total_cents: int
    account_id: str


def _build_net_worth(
    db: Session,
    month_starts: list[dt.date],
    end_date: dt.date,
    user_id: str,
    account_filter: set[str] | None = None,
) -> NetWorthReport:
    conditions = [Transaction.date <= end_date, Transaction.user_id == user_id]
    if account_filter:
        conditions.append(Transaction.account_id.in_(account_filter))

    stmt = (
        select(Transaction.date, Transaction.total_cents, Transaction.account_id)
        .where(*conditions)
        .order_by(Transaction.date)
    )
    rows = [
        _BalanceRow(date=row.date, total_cents=int(row.total_cents), account_id=row.account_id)
        for row in db.execute(stmt).all()
    ]

    month_list = sorted(set(month_starts))
    account_balances: dict[str, int] = defaultdict(int)
    idx = 0
    total_rows = len(rows)
    previous_net = None
    points: list[NetWorthPoint] = []

    for month in month_list:
        month_end = min(_month_end(month), end_date)
        while idx < total_rows and rows[idx].date <= month_end:
            account_balances[rows[idx].account_id] += rows[idx].total_cents
            idx += 1

        assets = sum(balance for balance in account_balances.values() if balance >= 0)
        debts = sum(balance for balance in account_balances.values() if balance < 0)
        net_value = assets + debts
        change = net_value - previous_net if previous_net is not None else net_value
        previous_net = net_value

        points.append(
            NetWorthPoint(
                month=_month_key(month),
                label=_month_label(month),
                assets_cents=assets,
                debts_cents=debts,
                net_worth_cents=net_value,
                change_cents=change,
            )
        )

    latest_assets = points[-1].assets_cents if points else 0
    latest_debts = points[-1].debts_cents if points else 0
    latest_net = points[-1].net_worth_cents if points else 0
    nonzero_balances = {
        account_id: balance for account_id, balance in account_balances.items() if balance != 0
    }
    account_rows = (
        db.execute(
            select(Account.id, Account.account_kind, Account.category_id).where(
                Account.user_id == user_id, Account.id.in_(nonzero_balances)
            )
        ).all()
        if nonzero_balances
        else []
    )
    accounts = [
        NetWorthAccountBalance(
            account_id=row.id,
            account_kind=_normalize_account_kind(row.account_kind) or "unknown",
            category_id=row.category_id,
            balance_cents=nonzero_balances[row.id],
        )
        for row in account_rows
    ]

    return NetWorthReport(
        months=points,
        latest_assets_cents=latest_assets,
        latest_debts_cents=latest_debts,
        latest_net_worth_cents=latest_net,
        accounts=accounts,
    )


def _age_of_money_snapshot(
    db: Session,
    window_end: dt.date,
    window_days: int,
    user_id: str,
    account_filter: set[str] | None = None,
) -> dict:
    window_days = max(1, window_days)
    sample_end = window_end
    sample_start = sample_end - dt.timedelta(days=window_days - 1)

    balance_conditions = [
        Account.is_on_budget.is_(True),
        Transaction.date <= sample_end,
        Transaction.user_id == user_id,
        Account.user_id == user_id,
    ]
    spending_conditions = [
        Transaction.date >= sample_start,
        Transaction.date <= sample_end,
        Account.is_on_budget.is_(True),
        Transaction.total_cents < 0,
        Transaction.user_id == user_id,
        Account.user_id == user_id,
    ]

    if account_filter:
        balance_conditions.append(Account.id.in_(account_filter))
        spending_conditions.append(Account.id.in_(account_filter))
    # transfery nie są wydatkiem – pomijamy je w outflow
    spending_conditions.append(~_is_internal_transfer())

    balance_stmt = (
        select(func.coalesce(func.sum(Transaction.total_cents), 0))
        .join(Account, Transaction.account_id == Account.id)
        .where(*balance_conditions)
    )
    on_budget_balance = int(db.scalar(balance_stmt) or 0)

    spending_stmt = (
        select(func.coalesce(func.sum(-Transaction.total_cents), 0))
        .join(Account, Transaction.account_id == Account.id)
        .where(*spending_conditions)
    )
    total_outflow = int(db.scalar(spending_stmt) or 0)
    sampled_days = (sample_end - sample_start).days + 1
    average_daily_outflow = int(total_outflow / sampled_days) if sampled_days else 0

    age_of_money = (
        round(on_budget_balance / average_daily_outflow, 1) if average_daily_outflow > 0 else None
    )

    return {
        "on_budget_balance_cents": on_budget_balance,
        "average_daily_outflow_cents": average_daily_outflow,
        "age_of_money_days": age_of_money,
        "sample_start": sample_start,
        "sample_end": sample_end,
        "sampled_days": sampled_days,
    }


def _build_age_of_money(
    db: Session,
    month_starts: list[dt.date],
    end_date: dt.date,
    window_days: int,
    user_id: str,
    account_filter: set[str] | None = None,
) -> AgeOfMoneyReport:
    month_sequence = sorted(set(month_starts)) or [_month_floor(end_date)]
    history: list[AgeOfMoneyPoint] = []

    for month in month_sequence:
        month_end = min(_month_end(month), end_date)
        snapshot = _age_of_money_snapshot(db, month_end, window_days, user_id, account_filter)
        history.append(
            AgeOfMoneyPoint(
                month=_month_key(month),
                label=_month_label(month),
                age_of_money_days=snapshot["age_of_money_days"],
                on_budget_balance_cents=snapshot["on_budget_balance_cents"],
                average_daily_outflow_cents=snapshot["average_daily_outflow_cents"],
                sample_start_date=snapshot["sample_start"],
                sample_end_date=snapshot["sample_end"],
                sampled_days=snapshot["sampled_days"],
            )
        )

    latest_snapshot = _age_of_money_snapshot(db, end_date, window_days, user_id, account_filter)

    return AgeOfMoneyReport(
        on_budget_balance_cents=latest_snapshot["on_budget_balance_cents"],
        average_daily_outflow_cents=latest_snapshot["average_daily_outflow_cents"],
        age_of_money_days=latest_snapshot["age_of_money_days"],
        sample_start_date=latest_snapshot["sample_start"],
        sample_end_date=latest_snapshot["sample_end"],
        sampled_days=latest_snapshot["sampled_days"],
        history=history,
    )


@router.get("/reports/monthly-category", response_model=list[MonthlyCategorySummary])
def monthly_category_report(
    year: int = Query(..., ge=1900),
    month: int = Query(..., ge=1, le=12),
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    first_day = dt.date(year, month, 1)
    next_month_year = year + (1 if month == 12 else 0)
    next_month_value = 1 if month == 12 else month + 1
    next_month = dt.date(next_month_year, next_month_value, 1)

    stmt = (
        select(
            Category.id,
            Category.name,
            func.coalesce(func.sum(Transaction.total_cents), 0).label("total_cents"),
        )
        .outerjoin(
            Transaction,
            and_(
                Transaction.category_id == Category.id,
                Transaction.user_id == current_user.id,
                Transaction.date >= first_day,
                Transaction.date < next_month,
            ),
        )
        .where(Category.user_id == current_user.id)
        .group_by(Category.id, Category.name)
        .order_by(func.coalesce(func.sum(Transaction.total_cents), 0).desc())
    )

    rows = db.execute(stmt).all()
    summaries: list[MonthlyCategorySummary] = []
    for category_id, category_name, total_cents in rows:
        total_cents_int = int(total_cents or 0)
        summaries.append(
            MonthlyCategorySummary(
                category_id=category_id,
                category_name=category_name,
                total_cents=total_cents_int,
                total_amount=float(Decimal(total_cents_int) / Decimal(100)),
            )
        )

    return summaries


def _earliest_transaction_date(db: Session, user_id: str) -> dt.date | None:
    return db.scalar(select(func.min(Transaction.date)).where(Transaction.user_id == user_id))


@router.get("/reports/dashboard", response_model=ReportsDashboardResponse)
def reports_dashboard(
    start_date: dt.date | None = Query(None),
    end_date: dt.date | None = Query(None),
    age_of_money_window_days: int = Query(90, ge=14, le=365),
    account_ids: list[str] | None = Query(None),
    category_ids: list[str] | None = Query(None),
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    today = dt.datetime.now(dt.UTC).date()
    resolved_end = end_date or today
    default_start = _shift_month(_month_floor(resolved_end), -2)
    resolved_start = start_date or default_start

    if resolved_start > resolved_end:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="start_date must be earlier than end_date",
        )

    month_starts = list(_iter_months(resolved_start, resolved_end))
    uncategorized_token = "__uncategorized__"  # noqa: S105 — sentinel grupowania, nie sekret

    range_conditions = [
        Transaction.date >= resolved_start,
        Transaction.date <= resolved_end,
        Transaction.user_id == current_user.id,
    ]
    if account_ids:
        count = db.scalar(
            select(func.count(Account.id)).where(
                Account.user_id == current_user.id, Account.id.in_(account_ids)
            )
        )
        if count != len(set(account_ids)):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid account filter."
            )
        range_conditions.append(Transaction.account_id.in_(account_ids))
    if category_ids:
        include_uncategorized = uncategorized_token in category_ids
        filtered_category_ids = [cid for cid in category_ids if cid != uncategorized_token]
        if filtered_category_ids:
            count = db.scalar(
                select(func.count(Category.id)).where(
                    Category.user_id == current_user.id, Category.id.in_(filtered_category_ids)
                )
            )
            if count != len(set(filtered_category_ids)):
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Invalid category filter.",
                )
        category_filters = []
        if filtered_category_ids:
            category_filters.append(Transaction.category_id.in_(filtered_category_ids))
        if include_uncategorized:
            category_filters.append(Transaction.category_id.is_(None))
        if category_filters:
            range_conditions.append(or_(*category_filters))
    # pomijamy transfery/adjustmenty między własnymi kontami przy liczeniu wydatków/przychodów
    range_conditions.append(~_is_internal_report_transaction())

    range_stmt = (
        select(
            Transaction.date,
            Transaction.total_cents,
            Transaction.category_id,
            Transaction.comment,
            Category.name.label("category_name"),
            Store.name.label("store_name"),
        )
        .outerjoin(Category, Transaction.category_id == Category.id)
        .outerjoin(Store, Transaction.store_id == Store.id)
        .where(*range_conditions)
        .order_by(Transaction.date)
    )
    range_rows = [
        {
            "date": row.date,
            "total_cents": int(row.total_cents),
            "category_id": row.category_id,
            "category_name": row.category_name,
            "store_name": row.store_name,
            "comment": row.comment,
        }
        for row in db.execute(range_stmt).all()
    ]

    spending_breakdown = _build_spending_breakdown(range_rows)
    spending_trends = _build_spending_trends(range_rows, month_starts)
    income_vs_expense = _build_income_expense(range_rows, month_starts)
    account_filter = set(account_ids) if account_ids else None

    net_worth = _build_net_worth(db, month_starts, resolved_end, current_user.id, account_filter)
    age_of_money = _build_age_of_money(
        db, month_starts, resolved_end, age_of_money_window_days, current_user.id, account_filter
    )

    return ReportsDashboardResponse(
        start_date=resolved_start,
        end_date=resolved_end,
        earliest_transaction_date=_earliest_transaction_date(db, current_user.id),
        spending_breakdown=spending_breakdown,
        spending_trends=spending_trends,
        net_worth=net_worth,
        income_vs_expense=income_vs_expense,
        age_of_money=age_of_money,
    )
