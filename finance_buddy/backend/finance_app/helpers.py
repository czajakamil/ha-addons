"""Pure utility helpers shared across multiple routers."""

from __future__ import annotations

import calendar
import datetime as dt
import re

from fastapi import HTTPException
from sqlalchemy import and_, func, or_

from .models import Category, Store, Transaction


def _split_csv(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


_NIP_WEIGHTS = (6, 5, 7, 2, 3, 4, 5, 6, 7)


def is_valid_nip(nip: str) -> bool:
    """Sprawdza sumę kontrolną polskiego NIP (10 cyfr, ostatnia to cyfra kontrolna)."""
    digits = re.sub(r"\D", "", nip)
    if len(digits) != 10:
        return False
    checksum = (
        sum(int(digit) * weight for digit, weight in zip(digits, _NIP_WEIGHTS, strict=False)) % 11
    )
    return checksum != 10 and checksum == int(digits[9])


def _month_floor(date_value: dt.date) -> dt.date:
    return dt.date(date_value.year, date_value.month, 1)


def _month_key(date_value: dt.date) -> str:
    return date_value.strftime("%Y-%m")


def _month_label(date_value: dt.date) -> str:
    return date_value.strftime("%b %Y")


def _month_end(date_value: dt.date) -> dt.date:
    last_day = calendar.monthrange(date_value.year, date_value.month)[1]
    return dt.date(date_value.year, date_value.month, last_day)


def _iter_months(start: dt.date, end: dt.date):
    current = _month_floor(start)
    end_month = _month_floor(end)
    while current <= end_month:
        yield current
        if current.month == 12:
            current = dt.date(current.year + 1, 1, 1)
        else:
            current = dt.date(current.year, current.month + 1, 1)


def _shift_month(date_value: dt.date, offset: int) -> dt.date:
    month_index = date_value.year * 12 + (date_value.month - 1) + offset
    year = month_index // 12
    month = month_index % 12 + 1
    day = min(date_value.day, calendar.monthrange(year, month)[1])
    return dt.date(year, month, day)


_MONTH_KEY_RE = re.compile(r"^\d{4}-\d{2}$")


def _parse_month_key(value: str) -> dt.date:
    if not value or not _MONTH_KEY_RE.match(value):
        raise HTTPException(status_code=400, detail="Invalid month format. Use YYYY-MM.")
    year_str, month_str = value.split("-", 1)
    try:
        return dt.date(int(year_str), int(month_str), 1)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid month value.") from None


def _normalize_account_kind(value: str | None) -> str:
    return (value or "").strip().lower().replace(" ", "").replace("_", "")


def _is_internal_transfer():
    """Identify transfers/adjustments that shouldn't count as spending or income."""
    return or_(
        Transaction.transfer_account_id.is_not(None),
        Transaction.transfer_transaction_id.is_not(None),
        and_(Transaction.category_id.is_(None), Transaction.store_id.is_(None)),
    )


def _is_reconciliation_adjustment():
    """Detect the balance corrections that reconciling an account leaves behind."""
    payee_label = func.lower(func.coalesce(Store.group_name, Store.name, ""))
    category_label = func.lower(func.coalesce(Category.name, ""))
    reconciliation_label = "reconciliation balance adjustment"

    return or_(
        payee_label.like(f"%{reconciliation_label}%"),
        category_label.like(f"%{reconciliation_label}%"),
    )


def _is_internal_label():
    """Detect internal labels that shouldn't count as spending or income."""
    payee_label = func.lower(func.coalesce(Store.group_name, Store.name, ""))
    category_label = func.lower(func.coalesce(Category.name, ""))

    transfer_patterns = ("transfer:%", "transfer :%")
    polish_transfer_pattern = "%przenies%konto%"

    return or_(
        _is_reconciliation_adjustment(),
        *[payee_label.like(pattern) for pattern in transfer_patterns],
        *[category_label.like(pattern) for pattern in transfer_patterns],
        payee_label.like(polish_transfer_pattern),
        category_label.like(polish_transfer_pattern),
    )


def _is_internal_report_transaction():
    """Exclude internal transfers and reconciliation adjustments from reports."""
    return or_(_is_internal_transfer(), _is_internal_label())
