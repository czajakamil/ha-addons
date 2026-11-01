"""Dane demonstracyjne dla przycisku "Zobacz budżet testowy" na ekranie powitalnym.

Zastępuje dawny `seed_demo.py` (osobny skrypt bijący w HTTP na konto `dev`).
Tu ładujemy dane bezpośrednio dla aktualnie zalogowanego użytkownika, przez te
same serwisy co reszta API — więc nowe tagi tworzymy po ścieżce (`ensure_path`),
tak jak robi to UI, a nie przez ręczne parent_id jak w starym skrypcie.
"""

from __future__ import annotations

import datetime as dt

from fastapi import HTTPException, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from ..helpers import _month_key
from ..models import Account, Store, Transaction
from ..schemas import BudgetAssignmentUpdate, TransactionCreate
from . import budget as budget_service
from . import transactions as transactions_service
from .categories import ensure_path
from .store_aliases import register_store_name

_ACCOUNTS = [
    ("Konto główne", "cash", True, 482050),
    ("Oszczędności", "cash", True, 1210000),
    ("Karta kredytowa", "creditcard", False, -64030),
    ("Gotówka", "cash", True, 21500),
]

_TAG_GROUPS = {
    "Rachunki": ["Czynsz", "Prąd i gaz", "Internet", "Telefon"],
    "Codzienne wydatki": ["Spożywcze", "Transport", "Restauracje", "Rozrywka"],
    "Subskrypcje": ["Spotify", "Netflix", "Siłownia"],
    "Cele i oszczędności": ["Wakacje", "Poduszka finansowa", "Nowy laptop"],
}

_STORES = ["Biedronka", "Żabka", "Lidl", "Orlen", "Multikino", "Pyszne.pl", "Netflix", "Spotify"]

_ASSIGNMENTS = {
    "Czynsz": 220000,
    "Prąd i gaz": 32000,
    "Internet": 8000,
    "Telefon": 5000,
    "Spożywcze": 90000,
    "Transport": 30000,
    "Restauracje": 25000,
    "Rozrywka": 20000,
    "Spotify": 2400,
    "Netflix": 4300,
    "Siłownia": 9900,
    "Wakacje": 60000,
    "Poduszka finansowa": 50000,
    "Nowy laptop": 40000,
}


def load_demo_data(db: Session, user_id: str) -> dict[str, int]:
    """Wypełnia konto użytkownika przykładowym budżetem. Odmawia, gdy user ma już dane."""
    has_account = db.scalar(select(Account.id).where(Account.user_id == user_id).limit(1))
    if has_account:
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            detail="Konto ma już dane — dane testowe można załadować tylko na pusty budżet.",
        )

    month_start = dt.datetime.now(dt.UTC).date().replace(day=1)
    month_key = _month_key(month_start)

    def day(n: int) -> dt.date:
        return month_start.replace(day=n)

    accounts: dict[str, str] = {}
    for name, kind, on_budget, balance_cents in _ACCOUNTS:
        account = Account(
            name=name, currency="PLN", account_kind=kind, is_on_budget=on_budget, user_id=user_id
        )
        db.add(account)
        db.flush()
        accounts[name] = account.id
        if balance_cents:
            db.add(
                Transaction(
                    user_id=user_id,
                    account_id=account.id,
                    total_cents=balance_cents,
                    currency="PLN",
                    comment="Saldo początkowe",
                    recorded_at=dt.datetime.now(dt.UTC),
                    date=day(1),
                )
            )
    db.commit()

    tags: dict[str, str] = {}
    for group_name, children in _TAG_GROUPS.items():
        for child_name in children:
            category = ensure_path(db, user_id, f"{group_name}/{child_name}")
            tags[child_name] = category.id
    db.commit()

    stores: dict[str, str] = {}
    for name in _STORES:
        store = Store(name=name, user_id=user_id)
        db.add(store)
        register_store_name(db, store)
        stores[name] = store.id
    db.commit()

    def add_transaction(
        account: str,
        total_cents: int,
        tag: str | None = None,
        store: str | None = None,
        *,
        date: dt.date,
        comment: str | None = None,
        cleared: bool = True,
    ) -> None:
        payload = TransactionCreate(
            account_id=accounts[account],
            total_cents=total_cents,
            date=date,
            currency="PLN",
            cleared=cleared,
            category_id=tags.get(tag) if tag else None,
            store_id=stores.get(store) if store else None,
            comment=comment,
        )
        transactions_service.create_transaction(db, payload, user_id)

    add_transaction("Konto główne", 720000, date=day(1), comment="Wypłata — Acme")
    add_transaction("Konto główne", -220000, "Czynsz", date=day(2), comment="Czynsz")
    add_transaction("Konto główne", -29800, "Prąd i gaz", date=day(3))
    add_transaction("Konto główne", -8000, "Internet", date=day(3))
    add_transaction("Konto główne", -5000, "Telefon", date=day(3))
    add_transaction("Konto główne", -16320, "Spożywcze", store="Biedronka", date=day(5))
    add_transaction("Konto główne", -21205, "Spożywcze", store="Lidl", date=day(8))
    add_transaction("Konto główne", -1890, "Spożywcze", store="Żabka", date=day(13))
    add_transaction(
        "Karta kredytowa", -28740, "Transport", store="Orlen", date=day(6), cleared=False
    )
    add_transaction("Konto główne", -4900, "Rozrywka", store="Multikino", date=day(9))
    add_transaction(
        "Karta kredytowa", -6450, "Restauracje", store="Pyszne.pl", date=day(10), cleared=False
    )
    add_transaction("Konto główne", -2399, "Spotify", store="Spotify", date=day(10))
    add_transaction("Konto główne", -4300, "Netflix", store="Netflix", date=day(6))

    for tag_name, cents in _ASSIGNMENTS.items():
        assignment = BudgetAssignmentUpdate(
            category_id=tags[tag_name], month=month_key, assigned_cents=cents
        )
        budget_service.upsert_budget_assignment(db, assignment, user_id)

    return {
        "accounts": len(accounts),
        "tags": len(tags),
        "stores": len(stores),
        "transactions": 13,
    }
