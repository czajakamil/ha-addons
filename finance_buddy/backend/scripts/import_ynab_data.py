from __future__ import annotations

import datetime as dt
import logging
import uuid
from dataclasses import dataclass
from datetime import UTC, date, datetime, time
from typing import Annotated

import ynab
from pydantic import AliasChoices, BaseModel, BeforeValidator, ConfigDict, Field, ValidationError
from sqlalchemy import delete, select
from sqlalchemy.orm import Session

# backend models / db helpers
from ..finance_app.config import PROJECT_ROOT, settings
from ..finance_app.logging_config import setup_logging
from ..finance_app.models import (
    Account,
    AccountDebtTerms,
    Budget,
    Category,
    Store,
    Transaction,
    YnabBudgetSnapshot,
)
from ..finance_app.services.categories import READY_TO_ASSIGN_TAG_NAME
from ..finance_app.services.store_aliases import find_store_by_name, register_store_name
from ..finance_app.services.store_merges import resolve_merged_store

setup_logging(
    log_level=settings.log_level,
    log_file=settings.log_file,
    project_root=PROJECT_ROOT,
    log_to_console=settings.log_to_console,
)
log = logging.getLogger("ynab-import")


# ---------------------- helpers ------------------------
def ynab_mu_to_cents(mu: int | None) -> int:
    """Przelicza kwotę YNAB (milliunits, 1 unit = 1000) na grosze.

    Grosze = milliunits / 10.
    """
    if mu is None:
        return 0
    # zaokrąglamy najbliżej grosza
    return round(mu / 10)


def ynab_rate_mu_to_bps(mu: int | None) -> int | None:
    """Oprocentowanie YNAB-a na punkty bazowe.

    YNAB podaje stopę w tej samej konwencji milliunits co kwoty, tyle że
    jednostką jest procent: 6550 to 6,550% rocznie, czyli 655 punktów bazowych.
    Dokumentacja jednostki nie podaje, więc to wniosek — ale wniosek sprawdzony
    na danych: przy saldzie 634 500 zł i racie 4034,36 zł odczyt jako 6,55%
    daje 3463 zł odsetek miesięcznie (rata spłaca kapitał, kredyt się kończy),
    a odczyt jako 65,5% daje 34 633 zł (rata nie pokrywa nawet odsetek,
    kredyt nie spłaca się nigdy). Tylko pierwsze jest kredytem hipotecznym.

    Zwraca None dla braku wartości, żeby odróżnić „nie wiem" od „zero procent".
    """
    if mu is None:
        return None
    return round(mu / 10)


def _debt_month_key(raw: str) -> str | None:
    """`2026-08-01` z klucza słownika długu na nasze `2026-08`.

    YNAB kluczuje szeregi długu pierwszym dniem miesiąca, a `Budget.month`
    w naszej bazie jest w formacie `RRRR-MM`. Trzymamy jeden format, żeby
    dało się te tabele zestawiać.
    """
    try:
        return dt.date.fromisoformat(raw).strftime("%Y-%m")
    except (TypeError, ValueError):
        log.warning("Niezrozumiały klucz miesiąca w polach długu: %r — pomijam.", raw)
        return None


@dataclass(frozen=True)
class DebtTermsRow:
    """Warunki kredytu na jeden miesiąc, już w naszych jednostkach."""

    month: str
    interest_rate_bps: int | None
    minimum_payment_cents: int | None
    escrow_cents: int | None


def debt_terms_from_account(acc) -> list[DebtTermsRow]:
    """Trzy słowniki długu z konta YNAB-a na listę wierszy po miesiącach.

    YNAB trzyma oprocentowanie, ratę i escrow jako osobne szeregi czasowe,
    które NIE muszą mieć tych samych kluczy — escrow bywa pustym słownikiem
    przy wypełnionych pozostałych. Bierzemy sumę miesięcy i zostawiamy None
    tam, gdzie danego szeregu nie ma; „brak" i „zero" to nie to samo.

    `debt_original_balance` jest pominięte świadomie: YNAB oznaczył je jako
    deprecated i zawsze zwraca null.
    """
    rates = getattr(acc, "debt_interest_rates", None) or {}
    payments = getattr(acc, "debt_minimum_payments", None) or {}
    escrows = getattr(acc, "debt_escrow_amounts", None) or {}

    by_month: dict[str, dict[str, int]] = {}
    for source, field in ((rates, "rate"), (payments, "payment"), (escrows, "escrow")):
        for raw_key, value in source.items():
            month = _debt_month_key(raw_key)
            if month is None or value is None:
                continue
            by_month.setdefault(month, {})[field] = value

    rows = []
    for month in sorted(by_month):
        values = by_month[month]
        payment = values.get("payment")
        escrow = values.get("escrow")
        rows.append(
            DebtTermsRow(
                month=month,
                interest_rate_bps=ynab_rate_mu_to_bps(values.get("rate")),
                minimum_payment_cents=None if payment is None else ynab_mu_to_cents(payment),
                escrow_cents=None if escrow is None else ynab_mu_to_cents(escrow),
            )
        )
    return rows


def _sync_debt_terms(db_acc: Account, user_id: str, acc) -> None:
    """Zapisuje warunki długu konta — upsertem po (account_id, month).

    Upsert, nie podmiana całości: nie wiadomo, czy YNAB dopisuje do tych
    słowników nowy klucz co miesiąc, czy tylko przy zmianie warunków.
    Upsert jest poprawny w obu wariantach, pełna podmiana tylko w jednym —
    i gubiłaby historię, gdyby API kiedyś zwróciło wycinek.
    """
    rows = debt_terms_from_account(acc)
    if not rows:
        return

    existing = {row.month: row for row in db_acc.debt_terms}
    for row in rows:
        current = existing.get(row.month)
        if current is None:
            db_acc.debt_terms.append(
                AccountDebtTerms(
                    account_id=db_acc.id,
                    user_id=user_id,
                    month=row.month,
                    interest_rate_bps=row.interest_rate_bps,
                    minimum_payment_cents=row.minimum_payment_cents,
                    escrow_cents=row.escrow_cents,
                )
            )
            continue
        current.interest_rate_bps = row.interest_rate_bps
        current.minimum_payment_cents = row.minimum_payment_cents
        current.escrow_cents = row.escrow_cents


# ---------------------- import reference data ----------------------
# YNAB ma 13 typów kont, ale rozróżnia je zachowaniem tylko na trzech osiach:
# aktywo budżetowe / kredyt odnawialny / zobowiązanie. Sześć typów pożyczkowych
# (mortgage, autoLoan, studentLoan, personalLoan, medicalDebt, otherDebt) ma
# IDENTYCZNY zestaw pól — różnią się nazwą i ikoną w YNAB-ie, niczym więcej.
# Nasz model ma te trzy wartości (ui/src/pages/accounts/model.ts) i to jest
# świadome uproszczenie, nie brak.
#
# Klucze są po `.lower()`, bo YNAB nadaje typy w camelCase (`creditCard`).
_YNAB_ACCOUNT_KINDS = {
    # Aktywa. `otherasset` to konta śledzone (inwestycje) — poza budżetem
    # trzyma je flaga `on_budget`, którą importujemy osobno.
    "checking": "cash",
    "savings": "cash",
    "cash": "cash",
    "otherasset": "cash",
    # Kredyt odnawialny. `lineofcredit` idzie tu, bo YNAB traktuje linię
    # kredytową tak samo jak kartę — z kategorią spłaty.
    "creditcard": "creditcard",
    "lineofcredit": "creditcard",
    # Zobowiązania.
    "mortgage": "liability",
    "autoloan": "liability",
    "studentloan": "liability",
    "personalloan": "liability",
    "medicaldebt": "liability",
    "otherdebt": "liability",
    "otherliability": "liability",
}

_DEFAULT_ACCOUNT_KIND = "cash"


def _as_utc(value: datetime | None) -> datetime | None:
    """Data z API na aware UTC.

    `UTCDateTime` w modelach odrzuca daty naiwne, a YNAB deklaruje to pole jako
    zwykły `datetime` — dziś przychodzi ze strefą, ale nie ma na to gwarancji.
    """
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def map_account_kind(ynab_type: str | None) -> str:
    """Typ konta z YNAB-a na jedną z naszych trzech wartości.

    Nieznany typ nie wywraca importu — wpada do aktywów i zostaje zgłoszony,
    bo YNAB może dołożyć nowy rodzaj konta, a my chcemy to zauważyć.
    """
    normalized = (ynab_type or "").strip().lower()
    if not normalized:
        return _DEFAULT_ACCOUNT_KIND
    kind = _YNAB_ACCOUNT_KINDS.get(normalized)
    if kind is None:
        log.warning(
            "Nieznany typ konta z YNAB-a: %r — wpisuję %r. Warto dodać go do _YNAB_ACCOUNT_KINDS.",
            ynab_type,
            _DEFAULT_ACCOUNT_KIND,
        )
        return _DEFAULT_ACCOUNT_KIND
    return kind


def import_accounts(s: Session, budget_detail, user_id: str) -> dict[str, str]:
    """Zwraca mapę: ynab_account_id -> db_account_id (to samo, bo używamy YNAB ID)."""
    mapping: dict[str, str] = {}
    for acc in budget_detail.accounts:
        account_uuid = str(acc.id)
        try:
            uuid.UUID(account_uuid)
        except Exception:
            log.warning("Account id is not a valid UUID: %r", account_uuid)
        # Konta usunięte w YNAB-ie WCHODZĄ do bazy z flagą, a nie są pomijane:
        # odwzorowujemy stan YNAB-a, a lokalnej historii transakcji z takiego
        # konta nie kasujemy. `closed` (archiwizacja) i `deleted` to dwie różne
        # rzeczy i trzymamy je w osobnych kolumnach.
        is_deleted = bool(getattr(acc, "deleted", False))
        closed_flag = getattr(acc, "closed", False)
        is_on_budget = bool(getattr(acc, "on_budget", False))
        account_kind = map_account_kind(getattr(acc, "type", None))
        last_reconciled_at = _as_utc(getattr(acc, "last_reconciled_at", None))
        note = getattr(acc, "note", None)

        db_acc = s.get(Account, account_uuid)
        if not db_acc:
            db_acc = Account(
                id=account_uuid,
                user_id=user_id,
                name=acc.name or "Unnamed",
                # YNAB nie zwraca currency dla konta, zakładamy PLN;
                # można potem ręcznie poprawić w appce
                currency="PLN",
                comment=note or f"Imported from YNAB: {budget_detail.name}",
                is_closed=closed_flag,
                is_deleted=is_deleted,
                last_reconciled_at=last_reconciled_at,
                is_on_budget=is_on_budget,
                account_kind=account_kind,
            )
            s.add(db_acc)
        else:
            if db_acc.user_id and db_acc.user_id != user_id:
                raise ValueError("YNAB import collision on Account ID.")
            # update nazwy jeśli się zmieniła
            db_acc.name = acc.name or db_acc.name
            db_acc.is_closed = closed_flag
            db_acc.is_deleted = is_deleted
            db_acc.last_reconciled_at = last_reconciled_at
            db_acc.is_on_budget = is_on_budget
            db_acc.account_kind = account_kind
            if note:
                db_acc.comment = note
        # Warunki długu (oprocentowanie / rata / escrow) idą do osobnej tabeli:
        # to szeregi czasowe po miesiącach, a nie pojedyncze wartości konta.
        # YNAB wypełnia te pola tylko dla kont kredytowych — dla pozostałych
        # słowniki są puste i nie powstaje żaden wiersz.
        _sync_debt_terms(db_acc, user_id, acc)
        mapping[account_uuid] = db_acc.id
    return mapping


def import_payees_as_stores(
    s: Session,
    budget_id: str,
    api_client: ynab.ApiClient,
    user_id: str,
    used_payee_ids: set[str] | None = None,
) -> dict[str, str]:
    """Payees → Stores.

    Payee o nazwie, którą ma już aktywny sklep, trafia do tego sklepu.
    Z `used_payee_ids` nowy sklep powstaje tylko dla payee z transakcjami: YNAB
    trzyma wszystkich dawnych payee, a puste sklepy wracałyby po każdym imporcie,
    także po usunięciu.
    Zwraca mapę: ynab_payee_id -> store_id.
    """
    payees_api = ynab.PayeesApi(api_client)
    payees = payees_api.get_payees(budget_id).data.payees
    mapping: dict[str, str] = {}
    for p in payees:
        if getattr(p, "deleted", False):
            continue
        if getattr(p, "transfer_account_id", None):
            # Transfery między kontami nie są sklepami – pomijamy te payee
            continue
        pid = str(p.id)
        try:
            uuid.UUID(pid)
        except Exception:
            log.warning("Payee id is not a valid UUID: %r", pid)
        store = s.get(Store, pid)
        if store and store.merged_into_id:
            mapping[pid] = resolve_merged_store(s, store).id
            continue
        if not store:
            # Ta sama nazwa (np. sklep z importu z banku albo drugi payee YNAB-a o tej
            # samej nazwie) to ten sam sklep. Osobny sklep dałby dwie identyczne nazwy
            # z banku, a importer nie używa niejednoznacznych aliasów.
            existing = find_store_by_name(s, user_id, p.name or "")
            if existing is not None:
                mapping[pid] = existing.id
                continue
            if used_payee_ids is not None and pid not in used_payee_ids:
                continue
            store = Store(
                id=pid,
                user_id=user_id,
                name=p.name or "Unknown",
                group_name=p.name or "Unknown",
                comment="Imported from YNAB (payee)",
            )
            s.add(store)
            register_store_name(s, store)
        elif not store.group_name:
            if store.user_id and store.user_id != user_id:
                raise ValueError("YNAB import collision on Store ID.")
            store.group_name = store.name
        mapping[pid] = store.id
    return mapping


# Grupa systemowa YNAB-a. Jej kategoria „Inflow: Ready to Assign" (dawniej
# „Inflow: To be Budgeted") to nie koperta, tylko źródło „Do przypisania":
# w API miesiąca `income` to suma transakcji z tą kategorią, a `activity` je pomija.
YNAB_INTERNAL_GROUP = "Internal Master Category"


def is_ynab_ready_to_assign(group, category) -> bool:
    """Czy kategoria YNAB-a to „Inflow: Ready to Assign".

    API oznacza kategorie systemowe flagą `internal`; nazwa grupy to zapas na
    wypadek odpowiedzi bez tej flagi. Samo „Inflow:" odróżnia ją od „Uncategorized".
    """
    is_internal = bool(getattr(category, "internal", False)) or group.name == YNAB_INTERNAL_GROUP
    return is_internal and (category.name or "").startswith("Inflow:")


def import_categories_tree(
    s: Session,
    budget_id: str,
    api_client: ynab.ApiClient,
    user_id: str,
    groups: list | None = None,
) -> dict[str, str]:
    """Category Groups + Categories → Category (drzewko).

    Zwraca: ynab_category_id -> local_category_id (to samo).
    Nazwy kategorii zapisujemy w formacie "Grupa/Subkategoria",
    żeby w UI pokazywać pełną ścieżkę jako tag.

    Wyjątek: „Inflow: Ready to Assign" trafia do korzenia drzewa jako tag
    przychodu (`READY_TO_ASSIGN_TAG_NAME`, flaga `is_ready_to_assign`).
    """
    if groups is None:
        categories_api = ynab.CategoriesApi(api_client)
        groups = categories_api.get_categories(budget_id).data.category_groups
    mapping: dict[str, str] = {}
    for g in groups:
        if getattr(g, "deleted", False):
            continue
        group_id = str(g.id)
        try:
            uuid.UUID(group_id)
        except Exception:
            log.warning("Category group id is not a valid UUID: %r", group_id)
        group_row = s.get(Category, group_id)
        if not group_row:
            group_row = Category(
                id=group_id,
                user_id=user_id,
                name=g.name or "Group",
                parent_id=None,
            )
            s.add(group_row)
        elif group_row.name != (g.name or group_row.name):
            if group_row.user_id and group_row.user_id != user_id:
                raise ValueError("YNAB import collision on Category ID.")
            group_row.name = g.name or group_row.name

        for c in g.categories or []:
            if getattr(c, "deleted", False):
                continue
            cat_id = str(c.id)
            try:
                uuid.UUID(cat_id)
            except Exception:
                log.warning("Category id is not a valid UUID: %r", cat_id)
            cat_row = s.get(Category, cat_id)
            if is_ynab_ready_to_assign(g, c):
                if not cat_row:
                    cat_row = Category(
                        id=cat_id,
                        user_id=user_id,
                        name=READY_TO_ASSIGN_TAG_NAME,
                        parent_id=None,
                        is_ready_to_assign=True,
                    )
                    s.add(cat_row)
                elif cat_row.user_id and cat_row.user_id != user_id:
                    raise ValueError("YNAB import collision on Category ID.")
                elif not cat_row.is_ready_to_assign:
                    cat_row.is_ready_to_assign = True
                    cat_row.name = READY_TO_ASSIGN_TAG_NAME
                    cat_row.parent_id = None
                # Już oznaczonego tagu nie ruszamy: nazwa i miejsce w drzewie
                # należą teraz do użytkownika, a nie do YNAB-a.
                mapping[cat_id] = cat_row.id
                continue

            cat_name = c.name or "Category"
            combined_name = f"{g.name or 'Group'}/{cat_name}"
            if not cat_row:
                cat_row = Category(
                    id=cat_id,
                    user_id=user_id,
                    name=combined_name,
                    parent_id=group_id,
                )
                s.add(cat_row)
            else:
                if cat_row.user_id and cat_row.user_id != user_id:
                    raise ValueError("YNAB import collision on Category ID.")
                # jeśli kiedyś zmienisz nazwę kategorii w YNAB
                cat_row.name = combined_name or cat_row.name
                if cat_row.parent_id != group_id:
                    cat_row.parent_id = group_id
            mapping[cat_id] = cat_row.id
    return mapping


def import_category_budgets(
    s: Session,
    budget_id: str,
    user_id: str,
    groups: list,
    server_knowledge: int | None = None,
    month: str | None = None,
) -> int:
    """Zapisz snapshot budżetu YNAB do lokalnej bazy (znormalizowane tabele).

    Zwraca liczbę zapisanych wpisów budżetowych.
    """
    now = datetime.now(UTC)
    month_key = month or f"{now.year}-{now.month:02d}"

    snapshot = s.scalar(
        select(YnabBudgetSnapshot).where(
            YnabBudgetSnapshot.user_id == user_id,
            YnabBudgetSnapshot.budget_id == budget_id,
            YnabBudgetSnapshot.month == month_key,
        )
    )
    if snapshot:
        s.execute(
            delete(Budget).where(
                Budget.entry_type == "ynab_category", Budget.snapshot_id == snapshot.id
            )
        )
        snapshot.server_knowledge = server_knowledge
        snapshot.created_at = now
    else:
        snapshot = YnabBudgetSnapshot(
            user_id=user_id,
            budget_id=budget_id,
            month=month_key,
            server_knowledge=server_knowledge,
            created_at=now,
        )
        s.add(snapshot)
        s.flush()

    inserted = 0

    for group in groups:
        if getattr(group, "deleted", False):
            continue
        group_id = str(group.id)
        try:
            uuid.UUID(group_id)
        except Exception:
            log.warning("Category group id is not a valid UUID: %r", group_id)
        if not s.get(Category, group_id):
            continue
        s.add(
            Budget(
                entry_type="ynab_category",
                user_id=user_id,
                snapshot_id=snapshot.id,
                category_id=group_id,
                month=month_key,
                hidden=bool(getattr(group, "hidden", False)),
                deleted=bool(getattr(group, "deleted", False)),
            )
        )
        inserted += 1

        for category in group.categories or []:
            if getattr(category, "deleted", False):
                continue
            category_id = str(category.id)
            try:
                uuid.UUID(category_id)
            except Exception:
                log.warning("Category id is not a valid UUID: %r", category_id)
            if not s.get(Category, category_id):
                continue
            s.add(
                Budget(
                    entry_type="ynab_category",
                    user_id=user_id,
                    snapshot_id=snapshot.id,
                    category_id=category_id,
                    month=month_key,
                    hidden=bool(getattr(category, "hidden", False)),
                    deleted=bool(getattr(category, "deleted", False)),
                    note=getattr(category, "note", None),
                    original_category_group_id=getattr(
                        category, "original_category_group_id", None
                    ),
                    budgeted_milliunits=int(getattr(category, "budgeted", 0) or 0),
                    activity_milliunits=int(getattr(category, "activity", 0) or 0),
                    balance_milliunits=int(getattr(category, "balance", 0) or 0),
                    goal_type=getattr(category, "goal_type", None),
                    goal_needs_whole_amount=getattr(category, "goal_needs_whole_amount", None),
                    goal_day=getattr(category, "goal_day", None),
                    goal_cadence=getattr(category, "goal_cadence", None),
                    goal_cadence_frequency=getattr(category, "goal_cadence_frequency", None),
                    goal_creation_month=getattr(category, "goal_creation_month", None),
                    goal_target_milliunits=int(getattr(category, "goal_target", 0) or 0),
                    goal_target_month=getattr(category, "goal_target_month", None),
                    goal_percentage_complete=getattr(category, "goal_percentage_complete", None),
                    goal_months_to_budget=getattr(category, "goal_months_to_budget", None),
                    goal_under_funded_milliunits=int(
                        getattr(category, "goal_under_funded", 0) or 0
                    ),
                    goal_overall_funded_milliunits=int(
                        getattr(category, "goal_overall_funded", 0) or 0
                    ),
                    goal_overall_left_milliunits=int(
                        getattr(category, "goal_overall_left", 0) or 0
                    ),
                    goal_snoozed_at=getattr(category, "goal_snoozed_at", None),
                )
            )
            inserted += 1

    return inserted


def import_month_assignments(s: Session, budget_detail, user_id: str) -> int:
    """Kwoty przypisane w YNAB-ie („budgeted") → nasze wpisy `assignment`.

    Budżet liczy „Dostępne" narastająco od pierwszego miesiąca, więc bierzemy
    całą historię miesięcy z planu, nie tylko bieżący. Dopóki YNAB jest
    podłączony, to on jest źródłem prawdy: dla kategorii z YNAB-a import
    nadpisuje lokalne przypisania, a zero w YNAB-ie kasuje lokalny wpis.
    Przypisań na tagach spoza YNAB-a ani w miesiącach, których YNAB nie zna,
    nie ruszamy. Tag przychodu (`is_ready_to_assign`) nie jest kopertą.

    Zwraca liczbę zapisanych (niezerowych) przypisań.
    """
    envelope_ids = set(
        s.scalars(
            select(Category.id).where(
                Category.user_id == user_id, Category.is_ready_to_assign.is_(False)
            )
        ).all()
    )
    wanted: dict[tuple[str, str], int] = {}
    for month in budget_detail.months or []:
        if getattr(month, "deleted", False):
            continue
        month_key = f"{month.month.year}-{month.month.month:02d}"
        for category in month.categories or []:
            category_id = str(category.id)
            if category_id not in envelope_ids:
                continue
            wanted[(month_key, category_id)] = ynab_mu_to_cents(category.budgeted)

    months = {month_key for month_key, _ in wanted}
    existing = {
        (row.month, row.category_id): row
        for row in s.scalars(
            select(Budget).where(
                Budget.entry_type == "assignment",
                Budget.user_id == user_id,
                Budget.month.in_(months),
            )
        ).all()
    }
    now = datetime.now(UTC)
    saved = 0
    for key, cents in wanted.items():
        row = existing.get(key)
        if cents == 0:
            if row is not None:
                s.delete(row)
            continue
        if row is None:
            month_key, category_id = key
            s.add(
                Budget(
                    entry_type="assignment",
                    user_id=user_id,
                    category_id=category_id,
                    month=month_key,
                    assigned_cents=cents,
                    created_at=now,
                    updated_at=now,
                )
            )
        elif row.assigned_cents != cents:
            row.assigned_cents = cents
            row.updated_at = now
        saved += 1
    return saved


# Grupa, w której YNAB trzyma koperty spłaty kart — po jednej na kartę,
# o nazwie konta. API nie podaje innego powiązania karty z jej kopertą.
YNAB_CREDIT_CARD_GROUP = "Credit Card Payments"


def link_credit_payment_categories(s: Session, budget_detail, groups: list) -> int:
    """Podpina konta kart pod YNAB-owe koperty „Credit Card Payments/<karta>".

    Bez tego budżet zakłada własną kopertę spłaty, a przypisania z YNAB-a na
    spłatę karty lądowałyby w osobnym tagu liczonym jak zwykła koperta.
    Zwraca liczbę podpiętych kont.
    """
    by_name: dict[str, str] = {}
    for group in groups:
        if getattr(group, "deleted", False) or group.name != YNAB_CREDIT_CARD_GROUP:
            continue
        for category in group.categories or []:
            if not getattr(category, "deleted", False) and category.name:
                by_name[category.name] = str(category.id)

    linked = 0
    for acc in budget_detail.accounts or []:
        if map_account_kind(getattr(acc, "type", None)) != "creditcard":
            continue
        category_id = by_name.get(acc.name or "")
        db_acc = s.get(Account, str(acc.id))
        if category_id is None or db_acc is None or s.get(Category, category_id) is None:
            continue
        db_acc.credit_payment_category_id = category_id
        linked += 1
    return linked


# ---------------------- import transactions ----------------------
@dataclass
class TxRow:
    id: str
    account_id: str | None
    store_id: str | None
    category_id: str | None
    transfer_account_id: str | None
    transfer_transaction_id: str | None
    date: date
    amount_cents: int  # signed: inflow > 0, outflow < 0
    currency: str
    memo: str | None
    cleared: bool
    reconciled: bool


def _parse_iso_date(value) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if not value:
        raise ValueError(f"Brak daty w payloadzie (value={value!r})")
    return date.fromisoformat(str(value))


def _map_cleared_flags(status_value: str | None) -> tuple[bool, bool]:
    """Zamień status YNAB (uncleared/cleared/reconciled) na nasze flagi bool."""
    status = (status_value or "").strip().lower()
    is_reconciled = status == "reconciled"
    is_cleared = status in ("cleared", "reconciled")
    return is_cleared, is_reconciled


def _normalize_id(value: object) -> str | None:
    """Identyfikator z API sprowadzony do stringa.

    Klient YNAB od 4.x zwraca `uuid.UUID`, wcześniej `str`. Nasze tabele i mapy
    (`acc_map`, `store_map`, `cat_map`) są kluczowane stringami, więc konwersja
    musi zdarzyć się raz, tutaj — inaczej `dict.get(UUID)` cicho zwraca None
    i transakcja traci powiązanie z kontem, sklepem albo kategorią.
    """
    if value is None:
        return None
    return str(value)


YnabId = Annotated[str | None, BeforeValidator(_normalize_id)]


def _warn_if_unusual_id(label: str, value: str | None) -> None:
    """Ostrzega o id, które nie jest ani UUID-em, ani znanym złożeniem.

    YNAB nadaje transakcjom cyklicznym i podzielonym identyfikatory w rodzaju
    `<uuid>_2023-11-01`. Wcześniejsza wersja ostrzegała o KAŻDYM takim id, przez
    co każdy import zostawiał w logu kilkanaście fałszywych alarmów i prawdziwy
    problem utonąłby wśród nich.
    """
    if value is None:
        return
    try:
        uuid.UUID(value.split("_", 1)[0])
    except ValueError:
        log.warning("%s nie wygląda na identyfikator YNAB-a: %r", label, value)


class YnabSubtransaction(BaseModel):
    """Pozycja transakcji dzielonej — tylko pola, na których stoi import."""

    model_config = ConfigDict(extra="ignore")

    id: YnabId = None
    amount: int = 0
    memo: str | None = None
    category_id: YnabId = None
    transfer_account_id: YnabId = None
    transfer_transaction_id: YnabId = None
    deleted: bool = False

    @property
    def is_transfer(self) -> bool:
        return bool(self.transfer_account_id or self.transfer_transaction_id)


class YnabTransaction(BaseModel):
    """Transakcja z API sprowadzona do pól, których naprawdę używamy.

    `extra="ignore"` jest tu celowe: YNAB dokłada pola między wydaniami (4.x
    przyniosło `amount_currency` i `amount_formatted`) i import nie ma powodu się
    o nie wywracać. Za to pola, które CZYTAMY, są zadeklarowane z typami — to ta
    warstwa łapie zmianę w rodzaju `str` -> `uuid.UUID`, zamiast przepuszczać ją
    dalej do bazy.
    """

    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    id: YnabId = None
    # Alternatywne nazwy to zaszłość po starszych kształtach odpowiedzi, przed
    # którymi broniła się poprzednia wersja tego kodu. Zostają jako alias, bo nic
    # nie kosztują, a wycięcie ich byłoby zmianą zachowania przy okazji.
    date: dt.date = Field(
        validation_alias=AliasChoices(
            "date", "transaction_date", "authorized_date", "approved_date"
        )
    )
    amount: int = 0
    memo: str | None = None
    cleared: str | None = None
    deleted: bool = False
    account_id: YnabId = None
    payee_id: YnabId = None
    category_id: YnabId = None
    transfer_account_id: YnabId = None
    transfer_transaction_id: YnabId = None
    subtransactions: list[YnabSubtransaction] = Field(default_factory=list)

    @property
    def is_transfer(self) -> bool:
        return bool(self.transfer_account_id or self.transfer_transaction_id)


# Data-podłoga dla pełnego importu. Od klienta YNAB 4.x `get_transactions`
# BEZ `since_date` zwraca tylko ostatni rok („Defaults to one year ago when not
# specified" w opisie parametru) — wcześniej zwracało całą historię. Pierwszy
# import budżetu woła tę funkcję właśnie bez daty, bo nie ma jeszcze
# `last_imported_at`, więc poleganie na domyślce ucinałoby historię po cichu:
# bez błędu, bez ostrzeżenia, z samymi brakującymi transakcjami w bazie.
_FULL_HISTORY_SINCE = date(1900, 1, 1)

# YNAB nie podaje waluty przy transakcji w kształcie, którego używamy —
# poprzednia wersja też wpisywała ją na sztywno, w tym samym miejscu.
_BASE_CURRENCY = "PLN"


def _fetch_transactions(api_client, budget_id, since_value, last_knowledge_of_server):
    """Surowa odpowiedź API jako dict — jedyne miejsce dotykające sieci."""
    tx_api = ynab.TransactionsApi(api_client)
    params = {"since_date": since_value}
    if last_knowledge_of_server is not None:
        params["last_knowledge_of_server"] = last_knowledge_of_server

    try:
        resp = tx_api.get_transactions(budget_id, **params)
    except TypeError:
        # Klient bez `last_knowledge_of_server`. `since_date` musi tu zostać —
        # bez niego wracamy do cichego ucięcia historii do roku.
        resp = tx_api.get_transactions(budget_id, since_value)

    if hasattr(resp, "to_dict"):
        return resp.to_dict()
    if hasattr(resp, "model_dump"):
        return resp.model_dump()
    if isinstance(resp, dict):
        return resp
    return getattr(resp, "data", resp)


def _row_from(tx: YnabTransaction, sub: YnabSubtransaction | None = None) -> TxRow:
    """Jeden wiersz importu: albo cała transakcja, albo jedna pozycja podziału."""
    cleared_flag, reconciled_flag = _map_cleared_flags(tx.cleared)
    store_id = None if tx.is_transfer else tx.payee_id

    if sub is None:
        return TxRow(
            id=tx.id,
            account_id=tx.account_id,
            store_id=store_id or None,
            category_id=tx.category_id,
            transfer_account_id=tx.transfer_account_id if tx.is_transfer else None,
            transfer_transaction_id=tx.transfer_transaction_id,
            date=tx.date,
            amount_cents=ynab_mu_to_cents(tx.amount),
            currency=_BASE_CURRENCY,
            memo=tx.memo,
            cleared=cleared_flag,
            reconciled=reconciled_flag,
        )

    return TxRow(
        id=sub.id,
        account_id=tx.account_id,
        store_id=(store_id if store_id and not sub.is_transfer else None),
        category_id=sub.category_id,
        transfer_account_id=sub.transfer_account_id if sub.is_transfer else None,
        transfer_transaction_id=sub.transfer_transaction_id,
        date=tx.date,
        amount_cents=ynab_mu_to_cents(sub.amount),
        currency=_BASE_CURRENCY,
        memo=sub.memo or tx.memo,
        cleared=cleared_flag,
        reconciled=reconciled_flag,
    )


def flatten_transactions(
    api_client,
    budget_id,
    since_date: date | None = None,
    last_knowledge_of_server: int | None = None,
) -> tuple[list[TxRow], int | None]:
    """Transakcje z YNAB-a spłaszczone do wierszy gotowych do zapisu.

    Transakcja dzielona rozkłada się na wiersz na każdą pozycję — nagłówek nie
    trafia do wyniku, bo zdublowałby kwotę.
    """
    since_value = (since_date or _FULL_HISTORY_SINCE).isoformat()
    raw = _fetch_transactions(api_client, budget_id, since_value, last_knowledge_of_server)

    # Struktura z API: { "data": { "transactions": [ ... ], "server_knowledge": ... } }
    data = raw.get("data", raw)
    server_knowledge = data.get("server_knowledge") if isinstance(data, dict) else None
    payload = data.get("transactions", [])

    rows: list[TxRow] = []
    skipped_invalid = 0

    for item in payload:
        try:
            tx = YnabTransaction.model_validate(item)
        except ValidationError as exc:
            # Poprzednio ta gałąź łapała wyłącznie brak daty. Teraz obejmuje
            # każdy kształt, którego nie umiemy odczytać — nadal pomijamy wiersz
            # zamiast wywracać cały import przez jedną nietypową transakcję.
            skipped_invalid += 1
            log.warning("Pomijam transakcję o nieoczekiwanym kształcie: %s", exc.errors())
            continue

        if tx.deleted:
            continue

        _warn_if_unusual_id("Id transakcji", tx.id)

        live_subs = [s for s in tx.subtransactions if not s.deleted]
        if live_subs:
            for sub in live_subs:
                _warn_if_unusual_id("Id pozycji", sub.id)
                rows.append(_row_from(tx, sub))
        else:
            rows.append(_row_from(tx))

    log.info(
        "flatten_transactions: total=%s, kept=%s, skipped_invalid=%s",
        len(payload),
        len(rows),
        skipped_invalid,
    )
    if skipped_invalid:
        # Podniesione do ERROR celowo: pominięta transakcja to brakujące dane
        # w bazie, a przy zmianie kształtu odpowiedzi w kolejnym wydaniu klienta
        # byłby to jedyny sygnał. W linii INFO wyżej utonąłby bez śladu.
        log.error(
            "Import pominął %s z %s transakcji — kształt odpowiedzi YNAB-a mógł się zmienić.",
            skipped_invalid,
            len(payload),
        )

    return rows, server_knowledge


def import_transactions(
    s: Session,
    rows: list[TxRow],
    acc_map: dict[str, str],
    store_map: dict[str, str],
    cat_map: dict[str, str],
    user_id: str,
):
    """Wstawia transakcje, omija duplikaty po ID."""
    inserted = 0
    skipped = 0

    for r in rows:
        db_account_uuid = acc_map.get(r.account_id) if r.account_id else None
        db_store_id = store_map.get(r.store_id) if r.store_id else None
        db_cat_id = cat_map.get(r.category_id) if r.category_id else None
        db_transfer_account_uuid = (
            acc_map.get(r.transfer_account_id) if r.transfer_account_id else None
        )

        if not db_account_uuid:
            # transakcja bez konta w YNAB? raczej rzadko; pomijamy
            skipped += 1
            continue

        existing = s.get(Transaction, r.id)
        if existing:
            if existing.user_id and existing.user_id != user_id:
                raise ValueError("YNAB import collision on Transaction ID.")
            existing.account_id = db_account_uuid
            existing.store_id = db_store_id
            existing.category_id = db_cat_id
            existing.transfer_account_id = db_transfer_account_uuid
            existing.transfer_transaction_id = r.transfer_transaction_id
            existing.date = r.date
            existing.recorded_at = datetime.combine(r.date, time.min, tzinfo=UTC)
            existing.total_cents = r.amount_cents
            existing.currency = r.currency
            existing.comment = r.memo
            existing.cleared = r.cleared
            existing.reconciled = r.reconciled
            skipped += 1
            continue

        tx = Transaction(
            id=r.id,
            user_id=user_id,
            account_id=db_account_uuid,
            store_id=db_store_id,
            category_id=db_cat_id,
            transfer_account_id=db_transfer_account_uuid,
            transfer_transaction_id=r.transfer_transaction_id,
            receipt_number=None,
            recorded_at=datetime.combine(r.date, time.min, tzinfo=UTC),
            created_at=datetime.now(UTC),
            date=r.date,
            total_cents=r.amount_cents,
            currency=r.currency,
            payment_method=None,
            comment=r.memo,
            cleared=r.cleared,
            reconciled=r.reconciled,
        )
        s.add(tx)
        inserted += 1

        # co X sztuk flush, żeby pamięć nie puchła
        if inserted % 1000 == 0:
            s.flush()
            log.info("Wstawiono %s transakcji...", inserted)

    log.info("Transakcje: inserted=%s, skipped=%s", inserted, skipped)
    return inserted, skipped
