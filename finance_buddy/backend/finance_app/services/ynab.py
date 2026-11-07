"""Business logic for the YNAB integration (settings, budgets, sync/import).

Wraps the YNAB API client, the system keychain, and the import pipeline in
``scripts.import_ynab_data`` so the router stays a thin HTTP layer.
"""

from __future__ import annotations

import datetime as dt
import logging
import os
from collections import defaultdict
from typing import Any

import ynab
from fastapi import HTTPException, status
from sqlalchemy import select
from sqlalchemy.orm import Session
from ynab import ApiException

try:  # Optional dependency for system keychain storage
    import keyring
    from keyring.errors import KeyringError, PasswordDeleteError
except Exception:  # pragma: no cover - optional dependency
    # Awaryjne podmianki są z definicji niezgodne typologicznie z tym, co
    # importujemy wyżej — kod i tak sprawdza `keyring is None` przed użyciem,
    # a puste klasy wyjątków sprowadzają `except` do łapania niczego.
    keyring = None  # ty: ignore[invalid-assignment]
    KeyringError = Exception  # ty: ignore[invalid-assignment]
    PasswordDeleteError = Exception

from ...scripts import import_ynab_data as ynab_import
from ..models import (
    Budget,
    Category,
    User,
    YnabBudgetSnapshot,
    YnabSettings,
)
from ..schemas import (
    YnabBudgetSummary,
    YnabImportRequest,
    YnabImportResponse,
    YnabSettingsRead,
    YnabSettingsUpdate,
)
from ..services.store_stats import apply_category_suggestions

log = logging.getLogger("finance_app.api")


# --- Keychain + client helpers -------------------------------------------


def _keychain_service_name() -> str:
    return "Finance-Buddy-YNAB"


def _keychain_username(user_id: str) -> str:
    return f"user:{user_id}"


def _get_keychain_api_key(user_id: str) -> str | None:
    if not keyring:
        return None
    try:
        return keyring.get_password(_keychain_service_name(), _keychain_username(user_id))
    except KeyringError as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Keychain read failed: {exc}",
        ) from exc


def _set_keychain_api_key(user_id: str, api_key: str) -> None:
    if not keyring:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Keychain backend is not available.",
        )
    try:
        keyring.set_password(_keychain_service_name(), _keychain_username(user_id), api_key)
    except KeyringError as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Keychain write failed: {exc}",
        ) from exc


def _delete_keychain_api_key(user_id: str) -> None:
    if not keyring:
        return
    try:
        keyring.delete_password(_keychain_service_name(), _keychain_username(user_id))
    except PasswordDeleteError:
        return
    except KeyringError as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Keychain delete failed: {exc}",
        ) from exc


def _config_api_key() -> str:
    return os.getenv("YNAB_API_KEY", "").strip()


def _resolve_ynab_api_key(
    raw_key: str | None,
    user: User | None = None,
) -> str:
    # Klucz z konfiguracji HA wygrywa z zapisanym: gdy jest ustawiony, panel nie ma
    # pola na klucz, więc starego wpisu z pęku kluczy nie dałoby się już zmienić.
    key = (raw_key or "").strip() or _config_api_key()
    if not key and user:
        key = _get_keychain_api_key(user.id) or ""
    if not key:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="YNAB API key is required",
        )
    return key


def _make_ynab_client(api_key: str) -> ynab.ApiClient:
    try:
        configuration = ynab.Configuration(access_token=api_key)
        return ynab.ApiClient(configuration)
    except Exception as exc:  # pragma: no cover - defensive
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid YNAB API key: {exc}",
        ) from exc


# --- Serialization --------------------------------------------------------


def _serialize_ynab_settings(
    settings: YnabSettings | None,
    user_id: str,
) -> YnabSettingsRead:
    # Ta sama kolejność co w _resolve_ynab_api_key: klucz z env też się liczy.
    config_key = _config_api_key()
    api_key = config_key or _get_keychain_api_key(user_id)
    if not settings:
        return YnabSettingsRead(
            api_key_set=bool(api_key),
            api_key_from_config=bool(config_key),
        )
    return YnabSettingsRead(
        api_key_set=bool(api_key),
        api_key_from_config=bool(config_key),
        budget_id=settings.budget_id,
        last_imported_at=settings.last_imported_at,
        last_server_knowledge=settings.last_server_knowledge,
    )


def build_ynab_budget_snapshot(db: Session, current_user: User) -> dict[str, Any] | None:
    snapshot = db.scalar(
        select(YnabBudgetSnapshot)
        .where(YnabBudgetSnapshot.user_id == current_user.id)
        .order_by(YnabBudgetSnapshot.created_at.desc())
    )
    if not snapshot:
        return None

    budget_rows = db.scalars(
        select(Budget).where(
            Budget.entry_type == "ynab_category",
            Budget.snapshot_id == snapshot.id,
        )
    ).all()
    if not budget_rows:
        return {"data": {"category_groups": [], "server_knowledge": snapshot.server_knowledge}}

    category_ids = {row.category_id for row in budget_rows}
    categories = db.scalars(
        select(Category).where(
            Category.user_id == current_user.id,
            Category.id.in_(category_ids),
        )
    ).all()
    category_map = {category.id: category for category in categories}
    budget_map = {row.category_id: row for row in budget_rows}

    children_by_group: dict[str, list[tuple[Category, Budget]]] = defaultdict(list)
    group_ids: set[str] = set()

    for row in budget_rows:
        category = category_map.get(row.category_id)
        if not category:
            continue
        if category.parent_id:
            children_by_group[category.parent_id].append((category, row))
            group_ids.add(category.parent_id)
        else:
            group_ids.add(category.id)

    def sort_key(category_id: str) -> str:
        category = category_map.get(category_id)
        return category.name if category else ""

    def resolve_child_name(child: Category, parent: Category | None) -> str:
        if not parent:
            return child.name
        prefix = f"{parent.name}/"
        return child.name.removeprefix(prefix)

    category_groups_payload: list[dict[str, Any]] = []
    for group_id in sorted(group_ids, key=sort_key):
        group_category = category_map.get(group_id)
        if not group_category:
            continue
        group_budget = budget_map.get(group_id)
        group_payload = {
            "id": group_category.id,
            "name": group_category.name,
            "hidden": bool(group_budget.hidden) if group_budget else False,
            "deleted": bool(group_budget.deleted) if group_budget else False,
            "categories": [],
        }
        children = children_by_group.get(group_id, [])
        children.sort(key=lambda pair: pair[0].name)
        for child_category, child_budget in children:
            group_payload["categories"].append(
                {
                    "id": child_category.id,
                    "category_group_id": group_category.id,
                    "category_group_name": group_category.name,
                    "name": resolve_child_name(child_category, group_category),
                    "hidden": bool(child_budget.hidden),
                    "original_category_group_id": child_budget.original_category_group_id,
                    "note": child_budget.note,
                    "budgeted": child_budget.budgeted_milliunits,
                    "activity": child_budget.activity_milliunits,
                    "balance": child_budget.balance_milliunits,
                    "goal_type": child_budget.goal_type,
                    "goal_needs_whole_amount": child_budget.goal_needs_whole_amount,
                    "goal_day": child_budget.goal_day,
                    "goal_cadence": child_budget.goal_cadence,
                    "goal_cadence_frequency": child_budget.goal_cadence_frequency,
                    "goal_creation_month": child_budget.goal_creation_month,
                    "goal_target": child_budget.goal_target_milliunits,
                    "goal_target_month": child_budget.goal_target_month,
                    "goal_percentage_complete": child_budget.goal_percentage_complete,
                    "goal_months_to_budget": child_budget.goal_months_to_budget,
                    "goal_under_funded": child_budget.goal_under_funded_milliunits,
                    "goal_overall_funded": child_budget.goal_overall_funded_milliunits,
                    "goal_overall_left": child_budget.goal_overall_left_milliunits,
                    "goal_snoozed_at": child_budget.goal_snoozed_at,
                    "deleted": bool(child_budget.deleted),
                }
            )
        category_groups_payload.append(group_payload)

    return {
        "data": {
            "category_groups": category_groups_payload,
            "server_knowledge": snapshot.server_knowledge,
        }
    }


# --- Use cases ------------------------------------------------------------


def get_settings(db: Session, current_user: User) -> YnabSettingsRead:
    settings = db.get(YnabSettings, current_user.id)
    return _serialize_ynab_settings(settings, current_user.id)


def update_settings(
    db: Session, payload: YnabSettingsUpdate, current_user: User
) -> YnabSettingsRead:
    now = dt.datetime.now(dt.UTC)
    settings = db.get(YnabSettings, current_user.id)
    if not settings:
        settings = YnabSettings(user_id=current_user.id, created_at=now, updated_at=now)
        db.add(settings)
    else:
        settings.updated_at = now

    if payload.api_key is not None:
        api_key = payload.api_key.strip()
        if api_key:
            _set_keychain_api_key(current_user.id, api_key)
        else:
            _delete_keychain_api_key(current_user.id)

    if payload.budget_id is not None:
        budget_id = payload.budget_id.strip()
        resolved_budget_id = budget_id or None
        if resolved_budget_id != settings.budget_id:
            settings.last_imported_at = None
            settings.last_server_knowledge = None
        settings.budget_id = resolved_budget_id

    db.commit()
    return _serialize_ynab_settings(settings, current_user.id)


def list_budgets(raw_api_key: str | None, current_user: User) -> list[YnabBudgetSummary]:
    api_key = _resolve_ynab_api_key(raw_api_key, current_user)
    try:
        with _make_ynab_client(api_key) as client:
            # YNAB przemianował w API „budget" na „plan" (klient 4.x): BudgetsApi
            # zniknęło na rzecz PlansApi. Nasze własne nazewnictwo zostaje przy
            # „budżecie" — jest w kolumnach bazy, schematach i UI.
            plans_api = ynab.PlansApi(client)
            budgets = plans_api.get_plans().data.plans or []
            log.info("Fetched %s budgets from YNAB", len(budgets))
    except ApiException as exc:
        raise HTTPException(
            status_code=exc.status or status.HTTP_400_BAD_REQUEST,
            detail=getattr(exc, "body", None) or getattr(exc, "reason", None) or str(exc),
        ) from exc
    except HTTPException:
        raise
    except Exception as exc:  # pragma: no cover - defensive
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(exc),
        ) from exc

    return [
        YnabBudgetSummary(
            # Od klienta 4.x `id` przychodzi jako `uuid.UUID`, a nie `str` —
            # pydantic nie konwertuje tego sam i schemat by się wywalił. Reszta
            # importu robi dokładnie to samo `str()` na każdym identyfikatorze.
            id=str(getattr(budget, "id", "")),
            name=getattr(budget, "name", "") or "Budżet",
            last_modified_on=getattr(budget, "last_modified_on", None),
        )
        for budget in budgets
    ]


def import_budget(
    db: Session, payload: YnabImportRequest, current_user: User
) -> YnabImportResponse:
    log.info("Starting YNAB import for budget_id=%s", payload.budget_id)
    settings = db.get(YnabSettings, current_user.id)

    api_key = _resolve_ynab_api_key(payload.api_key, current_user)
    try:
        with _make_ynab_client(api_key) as client:
            plans_api = ynab.PlansApi(client)
            budget_detail = plans_api.get_plan_by_id(payload.budget_id).data.plan
            categories_api = ynab.CategoriesApi(client)
            categories_resp = categories_api.get_categories(payload.budget_id)
            groups = categories_resp.data.category_groups

            # Transakcje przed sklepami: sklep powstaje tylko dla payee, który ich używa
            rows, server_knowledge = ynab_import.flatten_transactions(client, payload.budget_id)
            used_payee_ids = {row.store_id for row in rows if row.store_id}

            acc_map = ynab_import.import_accounts(db, budget_detail, current_user.id)
            store_map = ynab_import.import_payees_as_stores(
                db, payload.budget_id, client, current_user.id, used_payee_ids
            )
            cat_map = ynab_import.import_categories_tree(
                db, payload.budget_id, client, current_user.id, groups=groups
            )
            ynab_import.import_category_budgets(
                db,
                payload.budget_id,
                current_user.id,
                groups,
                server_knowledge=getattr(categories_resp.data, "server_knowledge", None),
            )
            db.flush()
            ynab_import.link_credit_payment_categories(db, budget_detail, groups)
            ynab_import.import_month_assignments(db, budget_detail, current_user.id)

            # Zawsze pełna historia, nigdy przyrost po `last_knowledge_of_server`.
            # Przyrost zwraca też „duchy": bankowe importy YNAB-a dopasowane do
            # ręcznych transakcji albo odrzucone — z `deleted: false`, choć nie
            # ma ich w pełnej liście, a GET po id daje 404. Nie da się ich
            # odróżnić po polach (prawdziwe niezatwierdzone importy wyglądają
            # tak samo), a wstawione dublowały kwoty: 18.10.2026 pięć takich
            # wierszy zaniżyło saldo Revolut (PLN) o 105 700 zł. Pełna lista to
            # kilka tysięcy transakcji w jednym zapytaniu, a przy okazji łapie
            # zmiany w starych transakcjach, których okno dat by nie objęło.
            tx_inserted, tx_skipped = ynab_import.import_transactions(
                db, rows, acc_map, store_map, cat_map, current_user.id
            )

            # Pierwszy import budżetu: sklepy z YNAB-a nie mają domyślnych kategorii, ale ich
            # transakcje mają. Kolejne importy nie ruszają tego, co użytkownik już ustawił.
            stores_categorized = 0
            if settings is None or settings.budget_id != payload.budget_id:
                db.flush()
                stores_categorized = apply_category_suggestions(db, current_user.id)

            now = dt.datetime.now(dt.UTC)
            if not settings:
                settings = YnabSettings(user_id=current_user.id, created_at=now, updated_at=now)
                db.add(settings)
            budget_changed = settings.budget_id != payload.budget_id
            settings.budget_id = payload.budget_id
            settings.last_imported_at = now
            if server_knowledge is not None:
                settings.last_server_knowledge = server_knowledge
            elif budget_changed:
                settings.last_server_knowledge = None
            settings.updated_at = now
            db.commit()
    except ApiException as exc:
        db.rollback()
        raise HTTPException(
            status_code=exc.status or status.HTTP_400_BAD_REQUEST,
            detail=getattr(exc, "body", None) or getattr(exc, "reason", None) or str(exc),
        ) from exc
    except HTTPException:
        db.rollback()
        raise
    except ValueError as exc:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc
    except Exception as exc:  # pragma: no cover - defensive
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(exc),
        ) from exc

    log.info(
        "YNAB import finished for budget_id=%s (name=%s): accounts=%s, stores=%s, "
        "categories=%s, transactions_inserted=%s, transactions_skipped=%s",
        payload.budget_id,
        getattr(budget_detail, "name", None),
        len(acc_map),
        len(store_map),
        len(cat_map),
        tx_inserted,
        tx_skipped,
    )

    return YnabImportResponse(
        budget_id=payload.budget_id,
        budget_name=getattr(budget_detail, "name", None),
        accounts=len(acc_map),
        stores=len(store_map),
        categories=len(cat_map),
        transactions_inserted=tx_inserted,
        transactions_skipped=tx_skipped,
        stores_categorized=stores_categorized,
    )
