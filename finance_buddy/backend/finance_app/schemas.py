"""Pydantic schemas shared across API routes."""

from __future__ import annotations

import datetime as dt
from typing import Any, Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator

from .helpers import is_valid_nip


class AccountBase(BaseModel):
    name: str
    currency: str = Field(min_length=3, max_length=3)
    comment: str | None = None
    category_id: str | None = None
    account_kind: str = Field(default="cash", min_length=1, max_length=16)
    icon: str | None = Field(default=None, max_length=48)
    color: str | None = Field(default=None, max_length=24)
    is_closed: bool = False
    is_deleted: bool = False


class AccountCreate(AccountBase):
    is_on_budget: bool = True


class AccountRead(AccountBase):
    id: str
    balance_cents: int
    is_on_budget: bool = True
    # Tylko do odczytu: ustawia to import z YNAB-a, nie użytkownik.
    last_reconciled_at: dt.datetime | None = None
    model_config = ConfigDict(from_attributes=True)


class YnabBudgetSnapshot(BaseModel):
    data: dict[str, Any]

    model_config = ConfigDict(extra="allow")


class AccountsWithYnabResponse(BaseModel):
    accounts: list[AccountRead]
    ynab_budget: YnabBudgetSnapshot | None = None


class AccountUpdate(BaseModel):
    name: str | None = None
    currency: str | None = Field(default=None, min_length=3, max_length=3)
    comment: str | None = None
    category_id: str | None = None
    account_kind: str | None = Field(default=None, min_length=1, max_length=16)
    icon: str | None = Field(default=None, max_length=48)
    color: str | None = Field(default=None, max_length=24)
    is_closed: bool | None = None
    is_deleted: bool | None = None
    is_on_budget: bool | None = None


class AccountBalanceAdjustment(BaseModel):
    target_balance_cents: int = Field()


class AccountDebtTermsRead(BaseModel):
    """Warunki kredytu w jednym miesiącu.

    `interest_rate_bps` to punkty bazowe (6,55% -> 655) — front dzieli przez
    100, a nie przez 10000. None znaczy „YNAB tego nie podał", nie „zero".
    """

    month: str
    interest_rate_bps: int | None = None
    minimum_payment_cents: int | None = None
    escrow_cents: int | None = None
    model_config = ConfigDict(from_attributes=True)


class DebtPaymentSplit(BaseModel):
    """Jedna rata rozbita na kapitał i odsetki."""

    month: str
    opening_balance_cents: int
    payment_cents: int
    interest_cents: int
    principal_cents: int
    closing_balance_cents: int


class DebtForecast(BaseModel):
    """Prognoza spłaty.

    Przy `status` innym niż `ok` pola liczbowe są None — front ma pokazać
    powód, a nie zero udające spłacony kredyt.
    """

    status: str
    months: int | None = None
    payoff_month: str | None = None
    total_interest_cents: int | None = None


class AccountDebtRead(BaseModel):
    account_id: str
    # Saldo konta tak, jak wszędzie indziej: dla długu ujemne.
    balance_cents: int
    # To samo saldo jako dodatnia wielkość długu — front nie musi zgadywać znaku.
    outstanding_cents: int
    current_terms: AccountDebtTermsRead | None = None
    terms: list[AccountDebtTermsRead] = Field(default_factory=list)
    forecast: DebtForecast
    schedule: list[DebtPaymentSplit] = Field(default_factory=list)


class StoreBase(BaseModel):
    name: str
    group_name: str | None = None
    include_in_payee_list: bool = True
    automatically_categorize_payee: bool = False
    automatic_category_id: str | None = None
    renaming_rules: list[str] = Field(default_factory=list)
    nip: str | None = None
    address: str | None = None
    comment: str | None = None
    is_deleted: bool = False
    verified_name: str | None = None

    @field_validator("nip")
    @classmethod
    def _validate_nip(cls, value: str | None) -> str | None:
        if value and not is_valid_nip(value):
            raise ValueError("Nieprawidłowy NIP (błędna suma kontrolna)")
        return value


class StoreCreate(StoreBase):
    pass


class StoreUpdate(BaseModel):
    name: str | None = None
    group_name: str | None = None
    include_in_payee_list: bool | None = None
    automatically_categorize_payee: bool | None = None
    automatic_category_id: str | None = None
    renaming_rules: list[str] | None = None
    nip: str | None = None
    address: str | None = None
    comment: str | None = None
    is_deleted: bool | None = None
    verified_name: str | None = None

    @field_validator("nip")
    @classmethod
    def _validate_nip(cls, value: str | None) -> str | None:
        if value and not is_valid_nip(value):
            raise ValueError("Nieprawidłowy NIP (błędna suma kontrolna)")
        return value


class StoreRead(StoreBase):
    id: str

    model_config = ConfigDict(from_attributes=True)


class StoreStats(BaseModel):
    store_id: str
    count: int
    spent_cents_12m: int
    last_date: dt.date | None = None


class StoreCategorySuggestion(BaseModel):
    store_id: str
    category_id: str
    count: int
    total: int


class StoreMonthlySpend(BaseModel):
    month: str
    cents: int


class StoreAliasRead(BaseModel):
    id: str
    store_id: str
    pattern: str
    match_type: str
    is_ambiguous: bool

    model_config = ConfigDict(from_attributes=True)


class StoreAliasCreate(BaseModel):
    pattern: str = Field(min_length=1, max_length=500)


class StoreAliasPreviewRequest(StoreAliasCreate):
    store_ids: list[str] = Field(default_factory=list)


class StoreAliasPreview(BaseModel):
    match_type: str
    matches: int
    matches_other_stores: int
    conflict: str | None = None


class StoreMergeRequest(BaseModel):
    source_ids: list[str] = Field(min_length=1)
    target_id: str
    name: str | None = Field(default=None, min_length=2)
    category_id: str | None = None
    dry_run: bool = False


class StoreMergeConflict(BaseModel):
    kind: str
    message: str
    blocking: bool


class StoreMergePreview(BaseModel):
    transactions: int
    receipts: int
    aliases: int
    conflicts: list[StoreMergeConflict]
    can_merge: bool


class StoreMergeResult(BaseModel):
    merge_id: str
    target_id: str
    transactions: int
    expires_at: AwareDatetime


class StoreMergeRead(BaseModel):
    id: str
    target_store_id: str
    source_names: list[str]
    transactions: int
    created_at: AwareDatetime
    expires_at: AwareDatetime


class StoreMergeUndoResult(BaseModel):
    restored_transactions: int
    # Przeszły do sklepu docelowego po scaleniu albo zmieniono im sklep, więc zostają.
    kept_transactions: int


class SelfTransferPreviewRequest(BaseModel):
    store_ids: list[str] = Field(min_length=1)


class SelfTransferCandidate(BaseModel):
    id: str
    date: dt.date
    amount_cents: int
    currency: str
    account_id: str
    account_name: str
    description: str | None = None


class SelfTransferRow(SelfTransferCandidate):
    # 'paired' | 'ambiguous' | 'other_currency' | 'no_partner' | 'covered' | 'has_items'
    status: str
    suggested: str | None = None
    candidates: list[SelfTransferCandidate]


class SelfTransferPreview(BaseModel):
    rows: list[SelfTransferRow]
    already_transfers: int


class SelfTransferDecision(BaseModel):
    transaction_id: str
    partner_id: str | None = None
    account_id: str | None = None


class SelfTransferConvertRequest(BaseModel):
    decisions: list[SelfTransferDecision] = Field(min_length=1)


class SelfTransferConvertResult(BaseModel):
    conversion_id: str
    converted: int
    expires_at: AwareDatetime


class SelfTransferConversionRead(BaseModel):
    id: str
    converted: int
    created_at: AwareDatetime
    expires_at: AwareDatetime


class SelfTransferUndoResult(BaseModel):
    restored: int
    kept: int


class ItemBase(BaseModel):
    name: str
    group_name: str | None = None
    name_simplified: str | None = None
    unit_of_measurement: str | None = None
    category_id: str | None = None
    tax_rate: float | None = Field(default=None, ge=0, le=100)
    manufacturer: str | None = None
    comment: str | None = None


class ItemCreate(ItemBase):
    pass


class ItemUpdate(BaseModel):
    name: str | None = None
    group_name: str | None = None
    name_simplified: str | None = None
    unit_of_measurement: str | None = None
    category_id: str | None = None
    tax_rate: float | None = Field(default=None, ge=0, le=100)
    manufacturer: str | None = None
    comment: str | None = None


class ItemRead(ItemBase):
    id: str

    model_config = ConfigDict(from_attributes=True)


class ItemMergeRequest(BaseModel):
    """Scalenie produktów `source_ids` w `target_id`; `dry_run` tylko liczy skutki."""

    source_ids: list[str] = Field(min_length=1)
    target_id: str
    dry_run: bool = False


class ItemMergeResult(BaseModel):
    target_id: str
    merged_count: int
    items_moved: int
    aliases_moved: int
    aliases_added: int
    dry_run: bool = False
    model_config = ConfigDict(from_attributes=True)


class ItemAliasRead(BaseModel):
    """Nazwa z paragonu przypisana do produktu; `store_id` NULL = we wszystkich sklepach."""

    id: str
    alias: str
    store_id: str | None = None
    store_name: str | None = None


class ItemStats(BaseModel):
    """Zakupy przedmiotu: liczba transakcji, wydatki z 12 miesięcy i ostatni zakup."""

    item_id: str
    count: int
    spent_cents_12m: int
    last_date: dt.date | None = None


class ItemMonthlySpend(BaseModel):
    month: str
    cents: int


class ItemUsageRead(BaseModel):
    """Liczba pozycji transakcji i aliasów produktu — do listy i scalania."""

    item_id: str
    transaction_items: int
    aliases: int


class CategoryCreate(BaseModel):
    """Tworzenie tagu: albo name (+ opcjonalny parent_id), albo cała ścieżka.

    Podanie path ("dom/remont/lazienka") tworzy brakujących przodków,
    jak wpisanie nowego tagu w Obsidianie.
    """

    name: str | None = None
    parent_id: str | None = None
    path: str | None = None
    is_for_accounts: bool = False
    color: str | None = Field(default=None, max_length=24)


class CategoryUpdate(BaseModel):
    name: str | None = None
    parent_id: str | None = None
    is_for_accounts: bool | None = None
    # None jest znaczące: jawnie wyczyszczony kolor = dziedzicz po rodzicu.
    # Rozróżniamy "nie podano" vs "ustaw na NULL" przez model_fields_set w routerze.
    color: str | None = Field(default=None, max_length=24)


class CategoryRead(BaseModel):
    id: str
    name: str
    parent_id: str | None
    is_for_accounts: bool
    # Klucz koloru z palety (np. "teal"); NULL = dziedziczony po przodku.
    color: str | None = None
    # Znormalizowana ścieżka tagu, np. "dom/remont/lazienka" (wyliczana z drzewa).
    path: str

    model_config = ConfigDict(from_attributes=True)


class CategoryDeletePreview(BaseModel):
    """Skala skutków usunięcia pod-drzewa — do potwierdzenia w UI."""

    category_count: int
    product_count: int
    transaction_count: int


class TransactionItemBase(BaseModel):
    product_id: str
    unit_price_cents: int = Field(ge=0)
    quantity: float = Field(gt=0)
    unit: str | None = None
    comment: str | None = None
    # Nazwa tak, jak wydrukował ją paragon — przy pozycjach dodanych ręcznie brak.
    raw_name: str | None = None


class TransactionItemCreate(TransactionItemBase):
    pass


class TransactionItemRead(TransactionItemBase):
    id: str

    model_config = ConfigDict(from_attributes=True)


class TransactionAttachmentRead(BaseModel):
    id: str
    transaction_id: str
    original_name: str
    content_type: str | None = None
    size_bytes: int
    created_at: dt.datetime
    download_url: str


class ReceiptDraftItem(BaseModel):
    """Pozycja etykiety paragonu — tak, jak jest wydrukowana.

    Kwoty dodatnie (konwencja znaku wydatków dotyczy transakcji, nie etykiety),
    rabaty ujemne. `line_total_cents` to wartość **sprzed** rabatu, gdy paragon
    drukuje rabat osobną linią — niezmiennik opisuje `receipt_checks`.
    """

    raw_name: str = ""
    product_code: str | None = None
    quantity: float | None = None
    unit: str | None = None
    unit_price_cents: int | None = None
    line_total_cents: int | None = None
    discount_cents: int | None = None
    discount_label: str | None = None
    tax_code: str | None = None


class ReceiptDraftPayload(BaseModel):
    """Etykieta paragonu: to, co widać na wydruku, po ewentualnych poprawkach."""

    store_name: str | None = None
    nip: str | None = None
    # Tekst `RRRR-MM-DDTGG:MM`, a nie `datetime`: pole w formularzu ma pokazywać
    # dokładnie to, co drukuje paragon, a przepisywanie ma być odczytem, a nie
    # tłumaczeniem na format ustawień systemu.
    happened_at: str | None = None
    total_cents: int | None = None
    currency: str = "PLN"
    items: list[ReceiptDraftItem] = Field(default_factory=list)
    # Suma kontrolna z linii „udzielono łącznie rabatów" — nigdy nie odejmowana
    # drugi raz, służy wyłącznie do porównania z rabatami przy pozycjach.
    discounts_declared_cents: int | None = None
    # Wyłącznie rabat, którego nie da się przypisać do żadnej pozycji.
    receipt_discount_cents: int | None = None
    notes: str | None = None


class ReceiptCheckIssue(BaseModel):
    """Zastrzeżenie kontroli etykiety (backend/finance_app/receipt_checks.py)."""

    level: str  # 'bad' — niemożliwe przy tych danych, 'warn' — podejrzane
    code: str
    message: str
    suggestion: str | None = None
    field: str | None = None


class ReceiptCheckRules(BaseModel):
    """Progi reguł kontroli — formularz liczy werdykt sam, ale nie zna stałych.

    Dzięki temu zmiana okna skanu czy tolerancji sumy jest zmianą w jednym
    miejscu (Pythonie) i dociera do frontu bez wypuszczania nowego builda.
    """

    scan_window_days: int
    item_sum_tolerance_ratio: float
    item_sum_tolerance_min_cents: int


class ReceiptDraftSummary(BaseModel):
    """Pozycja skrzynki — tyle, ile trzeba do listy i kolejki (bez obrazu i OCR)."""

    id: str
    status: str
    source_name: str
    page_number: int
    scan_date: dt.date | None = None
    store_name: str | None = None
    happened_at: str | None = None
    total_cents: int | None = None
    currency: str = "PLN"
    item_count: int = 0
    issues: list[ReceiptCheckIssue] = Field(default_factory=list)
    transaction_id: str | None = None
    error: str | None = None
    created_at: dt.datetime


class ReceiptDraftDetail(ReceiptDraftSummary):
    payload: ReceiptDraftPayload
    ocr_payload: ReceiptDraftPayload | None = None
    raw_text: str | None = None
    matched_store_id: str | None = None
    matched_store_name: str | None = None
    confidence: float = 0.0
    image_url: str | None = None


class ReceiptDraftCounts(BaseModel):
    total: int
    todo: int
    review: int
    skipped: int
    committed: int


class ReceiptDraftListResponse(BaseModel):
    drafts: list[ReceiptDraftSummary]
    counts: ReceiptDraftCounts
    rules: ReceiptCheckRules


class ReceiptDraftUpdate(BaseModel):
    payload: ReceiptDraftPayload | None = None
    status: str | None = None


class ReceiptDraftCommitItem(BaseModel):
    """Pozycja gotowa do zapisania jako pozycja transakcji.

    Albo wskazuje istniejący produkt (`product_id`), albo każe założyć nowy
    (`new_product_name` + opcjonalna ścieżka tagu). Pozycja bez jednego i bez
    drugiego jest pomijana — tak zapisujemy paragon, na którym linia rabatu
    stoi osobno i nie jest towarem.
    """

    raw_name: str
    quantity: float = 1
    unit: str | None = None
    unit_price_cents: int | None = None
    line_total_cents: int | None = None
    product_id: str | None = None
    new_product_name: str | None = None
    category_path: str | None = None
    # Pętla uczenia: zapisz alias „nazwa z paragonu → produkt" po zatwierdzeniu.
    learn_alias: bool = False


class ReceiptDraftCommit(BaseModel):
    account_id: str
    store_id: str | None = None
    category_id: str | None = None
    comment: str | None = None
    payload: ReceiptDraftPayload
    items: list[ReceiptDraftCommitItem] = Field(default_factory=list)
    # Kody zastrzeżeń, które użytkownik świadomie przyjął. Imiennie, nie flagą
    # `force`: taka flaga po miesiącu wisi w kodzie na stałe i wyłącza kontrole,
    # o których nikt już nie pamięta.
    acknowledged_issues: list[str] = Field(default_factory=list)
    # Transakcja z wyciągu bankowego, do której podpinamy paragon zamiast
    # tworzyć nową (docs/design/import-csv.md §5.5). Musi być na liście
    # `GET /receipt-drafts/{id}/bank-matches` — inaczej 422.
    merge_into_transaction_id: str | None = None


class ReceiptDraftCommitResult(BaseModel):
    transaction_id: str
    draft: ReceiptDraftDetail


class ProductCandidate(BaseModel):
    """Propozycja produktu z dopasowania fuzzy (Etap 3)."""

    product_id: str
    name: str
    score: float
    category_id: str | None = None
    category_path: str | None = None
    model_config = ConfigDict(from_attributes=True)


class ItemMatch(BaseModel):
    """Wynik dopasowania jednej pozycji paragonu do produktu."""

    raw_name: str
    # 'alias_store' | 'alias_global' | 'name' | 'fuzzy' | 'none'
    match_type: str
    matched_product_id: str | None = None
    matched_product_name: str | None = None
    candidates: list[ProductCandidate] = Field(default_factory=list)
    suggested_category_id: str | None = None
    suggested_category_path: str | None = None
    prefill_name: str = ""
    model_config = ConfigDict(from_attributes=True)


class ProductMatchRequest(BaseModel):
    """Nazwy pozycji paragonu do dopasowania; store_id włącza aliasy per-sklep."""

    store_id: str | None = None
    raw_names: list[str] = Field(default_factory=list)


class ProductMatchResponse(BaseModel):
    matches: list[ItemMatch]


class ProductAliasCreate(BaseModel):
    """Zapis aliasu z potwierdzenia użytkownika (pętla uczenia)."""

    product_id: str
    raw_name: str
    store_id: str | None = None


class ProductAliasRead(BaseModel):
    id: str
    product_id: str
    store_id: str | None = None
    alias: str
    model_config = ConfigDict(from_attributes=True)


class TransactionBase(BaseModel):
    account_id: str
    store_id: str | None = None
    category_id: str | None = None
    transfer_account_id: str | None = None
    transfer_transaction_id: str | None = None
    receipt_number: str | None = None
    recorded_at: AwareDatetime | None = None
    date: dt.date | None = None
    currency: str = "PLN"
    payment_method: str | None = None
    comment: str | None = None
    cleared: bool = False
    reconciled: bool = False


class TransactionCreate(TransactionBase):
    items: list[TransactionItemCreate] = Field(default_factory=list)
    total_cents: int | None = None


class TransactionUpdate(BaseModel):
    account_id: str | None = None
    store_id: str | None = None
    category_id: str | None = None
    transfer_account_id: str | None = None
    transfer_transaction_id: str | None = None
    receipt_number: str | None = None
    recorded_at: AwareDatetime | None = None
    date: dt.date | None = None
    currency: str | None = None
    payment_method: str | None = None
    comment: str | None = None
    items: list[TransactionItemCreate] | None = None
    cleared: bool | None = None
    reconciled: bool | None = None
    total_cents: int | None = None


class TransactionRead(TransactionBase):
    id: str
    total_cents: int
    bank_description: str | None = None
    items: list[TransactionItemRead]

    model_config = ConfigDict(from_attributes=True)


class TransactionPage(BaseModel):
    """Strona listy transakcji; ``total`` to liczba wszystkich pasujących do filtrów."""

    items: list[TransactionRead]
    total: int
    offset: int
    limit: int


class TransactionItemDetail(TransactionItemRead):
    product_name: str | None = None


class TransactionDetail(TransactionRead):
    account_name: str | None = None
    store_name: str | None = None
    category_name: str | None = None
    items: list[TransactionItemDetail]
    attachments: list[TransactionAttachmentRead]


class TransactionItemListEntry(BaseModel):
    id: str
    transaction_id: str
    product_id: str
    product_name: str | None
    transaction_date: dt.date | None
    store_name: str | None
    account_name: str | None
    quantity: float
    unit: str | None
    unit_price_cents: int
    total_cents: int
    comment: str | None
    currency: str | None


class TagSuggestionRead(BaseModel):
    """Podpowiedź tagu w kolejce „do otagowania" (services/tagging.py)."""

    category_id: str
    # 'store_rule' | 'store' | 'store_group' | 'similar'
    source: str
    matches: int = 0
    similar_to: str | None = None
    model_config = ConfigDict(from_attributes=True)


class UntaggedTransactionRead(BaseModel):
    id: str
    date: dt.date
    total_cents: int
    currency: str
    comment: str | None = None
    account_id: str
    account_name: str
    store_id: str | None = None
    store_name: str | None = None
    # Ile transakcji tego sklepu czeka w kolejce — podgląd dla „otaguj cały sklep".
    store_untagged_count: int = 0
    suggestion: TagSuggestionRead | None = None


class UntaggedTransactionsPage(BaseModel):
    total: int
    items: list[UntaggedTransactionRead]


class UntaggedCount(BaseModel):
    count: int


class TagStoreRequest(BaseModel):
    store_id: str
    category_id: str


class TagStoreResult(BaseModel):
    updated: int


class MonthlyCategorySummary(BaseModel):
    category_id: str | None
    category_name: str | None
    total_cents: int
    total_amount: float


class SpendingBreakdownEntry(BaseModel):
    category_id: str | None
    category_name: str | None
    total_cents: int
    total_amount: float
    percentage: float
    transaction_count: int = 0


class SpendingOutflowInfo(BaseModel):
    """Największy pojedynczy wydatek w zakresie (statystyka w stylu YNAB)."""

    store_name: str | None = None
    comment: str | None = None
    category_name: str | None = None
    date: dt.date
    total_cents: int
    total_amount: float


class SpendingBreakdownReport(BaseModel):
    total_spending_cents: int
    total_spending_amount: float
    entries: list[SpendingBreakdownEntry]
    largest_outflow: SpendingOutflowInfo | None = None


class MonthlySpendingPoint(BaseModel):
    month: str
    label: str
    total_spending_cents: int
    total_spending_amount: float


class SpendingTrendsReport(BaseModel):
    total_spending_cents: int
    average_monthly_spending_cents: int
    months: list[MonthlySpendingPoint]


class IncomeExpenseCategoryRow(BaseModel):
    category_id: str | None
    category_name: str | None
    total_cents: int
    total_amount: float


class IncomeExpenseMonthRow(BaseModel):
    month: str
    label: str
    income_cents: int
    expense_cents: int
    net_cents: int
    income_categories: list[IncomeExpenseCategoryRow]
    expense_categories: list[IncomeExpenseCategoryRow]


class IncomeExpenseReport(BaseModel):
    total_income_cents: int
    total_expense_cents: int
    months: list[IncomeExpenseMonthRow]


class NetWorthPoint(BaseModel):
    month: str
    label: str
    assets_cents: int
    debts_cents: int
    net_worth_cents: int
    change_cents: int


class NetWorthAccountBalance(BaseModel):
    account_id: str
    account_kind: str
    category_id: str | None
    balance_cents: int


class NetWorthReport(BaseModel):
    months: list[NetWorthPoint]
    latest_assets_cents: int
    latest_debts_cents: int
    latest_net_worth_cents: int
    accounts: list[NetWorthAccountBalance]


class AgeOfMoneyPoint(BaseModel):
    month: str
    label: str
    age_of_money_days: float | None
    on_budget_balance_cents: int
    average_daily_outflow_cents: int
    sample_start_date: dt.date
    sample_end_date: dt.date
    sampled_days: int


class AgeOfMoneyReport(BaseModel):
    on_budget_balance_cents: int
    average_daily_outflow_cents: int
    age_of_money_days: float | None
    sample_start_date: dt.date
    sample_end_date: dt.date
    sampled_days: int
    history: list[AgeOfMoneyPoint]


class ReportsDashboardResponse(BaseModel):
    start_date: dt.date
    end_date: dt.date
    earliest_transaction_date: dt.date | None = None
    spending_breakdown: SpendingBreakdownReport
    spending_trends: SpendingTrendsReport
    net_worth: NetWorthReport
    income_vs_expense: IncomeExpenseReport
    age_of_money: AgeOfMoneyReport


class BudgetCategoryRow(BaseModel):
    category_id: str
    category_name: str
    parent_id: str | None = None
    assigned_cents: int
    activity_cents: int
    available_cents: int
    is_credit_payment: bool = False
    status: str | None = None


class BudgetMonthSummary(BaseModel):
    month: str
    label: str
    rta_cents: int
    assigned_cents: int
    activity_cents: int
    available_cents: int
    on_budget_balance_cents: int


class BudgetMonthResponse(BaseModel):
    month: str
    label: str
    summary: BudgetMonthSummary
    categories: list[BudgetCategoryRow]


class BudgetAssignmentUpdate(BaseModel):
    category_id: str
    month: str
    assigned_cents: int


class HomeEnvelope(BaseModel):
    category_id: str
    category_name: str
    available_cents: int
    spent_cents: int
    # Ile było w kopercie na ten miesiąc: przeniesione + przypisane.
    funds_cents: int


class HomeStaleAccount(BaseModel):
    account_id: str
    account_name: str
    last_data_date: dt.date
    days_since: int


class HomeRecentTransaction(BaseModel):
    id: str
    date: dt.date
    total_cents: int
    currency: str
    account_name: str
    store_name: str | None = None
    comment: str | None = None
    category_id: str | None = None
    is_transfer: bool = False
    needs_tag: bool = False


class HomeSummary(BaseModel):
    today: dt.date
    month: str
    untagged_count: int
    receipts_todo: int
    ready_to_assign_cents: int
    overspent_count: int
    envelopes: list[HomeEnvelope]
    spent_month_cents: int
    spent_previous_month_cents: int
    on_budget_balance_cents: int
    last_transaction_date: dt.date | None = None
    stale_threshold_days: int
    stale_accounts: list[HomeStaleAccount]
    recent: list[HomeRecentTransaction]


class YnabAuth(BaseModel):
    api_key: str | None = Field(default=None, min_length=1)


class YnabSettingsRead(BaseModel):
    api_key_set: bool = False
    api_key_from_config: bool = False
    budget_id: str | None = None
    last_imported_at: dt.datetime | None = None
    last_server_knowledge: int | None = None


class YnabSettingsUpdate(BaseModel):
    api_key: str | None = None
    budget_id: str | None = None


class YnabBudgetSummary(BaseModel):
    id: str
    name: str
    last_modified_on: dt.datetime | None = None


class YnabImportRequest(YnabAuth):
    budget_id: str


class YnabImportResponse(BaseModel):
    budget_id: str
    budget_name: str | None = None
    accounts: int
    stores: int
    categories: int
    transactions_inserted: int
    transactions_skipped: int
    # Sklepy, którym pierwszy import ustawił kategorię z historii transakcji
    stores_categorized: int = 0


class DemoLoadResponse(BaseModel):
    accounts: int
    tags: int
    stores: int
    transactions: int


class HealthResponse(BaseModel):
    status: str = "ok"


class UserRead(BaseModel):
    id: str
    username: str | None = None
    display_name: str | None = None
    is_admin: bool = False

    model_config = ConfigDict(from_attributes=True)


class UserAdminRead(UserRead):
    created_at: dt.datetime
    passkey_count: int = 0

    model_config = ConfigDict(from_attributes=True)


class PasskeyRegistrationOptionsRequest(BaseModel):
    username: str | None = None
    display_name: str | None = None
    invite_token: str | None = None


class PasskeyRegistrationOptionsResponse(BaseModel):
    challenge_id: str
    options: dict[str, Any]


class PasskeyRegistrationVerifyRequest(BaseModel):
    challenge_id: str
    credential: dict[str, Any]


class PasskeyAuthenticationOptionsRequest(BaseModel):
    username: str | None = None


class PasskeyAuthenticationOptionsResponse(BaseModel):
    challenge_id: str
    options: dict[str, Any]


class PasskeyAuthenticationVerifyRequest(BaseModel):
    challenge_id: str
    credential: dict[str, Any]


class AuthSessionResponse(BaseModel):
    token: str
    user: UserRead


class DevLoginRequest(BaseModel):
    username: str | None = None


class UserCreateRequest(BaseModel):
    username: str = Field(min_length=3, max_length=64)
    display_name: str = Field(min_length=1, max_length=128)


class UserInviteResponse(BaseModel):
    user: UserRead
    invite_token: str
    invite_expires_at: dt.datetime


class UserResetResponse(UserInviteResponse):
    passkeys_revoked: int
    sessions_revoked: int


class TableColumnWidthsUpdate(BaseModel):
    widths: dict[str, int] = Field(default_factory=dict)


class TableColumnWidthsRead(TableColumnWidthsUpdate):
    table: str


class BackupFileRead(BaseModel):
    name: str
    created_at: dt.datetime
    size_bytes: int


class BackupStatusRead(BaseModel):
    enabled: bool
    directory: str
    interval_hours: float
    keep_daily: int
    keep_weekly: int
    keep_monthly: int
    include_attachments: bool
    last_backup_at: dt.datetime | None
    # Orientacyjnie: harmonogram liczy od ostatniej kopii, a gdy aplikacja była
    # wyłączona, nadrabia przy starcie.
    next_backup_at: dt.datetime | None
    running: bool
    last_error: str | None
    last_error_at: dt.datetime | None
    attachments_files: int
    attachments_bytes: int
    backups: list[BackupFileRead]


# ============ Import wyciągu CSV (docs/design/import-csv.md) ============

BankRowStatus = Literal[
    "new", "already_imported", "match", "transfer", "other_currency", "skipped", "invalid"
]
BankRowAction = Literal["create", "merge", "transfer", "skip"]


class BankParseOptions(BaseModel):
    """Ustawienia parsera. `None` = wykryj automatycznie."""

    encoding: str | None = None
    delimiter: str | None = None
    header_row: int | None = None
    date_format: str | None = None
    decimal_separator: Literal[",", "."] | None = None


class BankMapping(BaseModel):
    """Role kolumn kluczowane INDEKSEM kolumny (ING ma cztery „Waluty")."""

    columns: dict[str, str] = Field(default_factory=dict)
    amount_mode: Literal["signed", "signed_inverted", "split"] = "signed"
    status_accepted: list[str] = Field(default_factory=list)
    # Nazwy kolumn z chwili zapisu profilu — wykrywają przestawione kolumny.
    header_names: dict[str, str] = Field(default_factory=dict)


class BankImportProfileRead(BaseModel):
    id: str
    name: str
    account_id: str | None = None
    parse_options: BankParseOptions
    mapping: BankMapping
    last_used_at: dt.datetime | None = None
    model_config = ConfigDict(from_attributes=True)


class BankImportProfileUpdate(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=80)
    account_id: str | None = None


class BankImportColumn(BaseModel):
    index: int
    name: str
    samples: list[str]


class BankPreviousImport(BaseModel):
    batch_id: str
    created_at: dt.datetime
    account_id: str
    account_name: str


class BankInspectResponse(BaseModel):
    options: BankParseOptions
    columns: list[BankImportColumn]
    row_count: int
    file_sha256: str
    profile: BankImportProfileRead | None = None
    # "full" — nagłówek identyczny; "partial" — część zmapowanych kolumn ma
    # inne nazwy (bank przestawił kolumny), mapowanie trzeba sprawdzić.
    profile_match: Literal["full", "partial"] | None = None
    profile_mismatched_columns: list[int] = Field(default_factory=list)
    previous_import: BankPreviousImport | None = None


class BankImportSpec(BaseModel):
    account_id: str
    options: BankParseOptions = Field(default_factory=BankParseOptions)
    mapping: BankMapping


class BankMatchInfo(BaseModel):
    transaction_id: str
    account_id: str
    account_name: str
    date: dt.date
    amount_cents: int
    label: str
    # "receipt" — ma pozycje paragonu; "bank" — z wcześniejszego importu;
    # "other" — YNAB albo dodana ręcznie (bez znacznika nie da się ich odróżnić).
    source: Literal["receipt", "bank", "other"]


class BankPreviewRow(BaseModel):
    index: int
    line: int
    status: BankRowStatus
    reason: str | None = None
    date: dt.date | None = None
    amount_cents: int | None = None
    currency: str | None = None
    payee: str | None = None
    memo: str | None = None
    suggested_action: BankRowAction
    match: BankMatchInfo | None = None


class BankPreviewResponse(BaseModel):
    options: BankParseOptions
    file_sha256: str
    rows: list[BankPreviewRow]
    counts: dict[str, int]
    previous_import: BankPreviousImport | None = None


class BankRowDecision(BaseModel):
    row: int
    action: BankRowAction
    target_transaction_id: str | None = None
    transfer_account_id: str | None = None


class BankSaveProfile(BaseModel):
    name: str = Field(min_length=1, max_length=80)
    bind_account: bool = True


class BankCommitRequest(BankImportSpec):
    file_sha256: str
    decisions: list[BankRowDecision]
    profile_id: str | None = None
    save_profile: BankSaveProfile | None = None
    file_name: str | None = None


class BankImportBatchRead(BaseModel):
    id: str
    account_id: str
    account_name: str
    profile_id: str | None = None
    file_name: str
    date_from: dt.date | None = None
    date_to: dt.date | None = None
    created_count: int
    merged_count: int
    skipped_count: int
    created_at: dt.datetime


class BankImportUndoResult(BaseModel):
    deleted: int
    unlinked: int


__all__ = [
    "AccountBalanceAdjustment",
    "AccountCreate",
    "AccountRead",
    "AccountUpdate",
    "AgeOfMoneyPoint",
    "AgeOfMoneyReport",
    "AuthSessionResponse",
    "BackupFileRead",
    "BackupStatusRead",
    "BankCommitRequest",
    "BankImportBatchRead",
    "BankImportColumn",
    "BankImportProfileRead",
    "BankImportProfileUpdate",
    "BankImportSpec",
    "BankImportUndoResult",
    "BankInspectResponse",
    "BankMapping",
    "BankMatchInfo",
    "BankParseOptions",
    "BankPreviewResponse",
    "BankPreviewRow",
    "BankPreviousImport",
    "BankRowAction",
    "BankRowDecision",
    "BankRowStatus",
    "BankSaveProfile",
    "BudgetAssignmentUpdate",
    "BudgetCategoryRow",
    "BudgetMonthResponse",
    "BudgetMonthSummary",
    "CategoryCreate",
    "CategoryRead",
    "CategoryUpdate",
    "DevLoginRequest",
    "HealthResponse",
    "HomeEnvelope",
    "HomeRecentTransaction",
    "HomeStaleAccount",
    "HomeSummary",
    "IncomeExpenseCategoryRow",
    "IncomeExpenseMonthRow",
    "IncomeExpenseReport",
    "ItemAliasRead",
    "ItemBase",
    "ItemCreate",
    "ItemMatch",
    "ItemMergeRequest",
    "ItemMergeResult",
    "ItemRead",
    "ItemUpdate",
    "ItemUsageRead",
    "MonthlyCategorySummary",
    "MonthlySpendingPoint",
    "NetWorthAccountBalance",
    "NetWorthPoint",
    "NetWorthReport",
    "PasskeyAuthenticationOptionsRequest",
    "PasskeyAuthenticationOptionsResponse",
    "PasskeyAuthenticationVerifyRequest",
    "PasskeyRegistrationOptionsRequest",
    "PasskeyRegistrationOptionsResponse",
    "PasskeyRegistrationVerifyRequest",
    "ProductAliasCreate",
    "ProductAliasRead",
    "ProductCandidate",
    "ProductMatchRequest",
    "ProductMatchResponse",
    "ReceiptCheckIssue",
    "ReceiptCheckRules",
    "ReceiptDraftCommit",
    "ReceiptDraftCommitItem",
    "ReceiptDraftCommitResult",
    "ReceiptDraftCounts",
    "ReceiptDraftDetail",
    "ReceiptDraftItem",
    "ReceiptDraftListResponse",
    "ReceiptDraftPayload",
    "ReceiptDraftSummary",
    "ReceiptDraftUpdate",
    "ReportsDashboardResponse",
    "SpendingBreakdownEntry",
    "SpendingBreakdownReport",
    "SpendingTrendsReport",
    "StoreBase",
    "StoreCreate",
    "StoreRead",
    "StoreUpdate",
    "TableColumnWidthsRead",
    "TableColumnWidthsUpdate",
    "TagStoreRequest",
    "TagStoreResult",
    "TagSuggestionRead",
    "TransactionAttachmentRead",
    "TransactionBase",
    "TransactionCreate",
    "TransactionDetail",
    "TransactionItemBase",
    "TransactionItemCreate",
    "TransactionItemDetail",
    "TransactionItemListEntry",
    "TransactionItemRead",
    "TransactionRead",
    "TransactionUpdate",
    "UntaggedCount",
    "UntaggedTransactionRead",
    "UntaggedTransactionsPage",
    "UserAdminRead",
    "UserCreateRequest",
    "UserInviteResponse",
    "UserRead",
    "UserResetResponse",
    "YnabAuth",
    "YnabBudgetSummary",
    "YnabImportRequest",
    "YnabImportResponse",
    "YnabSettingsRead",
    "YnabSettingsUpdate",
]
