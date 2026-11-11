from __future__ import annotations

import datetime as dt
import uuid
from datetime import UTC, date, datetime
from decimal import Decimal

from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    CheckConstraint,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    LargeBinary,
    String,
    Text,
    TypeDecorator,
    UniqueConstraint,
    text,
)
from sqlalchemy.engine import Dialect
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    mapped_column,
    relationship,
)


# ============ SQLAlchemy Base ============
class Base(DeclarativeBase):
    pass


# ============ Helpers ============
def gen_uuid() -> str:
    return str(uuid.uuid4())


def utcnow() -> datetime:
    return datetime.now(UTC)


class UTCDateTime(TypeDecorator):
    """DateTime(timezone=True), naprawiony pod SQLite.

    SQLite ucina offset przy zapisie bez normalizacji do UTC (aware datetime
    z inną strefą zapisałby się jako to samo wall-clock, czyli po cichu zła
    wartość) i zwraca naive przy odczycie. Normalizujemy do UTC na wejściu i
    doczepiamy tzinfo=UTC na wyjściu, żeby cała aplikacja widziała spójne,
    aware wartości.
    """

    impl = DateTime(timezone=True)
    cache_ok = True

    def process_bind_param(
        self,
        value: datetime | None,
        # Nieużywany, ale nazwa i pozycja są narzucone przez `TypeDecorator`.
        dialect: Dialect,  # noqa: ARG002
    ) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None:
            raise ValueError("UTCDateTime wymaga aware datetime (użyj utcnow())")
        return value.astimezone(UTC)

    def process_result_value(
        self,
        value: datetime | None,
        # Nieużywany, ale nazwa i pozycja są narzucone przez `TypeDecorator`.
        dialect: Dialect,  # noqa: ARG002
    ) -> datetime | None:
        if value is not None and value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value


# ============ Modele ============


class User(Base):
    __tablename__ = "users"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=gen_uuid)
    username: Mapped[str | None] = mapped_column(String(64), unique=True)
    display_name: Mapped[str | None] = mapped_column(Text)
    is_admin: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default="0"
    )
    created_at: Mapped[datetime] = mapped_column(UTCDateTime, default=utcnow, nullable=False)

    passkeys: Mapped[list[PasskeyCredential]] = relationship(
        back_populates="user", cascade="all, delete-orphan"
    )
    sessions: Mapped[list[AuthSession]] = relationship(
        back_populates="user", cascade="all, delete-orphan"
    )
    invites: Mapped[list[UserInvite]] = relationship(
        back_populates="user", cascade="all, delete-orphan"
    )


class PasskeyCredential(Base):
    __tablename__ = "passkey_credentials"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=gen_uuid)
    user_id: Mapped[str] = mapped_column(
        String, ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    credential_id: Mapped[str] = mapped_column(String, unique=True, nullable=False)
    credential_data: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    sign_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    transports: Mapped[str | None] = mapped_column(Text)
    aaguid: Mapped[str | None] = mapped_column(String(64))
    created_at: Mapped[datetime] = mapped_column(UTCDateTime, default=utcnow, nullable=False)
    last_used_at: Mapped[datetime | None] = mapped_column(UTCDateTime)

    user: Mapped[User] = relationship(back_populates="passkeys")


class PasskeyChallenge(Base):
    __tablename__ = "passkey_challenges"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=gen_uuid)
    user_id: Mapped[str | None] = mapped_column(String, ForeignKey("users.id", ondelete="CASCADE"))
    invite_id: Mapped[str | None] = mapped_column(
        String, ForeignKey("user_invites.id", ondelete="SET NULL")
    )
    challenge: Mapped[str] = mapped_column(String, nullable=False)
    user_verification: Mapped[str | None] = mapped_column(String(32))
    challenge_type: Mapped[str] = mapped_column(String(32), nullable=False)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime, default=utcnow, nullable=False)
    expires_at: Mapped[datetime] = mapped_column(UTCDateTime, nullable=False)

    user: Mapped[User | None] = relationship()
    invite: Mapped[UserInvite | None] = relationship()


class UserInvite(Base):
    __tablename__ = "user_invites"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=gen_uuid)
    user_id: Mapped[str] = mapped_column(
        String, ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    token: Mapped[str] = mapped_column(String, unique=True, nullable=False)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime, default=utcnow, nullable=False)
    expires_at: Mapped[datetime] = mapped_column(UTCDateTime, nullable=False)
    used_at: Mapped[datetime | None] = mapped_column(UTCDateTime)

    user: Mapped[User] = relationship(back_populates="invites")


class YnabSettings(Base):
    __tablename__ = "ynab_settings"

    user_id: Mapped[str] = mapped_column(
        String, ForeignKey("users.id", ondelete="CASCADE"), primary_key=True
    )
    budget_id: Mapped[str | None] = mapped_column(String)
    last_imported_at: Mapped[datetime | None] = mapped_column(UTCDateTime)
    last_server_knowledge: Mapped[int | None] = mapped_column(Integer)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime, default=utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(UTCDateTime, default=utcnow, nullable=False)


class YnabBudgetSnapshot(Base):
    __tablename__ = "ynab_budget_snapshots"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=gen_uuid)
    user_id: Mapped[str] = mapped_column(
        String, ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    budget_id: Mapped[str] = mapped_column(String, nullable=False)
    month: Mapped[str] = mapped_column(String(7), nullable=False)
    server_knowledge: Mapped[int | None] = mapped_column(Integer)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime, default=utcnow, nullable=False)

    __table_args__ = (
        UniqueConstraint("user_id", "budget_id", "month", name="uq_ynab_budget_snapshot"),
    )


class Budget(Base):
    __tablename__ = "budget"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=gen_uuid)
    # "assignment" = lokalny wpis budżetowy użytkownika
    # "ynab_category" = snapshot kategorii z importu YNAB
    entry_type: Mapped[str] = mapped_column(String(32), nullable=False)
    user_id: Mapped[str | None] = mapped_column(
        String, ForeignKey("users.id", ondelete="CASCADE"), nullable=True
    )
    snapshot_id: Mapped[str | None] = mapped_column(
        String, ForeignKey("ynab_budget_snapshots.id", ondelete="CASCADE"), nullable=True
    )
    category_id: Mapped[str] = mapped_column(
        String, ForeignKey("categories.id", ondelete="CASCADE"), nullable=False
    )
    month: Mapped[str | None] = mapped_column(String(7))

    # Lokalny budżet (used by /budget endpoints)
    assigned_cents: Mapped[int] = mapped_column(
        BigInteger, nullable=False, default=0, server_default="0"
    )
    created_at: Mapped[datetime | None] = mapped_column(UTCDateTime, default=utcnow)
    updated_at: Mapped[datetime | None] = mapped_column(UTCDateTime, default=utcnow)

    # Snapshot YNAB
    hidden: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="0")
    deleted: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default="0"
    )
    note: Mapped[str | None] = mapped_column(Text)
    original_category_group_id: Mapped[str | None] = mapped_column(String)
    budgeted_milliunits: Mapped[int] = mapped_column(
        BigInteger, nullable=False, default=0, server_default="0"
    )
    activity_milliunits: Mapped[int] = mapped_column(
        BigInteger, nullable=False, default=0, server_default="0"
    )
    balance_milliunits: Mapped[int] = mapped_column(
        BigInteger, nullable=False, default=0, server_default="0"
    )
    goal_type: Mapped[str | None] = mapped_column(String(16))
    goal_needs_whole_amount: Mapped[bool | None] = mapped_column(Boolean)
    goal_day: Mapped[int | None] = mapped_column(Integer)
    goal_cadence: Mapped[int | None] = mapped_column(Integer)
    goal_cadence_frequency: Mapped[int | None] = mapped_column(Integer)
    goal_creation_month: Mapped[str | None] = mapped_column(String(10))
    goal_target_milliunits: Mapped[int] = mapped_column(
        BigInteger, nullable=False, default=0, server_default="0"
    )
    goal_target_month: Mapped[str | None] = mapped_column(String(10))
    goal_percentage_complete: Mapped[int | None] = mapped_column(Integer)
    goal_months_to_budget: Mapped[int | None] = mapped_column(Integer)
    goal_under_funded_milliunits: Mapped[int] = mapped_column(
        BigInteger, nullable=False, default=0, server_default="0"
    )
    goal_overall_funded_milliunits: Mapped[int] = mapped_column(
        BigInteger, nullable=False, default=0, server_default="0"
    )
    goal_overall_left_milliunits: Mapped[int] = mapped_column(
        BigInteger, nullable=False, default=0, server_default="0"
    )
    goal_snoozed_at: Mapped[str | None] = mapped_column(String(32))

    category: Mapped[Category] = relationship()

    __table_args__ = (
        CheckConstraint(
            "entry_type IN ('assignment', 'ynab_category')",
            name="chk_budget_entry_type",
        ),
        UniqueConstraint(
            "entry_type",
            "user_id",
            "category_id",
            "month",
            name="uq_budget_assignment_entry",
        ),
        UniqueConstraint(
            "entry_type",
            "snapshot_id",
            "category_id",
            name="uq_budget_ynab_snapshot_entry",
        ),
    )


class AuthSession(Base):
    __tablename__ = "auth_sessions"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=gen_uuid)
    user_id: Mapped[str] = mapped_column(
        String, ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    created_at: Mapped[datetime] = mapped_column(UTCDateTime, default=utcnow, nullable=False)
    expires_at: Mapped[datetime] = mapped_column(UTCDateTime, nullable=False)

    user: Mapped[User] = relationship(back_populates="sessions")


class Account(Base):
    __tablename__ = "accounts"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=gen_uuid)
    user_id: Mapped[str] = mapped_column(
        String, ForeignKey("users.id", ondelete="CASCADE"), nullable=True
    )
    name: Mapped[str] = mapped_column(Text, nullable=False)
    currency: Mapped[str] = mapped_column(String(3), nullable=False, default="PLN")
    comment: Mapped[str | None] = mapped_column(Text)
    # Prezentacja konta: nazwa ikony (z kuratorowanego zestawu na froncie) oraz
    # klucz koloru z palety motywu. Oba opcjonalne — brak = domyślny awatar.
    icon: Mapped[str | None] = mapped_column(String(48))
    color: Mapped[str | None] = mapped_column(String(24))
    is_closed: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default="0"
    )
    # Odwzorowanie YNAB-a: `closed` to archiwizacja (konto nadal widoczne),
    # `deleted` to usunięcie po stronie YNAB-a. Trzymamy je osobno, bo to dwie
    # różne rzeczy — a transakcji z usuniętego konta nie kasujemy, historia
    # lokalna zostaje. Ta sama flaga i to samo znaczenie co w `Store`.
    is_deleted: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default="0"
    )
    is_on_budget: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=True, server_default="1"
    )
    # Przypięte do paska bocznego (saldo + szybkie przejście), jak ulubione w YNAB-ie.
    is_favorite: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default="0"
    )
    # Data ostatniego uzgodnienia salda. Wypełnia ją wyłącznie import z YNAB-a;
    # dla kont zakładanych ręcznie zostaje None, bo nie mamy własnego procesu
    # uzgadniania — flaga `reconciled` na transakcji to co innego.
    last_reconciled_at: Mapped[datetime | None] = mapped_column(UTCDateTime)
    account_kind: Mapped[str] = mapped_column(
        String(16), nullable=False, default="cash", server_default="cash"
    )
    credit_payment_category_id: Mapped[str | None] = mapped_column(
        String, ForeignKey("categories.id", ondelete="SET NULL")
    )
    category_id: Mapped[str | None] = mapped_column(
        String, ForeignKey("categories.id", ondelete="SET NULL")
    )
    category: Mapped[Category | None] = relationship(foreign_keys=[category_id])
    credit_payment_category: Mapped[Category | None] = relationship(
        foreign_keys=[credit_payment_category_id]
    )

    transactions: Mapped[list[Transaction]] = relationship(
        back_populates="account",
        cascade="all, delete-orphan",
        foreign_keys="Transaction.account_id",
    )

    debt_terms: Mapped[list[AccountDebtTerms]] = relationship(
        back_populates="account",
        cascade="all, delete-orphan",
        order_by="AccountDebtTerms.month",
    )

    @property
    def is_tracking(self) -> bool:
        return not self.is_on_budget


class AccountDebtTerms(Base):
    """Warunki kredytu obowiązujące w danym miesiącu.

    YNAB trzyma oprocentowanie, ratę minimalną i escrow jako SZEREGI CZASOWE
    kluczowane miesiącem (`{"2026-08-01": 6550}`), a nie jako pojedyncze
    wartości na koncie. Stąd osobna tabela zamiast kolumn na `accounts`:
    pierwsza zmiana stopy nadpisałaby historię, a bez historii rozbicie raty
    na kapitał i odsetki przestaje się zgadzać wstecz.

    Kształt jest celowo bliźniaczy do `Budget` (wiersz = stan na miesiąc,
    `month` jako `RRRR-MM`) — ten wzorzec jest już w projekcie i nie ma powodu
    mnożyć konwencji.
    """

    __tablename__ = "account_debt_terms"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=gen_uuid)
    account_id: Mapped[str] = mapped_column(
        String, ForeignKey("accounts.id", ondelete="CASCADE"), nullable=False
    )
    user_id: Mapped[str] = mapped_column(
        String, ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    month: Mapped[str] = mapped_column(String(7), nullable=False)

    # Punkty bazowe, nie float: 6,55% to 655. Reszta projektu konsekwentnie
    # trzyma pieniądze i stawki w liczbach całkowitych i nie ma powodu robić
    # tu wyjątku. Konwersja z jednostek YNAB-a siedzi w jednym miejscu —
    # `ynab_rate_mu_to_bps` w scripts/import_ynab_data.py.
    interest_rate_bps: Mapped[int | None] = mapped_column(Integer)
    minimum_payment_cents: Mapped[int | None] = mapped_column(BigInteger)
    escrow_cents: Mapped[int | None] = mapped_column(BigInteger)

    account: Mapped[Account] = relationship(back_populates="debt_terms")

    __table_args__ = (
        UniqueConstraint("account_id", "month", name="uq_account_debt_terms_month"),
        # Widok długu czyta wszystkie miesiące jednego konta. Unikat wyżej też
        # to pokrywa, ale nazwany indeks przetrwa ewentualną zmianę unikatu.
        Index("ix_account_debt_terms_account_id", "account_id"),
    )


class Store(Base):
    __tablename__ = "stores"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=gen_uuid)
    user_id: Mapped[str] = mapped_column(
        String, ForeignKey("users.id", ondelete="CASCADE"), nullable=True
    )
    name: Mapped[str] = mapped_column(Text, nullable=False)
    group_name: Mapped[str | None] = mapped_column(Text)
    include_in_payee_list: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=True, server_default="1"
    )
    automatically_categorize_payee: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default="0"
    )
    automatic_category_id: Mapped[str | None] = mapped_column(
        String, ForeignKey("categories.id", ondelete="SET NULL")
    )
    renaming_rules: Mapped[list[str]] = mapped_column(
        JSON, nullable=False, default=list, server_default="[]"
    )
    nip: Mapped[str | None] = mapped_column(String)
    address: Mapped[str | None] = mapped_column(Text)
    comment: Mapped[str | None] = mapped_column(Text)
    is_deleted: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default="0"
    )
    # Nazwa grupy potwierdzona przez użytkownika mimo reguły „surowej nazwy z banku”.
    # Pasuje tylko do tej nazwy, więc po zmianie nazwy grupy reguła wraca.
    verified_name: Mapped[str | None] = mapped_column(Text)

    # Sklep scalony: ukryty, a import YNAB kieruje jego payee do sklepu docelowego.
    merged_into_id: Mapped[str | None] = mapped_column(
        String, ForeignKey("stores.id", ondelete="SET NULL")
    )

    transactions: Mapped[list[Transaction]] = relationship(back_populates="store")

    __table_args__ = (UniqueConstraint("user_id", "nip", name="uq_stores_user_nip"),)


class Category(Base):
    __tablename__ = "categories"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=gen_uuid)
    user_id: Mapped[str] = mapped_column(
        String, ForeignKey("users.id", ondelete="CASCADE"), nullable=True
    )
    name: Mapped[str] = mapped_column(Text, nullable=False)
    parent_id: Mapped[str | None] = mapped_column(
        String, ForeignKey("categories.id", ondelete="CASCADE")
    )
    is_for_accounts: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default="0"
    )
    # Tag przychodu: transakcje z nim zasilają „Do przypisania" (odpowiednik
    # YNAB-owego „Inflow: Ready to Assign"), więc w budżecie nie jest kopertą.
    is_ready_to_assign: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default="0"
    )
    # Klucz koloru z palety motywu (np. "teal"), analogicznie do Account.color.
    # NULL = dziedzicz kolor po najbliższym przodku z ustawionym kolorem.
    color: Mapped[str | None] = mapped_column(String(24))
    # Pozycja wśród rodzeństwa (ten sam parent_id), ustawiana przeciąganiem na ekranie Kategorie.
    sort_order: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")

    parent: Mapped[Category | None] = relationship(remote_side=[id])
    children: Mapped[list[Category]] = relationship(
        back_populates="parent", cascade="all, delete-orphan", foreign_keys=[parent_id]
    )

    __table_args__ = (
        UniqueConstraint("user_id", "name", "parent_id", name="uq_categories_name_parent"),
    )


class Product(Base):
    __tablename__ = "products"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=gen_uuid)
    user_id: Mapped[str] = mapped_column(
        String, ForeignKey("users.id", ondelete="CASCADE"), nullable=True
    )
    name: Mapped[str] = mapped_column(Text, nullable=False)
    group_name: Mapped[str | None] = mapped_column(Text)
    name_simplified: Mapped[str | None] = mapped_column(Text)
    unit_of_measurement: Mapped[str | None] = mapped_column(String(16))  # 'kg', 'szt', 'l'
    category_id: Mapped[str | None] = mapped_column(
        String, ForeignKey("categories.id", ondelete="SET NULL")
    )
    tax_rate: Mapped[float | None] = mapped_column(Float)  # 0..100, pilnujemy checkiem w appce
    manufacturer: Mapped[str | None] = mapped_column(Text)
    comment: Mapped[str | None] = mapped_column(Text)

    category: Mapped[Category | None] = relationship()
    items: Mapped[list[TransactionItem]] = relationship(back_populates="product")


class ProductAlias(Base):
    """Znormalizowana nazwa produktu z paragonu → produkt (Etap 3 pipeline'u OCR).

    Alias uczy się z potwierdzeń użytkownika w przeglądzie paragonu. Może być
    globalny (``store_id`` NULL) albo przypięty do konkretnego sklepu — nazwy na
    paragonach są skrótami specyficznymi dla sieci. Kolejny paragon z tego samego
    sklepu mapuje się wtedy automatycznie. Projekt: docs/design/ocr-pipeline.md.
    """

    __tablename__ = "product_aliases"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=gen_uuid)
    product_id: Mapped[str] = mapped_column(
        String, ForeignKey("products.id", ondelete="CASCADE"), nullable=False
    )
    store_id: Mapped[str | None] = mapped_column(
        String, ForeignKey("stores.id", ondelete="CASCADE")
    )
    alias: Mapped[str] = mapped_column(Text, nullable=False)

    product: Mapped[Product] = relationship()
    store: Mapped[Store | None] = relationship()

    __table_args__ = (UniqueConstraint("store_id", "alias", name="uq_product_alias_store"),)


class StoreAlias(Base):
    """Nazwa z banku przypisana do sklepu; po niej importer rozpoznaje transakcje."""

    __tablename__ = "store_aliases"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=gen_uuid)
    user_id: Mapped[str | None] = mapped_column(String, ForeignKey("users.id", ondelete="CASCADE"))
    store_id: Mapped[str] = mapped_column(
        String, ForeignKey("stores.id", ondelete="CASCADE"), nullable=False, index=True
    )
    pattern: Mapped[str] = mapped_column(Text, nullable=False)
    normalized: Mapped[str] = mapped_column(Text, nullable=False)
    match_type: Mapped[str] = mapped_column(String, nullable=False)  # 'exact' | 'prefix'
    # Tę nazwę ma kilka aktywnych sklepów, więc importer jej nie używa.
    is_ambiguous: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default="0"
    )
    source: Mapped[str] = mapped_column(String, nullable=False)  # 'store_name' | 'manual'
    created_at: Mapped[datetime] = mapped_column(UTCDateTime, default=utcnow, nullable=False)

    __table_args__ = (
        Index(
            "uq_store_aliases_active",
            "user_id",
            "normalized",
            "match_type",
            unique=True,
            sqlite_where=text("is_ambiguous = 0"),
        ),
    )


class StoreMerge(Base):
    """Scalenie sklepów ze stanem sprzed niego, potrzebnym do cofania."""

    __tablename__ = "store_merges"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=gen_uuid)
    user_id: Mapped[str | None] = mapped_column(String, ForeignKey("users.id", ondelete="CASCADE"))
    target_store_id: Mapped[str] = mapped_column(
        String, ForeignKey("stores.id", ondelete="CASCADE"), nullable=False, index=True
    )
    payload: Mapped[dict] = mapped_column(JSON, nullable=False)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime, default=utcnow, nullable=False)
    undone_at: Mapped[datetime | None] = mapped_column(UTCDateTime)


class StoreTransferConversion(Base):
    """Zamiana transakcji sklepu na przelewy ze stanem sprzed niej, potrzebnym do cofania."""

    __tablename__ = "store_transfer_conversions"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=gen_uuid)
    user_id: Mapped[str | None] = mapped_column(String, ForeignKey("users.id", ondelete="CASCADE"))
    payload: Mapped[dict] = mapped_column(JSON, nullable=False)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime, default=utcnow, nullable=False)
    undone_at: Mapped[datetime | None] = mapped_column(UTCDateTime)


class Transaction(Base):
    __tablename__ = "transactions"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=gen_uuid)
    user_id: Mapped[str] = mapped_column(
        String, ForeignKey("users.id", ondelete="CASCADE"), nullable=True
    )
    account_id: Mapped[str] = mapped_column(
        String, ForeignKey("accounts.id", ondelete="RESTRICT"), nullable=False
    )
    store_id: Mapped[str | None] = mapped_column(
        String, ForeignKey("stores.id", ondelete="SET NULL")
    )
    category_id: Mapped[str | None] = mapped_column(
        String, ForeignKey("categories.id", ondelete="SET NULL")
    )

    receipt_number: Mapped[str | None] = mapped_column(Text)

    # data księgowa (z YNAB / z formularza)
    # Kolumna nazywa się tak samo jak typ, więc `Mapped[date]` wskazywałoby
    # wewnątrz klasy na nią samą — stąd adnotacja przez alias modułu.
    date: Mapped[dt.date] = mapped_column(Date, nullable=False)

    # "moment zdarzenia" – przy imporcie z YNAB: północ tego dnia
    # przy ręcznym dodawaniu: domyślnie teraz (możesz nadpisać)
    recorded_at: Mapped[datetime] = mapped_column(
        UTCDateTime,
        default=utcnow,
        nullable=False,
    )

    # kiedy wpis trafił do naszej bazy
    created_at: Mapped[datetime] = mapped_column(
        UTCDateTime,
        default=utcnow,
        nullable=False,
    )

    total_cents: Mapped[int] = mapped_column(BigInteger, nullable=False)
    currency: Mapped[str] = mapped_column(String(3), nullable=False, default="PLN")
    payment_method: Mapped[str | None] = mapped_column(Text)
    comment: Mapped[str | None] = mapped_column(Text)
    cleared: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default="0"
    )
    reconciled: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default="0"
    )
    transfer_account_id: Mapped[str | None] = mapped_column(
        String, ForeignKey("accounts.id", ondelete="SET NULL")
    )
    transfer_transaction_id: Mapped[str | None] = mapped_column(String)

    # Tożsamość wiersza wyciągu bankowego: `id:<nr transakcji banku>` albo
    # `fp:<odcisk surowego wiersza>#<n>`. Ustawiana przy imporcie CSV — także
    # na transakcjach z YNAB-a czy z paragonu, które import scalił z wierszem
    # banku — dzięki czemu kolejny import tego okresu rozpoznaje je dokładnie.
    # Projekt: docs/design/import-csv.md §5.
    external_id: Mapped[str | None] = mapped_column(Text)
    # Tylko transakcje UTWORZONE przez import; scalone zapisuje batch u siebie.
    import_batch_id: Mapped[str | None] = mapped_column(
        String, ForeignKey("bank_import_batches.id", ondelete="SET NULL")
    )
    # Surowy kontrahent i tytuł z banku — nie do edycji, materiał pod reguły.
    bank_description: Mapped[str | None] = mapped_column(Text)

    __table_args__ = (
        UniqueConstraint("store_id", "date", "receipt_number", name="uq_tx_receipt"),
        UniqueConstraint("account_id", "external_id", name="uq_tx_account_external_id"),
    )

    account: Mapped[Account] = relationship(
        back_populates="transactions", foreign_keys=[account_id]
    )
    transfer_account: Mapped[Account | None] = relationship(foreign_keys=[transfer_account_id])
    store: Mapped[Store | None] = relationship(back_populates="transactions")
    category: Mapped[Category | None] = relationship()
    items: Mapped[list[TransactionItem]] = relationship(
        back_populates="transaction", cascade="all, delete-orphan"
    )
    attachments: Mapped[list[TransactionAttachment]] = relationship(
        back_populates="transaction", cascade="all, delete-orphan"
    )

    def recompute_total(self) -> int:
        total = Decimal(0)
        for it in self.items:
            total += Decimal(it.unit_price_cents) * Decimal(str(it.quantity))
        return int(total.to_integral_value(rounding="ROUND_HALF_UP"))


class TransactionAttachment(Base):
    __tablename__ = "transaction_attachments"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=gen_uuid)
    transaction_id: Mapped[str] = mapped_column(
        String, ForeignKey("transactions.id", ondelete="CASCADE"), nullable=False
    )
    original_name: Mapped[str] = mapped_column(Text, nullable=False)
    stored_name: Mapped[str] = mapped_column(Text, nullable=False)
    content_type: Mapped[str | None] = mapped_column(String(100))
    size_bytes: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime, default=utcnow, nullable=False)

    transaction: Mapped[Transaction] = relationship(back_populates="attachments")


class TransactionItem(Base):
    __tablename__ = "transaction_items"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=gen_uuid)
    transaction_id: Mapped[str] = mapped_column(
        String, ForeignKey("transactions.id", ondelete="CASCADE"), nullable=False
    )
    product_id: Mapped[str] = mapped_column(
        String, ForeignKey("products.id", ondelete="RESTRICT"), nullable=False
    )

    unit_price_cents: Mapped[int] = mapped_column(BigInteger, nullable=False)
    quantity: Mapped[float] = mapped_column(Float, nullable=False)  # 0.35 kg etc.
    unit: Mapped[str | None] = mapped_column(String(16))
    comment: Mapped[str | None] = mapped_column(Text)
    # Nazwa z paragonu, tak jak ją wydrukowano. Produkt to nazwa użytkownika
    # („Zupka chińska”), a tu zostaje, co sklep naprawdę sprzedał („ZUP.VIFON KURCZ 70G”).
    raw_name: Mapped[str | None] = mapped_column(Text)

    __table_args__ = (
        CheckConstraint("unit_price_cents >= 0", name="chk_item_price_nonneg"),
        CheckConstraint("quantity > 0", name="chk_item_qty_pos"),
    )

    transaction: Mapped[Transaction] = relationship(back_populates="items")
    product: Mapped[Product] = relationship(back_populates="items")


class ReceiptDraft(Base):
    """Paragon po OCR, zanim stanie się transakcją — jedna pozycja skrzynki.

    Wgranie plików nie tworzy transakcji, tylko szkice: jedna strona PDF-a albo
    jedno zdjęcie to jeden szkic. Dzięki temu wsad można przeglądać na raty,
    a nie w jednej sesji „przeklikaj wszystko albo strać wynik OCR".

    `ocr_payload` to nietknięty odczyt silnika, `payload` — ta sama etykieta po
    poprawkach. Trzymamy oba, bo różnica między nimi jest darmowym pomiarem
    jakości ekstrakcji (docs/design/ocr-quality-log.md) i jedynym miejscem,
    gdzie widać, czego użytkownik musiał poprawiać ręcznie.
    """

    __tablename__ = "receipt_drafts"

    # 'todo' — do zrobienia, 'skipped' — odłożony (np. nie-paragon),
    # 'committed' — zamieniony na transakcję. „Do przejrzenia" **nie jest**
    # statusem: to szkic z zastrzeżeniami (receipt_checks), liczony na bieżąco,
    # żeby zmiana reguły nie wymagała przeliczania kolumny w bazie.
    STATUS_TODO = "todo"
    STATUS_SKIPPED = "skipped"
    STATUS_COMMITTED = "committed"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=gen_uuid)
    user_id: Mapped[str] = mapped_column(
        String, ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )

    status: Mapped[str] = mapped_column(
        String(16), nullable=False, default=STATUS_TODO, server_default="todo"
    )

    source_name: Mapped[str] = mapped_column(Text, nullable=False)
    page_number: Mapped[int] = mapped_column(Integer, nullable=False, default=1)

    # Granica zdrowego rozsądku dla daty zakupu: paragon nie może być późniejszy
    # niż moment, w którym trafił pod skaner. Z nazwy pliku (Converted_2024_06_11)
    # albo — gdy jej tam nie ma — z dnia wgrania.
    scan_date: Mapped[date | None] = mapped_column(Date)

    # Render strony (PNG) leży na dysku obok załączników; przy zatwierdzeniu
    # kopiujemy go na załącznik transakcji, żeby usunięcie szkicu nigdy nie
    # zabrało pliku, który należy już do transakcji.
    image_name: Mapped[str | None] = mapped_column(Text)
    image_mime: Mapped[str | None] = mapped_column(String(100))
    image_bytes: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    raw_text: Mapped[str | None] = mapped_column(Text)
    ocr_payload: Mapped[dict | None] = mapped_column(JSON)
    payload: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)

    confidence: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    error: Mapped[str | None] = mapped_column(Text)

    matched_store_id: Mapped[str | None] = mapped_column(
        String, ForeignKey("stores.id", ondelete="SET NULL")
    )
    transaction_id: Mapped[str | None] = mapped_column(
        String, ForeignKey("transactions.id", ondelete="SET NULL")
    )

    created_at: Mapped[datetime] = mapped_column(UTCDateTime, default=utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        UTCDateTime, default=utcnow, onupdate=utcnow, nullable=False
    )

    matched_store: Mapped[Store | None] = relationship()
    transaction: Mapped[Transaction | None] = relationship()

    # Skrzynka listuje szkice użytkownika po statusie, od najstarszych.
    __table_args__ = (Index("ix_receipt_drafts_user_status", "user_id", "status", "created_at"),)


class BankImportProfile(Base):
    """„Jak czytać pliki z tego banku" — mapowanie kolumn i ustawienia parsera.

    Profil rozpoznaje plik po `header_signature`, więc kolejny import z tego
    samego banku to jedno kliknięcie. Konto jest tylko domyślne: jeden profil
    ING obsługuje kilka kont ING. Projekt: docs/design/import-csv.md §4.
    """

    __tablename__ = "bank_import_profiles"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=gen_uuid)
    user_id: Mapped[str] = mapped_column(
        String, ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    name: Mapped[str] = mapped_column(Text, nullable=False)
    account_id: Mapped[str | None] = mapped_column(
        String, ForeignKey("accounts.id", ondelete="SET NULL")
    )
    header_signature: Mapped[str] = mapped_column(Text, nullable=False)
    parse_options: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    mapping: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime, default=utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        UTCDateTime, default=utcnow, onupdate=utcnow, nullable=False
    )
    last_used_at: Mapped[datetime | None] = mapped_column(UTCDateTime)

    __table_args__ = (UniqueConstraint("user_id", "name", name="uq_bank_import_profile_name"),)


class BankImportBatch(Base):
    """Jeden zapisany import wyciągu — historia i podstawa do cofnięcia."""

    __tablename__ = "bank_import_batches"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=gen_uuid)
    user_id: Mapped[str] = mapped_column(
        String, ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    account_id: Mapped[str] = mapped_column(
        String, ForeignKey("accounts.id", ondelete="CASCADE"), nullable=False
    )
    profile_id: Mapped[str | None] = mapped_column(
        String, ForeignKey("bank_import_profiles.id", ondelete="SET NULL")
    )
    file_name: Mapped[str] = mapped_column(Text, nullable=False)
    file_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    date_from: Mapped[date | None] = mapped_column(Date)
    date_to: Mapped[date | None] = mapped_column(Date)
    created_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    merged_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    skipped_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    # Transakcje istniejące wcześniej (YNAB, paragon, ręczne), którym import
    # dopisał tożsamość banku — cofnięcie musi ją zdjąć, ale ich nie kasować.
    merged_transaction_ids: Mapped[list[str]] = mapped_column(JSON, nullable=False, default=list)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime, default=utcnow, nullable=False)

    account: Mapped[Account] = relationship()
    profile: Mapped[BankImportProfile | None] = relationship()


class BankConnection(Base):
    """Zgoda PSD2 w jednym banku przez Enable Banking.

    `pending` = użytkownik poszedł do banku, czekamy na wklejony adres powrotu
    (`auth_state` go weryfikuje). `active` = mamy `session_id`. `expired` =
    zgoda wygasła albo bank ją odrzucił — trzeba połączyć ponownie, a nowa
    sesja da kontom nowe `uid`. Projekt: docs/design/enable-banking.md §4.
    """

    __tablename__ = "bank_connections"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=gen_uuid)
    user_id: Mapped[str] = mapped_column(
        String, ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    aspsp_name: Mapped[str] = mapped_column(Text, nullable=False)
    aspsp_country: Mapped[str] = mapped_column(String(2), nullable=False)
    psu_type: Mapped[str] = mapped_column(String(16), nullable=False, default="personal")
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="pending")
    auth_state: Mapped[str | None] = mapped_column(String(64))
    session_id: Mapped[str | None] = mapped_column(String)
    valid_until: Mapped[datetime | None] = mapped_column(UTCDateTime)
    last_synced_at: Mapped[datetime | None] = mapped_column(UTCDateTime)
    last_error: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime, default=utcnow, nullable=False)

    accounts: Mapped[list[BankConnectionAccount]] = relationship(
        back_populates="connection",
        cascade="all, delete-orphan",
        order_by="BankConnectionAccount.created_at",
    )


class BankConnectionAccount(Base):
    """Rachunek w banku z powiązaniem do konta w aplikacji.

    Bez powiązania (`account_id` = None) rachunek się nie synchronizuje. IBAN
    trzymamy tylko jako końcówkę do wyświetlenia i odcisk do przepięcia
    powiązań po ponownej zgodzie.
    """

    __tablename__ = "bank_connection_accounts"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=gen_uuid)
    user_id: Mapped[str] = mapped_column(
        String, ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    connection_id: Mapped[str] = mapped_column(
        String, ForeignKey("bank_connections.id", ondelete="CASCADE"), nullable=False
    )
    uid: Mapped[str] = mapped_column(String, nullable=False)
    name: Mapped[str | None] = mapped_column(Text)
    iban_masked: Mapped[str | None] = mapped_column(String(16))
    iban_hash: Mapped[str | None] = mapped_column(String(64))
    currency: Mapped[str | None] = mapped_column(String(3))
    account_id: Mapped[str | None] = mapped_column(
        String, ForeignKey("accounts.id", ondelete="SET NULL")
    )
    # Najwcześniejsza data do pobrania. Domyślnie dzień po ostatniej transakcji
    # z wyciągu CSV na tym koncie — wiersze z CSV mają inne `external_id`, więc
    # sito 1 ich nie rozpozna, a sito 2 celowo pomija transakcje z banku.
    sync_from: Mapped[date | None] = mapped_column(Date)
    last_synced_at: Mapped[datetime | None] = mapped_column(UTCDateTime)
    # Najnowsza zaksięgowana data z banku — kolejne pobranie zaczyna kilka dni
    # przed nią, a duplikaty wycina sito 1.
    last_booked_date: Mapped[date | None] = mapped_column(Date)
    bank_balance_cents: Mapped[int | None] = mapped_column(BigInteger)
    bank_balance_at: Mapped[datetime | None] = mapped_column(UTCDateTime)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime, default=utcnow, nullable=False)

    connection: Mapped[BankConnection] = relationship(back_populates="accounts")
    account: Mapped[Account | None] = relationship()

    __table_args__ = (
        UniqueConstraint("connection_id", "uid", name="uq_bank_connection_account_uid"),
    )


# Database helpers live in backend/finance_app/db.py
