from __future__ import annotations

import datetime as dt
import os
import sqlite3
import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi import HTTPException
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from backend.finance_app.models import (
    Account,
    Base,
    Category,
    Product,
    ProductAlias,
    Store,
    StoreAlias,
    StoreMerge,
    Transaction,
    utcnow,
)
from backend.finance_app.receipt_import import build_store_index
from backend.finance_app.services.store_aliases import add_alias, load_matcher, register_store_name
from backend.finance_app.services.store_merges import (
    list_merges,
    merge_stores,
    preview_merge,
    undo_merge,
)
from backend.scripts import import_ynab_data

USER = "user-1"
PROJECT_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def db() -> Iterator[Session]:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        session.add(Account(id="a", user_id=USER, name="Konto", currency="PLN"))
        session.flush()
        yield session


def store(db: Session, name: str, **extra: object) -> Store:
    item = Store(user_id=USER, name=name, **extra)
    db.add(item)
    register_store_name(db, item)
    return item


def add_tx(db: Session, store_id: str, receipt: str | None = None) -> str:
    moment = dt.datetime(2026, 10, 1, tzinfo=dt.UTC)
    tx = Transaction(
        user_id=USER,
        account_id="a",
        store_id=store_id,
        date=moment.date(),
        recorded_at=moment,
        created_at=moment,
        total_cents=-100,
        currency="PLN",
        receipt_number=receipt,
    )
    db.add(tx)
    db.flush()
    return tx.id


def tx_store(db: Session, tx_id: str) -> str | None:
    db.expire_all()
    return db.get(Transaction, tx_id).store_id


def test_dry_run_changes_nothing_and_counts(db: Session) -> None:
    source, target = store(db, "Biedronka 1"), store(db, "Biedronka")
    tx = add_tx(db, source.id)
    add_tx(db, source.id, receipt="R1")
    add_alias(db, USER, source, "BIEDRONKA K*")

    result = preview_merge(db, USER, [source.id], target.id)

    assert result["transactions"] == 2
    assert result["receipts"] == 1
    assert result["aliases"] == 2
    assert result["can_merge"] is True
    assert tx_store(db, tx) == source.id
    assert db.get(Store, source.id).merged_into_id is None


def test_receipt_conflict_blocks_in_preview_and_guards_with_409(db: Session) -> None:
    source, target = store(db, "A sklep"), store(db, "B sklep")
    add_tx(db, source.id, receipt="R1")
    add_tx(db, target.id, receipt="R1")

    preview = preview_merge(db, USER, [source.id], target.id)
    assert preview["can_merge"] is False
    assert preview["conflicts"][0]["kind"] == "receipt"
    with pytest.raises(HTTPException) as exc:
        merge_stores(db, USER, [source.id], target.id)
    assert exc.value.status_code == 409


def test_target_cannot_be_source(db: Session) -> None:
    item = store(db, "Sklep")
    with pytest.raises(HTTPException) as exc:
        merge_stores(db, USER, [item.id], item.id)
    assert exc.value.status_code == 422


def test_merge_moves_everything_and_resolves_ambiguity(db: Session) -> None:
    db.add(Category(id="c", user_id=USER, name="Dom"))
    source, target = store(db, "Absolut Sp. z o.o."), store(db, "ABSOLUT")
    tx = add_tx(db, source.id)
    assert load_matcher(db, USER).match("absolut") is None

    result = merge_stores(db, USER, [source.id], target.id, name="Absolut", category_id="c")

    assert tx_store(db, tx) == target.id
    merged = db.get(Store, source.id)
    assert (merged.merged_into_id, merged.is_deleted) == (target.id, True)
    kept = db.get(Store, target.id)
    assert kept.group_name == "Absolut"
    assert kept.automatic_category_id == "c"
    assert kept.automatically_categorize_payee is True
    assert load_matcher(db, USER).match("absolut").id == target.id
    assert result["transactions"] == 1


def test_undo_restores_transactions_aliases_and_category(db: Session) -> None:
    db.add_all(
        [
            Category(id="old", user_id=USER, name="Stara"),
            Category(id="c", user_id=USER, name="Nowa"),
        ]
    )
    source = store(db, "Absolut Sp. z o.o.", nip="1111111111")
    target = store(db, "ABSOLUT", automatic_category_id="old", group_name="Absolut stary")
    extra = add_alias(db, USER, source, "ABSOLUT WAW*")
    tx_source, tx_target = add_tx(db, source.id), add_tx(db, target.id)

    result = merge_stores(db, USER, [source.id], target.id, name="Nowa", category_id="c")
    assert db.get(Store, target.id).nip == "1111111111"
    assert build_store_index(db, USER)["1111111111"].id == target.id
    late = add_tx(db, target.id)

    undone = undo_merge(db, USER, result["merge_id"])

    assert undone == {"restored_transactions": 1, "kept_transactions": 0}
    assert tx_store(db, tx_source) == source.id
    assert tx_store(db, tx_target) == target.id
    assert tx_store(db, late) == target.id
    back, kept = db.get(Store, source.id), db.get(Store, target.id)
    assert (back.merged_into_id, back.is_deleted, back.nip) == (None, False, "1111111111")
    assert (kept.automatic_category_id, kept.group_name, kept.nip) == ("old", "Absolut stary", None)
    assert db.get(StoreAlias, extra.id).store_id == source.id
    # Znów ta sama nazwa w dwóch sklepach, więc znów niejednoznaczna.
    assert load_matcher(db, USER).match("absolut") is None
    assert load_matcher(db, USER).match("ABSOLUT WAW 1").id == source.id
    with pytest.raises(HTTPException):
        undo_merge(db, USER, result["merge_id"])


def test_undo_expires_after_30_days(db: Session) -> None:
    source, target = store(db, "Aaaa"), store(db, "Bbbb")
    merge_id = merge_stores(db, USER, [source.id], target.id)["merge_id"]
    assert len(list_merges(db, USER, [target.id])) == 1

    db.get(StoreMerge, merge_id).created_at = utcnow() - dt.timedelta(days=31)
    db.flush()

    assert list_merges(db, USER, [target.id]) == []
    with pytest.raises(HTTPException) as exc:
        undo_merge(db, USER, merge_id)
    assert "30 dni" in exc.value.detail


def test_chains_are_flattened_and_undo_runs_from_the_end(db: Session) -> None:
    first, second, third = store(db, "Aaaa"), store(db, "Bbbb"), store(db, "Cccc")
    one = merge_stores(db, USER, [first.id], second.id)["merge_id"]
    two = merge_stores(db, USER, [second.id], third.id)["merge_id"]
    assert db.get(Store, first.id).merged_into_id == third.id

    with pytest.raises(HTTPException) as exc:
        undo_merge(db, USER, one)
    assert exc.value.status_code == 409

    undo_merge(db, USER, two)
    assert db.get(Store, first.id).merged_into_id == second.id
    undo_merge(db, USER, one)
    assert db.get(Store, first.id).merged_into_id is None


def test_merged_source_cannot_be_merged_again(db: Session) -> None:
    first, second, third = store(db, "Aaaa"), store(db, "Bbbb"), store(db, "Cccc")
    merge_stores(db, USER, [first.id], second.id)
    preview = preview_merge(db, USER, [first.id], third.id)
    assert preview["conflicts"][0]["kind"] == "already_merged"


def test_repeated_product_alias_stays_with_source_and_undo_restores_moved(db: Session) -> None:
    db.add(Product(id="p", user_id=USER, name="Mleko"))
    source, target = store(db, "Aaaa"), store(db, "Bbbb")
    moved = ProductAlias(id="m", product_id="p", store_id=source.id, alias="MLEKO 1L")
    repeated = ProductAlias(id="r", product_id="p", store_id=source.id, alias="SER")
    existing = ProductAlias(id="e", product_id="p", store_id=target.id, alias="SER")
    db.add_all([moved, repeated, existing])
    db.flush()

    assert (
        preview_merge(db, USER, [source.id], target.id)["conflicts"][0]["kind"] == "product_alias"
    )
    merge_id = merge_stores(db, USER, [source.id], target.id)["merge_id"]
    db.expire_all()
    assert db.get(ProductAlias, "m").store_id == target.id
    assert db.get(ProductAlias, "r").store_id == source.id

    undo_merge(db, USER, merge_id)
    db.expire_all()
    assert db.get(ProductAlias, "m").store_id == source.id


def test_ynab_resolves_merged_payee(db: Session, monkeypatch: pytest.MonkeyPatch) -> None:
    source, target = store(db, "Aaaa"), store(db, "Bbbb")
    merge_stores(db, USER, [source.id], target.id)
    payee = SimpleNamespace(id=source.id, name="Aaaa", deleted=False, transfer_account_id=None)
    fresh = SimpleNamespace(
        id="11111111-1111-4111-8111-111111111111",
        name="Nowy",
        deleted=False,
        transfer_account_id=None,
    )
    data = SimpleNamespace(data=SimpleNamespace(payees=[payee, fresh]))
    monkeypatch.setattr(
        import_ynab_data.ynab, "PayeesApi", lambda _: SimpleNamespace(get_payees=lambda _: data)
    )

    mapping = import_ynab_data.import_payees_as_stores(db, "b", None, USER)

    assert mapping[source.id] == target.id
    assert db.get(Store, source.id).is_deleted is True
    assert load_matcher(db, USER).match("Nowy").id == fresh.id


def test_ynab_reuses_store_with_same_name(db: Session, monkeypatch: pytest.MonkeyPatch) -> None:
    existing = store(db, "Nowy Targdis Sikorskie")
    payees = [
        SimpleNamespace(
            id="22222222-2222-4222-8222-222222222222",
            name="NOWY TARGDIS  Sikorskie",
            deleted=False,
            transfer_account_id=None,
        ),
        SimpleNamespace(
            id="33333333-3333-4333-8333-333333333333",
            name="Inny sklep",
            deleted=False,
            transfer_account_id=None,
        ),
        SimpleNamespace(
            id="44444444-4444-4444-8444-444444444444",
            name="Inny sklep",
            deleted=False,
            transfer_account_id=None,
        ),
    ]
    data = SimpleNamespace(data=SimpleNamespace(payees=payees))
    monkeypatch.setattr(
        import_ynab_data.ynab, "PayeesApi", lambda _: SimpleNamespace(get_payees=lambda _: data)
    )

    mapping = import_ynab_data.import_payees_as_stores(db, "b", None, USER)

    assert mapping[payees[0].id] == existing.id
    assert db.get(Store, payees[0].id) is None
    # Dwa payee YNAB-a o tej samej nazwie to jeden sklep, a nazwa zostaje jednoznaczna
    assert mapping[payees[1].id] == mapping[payees[2].id] == payees[1].id
    assert load_matcher(db, USER).match("Inny sklep").id == payees[1].id
    assert load_matcher(db, USER).match("Nowy Targdis Sikorskie").id == existing.id


def test_ynab_skips_new_payees_without_transactions(
    db: Session, monkeypatch: pytest.MonkeyPatch
) -> None:
    kept = store(db, "Stary sklep")
    used, unused = "55555555-5555-4555-8555-555555555555", "66666666-6666-4666-8666-666666666666"
    payees = [
        SimpleNamespace(id=used, name="Z transakcją", deleted=False, transfer_account_id=None),
        SimpleNamespace(id=unused, name="Bez transakcji", deleted=False, transfer_account_id=None),
        SimpleNamespace(id=kept.id, name="Stary sklep", deleted=False, transfer_account_id=None),
    ]
    data = SimpleNamespace(data=SimpleNamespace(payees=payees))
    monkeypatch.setattr(
        import_ynab_data.ynab, "PayeesApi", lambda _: SimpleNamespace(get_payees=lambda _: data)
    )

    mapping = import_ynab_data.import_payees_as_stores(db, "b", None, USER, {used})

    assert mapping == {used: used, kept.id: kept.id}
    assert db.get(Store, unused) is None


def test_migration_keeps_data_and_downgrade_keeps_relations(tmp_path: Path) -> None:
    def run(code: str) -> None:
        env = {
            **os.environ,
            "FINANCE_DB_PATH": str(tmp_path / "f.sqlite3"),
            "FINANCE_BACKUP_DIR": str(tmp_path / "b"),
            "FINANCE_LOG_FILE": str(tmp_path / "a.log"),
        }
        subprocess.run([sys.executable, "-c", code], cwd=PROJECT_ROOT, env=env, check=True)  # noqa: S603

    alembic = (
        "from alembic import command; from backend.finance_app.db import _alembic_config as c; "
    )
    run(alembic + "command.upgrade(c(), 'd2f4a6c8e1b3')")
    db_file = tmp_path / "f.sqlite3"
    with sqlite3.connect(db_file) as conn:
        conn.execute(
            "INSERT INTO users (id, username, display_name, created_at, is_admin) "
            "VALUES ('u', 'u', 'u', '2026-01-01', 1)"
        )
        conn.execute(
            "INSERT INTO stores (id, user_id, name, is_deleted, renaming_rules) "
            "VALUES ('s','u','S',0,'[]')"
        )
        conn.execute(
            "INSERT INTO accounts (id, user_id, name, currency) VALUES ('a','u','A','PLN')"
        )
        conn.execute(
            "INSERT INTO transactions (id, user_id, account_id, store_id, date, recorded_at, "
            "created_at, total_cents, currency) VALUES ('t','u','a','s','2026-10-01',"
            "'2026-10-01','2026-10-01',-1,'PLN')"
        )
        conn.execute(
            "INSERT INTO store_aliases (id, user_id, store_id, pattern, normalized, match_type, "
            "is_ambiguous, source, created_at) "
            "VALUES ('x','u','s','S','s','exact',0,'store_name','2026-10-01')"
        )

    run("from backend.finance_app.db import init_db; init_db()")
    assert list((tmp_path / "b" / "pre-migration").glob("finance-pre-migration-*.sqlite3"))
    with sqlite3.connect(db_file) as conn:
        assert conn.execute("SELECT merged_into_id FROM stores").fetchone() == (None,)

    run(alembic + "command.downgrade(c(), 'd2f4a6c8e1b3')")
    with sqlite3.connect(db_file) as conn:
        assert conn.execute("SELECT store_id FROM transactions").fetchone() == ("s",)
        assert conn.execute("SELECT store_id FROM store_aliases").fetchone() == ("s",)
        columns = {row[1] for row in conn.execute("PRAGMA table_info(stores)")}
    assert "merged_into_id" not in columns
