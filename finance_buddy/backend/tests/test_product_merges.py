from __future__ import annotations

import datetime as dt
from collections.abc import Iterator

import pytest
from fastapi import HTTPException
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from backend.finance_app.models import (
    Account,
    Base,
    Product,
    ProductAlias,
    Store,
    Transaction,
    TransactionItem,
)
from backend.finance_app.services.product_matching import (
    MATCH_FUZZY,
    MATCH_NAME,
    match_items,
    save_alias,
)
from backend.finance_app.services.product_merges import merge_products

USER = "user-1"


@pytest.fixture
def db() -> Iterator[Session]:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        session.add(Account(id="a", user_id=USER, name="Konto", currency="PLN"))
        session.flush()
        yield session


def product(db: Session, name: str, **extra: object) -> Product:
    item = Product(user_id=USER, name=name, **extra)
    db.add(item)
    db.flush()
    return item


def store(db: Session, name: str) -> Store:
    item = Store(user_id=USER, name=name)
    db.add(item)
    db.flush()
    return item


def add_item(db: Session, product_id: str, raw_name: str | None = None) -> str:
    moment = dt.datetime(2026, 10, 1, tzinfo=dt.UTC)
    tx = Transaction(
        user_id=USER,
        account_id="a",
        date=moment.date(),
        recorded_at=moment,
        created_at=moment,
        total_cents=-399,
        currency="PLN",
    )
    tx.items.append(
        TransactionItem(product_id=product_id, unit_price_cents=399, quantity=1, raw_name=raw_name)
    )
    db.add(tx)
    db.flush()
    return tx.items[0].id


def test_merge_moves_items_and_aliases_and_learns_names(db: Session) -> None:
    biedronka, lidl = store(db, "Biedronka"), store(db, "Lidl")
    target = product(db, "Zupka chińska")
    vifon = product(db, "Zup.chin.vifon kurcz", manufacturer="Vifon", unit_of_measurement="szt")
    kimlan = product(db, "Kim Lan kurczak")
    item_a = add_item(db, vifon.id, "ZUP.CHIN.VIFON KURCZ")
    item_b = add_item(db, kimlan.id)
    save_alias(db, USER, vifon.id, "ZUP.CHIN.VIFON KURCZ", biedronka.id)
    save_alias(db, USER, kimlan.id, "KIM LAN KURCZAK 60G", lidl.id)

    preview = merge_products(db, USER, [vifon.id, kimlan.id], target.id, preview=True)
    assert (preview.items_moved, preview.aliases_moved, preview.aliases_added) == (2, 2, 2)
    assert db.get(Product, vifon.id) is not None

    merge_products(db, USER, [vifon.id, kimlan.id], target.id)
    db.expire_all()

    assert db.get(Product, vifon.id) is None
    assert db.get(Product, kimlan.id) is None
    assert {db.get(TransactionItem, i).product_id for i in (item_a, item_b)} == {target.id}
    assert db.get(TransactionItem, item_a).raw_name == "ZUP.CHIN.VIFON KURCZ"
    aliases = {
        (a.store_id, a.alias)
        for a in db.scalars(select(ProductAlias).where(ProductAlias.product_id == target.id))
    }
    assert aliases == {
        (biedronka.id, "zup.chin.vifon kurcz"),
        (lidl.id, "kim lan kurczak 60g"),
        (None, "zup.chin.vifon kurcz"),
        (None, "kim lan kurczak"),
    }
    merged = db.get(Product, target.id)
    assert (merged.manufacturer, merged.unit_of_measurement) == ("Vifon", "szt")

    # Stara nazwa scalonego produktu dalej trafia, teraz w cel.
    [match] = match_items(db, USER, ["Kim Lan kurczak"])
    assert match.matched_product_id == target.id


def test_merge_keeps_target_fields_and_dedupes_global_alias(db: Session) -> None:
    target = product(db, "Mleko", manufacturer="Łaciate")
    source = product(db, "Mleko UHT", manufacturer="Mlekovita")
    save_alias(db, USER, target.id, "mleko uht 3,2%")
    db.add(ProductAlias(product_id=source.id, store_id=None, alias="mleko uht 3,2%"))
    db.flush()

    outcome = merge_products(db, USER, [source.id], target.id)
    db.expire_all()

    assert outcome.aliases_moved == 0
    assert db.get(Product, target.id).manufacturer == "Łaciate"
    globals_ = list(db.scalars(select(ProductAlias.alias).where(ProductAlias.store_id.is_(None))))
    assert sorted(globals_) == ["mleko uht", "mleko uht 3,2%"]


def test_merge_does_not_steal_alias_of_other_product(db: Session) -> None:
    target = product(db, "Zupka chińska")
    source = product(db, "Vifon")
    other = product(db, "Przyprawa")
    save_alias(db, USER, other.id, "vifon")

    outcome = merge_products(db, USER, [source.id], target.id)
    db.expire_all()

    assert outcome.aliases_added == 0
    assert db.scalar(select(ProductAlias.product_id).where(ProductAlias.alias == "vifon")) == (
        other.id
    )


def test_merge_rejects_target_in_sources_and_foreign_products(db: Session) -> None:
    target = product(db, "Zupka chińska")
    foreign = Product(user_id="someone-else", name="Obcy")
    db.add(foreign)
    db.flush()

    with pytest.raises(HTTPException) as err:
        merge_products(db, USER, [target.id], target.id)
    assert err.value.status_code == 422
    with pytest.raises(HTTPException) as err:
        merge_products(db, USER, [foreign.id], target.id)
    assert err.value.status_code == 404


def test_fuzzy_matches_alias_learned_in_other_store(db: Session) -> None:
    lidl, zabka = store(db, "Lidl"), store(db, "Żabka")
    soup = product(db, "Zupka chińska")
    save_alias(db, USER, soup.id, "VIFON KURCZAK 70G", lidl.id)

    [match] = match_items(db, USER, ["VIFON KURCZ. 70G"], store_id=zabka.id)

    assert match.match_type == MATCH_FUZZY
    assert match.candidates[0].product_id == soup.id
    assert match.candidates[0].name == "Zupka chińska"


def test_exact_product_name_still_wins(db: Session) -> None:
    soup = product(db, "Zupka chińska")
    [match] = match_items(db, USER, ["ZUPKA CHIŃSKA"])
    assert (match.match_type, match.matched_product_id) == (MATCH_NAME, soup.id)
