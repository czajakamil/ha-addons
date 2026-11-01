"""add store_aliases

Aliasy nazw z banku, po których importer przypisuje transakcję do sklepu.
Każdy sklep dostaje alias ze swojej nazwy. Gdy ta sama nazwa po normalizacji
należy do kilku aktywnych sklepów, wszystkie te aliasy są niejednoznaczne:
importer ich nie używa, a sklepy trafiają do „Do uporządkowania”. Sklep
zarchiwizowany nie odbiera aliasu jedynemu aktywnemu sklepowi o tej nazwie.

Revision ID: d2f4a6c8e1b3
Revises: 56ce42c3205a
Create Date: 2026-11-02 09:00:00.000000

"""
import re
import uuid
from collections import defaultdict
from datetime import UTC, datetime
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'd2f4a6c8e1b3'
down_revision: Union[str, Sequence[str], None] = '56ce42c3205a'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


# Świadomie powielone z kodu aplikacji, a nie zaimportowane: migracja musi
# opisywać stan z chwili jej powstania.
_LEGAL_FORMS = (
    re.compile(r"\bsp\.?\s*z\s*o\.?\s*o\.?", re.IGNORECASE),
    re.compile(r"\bs\.\s*a\.?(?=\s|$)", re.IGNORECASE),
    re.compile(r"\bsa$", re.IGNORECASE),
)


def _normalize(value: str) -> str:
    stripped = value
    for form in _LEGAL_FORMS:
        stripped = form.sub(" ", stripped)
    result = " ".join(stripped.casefold().replace(".", " ").replace(",", " ").split())
    return result or " ".join(value.casefold().split())


def upgrade() -> None:
    """Upgrade schema."""
    aliases = op.create_table(
        "store_aliases",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column("user_id", sa.String(), sa.ForeignKey("users.id", ondelete="CASCADE")),
        sa.Column(
            "store_id", sa.String(), sa.ForeignKey("stores.id", ondelete="CASCADE"), nullable=False
        ),
        sa.Column("pattern", sa.Text(), nullable=False),
        sa.Column("normalized", sa.Text(), nullable=False),
        sa.Column("match_type", sa.String(), nullable=False),
        sa.Column("is_ambiguous", sa.Boolean(), nullable=False, server_default="0"),
        sa.Column("source", sa.String(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_store_aliases_store_id", "store_aliases", ["store_id"])
    op.create_index(
        "uq_store_aliases_active",
        "store_aliases",
        ["user_id", "normalized", "match_type"],
        unique=True,
        sqlite_where=sa.text("is_ambiguous = 0"),
    )

    conn = op.get_bind()
    buckets = defaultdict(list)
    for store in conn.execute(sa.text("SELECT id, user_id, name, is_deleted FROM stores")).mappings():
        buckets[(store["user_id"], _normalize(store["name"]))].append(store)

    now = datetime.now(UTC)
    rows = []
    for (_, normalized), stores in buckets.items():
        active = [store for store in stores if not store["is_deleted"]]
        # Jedyny właściciel: jedyny aktywny albo jedyny w ogóle. Inaczej nikt.
        owner = active[0] if len(active) == 1 else stores[0] if len(stores) == 1 else None
        rows.extend(
            {
                "id": str(uuid.uuid4()),
                "user_id": store["user_id"],
                "store_id": store["id"],
                "pattern": store["name"],
                "normalized": normalized,
                "match_type": "exact",
                "is_ambiguous": store is not owner,
                "source": "store_name",
                "created_at": now,
            }
            for store in stores
        )
    if rows:
        op.bulk_insert(aliases, rows)


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index("uq_store_aliases_active", table_name="store_aliases")
    op.drop_index("ix_store_aliases_store_id", table_name="store_aliases")
    op.drop_table("store_aliases")
