"""add store merges

`stores.merged_into_id` to przekierowanie po scaleniu: wiersz sklepu zostaje, bo
import YNAB używa id payee jako id sklepu i inaczej założyłby go od nowa.
`store_merges` trzyma stan sprzed scalenia, potrzebny do cofania.

Kolumna przez zwykłe ALTER TABLE, nie batch: batch przebudowuje tabelę `stores`,
a usunięcie jej przy włączonych kluczach obcych skasowałoby zależne wiersze.

Revision ID: e7a1b3c5d9f2
Revises: d2f4a6c8e1b3
Create Date: 2026-11-02 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'e7a1b3c5d9f2'
down_revision: Union[str, Sequence[str], None] = 'd2f4a6c8e1b3'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.execute(
        "ALTER TABLE stores ADD COLUMN merged_into_id VARCHAR "
        "REFERENCES stores (id) ON DELETE SET NULL"
    )
    op.create_table(
        "store_merges",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column("user_id", sa.String(), sa.ForeignKey("users.id", ondelete="CASCADE")),
        sa.Column(
            "target_store_id",
            sa.String(),
            sa.ForeignKey("stores.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("undone_at", sa.DateTime(timezone=True)),
    )
    op.create_index("ix_store_merges_target_store_id", "store_merges", ["target_store_id"])


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index("ix_store_merges_target_store_id", table_name="store_merges")
    op.drop_table("store_merges")
    # Scalone sklepy wracają jako zwykłe zarchiwizowane. SQLite nie usunie kolumny
    # z kluczem obcym, więc tu przebudowa jest konieczna, ale przy wyłączonych kluczach.
    op.execute("PRAGMA foreign_keys=OFF")
    with op.batch_alter_table("stores", recreate="always") as batch:
        batch.drop_column("merged_into_id")
    op.execute("PRAGMA foreign_keys=ON")
