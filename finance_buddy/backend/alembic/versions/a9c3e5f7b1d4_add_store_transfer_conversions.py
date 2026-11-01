"""add store_transfer_conversions

Stan transakcji sprzed zamiany na przelewy własne, potrzebny do cofania.

Revision ID: a9c3e5f7b1d4
Revises: e7a1b3c5d9f2
Create Date: 2026-11-02 15:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'a9c3e5f7b1d4'
down_revision: Union[str, Sequence[str], None] = 'e7a1b3c5d9f2'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "store_transfer_conversions",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column("user_id", sa.String(), sa.ForeignKey("users.id", ondelete="CASCADE")),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("undone_at", sa.DateTime(timezone=True)),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_table("store_transfer_conversions")
