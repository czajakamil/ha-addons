"""add is_favorite to accounts

Ulubione konta przypięte do paska bocznego — saldo i szybkie przejście do
transakcji konta, jak w YNAB-ie.

Revision ID: d5f7b9c1e3a8
Revises: c4e6a8b0d2f1
Create Date: 2026-11-11 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "d5f7b9c1e3a8"
down_revision: Union[str, Sequence[str], None] = "c4e6a8b0d2f1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    with op.batch_alter_table("accounts") as batch_op:
        batch_op.add_column(
            sa.Column("is_favorite", sa.Boolean(), nullable=False, server_default="0")
        )


def downgrade() -> None:
    """Downgrade schema."""
    with op.batch_alter_table("accounts") as batch_op:
        batch_op.drop_column("is_favorite")
