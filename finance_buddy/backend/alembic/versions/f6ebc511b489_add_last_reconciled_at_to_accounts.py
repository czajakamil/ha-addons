"""add last_reconciled_at to accounts

Data ostatniego uzgodnienia salda konta, prosto z YNAB-a (22 z 27 kont na
koncie testowym ją miało). Nullable, bo konta zakładane ręcznie jej nie mają —
nie mamy własnego procesu uzgadniania.

Revision ID: f6ebc511b489
Revises: c1ea5474435e
Create Date: 2026-08-25 22:56:29.696126

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'f6ebc511b489'
down_revision: Union[str, Sequence[str], None] = 'c1ea5474435e'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column("accounts", sa.Column("last_reconciled_at", sa.DateTime(timezone=True)))


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column("accounts", "last_reconciled_at")
