"""add transaction_items.raw_name

Nazwa pozycji tak, jak wydrukował ją paragon. Produkt niesie nazwę użytkownika,
więc bez tej kolumny oryginał ginął po zatwierdzeniu paragonu.

Revision ID: b5e2c8d4f1a6
Revises: a9c3e5f7b1d4
Create Date: 2026-11-07 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'b5e2c8d4f1a6'
down_revision: Union[str, Sequence[str], None] = 'a9c3e5f7b1d4'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    with op.batch_alter_table("transaction_items") as batch:
        batch.add_column(sa.Column("raw_name", sa.Text(), nullable=True))


def downgrade() -> None:
    """Downgrade schema."""
    with op.batch_alter_table("transaction_items") as batch:
        batch.drop_column("raw_name")
