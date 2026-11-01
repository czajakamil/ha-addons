"""add is_deleted to accounts

Odwzorowanie YNAB-a: `closed` (archiwizacja) i `deleted` (usunięcie po stronie
YNAB-a) to dwie różne rzeczy, a `accounts` miało dotąd tylko `is_closed`.
Kolumna jest bliźniacza do `stores.is_deleted` — ten sam typ, ta sama domyślna.

Import z YNAB-a próbował zapisywać tę flagę od zawsze, mimo że kolumny nie było;
kończyło się to `TypeError` przy każdym nowym koncie.

Revision ID: 350ae25694a9
Revises: e364b0a6c5a7
Create Date: 2026-08-25 22:14:04.194392

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '350ae25694a9'
down_revision: Union[str, Sequence[str], None] = 'e364b0a6c5a7'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column(
        "accounts",
        sa.Column("is_deleted", sa.Boolean(), server_default="0", nullable=False),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column("accounts", "is_deleted")
