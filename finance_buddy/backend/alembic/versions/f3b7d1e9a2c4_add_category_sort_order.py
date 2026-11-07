"""add categories.sort_order

Kolejność kategorii w obrębie rodzica, ustawiana przeciąganiem na ekranie Kategorie.
Istniejące kategorie dostają kolejność alfabetyczną, czyli tę, którą widziały dotąd.

Kolumna przez zwykłe ALTER TABLE, nie batch: batch przebudowuje tabelę `categories`,
a usunięcie jej przy włączonych kluczach obcych skasowałoby zależne wiersze.

Revision ID: f3b7d1e9a2c4
Revises: c8f1d3a5e7b9
Create Date: 2026-11-07 21:00:00.000000

"""
from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = 'f3b7d1e9a2c4'
down_revision: Union[str, Sequence[str], None] = 'c8f1d3a5e7b9'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.execute("ALTER TABLE categories ADD COLUMN sort_order INTEGER NOT NULL DEFAULT 0")
    # Pozycja wśród rodzeństwa = liczba sióstr wcześniej w alfabecie (remis rozstrzyga id)
    op.execute(
        """
        UPDATE categories SET sort_order = (
            SELECT COUNT(*) FROM categories AS sibling
            WHERE sibling.user_id IS categories.user_id
              AND sibling.parent_id IS categories.parent_id
              AND (lower(sibling.name) < lower(categories.name)
                   OR (lower(sibling.name) = lower(categories.name) AND sibling.id < categories.id))
        )
        """
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.execute("ALTER TABLE categories DROP COLUMN sort_order")
