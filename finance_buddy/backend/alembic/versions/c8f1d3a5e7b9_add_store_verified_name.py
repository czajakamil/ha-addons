"""add stores.verified_name

Nazwa grupy, którą użytkownik potwierdził jako poprawną, choć reguły „surowej nazwy
z banku” uznały ją za podejrzaną (cyfry, adres, numer konta). Zapisujemy samą nazwę,
a nie flagę: po zmianie nazwy grupy potwierdzenie przestaje pasować i reguła wraca.

Kolumna przez zwykłe ALTER TABLE, nie batch: batch przebudowuje tabelę `stores`,
a usunięcie jej przy włączonych kluczach obcych skasowałoby zależne wiersze.

Revision ID: c8f1d3a5e7b9
Revises: b5e2c8d4f1a6
Create Date: 2026-11-07 18:00:00.000000

"""
from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = 'c8f1d3a5e7b9'
down_revision: Union[str, Sequence[str], None] = 'b5e2c8d4f1a6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.execute("ALTER TABLE stores ADD COLUMN verified_name TEXT")


def downgrade() -> None:
    """Downgrade schema."""
    # Kolumna bez klucza obcego i indeksu: SQLite 3.35+ usuwa ją bez przebudowy tabeli
    op.execute("ALTER TABLE stores DROP COLUMN verified_name")
