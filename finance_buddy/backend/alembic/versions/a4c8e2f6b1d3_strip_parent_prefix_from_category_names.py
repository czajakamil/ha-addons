"""strip parent prefix from category names

Import z YNAB zapisywał podkategorię jako „Grupa/Kategoria”, choć grupę wyraża już
parent_id, a ścieżkę wylicza drzewo. Pod „Delegacje” wisiało więc „Delegacje/Jedzenie”
zamiast „Jedzenie”. Obcinamy prefiks rodzica; resztę ewentualnych „/” zamieniamy na „-”,
jak przy każdym zapisie nazwy (ukośnik jest separatorem ścieżki).

Gdy po obcięciu nazwa zderzyłaby się z siostrą (bez rozróżniania wielkości liter),
kategorię zostawiamy bez zmian — scalenie to decyzja użytkownika.

Revision ID: a4c8e2f6b1d3
Revises: f3b7d1e9a2c4
Create Date: 2026-11-07 23:00:00.000000

"""
import re
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


# revision identifiers, used by Alembic.
revision: str = 'a4c8e2f6b1d3'
down_revision: Union[str, Sequence[str], None] = 'f3b7d1e9a2c4'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_WHITESPACE = re.compile(r"\s+")


def _clean(name: str) -> str:
    return _WHITESPACE.sub(" ", name.strip()).replace("/", "-")


def upgrade() -> None:
    """Upgrade schema."""
    bind = op.get_bind()
    rows = bind.execute(sa.text("SELECT id, user_id, parent_id, name FROM categories")).all()
    by_id = {row.id: row for row in rows}
    taken: dict[tuple, set[str]] = {}
    for row in rows:
        taken.setdefault((row.user_id, row.parent_id), set()).add(_clean(row.name).lower())

    for row in rows:
        parent = by_id.get(row.parent_id) if row.parent_id else None
        if parent is None:
            continue
        prefix = f"{parent.name.strip()}/"
        if not row.name.lower().startswith(prefix.lower()):
            continue
        new_name = _clean(row.name[len(prefix):])
        if not new_name:
            continue
        siblings = taken[(row.user_id, row.parent_id)]
        if new_name.lower() in siblings:
            continue
        siblings.discard(_clean(row.name).lower())
        siblings.add(new_name.lower())
        bind.execute(
            sa.text("UPDATE categories SET name = :name WHERE id = :id"),
            {"name": new_name, "id": row.id},
        )


def downgrade() -> None:
    """Downgrade schema."""
    # Nieodwracalne: stara nazwa była błędem importu, a ścieżka i tak jest taka sama.
