"""add is_ready_to_assign to categories

Import z YNAB-a zrobił z kategorii systemowej „Internal Master Category/Inflow:
Ready to Assign" zwykły tag. Budżet liczył go jak kopertę: cały przychód
odkładał się w jego „Dostępne", a „Do przypisania" (saldo kont budżetowych minus
suma „Dostępnego") wychodziło mocno na minusie.

Flaga oznacza tag przychodu, który nie jest kopertą — jego transakcje zasilają
„Do przypisania". Migracja ustawia ją na tagu zaimportowanym z YNAB-a, zmienia
mu nazwę na „Przychód (Do przypisania)" i przenosi go do korzenia drzewa, żeby
ścieżka nie zaczynała się od „Internal Master Category".

Revision ID: b3d8e1f07a52
Revises: a7f2d9c31b04
Create Date: 2026-10-18 18:10:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'b3d8e1f07a52'
down_revision: Union[str, Sequence[str], None] = 'a7f2d9c31b04'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


# Świadomie powielone z kodu aplikacji, a nie zaimportowane: migracja musi
# opisywać stan z chwili jej powstania.
_YNAB_INTERNAL_GROUP = "Internal Master Category"
_READY_TO_ASSIGN_NAME = "Przychód (Do przypisania)"


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column(
        "categories",
        sa.Column("is_ready_to_assign", sa.Boolean(), nullable=False, server_default="0"),
    )

    categories = sa.table(
        "categories",
        sa.column("id", sa.String),
        sa.column("name", sa.Text),
        sa.column("parent_id", sa.String),
        sa.column("is_ready_to_assign", sa.Boolean),
    )
    internal_groups = sa.select(categories.c.id).where(
        categories.c.parent_id.is_(None),
        categories.c.name == _YNAB_INTERNAL_GROUP,
    )
    # Importer zapisywał nazwę jako „Grupa/Kategoria", ale na wszelki wypadek
    # łapiemy też samą nazwę kategorii.
    op.get_bind().execute(
        categories.update()
        .where(
            categories.c.parent_id.in_(internal_groups),
            sa.or_(
                categories.c.name.like(f"{_YNAB_INTERNAL_GROUP}/Inflow:%"),
                categories.c.name.like("Inflow:%"),
            ),
        )
        .values(is_ready_to_assign=True, name=_READY_TO_ASSIGN_NAME, parent_id=None)
    )


def downgrade() -> None:
    """Downgrade schema."""
    # Nazwy i miejsca w drzewie nie przywracamy — ponowny import z YNAB-a
    # na starszym kodzie i tak by je nadpisał.
    op.drop_column("categories", "is_ready_to_assign")
