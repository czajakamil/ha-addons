"""merge bank csv import and ready to assign heads

Import CSV z banku (b3d1c0e5f7a2) i flaga „Do przypisania" (b3d8e1f07a52)
powstały równolegle na osobnych gałęziach od tej samej rewizji. Pusta migracja
scalająca zamiast przepinania `down_revision`: baza, która zdążyła już przejść
którąkolwiek z nich, i tak dojdzie do jednego heada.

Revision ID: 56ce42c3205a
Revises: b3d1c0e5f7a2, b3d8e1f07a52
Create Date: 2026-10-18 21:47:29.383226

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '56ce42c3205a'
down_revision: Union[str, Sequence[str], None] = ('b3d1c0e5f7a2', 'b3d8e1f07a52')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
