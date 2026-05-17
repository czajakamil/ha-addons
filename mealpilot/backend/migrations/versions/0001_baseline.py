"""baseline — all tables created via create_all or prior manual migrations

Revision ID: 0001
Revises:
Create Date: 2026-05-16
"""
from typing import Sequence, Union

revision: str = "0001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Tables are created by Base.metadata.create_all() in the lifespan.
    # Existing databases are stamped to this revision so that future
    # migrations (op.add_column, op.create_table, …) are applied correctly.
    pass


def downgrade() -> None:
    pass
