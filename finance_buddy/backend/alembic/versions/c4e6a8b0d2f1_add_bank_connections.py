"""add bank connections (Enable Banking)

Zgody PSD2 i rachunki bankowe powiązane z kontami aplikacji. Projekt:
docs/design/enable-banking.md §4.

Revision ID: c4e6a8b0d2f1
Revises: a4c8e2f6b1d3
Create Date: 2026-11-08 16:00:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "c4e6a8b0d2f1"
down_revision: Union[str, Sequence[str], None] = "a4c8e2f6b1d3"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "bank_connections",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column(
            "user_id", sa.String(), sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False
        ),
        sa.Column("aspsp_name", sa.Text(), nullable=False),
        sa.Column("aspsp_country", sa.String(2), nullable=False),
        sa.Column("psu_type", sa.String(16), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("auth_state", sa.String(64)),
        sa.Column("session_id", sa.String()),
        sa.Column("valid_until", sa.DateTime(timezone=True)),
        sa.Column("last_synced_at", sa.DateTime(timezone=True)),
        sa.Column("last_error", sa.Text()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_table(
        "bank_connection_accounts",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column(
            "user_id", sa.String(), sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False
        ),
        sa.Column(
            "connection_id",
            sa.String(),
            sa.ForeignKey("bank_connections.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("uid", sa.String(), nullable=False),
        sa.Column("name", sa.Text()),
        sa.Column("iban_masked", sa.String(16)),
        sa.Column("iban_hash", sa.String(64)),
        sa.Column("currency", sa.String(3)),
        sa.Column("account_id", sa.String(), sa.ForeignKey("accounts.id", ondelete="SET NULL")),
        sa.Column("sync_from", sa.Date()),
        sa.Column("last_synced_at", sa.DateTime(timezone=True)),
        sa.Column("last_booked_date", sa.Date()),
        sa.Column("bank_balance_cents", sa.BigInteger()),
        sa.Column("bank_balance_at", sa.DateTime(timezone=True)),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("connection_id", "uid", name="uq_bank_connection_account_uid"),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_table("bank_connection_accounts")
    op.drop_table("bank_connections")
