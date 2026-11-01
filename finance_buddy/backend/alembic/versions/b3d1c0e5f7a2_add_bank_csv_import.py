"""add bank csv import

Import wyciągów CSV (docs/design/import-csv.md §4):

- `bank_import_profiles` — mapowanie kolumn i ustawienia parsera per bank,
- `bank_import_batches` — historia importów i podstawa cofnięcia,
- na `transactions`: `external_id` (tożsamość wiersza banku, unikalna w
  obrębie konta), `import_batch_id` i surowy `bank_description`.

Unikat `(account_id, external_id)` to jedyna twarda gwarancja, że ten sam wiersz
wyciągu nie trafi do konta dwa razy — deduplikacja w serwisie jest pierwszą
linią, ten indeks ostatnią. NULL-e się nie powtarzają, więc istniejące
transakcje bez `external_id` mu nie przeszkadzają.

Revision ID: b3d1c0e5f7a2
Revises: a7f2d9c31b04
Create Date: 2026-10-18 18:30:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'b3d1c0e5f7a2'
down_revision: Union[str, Sequence[str], None] = 'a7f2d9c31b04'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        'bank_import_profiles',
        sa.Column('id', sa.String(), nullable=False),
        sa.Column('user_id', sa.String(), nullable=False),
        sa.Column('name', sa.Text(), nullable=False),
        sa.Column('account_id', sa.String(), nullable=True),
        sa.Column('header_signature', sa.Text(), nullable=False),
        sa.Column('parse_options', sa.JSON(), nullable=False),
        sa.Column('mapping', sa.JSON(), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('last_used_at', sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['account_id'], ['accounts.id'], ondelete='SET NULL'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('user_id', 'name', name='uq_bank_import_profile_name'),
    )
    op.create_table(
        'bank_import_batches',
        sa.Column('id', sa.String(), nullable=False),
        sa.Column('user_id', sa.String(), nullable=False),
        sa.Column('account_id', sa.String(), nullable=False),
        sa.Column('profile_id', sa.String(), nullable=True),
        sa.Column('file_name', sa.Text(), nullable=False),
        sa.Column('file_sha256', sa.String(length=64), nullable=False),
        sa.Column('date_from', sa.Date(), nullable=True),
        sa.Column('date_to', sa.Date(), nullable=True),
        sa.Column('created_count', sa.Integer(), nullable=False),
        sa.Column('merged_count', sa.Integer(), nullable=False),
        sa.Column('skipped_count', sa.Integer(), nullable=False),
        sa.Column('merged_transaction_ids', sa.JSON(), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['account_id'], ['accounts.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(
            ['profile_id'], ['bank_import_profiles.id'], ondelete='SET NULL'
        ),
        sa.PrimaryKeyConstraint('id'),
    )
    # Tryb wsadowy: SQLite nie umie dodać klucza obcego ani unikatu przez
    # ALTER TABLE, więc Alembic przebudowuje tabelę.
    with op.batch_alter_table('transactions') as batch:
        batch.add_column(sa.Column('external_id', sa.Text(), nullable=True))
        batch.add_column(sa.Column('import_batch_id', sa.String(), nullable=True))
        batch.add_column(sa.Column('bank_description', sa.Text(), nullable=True))
        batch.create_foreign_key(
            'fk_transactions_import_batch_id',
            'bank_import_batches',
            ['import_batch_id'],
            ['id'],
            ondelete='SET NULL',
        )
        batch.create_unique_constraint(
            'uq_tx_account_external_id', ['account_id', 'external_id']
        )


def downgrade() -> None:
    """Downgrade schema."""
    with op.batch_alter_table('transactions') as batch:
        batch.drop_constraint('uq_tx_account_external_id', type_='unique')
        batch.drop_constraint('fk_transactions_import_batch_id', type_='foreignkey')
        batch.drop_column('bank_description')
        batch.drop_column('import_batch_id')
        batch.drop_column('external_id')
    op.drop_table('bank_import_batches')
    op.drop_table('bank_import_profiles')
