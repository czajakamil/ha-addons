"""initial_schema

Revision ID: e364b0a6c5a7
Revises:
Create Date: 2026-06-13 20:11:46.052331

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'e364b0a6c5a7'
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table('users',
    sa.Column('id', sa.String(), nullable=False),
    sa.Column('username', sa.String(length=64), nullable=True),
    sa.Column('display_name', sa.Text(), nullable=True),
    sa.Column('is_admin', sa.Boolean(), server_default='0', nullable=False),
    sa.Column('created_at', sa.DateTime(), nullable=False),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('username')
    )
    op.create_table('auth_sessions',
    sa.Column('id', sa.String(), nullable=False),
    sa.Column('user_id', sa.String(), nullable=False),
    sa.Column('created_at', sa.DateTime(), nullable=False),
    sa.Column('expires_at', sa.DateTime(), nullable=False),
    sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_table('categories',
    sa.Column('id', sa.String(), nullable=False),
    sa.Column('user_id', sa.String(), nullable=True),
    sa.Column('name', sa.Text(), nullable=False),
    sa.Column('parent_id', sa.String(), nullable=True),
    sa.Column('is_for_accounts', sa.Boolean(), server_default='0', nullable=False),
    sa.Column('color', sa.String(length=24), nullable=True),
    sa.ForeignKeyConstraint(['parent_id'], ['categories.id'], ondelete='CASCADE'),
    sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('user_id', 'name', 'parent_id', name='uq_categories_name_parent')
    )
    op.create_table('passkey_credentials',
    sa.Column('id', sa.String(), nullable=False),
    sa.Column('user_id', sa.String(), nullable=False),
    sa.Column('credential_id', sa.String(), nullable=False),
    sa.Column('credential_data', sa.LargeBinary(), nullable=False),
    sa.Column('sign_count', sa.Integer(), server_default='0', nullable=False),
    sa.Column('transports', sa.Text(), nullable=True),
    sa.Column('aaguid', sa.String(length=64), nullable=True),
    sa.Column('created_at', sa.DateTime(), nullable=False),
    sa.Column('last_used_at', sa.DateTime(), nullable=True),
    sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('credential_id')
    )
    op.create_table('user_invites',
    sa.Column('id', sa.String(), nullable=False),
    sa.Column('user_id', sa.String(), nullable=False),
    sa.Column('token', sa.String(), nullable=False),
    sa.Column('created_at', sa.DateTime(), nullable=False),
    sa.Column('expires_at', sa.DateTime(), nullable=False),
    sa.Column('used_at', sa.DateTime(), nullable=True),
    sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('token')
    )
    op.create_table('ynab_budget_snapshots',
    sa.Column('id', sa.String(), nullable=False),
    sa.Column('user_id', sa.String(), nullable=False),
    sa.Column('budget_id', sa.String(), nullable=False),
    sa.Column('month', sa.String(length=7), nullable=False),
    sa.Column('server_knowledge', sa.Integer(), nullable=True),
    sa.Column('created_at', sa.DateTime(), nullable=False),
    sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('user_id', 'budget_id', 'month', name='uq_ynab_budget_snapshot')
    )
    op.create_table('ynab_settings',
    sa.Column('user_id', sa.String(), nullable=False),
    sa.Column('budget_id', sa.String(), nullable=True),
    sa.Column('last_imported_at', sa.DateTime(), nullable=True),
    sa.Column('last_server_knowledge', sa.Integer(), nullable=True),
    sa.Column('created_at', sa.DateTime(), nullable=False),
    sa.Column('updated_at', sa.DateTime(), nullable=False),
    sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('user_id')
    )
    op.create_table('accounts',
    sa.Column('id', sa.String(), nullable=False),
    sa.Column('user_id', sa.String(), nullable=True),
    sa.Column('name', sa.Text(), nullable=False),
    sa.Column('currency', sa.String(length=3), nullable=False),
    sa.Column('comment', sa.Text(), nullable=True),
    sa.Column('is_closed', sa.Boolean(), server_default='0', nullable=False),
    sa.Column('is_on_budget', sa.Boolean(), server_default='1', nullable=False),
    sa.Column('account_kind', sa.String(length=16), server_default='cash', nullable=False),
    sa.Column('credit_payment_category_id', sa.String(), nullable=True),
    sa.Column('category_id', sa.String(), nullable=True),
    sa.Column('icon', sa.String(length=48), nullable=True),
    sa.Column('color', sa.String(length=24), nullable=True),
    sa.ForeignKeyConstraint(['category_id'], ['categories.id'], ondelete='SET NULL'),
    sa.ForeignKeyConstraint(['credit_payment_category_id'], ['categories.id'], ondelete='SET NULL'),
    sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_table('budget',
    sa.Column('id', sa.String(), nullable=False),
    sa.Column('entry_type', sa.String(length=32), nullable=False),
    sa.Column('user_id', sa.String(), nullable=True),
    sa.Column('snapshot_id', sa.String(), nullable=True),
    sa.Column('category_id', sa.String(), nullable=False),
    sa.Column('month', sa.String(length=7), nullable=True),
    sa.Column('assigned_cents', sa.BigInteger(), server_default='0', nullable=False),
    sa.Column('created_at', sa.DateTime(), nullable=True),
    sa.Column('updated_at', sa.DateTime(), nullable=True),
    sa.Column('hidden', sa.Boolean(), server_default='0', nullable=False),
    sa.Column('deleted', sa.Boolean(), server_default='0', nullable=False),
    sa.Column('note', sa.Text(), nullable=True),
    sa.Column('original_category_group_id', sa.String(), nullable=True),
    sa.Column('budgeted_milliunits', sa.BigInteger(), server_default='0', nullable=False),
    sa.Column('activity_milliunits', sa.BigInteger(), server_default='0', nullable=False),
    sa.Column('balance_milliunits', sa.BigInteger(), server_default='0', nullable=False),
    sa.Column('goal_type', sa.String(length=16), nullable=True),
    sa.Column('goal_needs_whole_amount', sa.Boolean(), nullable=True),
    sa.Column('goal_day', sa.Integer(), nullable=True),
    sa.Column('goal_cadence', sa.Integer(), nullable=True),
    sa.Column('goal_cadence_frequency', sa.Integer(), nullable=True),
    sa.Column('goal_creation_month', sa.String(length=10), nullable=True),
    sa.Column('goal_target_milliunits', sa.BigInteger(), server_default='0', nullable=False),
    sa.Column('goal_target_month', sa.String(length=10), nullable=True),
    sa.Column('goal_percentage_complete', sa.Integer(), nullable=True),
    sa.Column('goal_months_to_budget', sa.Integer(), nullable=True),
    sa.Column('goal_under_funded_milliunits', sa.BigInteger(), server_default='0', nullable=False),
    sa.Column('goal_overall_funded_milliunits', sa.BigInteger(), server_default='0', nullable=False),
    sa.Column('goal_overall_left_milliunits', sa.BigInteger(), server_default='0', nullable=False),
    sa.Column('goal_snoozed_at', sa.String(length=32), nullable=True),
    sa.CheckConstraint("entry_type IN ('assignment', 'ynab_category')", name='chk_budget_entry_type'),
    sa.ForeignKeyConstraint(['category_id'], ['categories.id'], ondelete='CASCADE'),
    sa.ForeignKeyConstraint(['snapshot_id'], ['ynab_budget_snapshots.id'], ondelete='CASCADE'),
    sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('entry_type', 'snapshot_id', 'category_id', name='uq_budget_ynab_snapshot_entry'),
    sa.UniqueConstraint('entry_type', 'user_id', 'category_id', 'month', name='uq_budget_assignment_entry')
    )
    op.create_table('passkey_challenges',
    sa.Column('id', sa.String(), nullable=False),
    sa.Column('user_id', sa.String(), nullable=True),
    sa.Column('invite_id', sa.String(), nullable=True),
    sa.Column('challenge', sa.String(), nullable=False),
    sa.Column('user_verification', sa.String(length=32), nullable=True),
    sa.Column('challenge_type', sa.String(length=32), nullable=False),
    sa.Column('created_at', sa.DateTime(), nullable=False),
    sa.Column('expires_at', sa.DateTime(), nullable=False),
    sa.ForeignKeyConstraint(['invite_id'], ['user_invites.id'], ondelete='SET NULL'),
    sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_table('products',
    sa.Column('id', sa.String(), nullable=False),
    sa.Column('user_id', sa.String(), nullable=True),
    sa.Column('name', sa.Text(), nullable=False),
    sa.Column('group_name', sa.Text(), nullable=True),
    sa.Column('name_simplified', sa.Text(), nullable=True),
    sa.Column('unit_of_measurement', sa.String(length=16), nullable=True),
    sa.Column('category_id', sa.String(), nullable=True),
    sa.Column('tax_rate', sa.Float(), nullable=True),
    sa.Column('manufacturer', sa.Text(), nullable=True),
    sa.Column('comment', sa.Text(), nullable=True),
    sa.ForeignKeyConstraint(['category_id'], ['categories.id'], ondelete='SET NULL'),
    sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_table('stores',
    sa.Column('id', sa.String(), nullable=False),
    sa.Column('user_id', sa.String(), nullable=True),
    sa.Column('name', sa.Text(), nullable=False),
    sa.Column('group_name', sa.Text(), nullable=True),
    sa.Column('include_in_payee_list', sa.Boolean(), server_default='1', nullable=False),
    sa.Column('automatically_categorize_payee', sa.Boolean(), server_default='0', nullable=False),
    sa.Column('automatic_category_id', sa.String(), nullable=True),
    sa.Column('renaming_rules', sa.JSON(), server_default='[]', nullable=False),
    sa.Column('nip', sa.String(), nullable=True),
    sa.Column('address', sa.Text(), nullable=True),
    sa.Column('comment', sa.Text(), nullable=True),
    sa.Column('is_deleted', sa.Boolean(), server_default='0', nullable=False),
    sa.ForeignKeyConstraint(['automatic_category_id'], ['categories.id'], ondelete='SET NULL'),
    sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('user_id', 'nip', name='uq_stores_user_nip')
    )
    op.create_table('product_aliases',
    sa.Column('id', sa.String(), nullable=False),
    sa.Column('product_id', sa.String(), nullable=False),
    sa.Column('store_id', sa.String(), nullable=True),
    sa.Column('alias', sa.Text(), nullable=False),
    sa.ForeignKeyConstraint(['product_id'], ['products.id'], ondelete='CASCADE'),
    sa.ForeignKeyConstraint(['store_id'], ['stores.id'], ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('store_id', 'alias', name='uq_product_alias_store')
    )
    op.create_table('transactions',
    sa.Column('id', sa.String(), nullable=False),
    sa.Column('user_id', sa.String(), nullable=True),
    sa.Column('account_id', sa.String(), nullable=False),
    sa.Column('store_id', sa.String(), nullable=True),
    sa.Column('category_id', sa.String(), nullable=True),
    sa.Column('receipt_number', sa.Text(), nullable=True),
    sa.Column('date', sa.Date(), nullable=False),
    sa.Column('recorded_at', sa.DateTime(), nullable=False),
    sa.Column('created_at', sa.DateTime(), nullable=False),
    sa.Column('total_cents', sa.BigInteger(), nullable=False),
    sa.Column('currency', sa.String(length=3), nullable=False),
    sa.Column('payment_method', sa.Text(), nullable=True),
    sa.Column('comment', sa.Text(), nullable=True),
    sa.Column('cleared', sa.Boolean(), server_default='0', nullable=False),
    sa.Column('reconciled', sa.Boolean(), server_default='0', nullable=False),
    sa.Column('transfer_account_id', sa.String(), nullable=True),
    sa.Column('transfer_transaction_id', sa.String(), nullable=True),
    sa.ForeignKeyConstraint(['account_id'], ['accounts.id'], ondelete='RESTRICT'),
    sa.ForeignKeyConstraint(['category_id'], ['categories.id'], ondelete='SET NULL'),
    sa.ForeignKeyConstraint(['store_id'], ['stores.id'], ondelete='SET NULL'),
    sa.ForeignKeyConstraint(['transfer_account_id'], ['accounts.id'], ondelete='SET NULL'),
    sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('store_id', 'date', 'receipt_number', name='uq_tx_receipt')
    )
    op.create_table('receipt_drafts',
    sa.Column('id', sa.String(), nullable=False),
    sa.Column('user_id', sa.String(), nullable=False),
    sa.Column('status', sa.String(length=16), nullable=False, server_default='todo'),
    sa.Column('source_name', sa.Text(), nullable=False),
    sa.Column('page_number', sa.Integer(), nullable=False, server_default='1'),
    sa.Column('scan_date', sa.Date(), nullable=True),
    sa.Column('image_name', sa.Text(), nullable=True),
    sa.Column('image_mime', sa.String(length=100), nullable=True),
    sa.Column('image_bytes', sa.Integer(), nullable=False, server_default='0'),
    sa.Column('raw_text', sa.Text(), nullable=True),
    sa.Column('ocr_payload', sa.JSON(), nullable=True),
    sa.Column('payload', sa.JSON(), nullable=False),
    sa.Column('confidence', sa.Float(), nullable=False, server_default='0'),
    sa.Column('error', sa.Text(), nullable=True),
    sa.Column('matched_store_id', sa.String(), nullable=True),
    sa.Column('transaction_id', sa.String(), nullable=True),
    sa.Column('created_at', sa.DateTime(), nullable=False),
    sa.Column('updated_at', sa.DateTime(), nullable=False),
    sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE'),
    sa.ForeignKeyConstraint(['matched_store_id'], ['stores.id'], ondelete='SET NULL'),
    sa.ForeignKeyConstraint(['transaction_id'], ['transactions.id'], ondelete='SET NULL'),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index('ix_receipt_drafts_user_status', 'receipt_drafts', ['user_id', 'status', 'created_at'])
    op.create_table('transaction_attachments',
    sa.Column('id', sa.String(), nullable=False),
    sa.Column('transaction_id', sa.String(), nullable=False),
    sa.Column('original_name', sa.Text(), nullable=False),
    sa.Column('stored_name', sa.Text(), nullable=False),
    sa.Column('content_type', sa.String(length=100), nullable=True),
    sa.Column('size_bytes', sa.Integer(), nullable=False),
    sa.Column('created_at', sa.DateTime(), nullable=False),
    sa.ForeignKeyConstraint(['transaction_id'], ['transactions.id'], ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_table('transaction_items',
    sa.Column('id', sa.String(), nullable=False),
    sa.Column('transaction_id', sa.String(), nullable=False),
    sa.Column('product_id', sa.String(), nullable=False),
    sa.Column('unit_price_cents', sa.BigInteger(), nullable=False),
    sa.Column('quantity', sa.Float(), nullable=False),
    sa.Column('unit', sa.String(length=16), nullable=True),
    sa.Column('comment', sa.Text(), nullable=True),
    sa.CheckConstraint('quantity > 0', name='chk_item_qty_pos'),
    sa.CheckConstraint('unit_price_cents >= 0', name='chk_item_price_nonneg'),
    sa.ForeignKeyConstraint(['product_id'], ['products.id'], ondelete='RESTRICT'),
    sa.ForeignKeyConstraint(['transaction_id'], ['transactions.id'], ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('id')
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_table('transaction_items')
    op.drop_table('transaction_attachments')
    op.drop_index('ix_receipt_drafts_user_status', table_name='receipt_drafts')
    op.drop_table('receipt_drafts')
    op.drop_table('transactions')
    op.drop_table('product_aliases')
    op.drop_table('stores')
    op.drop_table('products')
    op.drop_table('passkey_challenges')
    op.drop_table('budget')
    op.drop_table('accounts')
    op.drop_table('ynab_settings')
    op.drop_table('ynab_budget_snapshots')
    op.drop_table('user_invites')
    op.drop_table('passkey_credentials')
    op.drop_table('categories')
    op.drop_table('auth_sessions')
    op.drop_table('users')
