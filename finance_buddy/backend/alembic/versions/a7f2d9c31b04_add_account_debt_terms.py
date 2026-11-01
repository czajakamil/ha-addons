"""add account_debt_terms

Warunki kredytu z YNAB-a: oprocentowanie, rata minimalna i escrow. YNAB trzyma
je jako SZEREGI CZASOWE kluczowane miesiącem (`{"2026-08-01": 6550}`), a nie
jako pojedyncze wartości konta — stąd osobna tabela, a nie trzy kolumny na
`accounts`. Kolumny spłaszczyłyby historię do ostatniej wartości i rozbicie
raty na kapitał i odsetki przestałoby się zgadzać wstecz po pierwszej
zmianie stopy.

Oprocentowanie ląduje w punktach bazowych (6,55% -> 655), a nie jako float:
reszta schematu konsekwentnie trzyma pieniądze i stawki w liczbach całkowitych.

Kształt tabeli jest bliźniaczy do `budget`: wiersz = stan na miesiąc,
`month` w formacie `RRRR-MM`.

Revision ID: a7f2d9c31b04
Revises: f6ebc511b489
Create Date: 2026-08-25 23:31:12.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'a7f2d9c31b04'
down_revision: Union[str, Sequence[str], None] = 'f6ebc511b489'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        'account_debt_terms',
        sa.Column('id', sa.String(), nullable=False),
        sa.Column('account_id', sa.String(), nullable=False),
        sa.Column('user_id', sa.String(), nullable=False),
        sa.Column('month', sa.String(length=7), nullable=False),
        # Nullable, i to nie z ostrożności: na żywym koncie escrow bywa pustym
        # słownikiem przy wypełnionej stopie i racie. „Brak" to nie „zero".
        sa.Column('interest_rate_bps', sa.Integer(), nullable=True),
        sa.Column('minimum_payment_cents', sa.BigInteger(), nullable=True),
        sa.Column('escrow_cents', sa.BigInteger(), nullable=True),
        sa.ForeignKeyConstraint(['account_id'], ['accounts.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('account_id', 'month', name='uq_account_debt_terms_month'),
    )
    # Widok długu czyta zawsze „wszystkie miesiące jednego konta", więc indeks
    # idzie po account_id — unikat wyżej i tak go pokrywa, ale nazwany indeks
    # przetrwa ewentualną zmianę unikatu.
    op.create_index(
        'ix_account_debt_terms_account_id',
        'account_debt_terms',
        ['account_id'],
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index('ix_account_debt_terms_account_id', table_name='account_debt_terms')
    op.drop_table('account_debt_terms')
