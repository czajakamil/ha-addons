"""normalize account_kind from ynab types

Import z YNAB-a wpisywał do `accounts.account_kind` surowy typ z API (tylko
zlowercase'owany), a nasz model zna trzy wartości: cash / creditcard / liability.
Skutkiem były konta z wartościami `savings`, `checking`, `otherasset`, `mortgage`
czy `otherliability`, dla których front nie ma etykiety i pokazywał surowy
angielski string.

Import mapuje już poprawnie (`map_account_kind`), ale to nie naprawia wierszy,
które są w bazie — zwłaszcza kont, których w YNAB-ie już nie ma i których
kolejny import nie dotknie.

Revision ID: c1ea5474435e
Revises: 350ae25694a9
Create Date: 2026-08-25 22:41:31.630393

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'c1ea5474435e'
down_revision: Union[str, Sequence[str], None] = '350ae25694a9'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


# Świadomie powielone z `backend/scripts/import_ynab_data.py`, a nie zaimportowane:
# migracja musi opisywać stan schematu z chwili jej powstania i nie może się
# zmieniać razem z kodem aplikacji.
_YNAB_TO_KIND = {
    "checking": "cash",
    "savings": "cash",
    "otherasset": "cash",
    "creditcard": "creditcard",
    "lineofcredit": "creditcard",
    "mortgage": "liability",
    "autoloan": "liability",
    "studentloan": "liability",
    "personalloan": "liability",
    "medicaldebt": "liability",
    "otherdebt": "liability",
    "otherliability": "liability",
}


def upgrade() -> None:
    """Upgrade schema."""
    connection = op.get_bind()
    accounts = sa.table(
        "accounts",
        sa.column("account_kind", sa.String),
    )
    for ynab_type, kind in _YNAB_TO_KIND.items():
        connection.execute(
            accounts.update()
            .where(sa.func.lower(accounts.c.account_kind) == ynab_type)
            .values(account_kind=kind)
        )


def downgrade() -> None:
    """Downgrade schema."""
    # Bez odwrotu: z `cash` nie da się odtworzyć, czy konto było `checking`,
    # `savings` czy `otherasset`. Informacja jest w YNAB-ie, nie u nas —
    # ponowny import odtworzy stan, jeśli kiedykolwiek będzie potrzebny.
