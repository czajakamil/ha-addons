"""Spłata długu: martwy constraint zakupów i martwe kolumny ustawień agenta.

Obie zmiany są niewykonalne w SQLite bez przebudowy tabeli (`ALTER TABLE DROP
COLUMN` i zdejmowanie constraintów), więc czekały na Alembica i tryb batch.

1. `shopping_items.uq_shop_user_week_name_unit` — klucz `(created_by,
   week_start, name, unit)` przestał odpowiadać modelowi własności: odkąd lista
   zakupów dziedziczy własność planu, dwie osoby mogą legalnie mieć tę samą
   `(name, unit)` w jednym tygodniu, jeśli jedna ma wiersz prywatny, a druga
   wspólny. Deduplikacja jest w `services/shopping.py` (scalanie po
   `visible_query`), więc constraint tylko blokował poprawne zapisy.

   Świadomie NIE zastępujemy go kluczem
   `(COALESCE(owner_household_id, owner_user_id), week_start, lower(name), unit)`:
   `COALESCE` miesza dwie różne przestrzenie identyfikatorów (household 3
   i użytkownik 3 dają tę samą wartość), a unikalny indeks na wyrażeniu jest
   pod SQLite źle odczytywany przez autogenerację — psułby test antydryfowy,
   czyli jedyną rzecz, która pilnuje zgodności migracji z modelami.

2. `agent_settings.endpoint` / `agent_settings.api_key` — endpoint i klucz LLM
   biorą się ze zmiennych środowiskowych add-onu (`MEALPILOT_AI_API_URL`,
   `MEALPILOT_AI_API_KEY`). Kolumny wypadły już ze schematów Pydantic i
   z frontendu; nikt ich nie czyta ani nie zapisuje.

Obie operacje są warunkowe (sprawdzane przez reflection). Rewizja 0001 jest dla
starych instalacji tylko *ostemplowana*, a stary `_migrate()` nigdy nie
gwarantował constraintów ani indeksów na tabelach, które zastał — więc baza
w terenie może nie mieć constraintu ani kolumn, a migracja i tak musi przejść.

Revision ID: 0002_drop_dead_schema
Revises: 0001_baseline
Create Date: 2026-08-30

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0002_drop_dead_schema"
down_revision: str | None = "0001_baseline"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_UQ_SHOPPING = "uq_shop_user_week_name_unit"
_DEAD_AGENT_COLUMNS = ("endpoint", "api_key")


def _inspector():
    return sa.inspect(op.get_bind())


def upgrade() -> None:
    insp = _inspector()

    if _UQ_SHOPPING in {c["name"] for c in insp.get_unique_constraints("shopping_items")}:
        with op.batch_alter_table("shopping_items", schema=None) as batch_op:
            batch_op.drop_constraint(_UQ_SHOPPING, type_="unique")

    present = {c["name"] for c in insp.get_columns("agent_settings")}
    dead = [name for name in _DEAD_AGENT_COLUMNS if name in present]
    if dead:
        with op.batch_alter_table("agent_settings", schema=None) as batch_op:
            for name in dead:
                batch_op.drop_column(name)


def downgrade() -> None:
    insp = _inspector()

    present = {c["name"] for c in insp.get_columns("agent_settings")}
    missing = [name for name in _DEAD_AGENT_COLUMNS if name not in present]
    if missing:
        with op.batch_alter_table("agent_settings", schema=None) as batch_op:
            for name in missing:
                # server_default jest konieczny: kolumna jest NOT NULL, a wiersze
                # już istnieją. Zostaje w schemacie — to i tak stan historyczny.
                batch_op.add_column(sa.Column(name, sa.String(), nullable=False, server_default=""))

    if _UQ_SHOPPING not in {c["name"] for c in insp.get_unique_constraints("shopping_items")}:
        with op.batch_alter_table("shopping_items", schema=None) as batch_op:
            batch_op.create_unique_constraint(_UQ_SHOPPING, ["user_id", "week_start", "name", "unit"])
