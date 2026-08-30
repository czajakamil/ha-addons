"""Serwer autoryzacji OAuth 2.1 — klienci, kody autoryzacyjne, tokeny.

Powód: konektor dodany na claude.ai dostaje sam URL i nie ma jak wysłać
nagłówka `X-MealPilot-Token`, więc jedyne uwierzytelnienie, jakie potrafi
przeprowadzić, to OAuth. Szczegóły przepływu w nagłówku `app/oauth.py`.

Trzy tabele, wszystkie świeże — nic nie jest przenoszone ani przebudowywane,
więc migracja jest czysto addytywna i `downgrade` po prostu je kasuje.

Tworzenie jest warunkowe (przez reflection), z tego samego powodu co warunki
w 0002: zamrożony `app.main._migrate()` zaczyna od `Base.metadata.create_all()`,
więc na ścieżce adopcji starej bazy tabele z *tej* rewizji istnieją, zanim
rewizja w ogóle się wykona. Bezwarunkowe `CREATE TABLE` wywala wtedy start
add-onu na „table oauth_clients already exists". Świeża instalacja idzie realnym
łańcuchem migracji i tworzy je tutaj naprawdę.

Sekrety trzymamy wyłącznie jako SHA-256 (`code_hash`, `token_hash`,
`client_secret_hash`), tak samo jak `api_keys.key_hash`. Unikalny indeks na
haszu jest jednocześnie kluczem wyszukiwania przy każdym wywołaniu MCP.

Revision ID: 0003_oauth
Revises: 0002_drop_dead_schema
Create Date: 2026-08-30

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0003_oauth"
down_revision: str | None = "0002_drop_dead_schema"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _existing_tables() -> set[str]:
    return set(sa.inspect(op.get_bind()).get_table_names())


def upgrade() -> None:
    present = _existing_tables()

    if "oauth_clients" not in present:
        _create_clients()
    if "oauth_auth_codes" not in present:
        _create_auth_codes()
    if "oauth_tokens" not in present:
        _create_tokens()


def _create_clients() -> None:
    op.create_table(
        "oauth_clients",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("client_id", sa.String(), nullable=False),
        sa.Column("client_secret_hash", sa.String(), nullable=True),
        sa.Column("client_name", sa.String(), nullable=False),
        sa.Column("redirect_uris", sa.JSON(), nullable=False),
        sa.Column("grant_types", sa.JSON(), nullable=False),
        sa.Column("scope", sa.String(), nullable=False),
        sa.Column("token_endpoint_auth_method", sa.String(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    with op.batch_alter_table("oauth_clients", schema=None) as batch_op:
        batch_op.create_index(batch_op.f("ix_oauth_clients_client_id"), ["client_id"], unique=True)


def _create_auth_codes() -> None:
    op.create_table(
        "oauth_auth_codes",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("code_hash", sa.String(), nullable=False),
        sa.Column("client_id", sa.String(), nullable=False),
        sa.Column("user_id", sa.Integer(), nullable=False),
        sa.Column("redirect_uri", sa.String(), nullable=False),
        sa.Column("code_challenge", sa.String(), nullable=False),
        sa.Column("code_challenge_method", sa.String(), nullable=False),
        sa.Column("scope", sa.String(), nullable=False),
        sa.Column("resource", sa.String(), nullable=True),
        sa.Column("expires_at", sa.DateTime(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    with op.batch_alter_table("oauth_auth_codes", schema=None) as batch_op:
        batch_op.create_index(batch_op.f("ix_oauth_auth_codes_code_hash"), ["code_hash"], unique=True)
        batch_op.create_index(batch_op.f("ix_oauth_auth_codes_client_id"), ["client_id"], unique=False)
        batch_op.create_index(batch_op.f("ix_oauth_auth_codes_user_id"), ["user_id"], unique=False)


def _create_tokens() -> None:
    op.create_table(
        "oauth_tokens",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("token_hash", sa.String(), nullable=False),
        sa.Column("kind", sa.String(), nullable=False),
        sa.Column("client_id", sa.String(), nullable=False),
        sa.Column("user_id", sa.Integer(), nullable=False),
        sa.Column("scope", sa.String(), nullable=False),
        sa.Column("resource", sa.String(), nullable=True),
        sa.Column("expires_at", sa.DateTime(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("last_used_at", sa.DateTime(), nullable=True),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    with op.batch_alter_table("oauth_tokens", schema=None) as batch_op:
        batch_op.create_index(batch_op.f("ix_oauth_tokens_token_hash"), ["token_hash"], unique=True)
        batch_op.create_index(batch_op.f("ix_oauth_tokens_client_id"), ["client_id"], unique=False)
        batch_op.create_index(batch_op.f("ix_oauth_tokens_user_id"), ["user_id"], unique=False)
        batch_op.create_index("ix_oauth_tokens_user_kind", ["user_id", "kind"], unique=False)


def downgrade() -> None:
    present = _existing_tables()
    for name in ("oauth_tokens", "oauth_auth_codes", "oauth_clients"):
        if name in present:
            op.drop_table(name)
