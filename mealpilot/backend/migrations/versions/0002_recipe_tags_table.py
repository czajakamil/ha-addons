"""recipe_tags and recipe_meal_types — denormalized for indexed filtering

Revision ID: 0002
Revises: 0001
Create Date: 2026-05-16
"""
import json
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0002"
down_revision: Union[str, None] = "0001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "recipe_tags",
        sa.Column("recipe_id", sa.String(), sa.ForeignKey("recipes.id", ondelete="CASCADE"), primary_key=True),
        sa.Column("tag", sa.String(), primary_key=True),
    )
    op.create_index("ix_recipe_tags_tag", "recipe_tags", ["tag", "recipe_id"])

    op.create_table(
        "recipe_meal_types",
        sa.Column("recipe_id", sa.String(), sa.ForeignKey("recipes.id", ondelete="CASCADE"), primary_key=True),
        sa.Column("meal_type", sa.String(), primary_key=True),
    )
    op.create_index("ix_recipe_meal_types_type", "recipe_meal_types", ["meal_type", "recipe_id"])

    conn = op.get_bind()
    rows = conn.execute(sa.text("SELECT id, tags, meal_types FROM recipes")).fetchall()
    for recipe_id, raw_tags, raw_mts in rows:
        tags = json.loads(raw_tags) if isinstance(raw_tags, str) else (raw_tags or [])
        mts = json.loads(raw_mts) if isinstance(raw_mts, str) else (raw_mts or [])
        for tag in tags:
            if tag and isinstance(tag, str):
                conn.execute(
                    sa.text("INSERT OR IGNORE INTO recipe_tags (recipe_id, tag) VALUES (:rid, :tag)"),
                    {"rid": recipe_id, "tag": tag},
                )
        for mt in mts:
            if mt and isinstance(mt, str):
                conn.execute(
                    sa.text("INSERT OR IGNORE INTO recipe_meal_types (recipe_id, meal_type) VALUES (:rid, :mt)"),
                    {"rid": recipe_id, "mt": mt},
                )


def downgrade() -> None:
    op.drop_index("ix_recipe_meal_types_type", table_name="recipe_meal_types")
    op.drop_table("recipe_meal_types")
    op.drop_index("ix_recipe_tags_tag", table_name="recipe_tags")
    op.drop_table("recipe_tags")
