from datetime import datetime, timezone

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    JSON,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import relationship

from .db import Base


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(String, nullable=False, unique=True, index=True)
    password_hash = Column(String, nullable=False)
    role = Column(String, nullable=False, default="user")
    is_active = Column(Integer, nullable=False, default=1)
    session_version = Column(Integer, nullable=False, default=0)
    can_use_ai = Column(Boolean, nullable=False, default=True)
    ai_monthly_token_limit = Column(Integer, nullable=True)
    ai_monthly_cost_limit_cents = Column(Integer, nullable=True)
    ai_used_tokens_this_month = Column(Integer, nullable=False, default=0)
    ai_used_cost_cents_this_month = Column(Integer, nullable=False, default=0)
    ai_usage_period_start = Column(DateTime, nullable=False, default=_utcnow)
    created_at = Column(DateTime, nullable=False, default=_utcnow)


class Household(Base):
    __tablename__ = "households"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)
    created_at = Column(DateTime, nullable=False, default=_utcnow)


class HouseholdMember(Base):
    __tablename__ = "household_members"

    user_id = Column(Integer, ForeignKey("users.id"), primary_key=True)
    household_id = Column(Integer, ForeignKey("households.id"), nullable=False, index=True)
    can_edit = Column(Boolean, nullable=False, default=False)
    joined_at = Column(DateTime, nullable=False, default=_utcnow)


class AgentSettings(Base):
    __tablename__ = "agent_settings"

    user_id = Column(Integer, ForeignKey("users.id"), primary_key=True)
    endpoint = Column(String, nullable=False, default="")
    api_key = Column(String, nullable=False, default="")
    model = Column(String, nullable=False, default="")
    system_prompt = Column(String, nullable=False, default="")
    ui_prefs = Column(JSON, nullable=False, default=dict)
    updated_at = Column(DateTime, nullable=False, default=_utcnow, onupdate=_utcnow)


class ApiKey(Base):
    __tablename__ = "api_keys"

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    name = Column(String, nullable=False)
    prefix = Column(String, nullable=False, index=True)
    key_hash = Column(String, nullable=False, unique=True, index=True)
    created_at = Column(DateTime, nullable=False, default=_utcnow)
    last_used_at = Column(DateTime, nullable=True)


class Recipe(Base):
    __tablename__ = "recipes"
    __table_args__ = (
        CheckConstraint(
            "(owner_user_id IS NOT NULL) <> (owner_household_id IS NOT NULL)",
            name="ck_recipes_owner_exactly_one",
        ),
        Index("ix_recipes_owner_household", "owner_household_id"),
        Index("ix_recipes_owner_user", "owner_user_id"),
    )

    id = Column(String, primary_key=True)
    created_by = Column("user_id", Integer, ForeignKey("users.id"), nullable=False, index=True)
    owner_user_id = Column(Integer, ForeignKey("users.id"), nullable=True)
    owner_household_id = Column(Integer, ForeignKey("households.id"), nullable=True)
    title = Column(String, nullable=False)
    tags = Column(JSON, nullable=False, default=list)
    servings = Column(Integer, nullable=False, default=1)
    prep_time = Column(Integer, nullable=False, default=0)
    cook_time = Column(Integer, nullable=False, default=0)
    kcal = Column(Float, nullable=False, default=0)
    p = Column(Float, nullable=False, default=0)
    f = Column(Float, nullable=False, default=0)
    c = Column(Float, nullable=False, default=0)
    hue = Column(Integer, nullable=False, default=40)
    ingredients = Column(JSON, nullable=False, default=list)
    steps = Column(JSON, nullable=False, default=list)
    meal_types = Column(JSON, nullable=False, default=list)
    image_filename = Column(String, nullable=True)
    is_meal_prep = Column(Boolean, nullable=False, default=False)
    meal_prep_days = Column(Integer, nullable=True)
    meal_prep_steps = Column(JSON, nullable=False, default=list)


class MealPlanEntry(Base):
    __tablename__ = "meal_plan_entries"
    __table_args__ = (
        Index("ix_plan_user_week", "user_id", "week_start"),
        Index("ix_plan_owner_household", "owner_household_id", "week_start"),
        Index("ix_plan_owner_user", "owner_user_id", "week_start"),
        CheckConstraint(
            "(owner_user_id IS NOT NULL) <> (owner_household_id IS NOT NULL)",
            name="ck_plan_owner_exactly_one",
        ),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    created_by = Column("user_id", Integer, ForeignKey("users.id"), nullable=False, index=True)
    owner_user_id = Column(Integer, ForeignKey("users.id"), nullable=True)
    owner_household_id = Column(Integer, ForeignKey("households.id"), nullable=True)
    week_start = Column(String, nullable=False, index=True)
    day = Column(Integer, nullable=False)
    meal = Column(String, nullable=False)
    recipe_id = Column(String, ForeignKey("recipes.id"), nullable=False)
    servings = Column(Integer, nullable=False, default=1)


class AgentConversation(Base):
    __tablename__ = "agent_conversations"

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    title = Column(String, nullable=True)
    model = Column(String, nullable=False, default="")
    created_at = Column(DateTime, nullable=False, default=_utcnow)
    updated_at = Column(DateTime, nullable=False, default=_utcnow, onupdate=_utcnow)


class AgentMessage(Base):
    __tablename__ = "agent_messages"
    __table_args__ = (Index("ix_agent_msg_conv_created", "conversation_id", "created_at"),)

    id = Column(Integer, primary_key=True, autoincrement=True)
    conversation_id = Column(
        Integer,
        ForeignKey("agent_conversations.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    role = Column(String, nullable=False)  # "user" | "assistant"
    content = Column(String, nullable=False, default="")
    created_at = Column(DateTime, nullable=False, default=_utcnow)


class AgentToolUse(Base):
    __tablename__ = "agent_tool_uses"

    id = Column(Integer, primary_key=True, autoincrement=True)
    message_id = Column(
        Integer,
        ForeignKey("agent_messages.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    tool_use_id = Column(String, nullable=False)
    tool_name = Column(String, nullable=False)
    input = Column(JSON, nullable=False, default=dict)
    output = Column(JSON, nullable=True)
    is_error = Column(Integer, nullable=False, default=0)
    started_at = Column(DateTime, nullable=False, default=_utcnow)
    finished_at = Column(DateTime, nullable=True)


class WeekTemplate(Base):
    __tablename__ = "week_templates"
    __table_args__ = (
        CheckConstraint(
            "(owner_user_id IS NOT NULL) <> (owner_household_id IS NOT NULL)",
            name="ck_tpl_owner_exactly_one",
        ),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    created_by = Column("user_id", Integer, ForeignKey("users.id"), nullable=False, index=True)
    owner_user_id = Column(Integer, ForeignKey("users.id"), nullable=True)
    owner_household_id = Column(Integer, ForeignKey("households.id"), nullable=True)
    name = Column(String, nullable=False)
    entries = Column(JSON, nullable=False, default=list)
    created_at = Column(DateTime, nullable=False, default=_utcnow)


class RecipeRating(Base):
    __tablename__ = "recipe_ratings"
    __table_args__ = (
        UniqueConstraint("recipe_id", "user_id", name="uq_rating_recipe_user"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    recipe_id = Column(String, ForeignKey("recipes.id", ondelete="CASCADE"), nullable=False, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    rating = Column(Integer, nullable=False)
    created_at = Column(DateTime, nullable=False, default=_utcnow)
    updated_at = Column(DateTime, nullable=False, default=_utcnow, onupdate=_utcnow)


class RecipeNote(Base):
    __tablename__ = "recipe_notes"
    __table_args__ = (
        UniqueConstraint("recipe_id", "user_id", name="uq_note_recipe_user"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    recipe_id = Column(String, ForeignKey("recipes.id", ondelete="CASCADE"), nullable=False, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    note = Column(String, nullable=False, default="")
    created_at = Column(DateTime, nullable=False, default=_utcnow)
    updated_at = Column(DateTime, nullable=False, default=_utcnow, onupdate=_utcnow)


class ShoppingItem(Base):
    __tablename__ = "shopping_items"
    __table_args__ = (
        UniqueConstraint(
            "user_id", "week_start", "name", "unit", name="uq_shop_user_week_name_unit"
        ),
        Index("ix_shop_user_week", "user_id", "week_start"),
        Index("ix_shop_owner_household_week", "owner_household_id", "week_start"),
        Index("ix_shop_owner_user_week", "owner_user_id", "week_start"),
        CheckConstraint(
            "(owner_user_id IS NOT NULL) <> (owner_household_id IS NOT NULL)",
            name="ck_shop_owner_exactly_one",
        ),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    created_by = Column("user_id", Integer, ForeignKey("users.id"), nullable=False, index=True)
    owner_user_id = Column(Integer, ForeignKey("users.id"), nullable=True)
    owner_household_id = Column(Integer, ForeignKey("households.id"), nullable=True)
    week_start = Column(String, nullable=False, index=True)
    name = Column(String, nullable=False)
    qty = Column(Float, nullable=False, default=0)
    unit = Column(String, nullable=False, default="")
    category = Column(String, nullable=False, default="Inne")
    checked = Column(Integer, nullable=False, default=0)
    is_custom = Column(Integer, nullable=False, default=0)

    sources = relationship(
        "ShoppingItemRecipe", cascade="all, delete-orphan", lazy="selectin"
    )

    @property
    def recipe_ids(self) -> list[str]:
        return [s.recipe_id for s in self.sources]


class ShoppingItemRecipe(Base):
    """Which recipe(s) contributed a given shopping-list item (secondary display info)."""

    __tablename__ = "shopping_item_recipes"
    __table_args__ = (
        UniqueConstraint("item_id", "recipe_id", name="uq_shop_item_recipe"),
        Index("ix_shop_item_recipes_item", "item_id"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    item_id = Column(Integer, ForeignKey("shopping_items.id"), nullable=False)
    recipe_id = Column(String, ForeignKey("recipes.id"), nullable=False)
