import re
from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

_USERNAME_RE = re.compile(r"^[a-zA-ZąćęłńóśźżĄĆĘŁŃÓŚŹŻ0-9_]+$")
_USERNAME_ERR = "Login może zawierać tylko litery, cyfry i podkreślnik."

_PASSWORD_MIN = 12
_PASSWORD_ERR = f"Hasło musi mieć co najmniej {_PASSWORD_MIN} znaków i zawierać co najmniej jedną literę i jedną cyfrę."


def _validate_password(v: str) -> str:
    if len(v) < _PASSWORD_MIN:
        raise ValueError(_PASSWORD_ERR)
    has_alpha = any(c.isalpha() for c in v)
    has_digit = any(c.isdigit() for c in v)
    if not (has_alpha and has_digit):
        raise ValueError(_PASSWORD_ERR)
    return v


Role = Literal["admin", "user"]


class LoginRequest(BaseModel):
    username: str = Field(min_length=1, max_length=64)
    password: str = Field(min_length=1, max_length=256)


class SetupRequest(BaseModel):
    username: str = Field(min_length=1, max_length=64)
    password: str = Field(min_length=_PASSWORD_MIN, max_length=256)
    setup_token: str | None = Field(default=None, max_length=256)

    @field_validator("username")
    @classmethod
    def username_letters_only(cls, v: str) -> str:
        if not _USERNAME_RE.match(v):
            raise ValueError(_USERNAME_ERR)
        return v

    @field_validator("password")
    @classmethod
    def password_strength(cls, v: str) -> str:
        return _validate_password(v)


class ChangePasswordRequest(BaseModel):
    old_password: str = Field(min_length=1, max_length=256)
    new_password: str = Field(min_length=_PASSWORD_MIN, max_length=256)

    @field_validator("new_password")
    @classmethod
    def password_strength(cls, v: str) -> str:
        return _validate_password(v)


class CreateUserRequest(BaseModel):
    username: str = Field(min_length=1, max_length=64)
    password: str = Field(min_length=_PASSWORD_MIN, max_length=256)
    role: Role = "user"

    @field_validator("username")
    @classmethod
    def username_letters_only(cls, v: str) -> str:
        if not _USERNAME_RE.match(v):
            raise ValueError(_USERNAME_ERR)
        return v

    @field_validator("password")
    @classmethod
    def password_strength(cls, v: str) -> str:
        return _validate_password(v)


class UpdateUserRequest(BaseModel):
    username: str | None = Field(default=None, min_length=1, max_length=64)
    password: str | None = Field(default=None, min_length=_PASSWORD_MIN, max_length=256)
    role: Role | None = None
    is_active: bool | None = None

    @field_validator("username")
    @classmethod
    def username_letters_only(cls, v: str | None) -> str | None:
        if v is not None and not _USERNAME_RE.match(v):
            raise ValueError(_USERNAME_ERR)
        return v

    @field_validator("password")
    @classmethod
    def password_strength(cls, v: str | None) -> str | None:
        if v is None:
            return v
        return _validate_password(v)


class UserOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    username: str
    role: Role
    is_active: bool
    created_at: datetime


class UserAdminOut(UserOut):
    can_use_ai: bool = True
    ai_monthly_token_limit: int | None = None
    ai_monthly_cost_limit_cents: int | None = None
    ai_used_tokens_this_month: int = 0
    ai_used_cost_cents_this_month: int = 0
    household_id: int | None = None
    can_edit_in_household: bool = False


class UserAiLimitsPatch(BaseModel):
    can_use_ai: bool | None = None
    ai_monthly_token_limit: int | None = Field(default=None, ge=0)
    ai_monthly_cost_limit_cents: int | None = Field(default=None, ge=0)
    # Use sentinels via separate boolean to allow clearing — keep simple: 0/None means "no limit"
    clear_token_limit: bool = False
    clear_cost_limit: bool = False


class HouseholdOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    name: str
    created_at: datetime
    member_count: int = 0


class HouseholdCreate(BaseModel):
    name: str = Field(min_length=1, max_length=100)


class HouseholdUpdate(BaseModel):
    name: str = Field(min_length=1, max_length=100)


class HouseholdMemberOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    user_id: int
    username: str
    household_id: int
    can_edit: bool
    joined_at: datetime


class HouseholdAssignRequest(BaseModel):
    household_id: int | None = None  # None = remove from household
    can_edit: bool = False


class OwnershipPatch(BaseModel):
    """Re-pin resource between personal and household scope. Only creator may call."""

    share_with_household: bool


class SetupStatus(BaseModel):
    setup_required: bool


class Ingredient(BaseModel):
    name: str
    qty: float
    unit: str


class RecipeStep(BaseModel):
    text: str
    duration_minutes: int | None = None

    @classmethod
    def coerce(cls, v: Any) -> "RecipeStep":
        if isinstance(v, str):
            return cls(text=v)
        return cls.model_validate(v)


def _coerce_steps(v: Any) -> list[Any]:
    if not isinstance(v, list):
        return v
    return [{"text": s} if isinstance(s, str) else s for s in v]


class RecipeBase(BaseModel):
    title: str
    tags: list[str] = []
    servings: int = 1
    prep_time: int = 0
    cook_time: int = 0
    kcal: float = 0
    p: float = 0
    f: float = 0
    c: float = 0
    hue: int = 40
    ingredients: list[Ingredient] = []
    steps: list[RecipeStep] = []
    meal_types: list[str] = []
    is_meal_prep: bool = False
    meal_prep_days: int | None = None
    meal_prep_steps: list[RecipeStep] = []

    @field_validator("steps", "meal_prep_steps", mode="before")
    @classmethod
    def coerce_legacy_steps(cls, v: Any) -> list[Any]:
        return _coerce_steps(v)


class RecipeCreate(RecipeBase):
    """No ``id`` field: it is a surrogate key the database assigns."""


class RecipeUpdate(BaseModel):
    title: str | None = None
    tags: list[str] | None = None
    servings: int | None = None
    prep_time: int | None = None
    cook_time: int | None = None
    kcal: float | None = None
    p: float | None = None
    f: float | None = None
    c: float | None = None
    hue: int | None = None
    ingredients: list[Ingredient] | None = None
    steps: list[RecipeStep] | None = None
    meal_types: list[str] | None = None
    is_meal_prep: bool | None = None
    meal_prep_days: int | None = None
    meal_prep_steps: list[RecipeStep] | None = None

    @field_validator("steps", "meal_prep_steps", mode="before")
    @classmethod
    def coerce_legacy_steps(cls, v: Any) -> list[Any] | None:
        if v is None:
            return v
        return _coerce_steps(v)


class Recipe(RecipeBase):
    model_config = ConfigDict(from_attributes=True)
    id: int
    image_filename: str | None = None
    created_by: int
    owner_user_id: int | None = None
    owner_household_id: int | None = None
    avg_rating: float | None = None
    rating_count: int = 0
    my_rating: int | None = None
    my_note: str | None = None


class RatingUpsert(BaseModel):
    rating: int = Field(ge=1, le=5)


class RatingOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    recipe_id: int
    rating: int


class RecipeNoteUpsert(BaseModel):
    note: str = Field(max_length=5000)


class RecipeNoteOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    recipe_id: int
    note: str
    updated_at: datetime


class PlanEntry(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    day: int = Field(ge=0, le=6)
    meal: str
    recipe_id: int
    servings: int = 1


class WeekPlan(BaseModel):
    week_start: str
    entries: list[PlanEntry]


class ShoppingItemOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    week_start: str
    name: str
    qty: float
    unit: str
    category: str
    checked: bool
    is_custom: bool
    recipe_ids: list[int] = []


class ShoppingItemPatch(BaseModel):
    checked: bool


class ShoppingItemCreate(BaseModel):
    name: str = Field(min_length=1, max_length=200)
    qty: float = 1.0
    unit: str = "szt"
    category: str | None = None
    recipe_id: int | None = None


class ApiKeyCreateRequest(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    scope: Literal["read", "write"] = "write"


class ApiKeyOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    name: str
    prefix: str
    scope: Literal["read", "write"] = "write"
    created_at: datetime
    last_used_at: datetime | None = None


class ApiKeyCreatedOut(ApiKeyOut):
    key: str  # full plaintext key — shown only once on creation


class AgentSettingsOut(BaseModel):
    model: str = ""
    # Pusty `system_prompt` oznacza "użyj domyślnego" — backend podstawia go
    # w czasie rozmowy. `default_system_prompt` to jedyne źródło prawdy dla UI,
    # żeby frontend nie trzymał własnej (dryfującej) kopii.
    system_prompt: str = ""
    default_system_prompt: str = ""


class AgentSettingsUpdate(BaseModel):
    model: str = Field(default="", max_length=200)
    system_prompt: str = Field(default="", max_length=20000)


class MacroTargets(BaseModel):
    kcal: float = 2200
    p: float = 130
    f: float = 70
    c: float = 260


class UiPrefsOut(BaseModel):
    recipes_grouped: bool = False
    macro_targets: MacroTargets = MacroTargets()
    favorite_recipe_ids: list[int] = []


class UiPrefsPatch(BaseModel):
    recipes_grouped: bool | None = None
    macro_targets: MacroTargets | None = None
    favorite_recipe_ids: list[int] | None = None


AgentRole = Literal["user", "assistant"]


class AgentToolUseOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    tool_use_id: str
    tool_name: str
    input: dict[str, Any]
    output: Any | None = None
    is_error: bool
    started_at: datetime
    finished_at: datetime | None = None


class AgentToolUseCreate(BaseModel):
    tool_use_id: str = Field(min_length=1, max_length=200)
    tool_name: str = Field(min_length=1, max_length=200)
    input: dict[str, Any] = Field(default_factory=dict)
    output: Any | None = None
    is_error: bool = False
    finished_at: datetime | None = None


class AgentToolUsePatch(BaseModel):
    output: Any | None = None
    is_error: bool | None = None
    finished_at: datetime | None = None


class AgentMessageOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    role: AgentRole
    content: str
    created_at: datetime
    tool_uses: list[AgentToolUseOut] = []


class AgentMessageCreate(BaseModel):
    role: AgentRole
    content: str = ""
    tool_uses: list[AgentToolUseCreate] = []


class AgentMessageEdit(BaseModel):
    content: str = Field(min_length=1)


class AgentConversationOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    title: str | None = None
    model: str
    created_at: datetime
    updated_at: datetime


class AgentConversationDetail(AgentConversationOut):
    messages: list[AgentMessageOut] = []


class AgentConversationCreate(BaseModel):
    title: str | None = Field(default=None, max_length=200)
    model: str = Field(default="", max_length=200)


class AgentConversationPatch(BaseModel):
    title: str | None = Field(default=None, max_length=200)


class AiUsageReport(BaseModel):
    tokens: int = Field(default=0, ge=0)
    cost_cents: int = Field(default=0, ge=0)


class AiUsageStatus(BaseModel):
    can_use_ai: bool
    ai_monthly_token_limit: int | None = None
    ai_monthly_cost_limit_cents: int | None = None
    ai_used_tokens_this_month: int = 0
    ai_used_cost_cents_this_month: int = 0


class WeekTemplateOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    name: str
    entries: list[PlanEntry]
    created_at: datetime
    created_by: int
    owner_user_id: int | None = None
    owner_household_id: int | None = None


class WeekTemplateCreate(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    entries: list[PlanEntry]


class AgentToolEventOut(BaseModel):
    tool_use_id: str
    name: str
    input: dict[str, Any]
    output: Any | None = None
    error: str | None = None


class AgentRunResponse(BaseModel):
    reply: str
    tool_events: list[AgentToolEventOut] = []
    changed: list[str] = []
    message_id: int
    title: str | None = None


class MacroEstimateRequest(BaseModel):
    title: str
    servings: int = 1
    ingredients: list[Ingredient]


class MacroEstimateOut(BaseModel):
    kcal: float
    p: float
    f: float
    c: float
