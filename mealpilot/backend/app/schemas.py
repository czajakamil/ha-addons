import re
from datetime import datetime
from typing import Any, Dict, List, Literal, Optional
from pydantic import BaseModel, Field, ConfigDict, field_validator

_USERNAME_RE = re.compile(r"^[a-zA-ZąćęłńóśźżĄĆĘŁŃÓŚŹŻ0-9_]+$")
_USERNAME_ERR = "Login może zawierać tylko litery, cyfry i podkreślnik."

_PASSWORD_MIN = 12
_PASSWORD_ERR = (
    f"Hasło musi mieć co najmniej {_PASSWORD_MIN} znaków i zawierać "
    "co najmniej jedną literę i jedną cyfrę."
)


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
    setup_token: Optional[str] = Field(default=None, max_length=256)

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
    username: Optional[str] = Field(default=None, min_length=1, max_length=64)
    password: Optional[str] = Field(default=None, min_length=_PASSWORD_MIN, max_length=256)
    role: Optional[Role] = None
    is_active: Optional[bool] = None

    @field_validator("username")
    @classmethod
    def username_letters_only(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and not _USERNAME_RE.match(v):
            raise ValueError(_USERNAME_ERR)
        return v

    @field_validator("password")
    @classmethod
    def password_strength(cls, v: Optional[str]) -> Optional[str]:
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
    ai_monthly_token_limit: Optional[int] = None
    ai_monthly_cost_limit_cents: Optional[int] = None
    ai_used_tokens_this_month: int = 0
    ai_used_cost_cents_this_month: int = 0
    household_id: Optional[int] = None
    can_edit_in_household: bool = False


class UserAiLimitsPatch(BaseModel):
    can_use_ai: Optional[bool] = None
    ai_monthly_token_limit: Optional[int] = Field(default=None, ge=0)
    ai_monthly_cost_limit_cents: Optional[int] = Field(default=None, ge=0)
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
    household_id: Optional[int] = None  # None = remove from household
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
    duration_minutes: Optional[int] = None

    @classmethod
    def coerce(cls, v: Any) -> "RecipeStep":
        if isinstance(v, str):
            return cls(text=v)
        return cls.model_validate(v)


class RecipeBase(BaseModel):
    title: str
    tags: List[str] = []
    servings: int = 1
    prep_time: int = 0
    cook_time: int = 0
    kcal: float = 0
    p: float = 0
    f: float = 0
    c: float = 0
    hue: int = 40
    ingredients: List[Ingredient] = []
    steps: List[RecipeStep] = []
    meal_types: List[str] = []

    @field_validator("steps", mode="before")
    @classmethod
    def coerce_legacy_steps(cls, v: Any) -> List[Any]:
        if not isinstance(v, list):
            return v
        return [{"text": s} if isinstance(s, str) else s for s in v]


class RecipeCreate(RecipeBase):
    id: str


class RecipeUpdate(BaseModel):
    title: Optional[str] = None
    tags: Optional[List[str]] = None
    servings: Optional[int] = None
    prep_time: Optional[int] = None
    cook_time: Optional[int] = None
    kcal: Optional[float] = None
    p: Optional[float] = None
    f: Optional[float] = None
    c: Optional[float] = None
    hue: Optional[int] = None
    ingredients: Optional[List[Ingredient]] = None
    steps: Optional[List[RecipeStep]] = None
    meal_types: Optional[List[str]] = None

    @field_validator("steps", mode="before")
    @classmethod
    def coerce_legacy_steps(cls, v: Any) -> Optional[List[Any]]:
        if v is None or not isinstance(v, list):
            return v
        return [{"text": s} if isinstance(s, str) else s for s in v]


class Recipe(RecipeBase):
    model_config = ConfigDict(from_attributes=True)
    id: str
    image_filename: Optional[str] = None
    created_by: int
    owner_user_id: Optional[int] = None
    owner_household_id: Optional[int] = None
    avg_rating: Optional[float] = None
    rating_count: int = 0
    my_rating: Optional[int] = None
    my_note: Optional[str] = None


class RatingUpsert(BaseModel):
    rating: int = Field(ge=1, le=5)


class RatingOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    recipe_id: str
    rating: int


class RecipeNoteUpsert(BaseModel):
    note: str = Field(max_length=5000)


class RecipeNoteOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    recipe_id: str
    note: str
    updated_at: datetime


class PlanEntry(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    day: int = Field(ge=0, le=6)
    meal: str
    recipe_id: str
    servings: int = 1


class WeekPlan(BaseModel):
    week_start: str
    entries: List[PlanEntry]


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


class ShoppingItemPatch(BaseModel):
    checked: bool


class ShoppingItemCreate(BaseModel):
    name: str = Field(min_length=1, max_length=200)
    qty: float = 1.0
    unit: str = "szt"
    category: str | None = None


class ApiKeyCreateRequest(BaseModel):
    name: str = Field(min_length=1, max_length=100)


class ApiKeyOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    name: str
    prefix: str
    created_at: datetime
    last_used_at: Optional[datetime] = None


class ApiKeyCreatedOut(ApiKeyOut):
    key: str  # full plaintext key — shown only once on creation


class AgentSettingsOut(BaseModel):
    endpoint: str = ""
    api_key: str = ""
    model: str = ""
    system_prompt: str = ""


class AgentSettingsUpdate(BaseModel):
    endpoint: str = Field(default="", max_length=2000)
    api_key: str = Field(default="", max_length=4000)
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
    favorite_recipe_ids: List[str] = []


class UiPrefsPatch(BaseModel):
    recipes_grouped: Optional[bool] = None
    macro_targets: Optional[MacroTargets] = None
    favorite_recipe_ids: Optional[List[str]] = None


AgentRole = Literal["user", "assistant"]


class AgentToolUseOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    tool_use_id: str
    tool_name: str
    input: Dict[str, Any]
    output: Optional[Any] = None
    is_error: bool
    started_at: datetime
    finished_at: Optional[datetime] = None


class AgentToolUseCreate(BaseModel):
    tool_use_id: str = Field(min_length=1, max_length=200)
    tool_name: str = Field(min_length=1, max_length=200)
    input: Dict[str, Any] = Field(default_factory=dict)
    output: Optional[Any] = None
    is_error: bool = False
    finished_at: Optional[datetime] = None


class AgentToolUsePatch(BaseModel):
    output: Optional[Any] = None
    is_error: Optional[bool] = None
    finished_at: Optional[datetime] = None


class AgentMessageOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    role: AgentRole
    content: str
    created_at: datetime
    tool_uses: List[AgentToolUseOut] = []


class AgentMessageCreate(BaseModel):
    role: AgentRole
    content: str = ""
    tool_uses: List[AgentToolUseCreate] = []


class AgentMessageEdit(BaseModel):
    content: str = Field(min_length=1)


class AgentConversationOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    title: Optional[str] = None
    model: str
    created_at: datetime
    updated_at: datetime


class AgentConversationDetail(AgentConversationOut):
    messages: List[AgentMessageOut] = []


class AgentConversationCreate(BaseModel):
    title: Optional[str] = Field(default=None, max_length=200)
    model: str = Field(default="", max_length=200)


class AgentConversationPatch(BaseModel):
    title: Optional[str] = Field(default=None, max_length=200)


class AiUsageReport(BaseModel):
    tokens: int = Field(default=0, ge=0)
    cost_cents: int = Field(default=0, ge=0)


class AiUsageStatus(BaseModel):
    can_use_ai: bool
    ai_monthly_token_limit: Optional[int] = None
    ai_monthly_cost_limit_cents: Optional[int] = None
    ai_used_tokens_this_month: int = 0
    ai_used_cost_cents_this_month: int = 0


class WeekTemplateOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    name: str
    entries: List[PlanEntry]
    created_at: datetime
    created_by: int
    owner_user_id: Optional[int] = None
    owner_household_id: Optional[int] = None


class WeekTemplateCreate(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    entries: List[PlanEntry]


class AgentToolEventOut(BaseModel):
    tool_use_id: str
    name: str
    input: Dict[str, Any]
    output: Optional[Any] = None
    error: Optional[str] = None


class AgentRunResponse(BaseModel):
    reply: str
    tool_events: List[AgentToolEventOut] = []
    changed: List[str] = []
    message_id: int
    title: Optional[str] = None


class MacroEstimateRequest(BaseModel):
    title: str
    servings: int = 1
    ingredients: List[Ingredient]


class MacroEstimateOut(BaseModel):
    kcal: float
    p: float
    f: float
    c: float
