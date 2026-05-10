import re
from datetime import datetime
from typing import Any, Dict, List, Literal, Optional
from pydantic import BaseModel, Field, ConfigDict, field_validator

_USERNAME_RE = re.compile(r"^[a-zA-ZąćęłńóśźżĄĆĘŁŃÓŚŹŻ0-9_]+$")
_USERNAME_ERR = "Login może zawierać tylko litery, cyfry i podkreślnik."


Role = Literal["admin", "user"]


class LoginRequest(BaseModel):
    username: str = Field(min_length=1, max_length=64)
    password: str = Field(min_length=1, max_length=256)


class SetupRequest(BaseModel):
    username: str = Field(min_length=1, max_length=64)
    password: str = Field(min_length=8, max_length=256)

    @field_validator("username")
    @classmethod
    def username_letters_only(cls, v: str) -> str:
        if not _USERNAME_RE.match(v):
            raise ValueError(_USERNAME_ERR)
        return v


class ChangePasswordRequest(BaseModel):
    old_password: str = Field(min_length=1, max_length=256)
    new_password: str = Field(min_length=8, max_length=256)


class CreateUserRequest(BaseModel):
    username: str = Field(min_length=1, max_length=64)
    password: str = Field(min_length=8, max_length=256)
    role: Role = "user"

    @field_validator("username")
    @classmethod
    def username_letters_only(cls, v: str) -> str:
        if not _USERNAME_RE.match(v):
            raise ValueError(_USERNAME_ERR)
        return v


class UpdateUserRequest(BaseModel):
    username: Optional[str] = Field(default=None, min_length=1, max_length=64)
    password: Optional[str] = Field(default=None, min_length=8, max_length=256)
    role: Optional[Role] = None
    is_active: Optional[bool] = None

    @field_validator("username")
    @classmethod
    def username_letters_only(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and not _USERNAME_RE.match(v):
            raise ValueError(_USERNAME_ERR)
        return v


class UserOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    username: str
    role: Role
    is_active: bool
    created_at: datetime


class SetupStatus(BaseModel):
    setup_required: bool


class Ingredient(BaseModel):
    name: str
    qty: float
    unit: str


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
    steps: List[str] = []
    meal_types: List[str] = []


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
    steps: Optional[List[str]] = None
    meal_types: Optional[List[str]] = None


class Recipe(RecipeBase):
    model_config = ConfigDict(from_attributes=True)
    id: str
    image_filename: Optional[str] = None


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


class WeekTemplateOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    name: str
    entries: List[PlanEntry]
    created_at: datetime


class WeekTemplateCreate(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    entries: List[PlanEntry]


class MacroEstimateRequest(BaseModel):
    title: str
    servings: int = 1
    ingredients: List[Ingredient]


class MacroEstimateOut(BaseModel):
    kcal: float
    p: float
    f: float
    c: float
