# MealPilot — specyfikacja serwera MCP dla agenta AI

> Dokument odzwierciedla **aktualny stan kodu** (maj 2026).  
> Poprzednia wersja opisywała planowaną implementację; wszystkie wymienione endpointy i narzędzia już istnieją.

## Kontekst projektu

Aplikacja **MealPilot** do planowania posiłków. Stack:

- **Backend**: FastAPI (Python), SQLite przez SQLAlchemy
- **Frontend**: React + TypeScript (Vite)
- **Auth**: sesja cookie (`mealpilot_session`) **lub** API key przez nagłówek `X-MealPilot-Token`

### Modele bazy danych

```python
class User:
    id: int (PK)
    username: str (unique)
    password_hash: str
    role: str              # "admin" | "user"
    is_active: int
    session_version: int
    can_use_ai: bool
    ai_monthly_token_limit: int | null
    ai_monthly_cost_limit_cents: int | null
    ai_used_tokens_this_month: int
    ai_used_cost_cents_this_month: int
    ai_usage_period_start: datetime
    created_at: datetime

class Household:
    id: int (PK)
    name: str
    created_at: datetime

class HouseholdMember:
    user_id: int (FK→users, PK)
    household_id: int (FK→households)
    can_edit: bool
    joined_at: datetime

class AgentSettings:
    user_id: int (FK, PK)
    endpoint: str          # URL endpointu LLM (np. OpenAI-compatible lub Anthropic)
    api_key: str
    model: str
    system_prompt: str
    ui_prefs: JSON         # { recipes_grouped, macro_targets, favorite_recipe_ids }
    updated_at: datetime

class ApiKey:
    id: int (PK)
    user_id: int (FK)
    name: str
    prefix: str
    key_hash: str (unique)
    created_at: datetime
    last_used_at: datetime | null

class Recipe:
    id: str (PK)               # slug, np. "kurczak-z-ryzem"
    created_by: int (FK→users) # kolumna bazy: "user_id"
    owner_user_id: int | null  # dokładnie jedno z owner_* musi być ustawione
    owner_household_id: int | null
    title: str
    tags: JSON[str]
    meal_types: JSON[str]
    servings: int
    prep_time: int             # minuty
    cook_time: int             # minuty
    kcal: float                # SUMA dla całego przepisu (wszystkich porcji)
    p: float                   # białko [g] — SUMA
    f: float                   # tłuszcze [g] — SUMA
    c: float                   # węglowodany [g] — SUMA
    hue: int                   # kolor karty (0–360)
    ingredients: JSON          # [{name, qty, unit}]
    steps: JSON[str]
    image_filename: str | null

class RecipeRating:
    id: int (PK)
    recipe_id: str (FK, CASCADE)
    user_id: int (FK)
    rating: int (1–5)
    created_at: datetime
    updated_at: datetime
    # UNIQUE(recipe_id, user_id)

class RecipeNote:
    id: int (PK)
    recipe_id: str (FK, CASCADE)
    user_id: int (FK)
    note: str (max 5000 znaków)
    created_at: datetime
    updated_at: datetime
    # UNIQUE(recipe_id, user_id)

class MealPlanEntry:
    id: int (PK, autoincrement)
    created_by: int (FK→users)  # kolumna bazy: "user_id"
    owner_user_id: int | null
    owner_household_id: int | null
    week_start: str             # "YYYY-MM-DD" (poniedziałek)
    day: int                    # 0=poniedziałek … 6=niedziela
    meal: str                   # "Śniadanie"|"II Śniadanie"|"Obiad"|"Przekąska"|"Kolacja"
    recipe_id: str (FK)
    servings: int               # porcje do zjedzenia w tym slocie

class WeekTemplate:
    id: int (PK)
    created_by: int (FK→users)
    owner_user_id: int | null
    owner_household_id: int | null
    name: str
    entries: JSON              # [{day, meal, recipe_id, servings}]
    created_at: datetime

class AgentConversation:
    id: int (PK)
    user_id: int (FK)
    title: str | null
    model: str
    created_at: datetime
    updated_at: datetime

class AgentMessage:
    id: int (PK)
    conversation_id: int (FK, CASCADE)
    role: str                  # "user" | "assistant"
    content: str
    created_at: datetime

class AgentToolUse:
    id: int (PK)
    message_id: int (FK, CASCADE)
    tool_use_id: str
    tool_name: str
    input: JSON
    output: JSON | null
    is_error: int              # 0 | 1
    started_at: datetime
    finished_at: datetime | null

class ShoppingItem:
    id: int (PK, autoincrement)
    created_by: int (FK→users)  # kolumna bazy: "user_id"
    owner_user_id: int | null
    owner_household_id: int | null
    week_start: str
    name: str
    qty: float
    unit: str
    category: str              # patrz sekcja kategorii poniżej
    checked: int               # 0 | 1
    is_custom: int             # 0=wygenerowane z planu, 1=dopisane ręcznie
    # UNIQUE(user_id, week_start, name, unit)
```

**Kategorie zakupów** (przydzielane przez słownik regex):
`"Mięso, ryby, białko"` | `"Nabiał"` | `"Warzywa i owoce"` | `"Suche i zboża"` | `"Tłuszcze i przyprawy"` | `"Spiżarnia"` | `"Inne"`

---

## Istniejące endpointy REST

### Przepisy (`/api/recipes`)

```
GET    /api/recipes                          → lista przepisów widocznych dla usera
GET    /api/recipes/meta/tags                → {tags: string[]}  (unikalne, posortowane)
GET    /api/recipes/meta/meal_types          → {meal_types: string[]}
GET    /api/recipes/{id}                     → szczegóły przepisu (z ratings, my_note)
POST   /api/recipes                          → utwórz przepis  (201)
PUT    /api/recipes/{id}                     → zaktualizuj przepis
DELETE /api/recipes/{id}                     → usuń przepis + wpisy planu (204)
PUT    /api/recipes/{id}/rating              → upsert oceny {rating: 1-5}
DELETE /api/recipes/{id}/rating              → usuń ocenę (204)
PUT    /api/recipes/{id}/note                → upsert notatki {note: str}
DELETE /api/recipes/{id}/note                → usuń notatkę (204)
PUT    /api/recipes/{id}/ownership           → {share_with_household: bool}
POST   /api/recipes/{id}/image               → upload zdjęcia
DELETE /api/recipes/{id}/image               → usuń zdjęcie
POST   /api/recipes/estimate-macros          → LLM-szacowanie makro z listy składników
```

**Filtry GET /api/recipes** (wszystkie opcjonalne):
- `tags: str` — CSV; przepis musi mieć **wszystkie** (AND, case-sensitive)
- `meal_types: str` — CSV; przepis musi pasować do **co najmniej jednego** (OR, case-sensitive)
- `max_kcal: float` — kcal sumarycznie dla całego przepisu
- `min_protein: float` — białko (g) sumarycznie
- `min_my_rating: int (1–5)` — brak oceny = wykluczony
- `min_avg_rating: float (1.0–5.0)` — brak ocen = wykluczony

**Schemat Recipe** (w odpowiedzi):
```
{
  id, title, tags, meal_types, servings, prep_time, cook_time,
  kcal, p, f, c,              ← SUMY dla całego przepisu (nie na porcję!)
  hue, ingredients [{name, qty, unit}], steps,
  image_filename, created_by,
  owner_user_id, owner_household_id,
  avg_rating, rating_count,   ← agregaty wszystkich ocen
  my_rating, my_note          ← ocena i notatka zalogowanego usera
}
```

### Plan tygodnia (`/api/plan`)

```
GET    /api/plan/{week_start}   → WeekPlan = {week_start, entries:[{day,meal,recipe_id,servings}]}
PUT    /api/plan/{week_start}   → zastąp cały plan (tablica PlanEntry)
```

`week_start` musi być **poniedziałkiem** (YYYY-MM-DD). `PUT` zwraca błąd 400 z listą nieznanych recipe_id.

### Lista zakupów (`/api/shopping`)

```
GET    /api/shopping/{week_start}             → ShoppingItem[]  (posortowane po category, name)
POST   /api/shopping/{week_start}/generate    → generuje/odświeża listę z planu (is_custom=false
                                                 pozycje usuwane i tworzone od nowa; is_custom=true ZOSTAJĄ)
PATCH  /api/shopping/{week_start}/items/{id}  → {checked: bool}  → ShoppingItemOut
PATCH  /api/shopping/items/{id}               → j.w. (skrót bez week_start w ścieżce)
POST   /api/shopping/{week_start}/items       → dodaj ręczną pozycję (is_custom=1)
DELETE /api/shopping/{week_start}/items/{id}  → usuń pozycję (204)
DELETE /api/shopping/items/{id}               → j.w. (skrót)
DELETE /api/shopping/{week_start}             → wyczyść całą listę (204)
```

**Logika `generate`**: Pobiera `MealPlanEntry` tygodnia, dla każdego mnoży składniki przez `(entry.servings / recipe.servings)`, sumuje po `(name.lower(), unit)`, normalizuje `kg→g` i `l→ml`, przypisuje kategorię przez słownik regex. Poprzednie `is_custom=0` są usuwane; stan `checked` z poprzedniej generacji jest zachowywany.

### Pozostałe

```
GET    /healthz
POST   /api/auth/login
POST   /api/auth/logout
GET    /api/auth/me
POST   /api/auth/api-keys        → tworzy API key {name} → {id, name, prefix, key (tylko raz!)}
GET    /api/auth/api-keys        → lista kluczy (bez wartości)
DELETE /api/auth/api-keys/{id}   → usuń klucz (204)
GET    /api/settings/agent       → AgentSettings
PUT    /api/settings/agent       → zapisz ustawienia LLM/agenta
GET    /api/settings/ui-prefs    → UiPrefs
PATCH  /api/settings/ui-prefs    → zaktualizuj prefs
...plan templates, admin, households (pominięto — nieistotne dla agenta MCP)
```

---

## Serwer MCP (`backend/mcp_server.py`)

### Uruchomienie

```bash
MEALPILOT_API_KEY=mp_xxx python mcp_server.py
```

Transport: **stdio**. Zmienne środowiskowe:

| Zmienna | Domyślnie | Opis |
|---|---|---|
| `MEALPILOT_BASE_URL` | `http://localhost:8000` | Adres backendu |
| `MEALPILOT_API_KEY` | `""` | API key (preferowany) |
| `MEALPILOT_SESSION_COOKIE` | `""` | Ciasteczko sesji (fallback) |
| `MEALPILOT_COOKIE_NAME` | `mealpilot_session` | Nazwa cookie |

Auth: jeśli `API_KEY` ustawione → nagłówek `X-MealPilot-Token`; inaczej → `Cookie: mealpilot_session=<wartość>`.

### Narzędzia MCP

#### Grupa 1 — Przepisy

| Narzędzie | HTTP | Opis |
|---|---|---|
| `list_recipes` | `GET /api/recipes` | Wszystkie widoczne przepisy |
| `get_recipe` | `GET /api/recipes/{recipe_id}` | Szczegóły jednego przepisu |
| `list_tags` | `GET /api/recipes/meta/tags` | `{tags: string[]}` — wywołaj przed filter/create |
| `list_meal_types` | `GET /api/recipes/meta/meal_types` | `{meal_types: string[]}` |
| `filter_recipes` | `GET /api/recipes?...` | Filtrowanie po tags/meal_types/max_kcal/min_protein/min_my_rating/min_avg_rating |
| `create_recipe` | `POST /api/recipes` | Tworzy przepis; przy 409 zmień slug i ponów |
| `update_recipe` | `PUT /api/recipes/{recipe_id}` | PATCH-semantyka; tablice nadpisywane w całości |
| `delete_recipe` | `DELETE /api/recipes/{recipe_id}` | [DESTRUKCYJNE] usuwa przepis + wpisy planu |
| `rate_recipe` | `PUT/DELETE /api/recipes/{id}/rating` | rating 1–5; rating=0 usuwa ocenę |
| `estimate_recipe_macros` | `POST /api/recipes/estimate-macros` | LLM szacuje kcal/p/f/c z listy składników |

**Uwaga do `filter_recipes`**: parametry `tags`/`meal_types` są **case-sensitive** — zawsze najpierw `list_tags`/`list_meal_types`. Pola `kcal/p/f/c` to sumy całego przepisu, nie na porcję.

**Uwaga do `estimate_recipe_macros`**: Błąd 424 = brak LLM lub wyczerpana kwota (nie ponawiaj, poproś o makro ręcznie). Błąd 502 = zły format odpowiedzi (można ponowić raz).

#### Grupa 2 — Plan tygodnia

| Narzędzie | HTTP | Opis |
|---|---|---|
| `get_week_plan` | `GET /api/plan/{week_start}` + enrichment | Plan z `recipe_title` doklejonym do każdego entry |
| `get_current_week_plan` | j.w., `week_start` liczony automatycznie | Skrót dla "bieżący tydzień" |
| `set_week_plan` | `PUT /api/plan/{week_start}` | [DESTRUKCYJNE][POTWIERDŹ] zastępuje cały plan |
| `add_plan_entry` | `GET` + `PUT /api/plan/{week_start}` | Dodaje/nadpisuje slot (day+meal), reszta bez zmian |
| `remove_plan_entry` | `GET` + `PUT /api/plan/{week_start}` | Usuwa slot, no-op gdy nie istnieje |
| `get_week_nutrition_summary` | obliczenia lokalne na danych z GET | Sumy makro per dzień, skalowane wg servings |

`week_start` **musi być poniedziałkiem** (YYYY-MM-DD). Oblicz: `data - timedelta(days=data.weekday())`.

`get_week_plan` / `get_current_week_plan` zwracają enriched entries z `recipe_title`, `avg_rating`, `rating_count`, `my_rating`.  
`set_/add_/remove_plan_entry` zwracają WeekPlan bez tych pól.

`get_week_nutrition_summary` → `{"0":{kcal,p,f,c}, ..., "6":{...}}` — wartości zaokrąglone do 2 miejsc; dni bez wpisów = same zera.

#### Grupa 3 — Lista zakupów

| Narzędzie | HTTP | Opis |
|---|---|---|
| `get_shopping_list` | `GET /api/shopping/{week_start}` | Lista pozycji, posortowana po (category, name) |
| `generate_shopping_list` | `POST /api/shopping/{week_start}/generate` | [DESTRUKCYJNE] regeneruje z planu; is_custom=true ZOSTAJĄ |
| `check_shopping_item` | `PATCH /api/shopping/items/{item_id}` | Odhacza/odznacza; `item_id` to pole `id` z get_shopping_list |
| `clear_shopping_list` | `DELETE /api/shopping/{week_start}` | [DESTRUKCYJNE][POTWIERDŹ] usuwa wszystkie pozycje (też ręczne) |

**Schemat ShoppingItem** (zwracany przez narzędzia):
```
{
  id: int,         ← użyj w check_shopping_item, NIE recipe_id ani nazwa
  week_start: str,
  name: str,
  qty: float,
  unit: str,       ← znormalizowana: "g"/"ml"/"szt"...
  category: str,   ← PL, np. "Nabiał"|"Warzywa i owoce"|"Inne"
  checked: bool,
  is_custom: bool  ← true = dopisane ręcznie
}
```

---

## System prompt agenta

```
Jesteś agentem MealPilot — pomagasz użytkownikowi planować posiłki na tydzień.

Zasady:
1. Zanim zaproponujesz plan, wywołaj list_tags i list_meal_types, żeby znać dostępne wartości.
2. Zanim cokolwiek dostosujesz, sprawdź get_current_week_plan — nie nadpisuj tego co już jest,
   chyba że user tego chce.
3. Przed wywołaniem set_week_plan zawsze pokaż użytkownikowi propozycję i czekaj na potwierdzenie.
4. Przy planowaniu uwzględniaj różnorodność — nie powtarzaj tego samego przepisu więcej niż 3 razy
   w tygodniu.
5. Jeśli user pyta o kalorie/makra, użyj get_week_nutrition_summary.
6. Przy tworzeniu przepisu bez podanych makro — wywołaj estimate_recipe_macros przed create_recipe.
7. Jeśli user chwali lub krytykuje przepis, zaproponuj zaktualizowanie oceny (rate_recipe).
8. Odpowiadaj po polsku. Bądź konkretny i zwięzły.
```

---

## Struktura plików (aktualny stan)

```
backend/
  app/
    routers/
      recipes.py        ← filtrowanie + /meta/tags + /meta/meal_types + /estimate-macros + ratings + notes
      plan.py           ← GET/PUT plan tygodnia z household support
      shopping.py       ← pełny CRUD listy zakupów + generate + ręczne pozycje
      agent.py          ← konwersacje agenta (historia, LLM call)
      settings.py       ← AgentSettings, UiPrefs, ApiKeys
      auth.py           ← login/logout/me/api-keys
      templates.py      ← szablony tygodnia
      admin_users.py    ← zarządzanie userami (admin)
      admin_households.py ← zarządzanie household (admin)
    models.py           ← wszystkie modele SQLAlchemy (patrz wyżej)
    schemas.py          ← Pydantic: Recipe, ShoppingItemOut, PlanEntry, AgentSettings, ...
    ownership.py        ← logika widoczności przepisów/planu (personal vs household)
    main.py             ← rejestracja wszystkich routerów
  mcp_server.py         ← serwer MCP (stdio), 14 narzędzi
  requirements.txt      ← mcp>=1.0, httpx, fastapi, sqlalchemy, ...
```
