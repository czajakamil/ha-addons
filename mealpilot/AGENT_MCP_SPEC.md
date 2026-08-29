# MealPilot — specyfikacja warstwy narzędzi (agent AI + MCP)

> **29 narzędzi** pochodzących z jednego rejestru (`backend/app/services/registry.py`),
> udostępnianych trzem konsumentom: wbudowanemu agentowi AI, serwerowi MCP
> (Claude Desktop itp.) oraz UI (modal „co potrafi agent”).

## Kontekst projektu

Aplikacja **MealPilot** do planowania posiłków. Stack:

- **Backend**: FastAPI (Python), SQLite przez SQLAlchemy
- **Frontend**: React + TypeScript (Vite)
- **Auth**: sesja cookie (`mealpilot_session`) **lub** API key przez nagłówek `X-MealPilot-Token`

---

## Architektura warstwy narzędzi

```
                        app/services/registry.py
                         (TOOL_SPECS — jedyne źródło prawdy)
                                    │
        ┌───────────────────────────┼───────────────────────────┐
        ▼                           ▼                           ▼
app/agent/tools/schemas.py   app/mcpserver/server.py     GET /api/agent/tools
 TOOL_DEFS / _OPENAI          mcp.types.Tool[]            (modal w UI)
   (agent w aplikacji)        (Claude Desktop…)
```

Każde narzędzie to jeden `ToolSpec`: nazwa, tytuł, grupa, opis, `input_schema`,
`output_schema`, flagi `read_only` / `destructive` / `idempotent` / `confirm`,
lista domen do odświeżenia w UI (`changed`), aliasy oraz handler z warstwy domenowej.
**Dodanie narzędzia = dodanie jednego `ToolSpec`.** Nie ma drugiej listy do
synchronizacji — w szczególności zniknęła ręcznie utrzymywana kopia listy narzędzi
w TypeScripcie po stronie frontendu; UI pobiera opis narzędzi z `GET /api/agent/tools`.

Warstwa domenowa (`app/services/`) jest wspólna dla REST, agenta i MCP:

| Moduł | Zawartość |
|---|---|
| `common.py` | walidacja `week_start`, slugi, normalizacja jednostek/kroków, strażnicy uprawnień (`require_visible`, `require_editable`, `assert_can_edit`), serializery |
| `recipes.py` | wyszukiwanie, filtrowanie, CRUD przepisów, oceny, notatki, udostępnianie |
| `plan.py` | odczyt i uzgadnianie planu tygodnia, makro tygodnia |
| `shopping.py` | generowanie i edycja listy zakupów |
| `templates.py` | szablony tygodnia |
| `macros.py` | szacowanie makro modelem LLM (liczy się do kwoty AI) |
| `errors.py` | `Invalid` / `NotFound` / `Forbidden` / `Conflict` / `Unavailable` / `Upstream` |
| `registry.py` | rejestr `ToolSpec` + `invoke()` |

Ten sam kod obsługuje REST i narzędzia, więc **widoczność i prawa edycji są identyczne
we wszystkich trzech kanałach**.

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
    scope: str             # "write" (domyślnie) | "read"
    created_at: datetime
    last_used_at: datetime | null

class Recipe:
    id: str (PK)               # slug, np. "kurczak-z-ryzem" — nadawany przez serwer
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
    steps: JSON                # [{text, duration_minutes}]
    is_meal_prep: bool
    meal_prep_days: int | null
    meal_prep_steps: JSON      # [{text, duration_minutes}]
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
    # recipe_ids: property → lista przepisów, z których pochodzi (ShoppingItemRecipe)

class ShoppingItemRecipe:
    id: int (PK)
    item_id: int (FK→shopping_items)
    recipe_id: str (FK→recipes)
    # UNIQUE(item_id, recipe_id)
```

**Kategorie zakupów** (przydzielane przez słownik regex):
`"Mięso, ryby, białko"` | `"Nabiał"` | `"Warzywa i owoce"` | `"Suche i zboża"` | `"Tłuszcze i przyprawy"` | `"Spiżarnia"` | `"Inne"`

---

## Istniejące endpointy REST

### Przepisy (`/api/recipes`)

```
GET    /api/recipes                          → lista przepisów widocznych dla usera (pełne obiekty)
GET    /api/recipes/meta/tags                → {tags: string[]}  (unikalne, posortowane)
GET    /api/recipes/meta/meal_types          → {meal_types: string[]}
GET    /api/recipes/{id}                     → szczegóły przepisu (z ratings, my_note)
POST   /api/recipes                          → utwórz przepis  (201; wymaga `id` w body, 409 przy kolizji)
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

> **Uwaga:** REST `POST /api/recipes` nadal przyjmuje `id` od klienta (UI je generuje).
> Narzędzie `create_recipe` — nie; tam slug nadaje serwer.

**Filtry GET /api/recipes** (wszystkie opcjonalne):
- `q: str` — wyszukiwanie tekstowe (tytuł, tagi, typy posiłku, nazwy składników)
- `tags: str` — CSV; przepis musi mieć **wszystkie** (AND, case-sensitive)
- `meal_types: str` — CSV; przepis musi pasować do **co najmniej jednego** (OR, case-sensitive)
- `max_kcal: float` — kcal sumarycznie dla całego przepisu
- `min_protein: float` — białko (g) sumarycznie
- `max_total_time: float` — `prep_time + cook_time` (min)
- `is_meal_prep: bool`
- `min_my_rating: int (1–5)` — brak oceny = wykluczony
- `min_avg_rating: float (1.0–5.0)` — brak ocen = wykluczony

**Schemat Recipe** (w odpowiedzi REST):
```
{
  id, title, tags, meal_types, servings, prep_time, cook_time,
  kcal, p, f, c,              ← SUMY dla całego przepisu (nie na porcję!)
  hue, ingredients [{name, qty, unit}],
  steps [{text, duration_minutes}],
  is_meal_prep, meal_prep_days, meal_prep_steps [{text, duration_minutes}],
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

`week_start` musi być **poniedziałkiem** (YYYY-MM-DD) — inny dzień to błąd 400 z podpowiedzią
właściwej daty. `PUT` uzgadnia tydzień slot po slocie (patrz „Uprawnienia i własność”).

### Lista zakupów (`/api/shopping`)

```
GET    /api/shopping/{week_start}             → ShoppingItem[]  (posortowane po category, name)
POST   /api/shopping/{week_start}/generate    → przelicza wygenerowaną część listy z planu
PATCH  /api/shopping/{week_start}/items/{id}  → {checked: bool}  → ShoppingItemOut
PATCH  /api/shopping/items/{id}               → j.w. (skrót bez week_start w ścieżce)
POST   /api/shopping/{week_start}/items       → dodaj ręczną pozycję (is_custom=1)
DELETE /api/shopping/{week_start}/items/{id}  → usuń pozycję (204)
DELETE /api/shopping/items/{id}               → j.w. (skrót)
DELETE /api/shopping/{week_start}             → wyczyść całą listę (204)
```

**Logika `generate`**: Pobiera `MealPlanEntry` tygodnia, dla każdego mnoży składniki przez
`(entry.servings / recipe.servings)`, sumuje po `(name.lower(), unit)`, normalizuje `kg→g`
i `l→ml`, przypisuje kategorię przez słownik regex. Pozycje `is_custom=1` są nietykane.
Wygenerowane pozycje, które nadal wynikają z planu, są **aktualizowane w miejscu** —
zachowują `id`, właściciela i stan `checked`; usuwane są tylko te, które z planu wypadły.

### Szablony tygodnia (`/api/templates`)

```
GET    /api/templates                         → szablony widoczne dla usera
POST   /api/templates                         → utwórz szablon {name, entries} (201)
DELETE /api/templates/{id}                    → usuń szablon (204; wymaga prawa edycji)
PUT    /api/templates/{id}/ownership          → {share_with_household: bool} (tylko twórca)
POST   /api/templates/{id}/apply/{week_start} → nadpisz plan tygodnia szablonem → WeekPlan
```

### Pozostałe

```
GET    /healthz
POST   /api/auth/login
POST   /api/auth/logout
GET    /api/auth/me
POST   /api/auth/api-keys        → tworzy API key {name, scope?} → {id, name, prefix, scope, key (tylko raz!)}
GET    /api/auth/api-keys        → lista kluczy (bez wartości, ze scope)
DELETE /api/auth/api-keys/{id}   → usuń klucz (204)
GET    /api/agent/tools          → {groups, tools} — opis narzędzi prosto z rejestru
GET    /api/settings/agent       → AgentSettings
PUT    /api/settings/agent       → zapisz ustawienia LLM/agenta
GET    /api/settings/ui          → UiPrefs
PATCH  /api/settings/ui          → zaktualizuj prefs
GET|POST|DELETE /mcp             → MCP (Streamable HTTP)
GET    /mcp/sse, POST /mcp/messages  → MCP (SSE, starszy transport)
...admin, households (pominięto — nieistotne dla agenta MCP)
```

---

## Serwer MCP

Serwer żyje w `backend/app/mcpserver/server.py` i jest **wyłącznie adapterem transportu** —
lista narzędzi, schematy i zachowanie pochodzą z rejestru.

Narzędzia wykonują się **w procesie, bezpośrednio na bazie**. Serwer nie wykonuje już
zapytań HTTP z powrotem do aplikacji FastAPI, w której działa: nie ma pętli loopback,
drugiego uwierzytelnienia per wywołanie ani drugiego modelu uprawnień.

Zależność `mcp` jest przypięta w `requirements.txt` do wersji **1.12.4**.

Po HTTP serwer wystawiony jest dwoma transportami — nowszym Streamable HTTP (`/mcp`)
i starszym SSE (`/mcp/sse`). Oba działają równolegle, mają to samo uwierzytelnianie
i tę samą listę narzędzi; oba są sprawdzone end-to-end realnym klientem `mcp`.

### Transport 1 — Streamable HTTP (`/mcp`) — zalecany

Transport, który w rewizji specyfikacji MCP 2025-03-26 zastąpił SSE: jeden endpoint zamiast
pary strumień + kanał zwrotny.

```
GET | POST | DELETE /mcp
```

```json
{
  "mcpServers": {
    "mealpilot": {
      "url": "http://<HA_IP>:8000/mcp",
      "headers": { "X-MealPilot-Token": "mp_xxx" }
    }
  }
}
```

Menedżer sesji pracuje **bezstanowo**: świeży transport na każde żądanie, brak tablicy sesji.
Pasuje to dokładnie do modelu uwierzytelniania MealPilota — każde żądanie niesie własny
`X-MealPilot-Token`, więc nie istnieje długo żyjąca sesja, której właściciela trzeba by
pilnować. Menedżer jest uruchamiany w `lifespan` aplikacji
(`mcp_http.session_manager_lifespan`); żądanie przed jego startem kończy się `503`.

### Transport 2 — HTTP/SSE (`/mcp/sse`)

Starszy transport, **nadal w pełni obsługiwany** — istniejące konfiguracje klientów działają
bez zmian i nie trzeba ich przepinać. To do niego odsyła podpowiedź konfiguracji w aplikacji
(Ustawienia → Klucze API). W specyfikacji MCP jest oznaczony jako przestarzały, więc dla
nowych integracji lepszym wyborem jest `/mcp`.

```
GET  /mcp/sse        → strumień SSE; uruchamia server.run() na czas połączenia
POST /mcp/messages   → kanał zwrotny klient→serwer (SseServerTransport)
```

```json
{
  "mcpServers": {
    "mealpilot": {
      "url": "http://<HA_IP>:8000/mcp/sse",
      "headers": { "X-MealPilot-Token": "mp_xxx" }
    }
  }
}
```

**Powiązanie sesji z właścicielem** (dotyczy wyłącznie SSE — w `/mcp` nie ma sesji, którą
można by przejąć). `POST /mcp/messages` jest routowane po `session_id` z query stringa.
Serwer pamięta, który użytkownik otworzył daną sesję SSE, i odrzuca POST z kluczem innego
użytkownika (`403`) oraz do nieznanej lub zakończonej sesji (`404`). Jeśli sesji nie da się
powiązać z właścicielem, połączenie jest zrywane (fail closed).

### Uwierzytelnianie i limity (oba transporty HTTP)

**Wymagany** nagłówek `X-MealPilot-Token` z prawidłowym API key — brak nagłówka lub
nieznany/nieaktywny klucz → `401`. Klucz jest rozwiązywany do `Principal(user_id, scope)`
(hash SHA-256 porównywany z `ApiKey.key_hash`); `last_used_at` aktualizowane co najwyżej
raz na 60 s.

**Rate limiting** (per IP, okna przesuwne; `cf-connecting-ip` → `x-forwarded-for` → adres klienta):

| Kanał | Limit |
|---|---|
| `GET /mcp/sse` (handshake) | 30 / 5 min |
| `POST /mcp/messages` oraz `/mcp` | 600 / 60 s |
| nieudane uwierzytelnienia | 20 / 5 min |

Przekroczenie → `429` z nagłówkiem `Retry-After`.

### Transport 3 — stdio (lokalny proces przy bazie)

```bash
MEALPILOT_API_KEY=mp_xxx MEALPILOT_DB=/data/mealpilot.db python mcp_server.py
```

| Zmienna | Opis |
|---|---|
| `MEALPILOT_API_KEY` | API key MealPilota — rozwiązywany do użytkownika i zakresu klucza |
| `MEALPILOT_DB` | ścieżka do pliku bazy SQLite |

> **Ograniczenie:** ponieważ narzędzia wykonują się w procesie na bazie, `mcp_server.py`
> **musi działać na hoście, który ma dostęp do pliku bazy MealPilota**. Ten transport nie
> łączy się z instancją MealPilota po HTTP i nie nadaje się do pracy zdalnej.
> Wewnątrz add-onu używaj `/mcp` albo `/mcp/sse`.


### Zakresy kluczy API

`ApiKey.scope` przyjmuje `"write"` (domyślnie) albo `"read"`. Klucz **read**:

- po REST — dopuszczony tylko dla metod `GET` / `HEAD` / `OPTIONS`; każda inna metoda → `403`;
- po MCP — dopuszczony tylko dla narzędzi z `readOnlyHint=true`; próba wywołania narzędzia
  zapisującego kończy się błędem z informacją, żeby utworzyć klucz o zakresie `write`.

Klucz tworzy się w **Ustawienia → Klucze API** (albo `POST /api/auth/api-keys` z `{"name": ..., "scope": "read"}`).
Zakres `read` jest właściwym wyborem dla klienta, który ma tylko czytać (dashboard,
automatyzacja HA, integracja raportująca) — nawet przejęty klucz nie zmieni wtedy danych.
Uwaga: `estimate_recipe_macros` liczy się jako narzędzie zapisujące (zużywa kwotę AI),
więc klucz `read` go nie wywoła.

### Adnotacje, schematy wyjściowe i błędy

Każde narzędzie niesie `ToolAnnotations`:

| Pole | Znaczenie |
|---|---|
| `title` | czytelna nazwa do pokazania użytkownikowi |
| `readOnlyHint` | narzędzie niczego nie zmienia |
| `destructiveHint` | usuwa lub nadpisuje istniejące dane |
| `idempotentHint` | powtórzenie z tymi samymi argumentami nic nie zmienia |
| `openWorldHint` | zawsze `false` — narzędzia działają tylko na danych MealPilota |

Dzięki temu klient sam decyduje, kiedy poprosić o potwierdzenie. Dawne prozaiczne znaczniki
`[DESTRUKCYJNE]` / `[POTWIERDŹ]` w opisach **zostały usunięte** — nie szukaj ich w tekście.

Każde narzędzie ma też `outputSchema`, a wynik wraca jako `structuredContent` + blok tekstowy JSON.

**Błędy są prawdziwymi błędami protokołu** (`CallToolResult.isError = true`), a nie „udaną”
odpowiedzią zaczynającą się od `ERROR:`. Treść błędu ma postać `[kod] komunikat`:

| Kod | HTTP (REST) | Kiedy |
|---|---|---|
| `invalid_request` | 400 | złe argumenty, `week_start` nie jest poniedziałkiem, nieznane `recipe_id` |
| `not_found` | 404 | zasób nie istnieje albo jest niewidoczny dla użytkownika |
| `forbidden` | 403 | brak prawa edycji zasobu household, klucz tylko-do-odczytu |
| `conflict` | 409 | kolizja stanu |
| `unavailable` | 424 | brak skonfigurowanego LLM lub wyczerpana kwota AI — nie ponawiaj |
| `upstream_error` | 502 | LLM odpowiedział, ale nie w użytecznej formie — można ponowić raz |

---

## Narzędzia

Legenda flag: **R** = tylko odczyt, **D** = destrukcyjne, **I** = idempotentne,
**C** = warto poprosić użytkownika o potwierdzenie.

### Grupa 1 — Przepisy 📖

| Narzędzie | Flagi | Opis | Zwraca |
|---|---|---|---|
| `search_recipes` | R I | Wyszukiwanie pełnotekstowe po tytule, tagach, typach posiłku i **nazwach składników**; nieczułe na wielkość liter i polskie znaki; wszystkie słowa muszą wystąpić, sortowanie wg trafności (tytuł > tag > składnik). Można łączyć z filtrami. | `{items, total, limit, offset, has_more, query}` |
| `list_recipes` | R I | Stronicowana lista **skrótów** widocznych przepisów. | `{items, total, limit, offset, has_more}` |
| `filter_recipes` | R I | Skróty przepisów spełniające **wszystkie** kryteria: `tags` (AND), `meal_types` (OR), `max_kcal`, `min_protein`, `max_total_time`, `is_meal_prep`, `min_my_rating`, `min_avg_rating`. | j.w. |
| `get_recipe` | R I | Jeden **pełny** przepis po `id`. | `Recipe` |
| `list_tags` | R I | Unikalne tagi z widocznych przepisów. | `{tags: string[]}` |
| `list_meal_types` | R I | Unikalne typy posiłków. | `{meal_types: string[]}` |
| `create_recipe` | C | Tworzy przepis; **`id` nadaje serwer** (slug z tytułu, sufiks `-2`/`-3` przy kolizji). | `Recipe` |
| `update_recipe` | — | PATCH-semantyka; tablice (`ingredients`, `steps`, `tags`, `meal_types`, `meal_prep_steps`) **nadpisywane w całości**. | `Recipe` |
| `delete_recipe` | D C | Usuwa przepis, jego wpisy w planach i regeneruje listy dotkniętych tygodni. | `{deleted, affected_weeks}` |
| `rate_recipe` | I C | Ocena 1–5; `rating=0` usuwa ocenę. | `Recipe` |
| `set_recipe_note` | I | Prywatna notatka użytkownika przy przepisie; pusty `note` kasuje. | `Recipe` |
| `share_recipe_with_household` | I C | Przepina przepis prywatny ↔ wspólny. **Tylko twórca.** | `Recipe` |

**Stronicowanie:** `list_recipes` / `filter_recipes` — `limit` domyślnie 50 (maks. 200),
`offset` domyślnie 0. `search_recipes` — `limit` domyślnie 20 (maks. 200).
Pola `kcal/p/f/c` to **sumy całego przepisu**, nie na porcję.
`tags` / `meal_types` są **case-sensitive** — najpierw `list_tags` / `list_meal_types`.

**RecipeSummary** (`items` z list/filter/search):
```
{ id, title, tags, meal_types, servings, prep_time, cook_time,
  kcal, p, f, c, is_meal_prep, ingredients_count, steps_count,
  avg_rating, rating_count, my_rating, my_note }
```

**Recipe** (pełny; `get_recipe` i wszystkie zapisy przepisów):
```
{ …pola RecipeSummary bez ingredients_count/steps_count…,
  hue, ingredients [{name, qty, unit}],
  steps [{text, duration_minutes}],
  meal_prep_days, meal_prep_steps [{text, duration_minutes}],
  image_filename, shared_with_household }
```

### Grupa 2 — Plan tygodnia 📅

| Narzędzie | Flagi | Opis | Zwraca |
|---|---|---|---|
| `get_week_plan` | R I | Plan wskazanego tygodnia z doklejonym `recipe_title`. | `WeekPlan` |
| `get_current_week_plan` | R I | Jak wyżej, ale `week_start` (bieżący poniedziałek w strefie serwera) liczy serwer. | `WeekPlan` |
| `set_week_plan` | D C | Zastępuje **cały** plan listą `entries` (pusta = czyszczenie). | `WeekPlan` |
| `add_plan_entry` | I | Dodaje/nadpisuje jeden slot `(day, meal)`, reszta bez zmian. | `WeekPlan` |
| `remove_plan_entry` | D I | Usuwa slot `(day, meal)`; no-op, gdy pusty. | `WeekPlan` |
| `get_week_nutrition_summary` | R I | Sumy makro per dzień i za cały tydzień, skalowane wg `entry.servings / recipe.servings`. | `{week_start, days, week_total}` |

```
WeekPlan = { week_start, entries: [{day, meal, recipe_id, servings, recipe_title}] }

get_week_nutrition_summary →
{
  week_start: "YYYY-MM-DD",
  days: { "0": {kcal,p,f,c}, …, "6": {kcal,p,f,c} },   ← 0=poniedziałek, dni bez wpisów = zera
  week_total: {kcal,p,f,c}
}
```
Wartości zaokrąglone do 2 miejsc.

### Grupa 3 — Lista zakupów 🛒

| Narzędzie | Flagi | Opis | Zwraca |
|---|---|---|---|
| `get_shopping_list` | R I | Lista tygodnia, posortowana po `(category, name)`. | `ShoppingList` |
| `generate_shopping_list` | I | Przelicza wygenerowaną część listy z planu; pozycje ręczne i odhaczenia zostają. | `ShoppingList` |
| `check_shopping_item` | I | Odhacza/odznacza pozycję po `item_id`. | `ShoppingItem` |
| `add_shopping_item` | — | Dodaje ręczną pozycję (`is_custom=true`); ta sama `(name, unit)` w tygodniu → ilości **sumowane**. | `ShoppingItem` |
| `delete_shopping_item` | D | Usuwa jedną pozycję. Alias: `remove_shopping_item`. | `{deleted}` |
| `clear_shopping_list` | D C | Usuwa **wszystkie** pozycje tygodnia, też ręczne. | `{cleared, removed}` |

```
ShoppingList = { week_start, items: ShoppingItem[], total, checked }

ShoppingItem = {
  id: int,          ← argument do check_shopping_item / delete_shopping_item; NIE recipe_id ani nazwa
  week_start: str,
  name: str,
  qty: float,
  unit: str,        ← znormalizowana: "g"/"ml"/"szt"…
  category: str,    ← PL, np. "Nabiał"|"Warzywa i owoce"|"Inne"
  checked: bool,
  is_custom: bool,  ← true = dopisane ręcznie
  recipe_ids: str[] ← z jakich przepisów pochodzi
}
```

### Grupa 4 — Szablony tygodnia 🗂️

| Narzędzie | Flagi | Opis | Zwraca |
|---|---|---|---|
| `list_week_templates` | R I | Szablony widoczne dla użytkownika (własne + dzielone w household). | `{templates, total}` |
| `save_week_as_template` | — | Zapisuje plan tygodnia jako nazwany szablon; błąd, gdy plan pusty. | `WeekTemplate` |
| `apply_week_template` | D C | Nadpisuje plan tygodnia szablonem; przepisy usunięte z biblioteki są pomijane. | `WeekPlan` + `applied_template`, `skipped_recipe_ids` |
| `delete_week_template` | D C | Trwale usuwa szablon. | `{deleted}` |

```
WeekTemplate = { id, name, entries: [{day, meal, recipe_id, servings}], entry_count, shared_with_household }
```

### Grupa 5 — Pomocnicze 🧮

| Narzędzie | Flagi | Opis | Zwraca |
|---|---|---|---|
| `estimate_recipe_macros` | — | Szacuje makro przepisu modelem LLM na podstawie składników. Zużywa kwotę AI użytkownika. | `{kcal, p, f, c}` |

Wynik to **sumy dla całego przepisu**, gotowe do przekazania do `create_recipe` / `update_recipe`.
Błędy: `unavailable` (brak LLM lub wyczerpana kwota — nie ponawiaj, poproś użytkownika o makro),
`upstream_error` (odpowiedź nie-JSON — można ponowić raz).

---

## Kontrakty wspólne

### `week_start` musi być poniedziałkiem — i jest to walidowane

Wszystkie tabele tygodniowe są kluczowane literalnym stringiem `week_start`, więc wtorek
nie pasował do niczego i czytało się to jako „plan jest pusty”. Teraz niepoprawna data
kończy się błędem `invalid_request`, który **podaje właściwy poniedziałek**, np.:

```
week_start musi być poniedziałkiem; 2026-08-27 to czwartek. Użyj 2026-08-24.
```

Nie licz daty samodzielnie, jeśli chodzi o „ten tydzień” — użyj `get_current_week_plan`
i przekaż zwrócone `week_start` do pozostałych narzędzi tygodniowych.

### Uprawnienia i własność (household)

- **Widoczność**: zasoby prywatne widzi tylko właściciel; zasoby household — wszyscy domownicy.
- **Edycja**: zasób household może edytować domownik z `can_edit` **albo** jego twórca.
  Inaczej → `forbidden`. Reguła jest ta sama w REST, u agenta i w MCP.
- **Przepięcie prywatny ↔ household** (`share_recipe_with_household`, `PUT .../ownership`) —
  wyłącznie twórca zasobu.
- **Zastąpienie planu tygodnia nie zmienia właściciela wierszy.** Tydzień jest uzgadniany
  slot po slocie: istniejące sloty są aktualizowane w miejscu, więc plan wspólny pozostaje
  wspólny (kiedyś zamieniał się po cichu w prywatny).
- **Regeneracja listy zakupów** działa tak samo: zachowuje `id` pozycji, właściciela i odhaczenia.

### Kroki przepisu

Krok to obiekt `{text, duration_minutes}`. Na wejściu akceptowany jest też zwykły string —
zostanie znormalizowany do `{text: "...", duration_minutes: null}`. Dotyczy to zarówno
`steps`, jak i `meal_prep_steps`.

### Meal prep

Pola `is_meal_prep`, `meal_prep_days`, `meal_prep_steps` są dostępne z poziomu narzędzi
(zapis przez `create_recipe` / `update_recipe`, filtrowanie przez `filter_recipes.is_meal_prep`).

---

## Znane ograniczenia i plany

- **Transport SSE jest w specyfikacji MCP oznaczony jako przestarzały.** Zastępujący go
  Streamable HTTP (`/mcp`) jest już dostępny i to on jest zalecany dla nowych integracji;
  `/mcp/sse` pozostaje obsługiwany dla istniejących konfiguracji. Podpowiedź konfiguracji
  w UI (Ustawienia → Klucze API) nadal wskazuje `/mcp/sse`.
- Rate limiter jest w pamięci procesu — sensowny dla pojedynczej instancji add-onu,
  przy wielu replikach wymagałby wspólnego magazynu.
