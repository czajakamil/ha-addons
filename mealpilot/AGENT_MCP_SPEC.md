# MealPilot — specyfikacja serwera MCP dla agenta AI

## Kontekst projektu

Aplikacja **MealPilot** do planowania posiłków. Stack:

- **Backend**: FastAPI (Python), SQLite przez SQLAlchemy, sesje cookie (`mealpilot_session`)
- **Frontend**: React + TypeScript (Vite)
- **Auth**: sesja po stronie serwera — każde żądanie musi nieść ciasteczko sesji; zależność `get_current_user` wstrzykuje obiekt `User` do każdego endpointu

### Modele bazy danych

```python
class User:
    id: int (PK)
    username: str (unique)
    password_hash: str
    role: str  # "admin" | "user"
    is_active: int

class Recipe:
    id: str (PK)          # slug czytelny dla ludzi, np. "kurczak-z-ryzem"
    user_id: int (FK)
    title: str
    tags: JSON[str]       # np. ["azjatycki", "zdrowe", "bez-glutenu"]
    meal_types: JSON[str] # np. ["śniadanie", "obiad", "kolacja"]
    servings: int
    prep_time: int        # minuty
    cook_time: int        # minuty
    kcal: float
    p: float              # białko [g]
    f: float              # tłuszcze [g]
    c: float              # węglowodany [g]
    hue: int              # kolor karty w UI (0-360)
    ingredients: JSON     # [{name, qty, unit}]
    steps: JSON[str]
    image_filename: str | null

class MealPlanEntry:
    id: int (PK, autoincrement)
    user_id: int (FK)
    week_start: str       # format "YYYY-MM-DD" (poniedziałek)
    day: int              # 0=poniedziałek … 6=niedziela
    meal: str             # "śniadanie" | "obiad" | "kolacja" | dowolna wartość
    recipe_id: str (FK)
    servings: int

class ShoppingItem:
    id: int (PK, autoincrement)
    user_id: int (FK)
    week_start: str
    name: str
    qty: float
    unit: str
    category: str         # np. "Mięso, ryby, białko" | "Nabiał" | "Warzywa i owoce" | "Suche i zboża" | "Tłuszcze i przyprawy" | "Spiżarnia" | "Inne"
    checked: int          # 0 | 1
```

### Istniejące endpointy REST

```
GET    /api/recipes                  → lista przepisów usera
GET    /api/recipes/{id}             → szczegóły przepisu
POST   /api/recipes                  → utwórz przepis
PUT    /api/recipes/{id}             → zaktualizuj przepis
DELETE /api/recipes/{id}             → usuń przepis
POST   /api/recipes/{id}/image       → upload zdjęcia
DELETE /api/recipes/{id}/image       → usuń zdjęcie

GET    /api/plan/{week_start}        → plan tygodnia
PUT    /api/plan/{week_start}        → zastąp cały plan (tablica PlanEntry)

GET    /healthz
POST   /api/auth/login
POST   /api/auth/logout
GET    /api/auth/me
```

> **Uwaga**: Endpointy listy zakupów (`/api/shopping/…`) **jeszcze nie istnieją** — model `ShoppingItem` jest w bazie, ale brakuje routera. ShoppingScreen w obecnej wersji agreguje składniki lokalnie z danych w pamięci.

---

## Cel zadania

Zaimplementuj:

1. **Brakujące endpointy REST** potrzebne agentowi (opisane poniżej)
2. **Serwer MCP** (`mcp_server.py`) eksponujący narzędzia agenta jako tools MCP
3. Serwer MCP ma działać jako **stdio server** (standardowy transport dla MCP)

---

## Brakujące endpointy REST do dodania

### A. Filtrowanie przepisów

```
GET /api/recipes?tags=azjatycki,kolacja&meal_types=obiad&max_kcal=700&min_protein=30
```

Parametry query (wszystkie opcjonalne):
- `tags: str` — lista tagów oddzielona przecinkami; przepis musi mieć **wszystkie** podane tagi
- `meal_types: str` — lista typów posiłku oddzielona przecinkami; przepis musi pasować do **co najmniej jednego**
- `max_kcal: float`
- `min_protein: float` — minimalne białko (pole `p`) na porcję

### B. Metadane słownikowe

```
GET /api/recipes/meta/tags        → {"tags": ["azjatycki", "vege", ...]}  (unikalne, posortowane)
GET /api/recipes/meta/meal_types  → {"meal_types": ["śniadanie", "obiad", ...]}
```

### C. Lista zakupów (nowy router `/api/shopping`)

```
GET    /api/shopping/{week_start}             → lista pozycji (ShoppingItem[])
POST   /api/shopping/{week_start}/generate    → agreguje składniki z planu → zapisuje/nadpisuje listę
PATCH  /api/shopping/{week_start}/items/{id}  → {"checked": true/false}
DELETE /api/shopping/{week_start}             → czyści całą listę
```

**Logika `generate`**: Dla każdego `MealPlanEntry` z danego tygodnia pobierz przepis, przemnóż składniki przez `(entry.servings / recipe.servings)`, zsumuj po `(name.lower(), unit)`. Jeśli `unit` to `g` i `kg` dla tej samej nazwy — przelicz do `g`. Przypisz kategorię przez słownik słów kluczowych (taki sam jak w `ShoppingScreen.tsx` w frontendzie). Wstaw do `shopping_items` z `ON CONFLICT ... DO UPDATE`.

---

## Specyfikacja narzędzi MCP

Serwer MCP należy napisać używając biblioteki `mcp` (PyPI: `mcp`).  
Każde narzędzie wykonuje request HTTP do lokalnego backendu FastAPI (`http://localhost:8000`) z sesją cookie.  
Serwer MCP musi przyjąć jako argument (env var `MEALPILOT_SESSION_COOKIE`) wartość ciasteczka sesji do wstrzyknięcia w każdy request.

### Grupa 1 — Przepisy

#### `list_recipes`
- Opis: Zwraca wszystkie przepisy zalogowanego użytkownika.
- Parametry: brak
- HTTP: `GET /api/recipes`
- Zwraca: lista obiektów `Recipe` (id, title, tags, meal_types, kcal, p, f, c, servings, prep_time, cook_time, ingredients)

#### `get_recipe`
- Opis: Szczegóły jednego przepisu łącznie ze składnikami i krokami.
- Parametry: `recipe_id: string`
- HTTP: `GET /api/recipes/{recipe_id}`

#### `list_tags`
- Opis: Wszystkie unikalne tagi używane w bibliotece przepisów. Wywołaj jako pierwsze, by wiedzieć jakimi wartościami operuje użytkownik.
- Parametry: brak
- HTTP: `GET /api/recipes/meta/tags`
- Zwraca: `{tags: string[]}`

#### `list_meal_types`
- Opis: Wszystkie unikalne typy posiłków zdefiniowane w przepisach.
- Parametry: brak
- HTTP: `GET /api/recipes/meta/meal_types`
- Zwraca: `{meal_types: string[]}`

#### `filter_recipes`
- Opis: Zwraca przepisy spełniające podane kryteria. Użyj gdy użytkownik chce np. "coś azjatyckiego na obiad" lub "lekkie śniadanie poniżej 400 kcal".
- Parametry (wszystkie opcjonalne):
  - `tags: string[]` — przepis musi zawierać wszystkie podane tagi
  - `meal_types: string[]` — przepis musi pasować do co najmniej jednego
  - `max_kcal: number`
  - `min_protein: number` — minimalne białko na porcję [g]
- HTTP: `GET /api/recipes?tags=...&meal_types=...&max_kcal=...&min_protein=...`

#### `create_recipe`
- Opis: Dodaje nowy przepis do biblioteki użytkownika.
- Parametry: `id, title, tags, meal_types, servings, prep_time, cook_time, kcal, p, f, c, hue, ingredients [{name, qty, unit}], steps`
- HTTP: `POST /api/recipes`

#### `update_recipe`
- Opis: Aktualizuje wybrane pola istniejącego przepisu.
- Parametry: `recipe_id` + dowolny podzbiór pól (title, tags, meal_types, kcal, p, f, c, ingredients, steps…)
- HTTP: `PUT /api/recipes/{recipe_id}`

#### `delete_recipe`
- Opis: Usuwa przepis. Usuwa też powiązane wpisy w planie tygodnia.
- Parametry: `recipe_id: string`
- HTTP: `DELETE /api/recipes/{recipe_id}`

---

### Grupa 2 — Plan tygodnia

#### `get_week_plan`
- Opis: Zwraca plan posiłków na dany tydzień wraz z rozwinięciem nazw przepisów (dołącz title każdego recipe_id).
- Parametry: `week_start: string` (format `YYYY-MM-DD`, musi być poniedziałek)
- HTTP: `GET /api/plan/{week_start}` + enrichment przez `GET /api/recipes`

#### `get_current_week_plan`
- Opis: Skrót — plan na bieżący tydzień (oblicz `week_start` sam na podstawie dzisiejszej daty).
- Parametry: brak
- HTTP: jak wyżej z automatycznie obliczonym `week_start`

#### `set_week_plan`
- Opis: Zastępuje cały plan tygodnia. **Uwaga**: zawsze pokaż podgląd użytkownikowi i poczekaj na potwierdzenie przed wywołaniem tego narzędzia.
- Parametry:
  - `week_start: string`
  - `entries: [{day: 0-6, meal: string, recipe_id: string, servings: int}]`
- HTTP: `PUT /api/plan/{week_start}`

#### `add_plan_entry`
- Opis: Dodaje jeden slot do planu bez kasowania reszty. Najpierw pobiera aktualny plan, dodaje wpis, wysyła całość.
- Parametry: `week_start, day: 0-6, meal: string, recipe_id: string, servings: int`
- HTTP: `GET` + `PUT /api/plan/{week_start}`

#### `remove_plan_entry`
- Opis: Usuwa konkretny slot (dzień + posiłek) z planu.
- Parametry: `week_start, day: 0-6, meal: string`
- HTTP: `GET` + `PUT /api/plan/{week_start}` (z filtrowaniem)

#### `get_week_nutrition_summary`
- Opis: Oblicza sumę kcal, białka, tłuszczu i węglowodanów dla każdego dnia tygodnia na podstawie planu.
- Parametry: `week_start: string`
- Logika: pobierz plan + przepisy, przelicz `wartość * (entry.servings / recipe.servings)`, pogrupuj po `day`
- Zwraca: `{0: {kcal, p, f, c}, 1: {...}, ...}`

---

### Grupa 3 — Lista zakupów

#### `get_shopping_list`
- Opis: Zwraca aktualną listę zakupów na dany tydzień.
- Parametry: `week_start: string`
- HTTP: `GET /api/shopping/{week_start}`

#### `generate_shopping_list`
- Opis: Generuje listę zakupów z planu tygodnia (agreguje składniki wszystkich przepisów). Nadpisuje poprzednią listę.
- Parametry: `week_start: string`
- HTTP: `POST /api/shopping/{week_start}/generate`

#### `check_shopping_item`
- Opis: Oznacza pozycję jako kupioną lub odznacza.
- Parametry: `week_start: string, item_id: int, checked: boolean`
- HTTP: `PATCH /api/shopping/{week_start}/items/{item_id}`

#### `clear_shopping_list`
- Opis: Usuwa wszystkie pozycje z listy zakupów danego tygodnia.
- Parametry: `week_start: string`
- HTTP: `DELETE /api/shopping/{week_start}`

---

## System prompt agenta

```
Jesteś agentem MealPilot — pomagasz użytkownikowi planować posiłki na tydzień.

Zasady:
1. Zanim zaproponujesz plan, wywołaj list_tags i list_meal_types, żeby znać dostępne wartości.
2. Zanim cokolwiek dostosujesz, sprawdź get_current_week_plan — nie nadpisuj tego co już jest, chyba że user tego chce.
3. Przed wywołaniem set_week_plan zawsze pokaż użytkownikowi propozycję i czekaj na potwierdzenie.
4. Przy planowaniu uwzględniaj różnorodność — nie powtarzaj tego samego przepisu więcej niż 3 razy w tygodniu.
5. Jeśli user pyta o kalorie/makra, użyj get_week_nutrition_summary.
6. Odpowiadaj po polsku. Bądź konkretny i zwięzły.
```

---

## Struktura plików do stworzenia

```
backend/
  app/
    routers/
      recipes.py        ← dodaj filtry + /meta/tags + /meta/meal_types
      plan.py           ← bez zmian
      shopping.py       ← NOWY router (cały CRUD listy zakupów + generate)
    schemas.py          ← dodaj ShoppingItemOut, ShoppingListOut
    main.py             ← zarejestruj shopping.router
  mcp_server.py         ← NOWY serwer MCP (stdio)
  requirements.txt      ← dodaj: mcp>=1.0, httpx
```

---

## Wymagania techniczne

- `mcp_server.py` uruchamiany przez `python mcp_server.py`; transport: **stdio**
- Do requestów HTTP użyj `httpx` (async) z `httpx.AsyncClient`
- Sesja przekazywana przez env var `MEALPILOT_SESSION_COOKIE` — wstrzykiwana jako nagłówek `Cookie: mealpilot_session=<wartość>`
- Url backendu przez env var `MEALPILOT_BASE_URL` (domyślnie `http://localhost:8000`)
- Każde narzędzie MCP zwraca `TextContent` z JSON lub czytelnym komunikatem błędu
- Błędy HTTP (4xx, 5xx) zwracaj jako `isError=True` z treścią odpowiedzi
