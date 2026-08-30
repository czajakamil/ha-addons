# Testowanie MealPilot

Strategia testów przed wypuszczeniem kolejnych wersji add-onu. Dwie warstwy:

| Warstwa | Narzędzie | Lokalizacja | Co pokrywa |
|---|---|---|---|
| **API E2E + unit** | `pytest` + Starlette `TestClient` na realnym SQLite | `backend/tests/` | auth, ownership, warstwa narzędzi (`app/services/`), plan/zakupy/szablony, agent, MCP, admin, migracje |
| **Browser smoke** | `Playwright` | `frontend/e2e/` | że UI się renderuje i kluczowe ekrany działają end-to-end |

LLM jest **zawsze mockowany** — testy nie wykonują płatnych wywołań do API modeli.

---

## Backend — pytest

```bash
cd mealpilot/backend
# venv projektu (lub dowolny python 3.12 z requirements-test.txt)
./env/bin/python3.12 -m pytest                 # całość
./env/bin/python3.12 -m pytest -m smoke        # tylko release-blockery
./env/bin/python3.12 -m pytest -m unit         # tylko jednostkowe
./env/bin/python3.12 -m pytest tests/integration/test_auth.py -v
```

Instalacja zależności w świeżym środowisku:

```bash
pip install -r backend/requirements-test.txt
```

### Izolacja
- Każdy test dostaje **świeży schemat bazy** (`conftest.py::clean_state`) w pliku tymczasowym
  (`MEALPILOT_DB` ustawiany przed importem aplikacji).
- Rate limiter i katalog obrazów są czyszczone między testami.
- Fixtury: `client` (bez logowania), `admin_client` (po `/setup`), `make_user(...)`
  (tworzy usera przez API admina i zwraca zalogowanego klienta), `new_client` (osobna sesja),
  `db_session` (bezpośredni dostęp ORM).

### Co jest pokryte
- **Smoke / release-blockery** (`-m smoke`): healthz, setup, login, recipe CRUD, plan→zakupy,
  migracja w miejscu, migracje Alembica.
- **Migracje** (`test_migrations.py`, `test_alembic.py`): stara baza podnosi się bez utraty danych
  (ręczne ALTER TABLE + przenumerowanie id przepisów), a Alembic ma test antydryfowy —
  `upgrade head` na pustej bazie musi dać schemat identyczny z `Base.metadata`. Pokryte też trzy
  ścieżki startu (pusta baza / adopcja bazy sprzed Alembica / baza już ostemplowana) i `downgrade`.
- **Auth**: setup raz, walidacja hasła/loginu, błędne logowanie, rate-limit (429),
  rotacja hasła unieważnia sesje + kasuje klucze API, klucze API (auth nagłówkiem, odwołanie).
- **Recipes**: walidacja, wyszukiwanie tekstowe i filtry (tagi, makro, czas, meal prep, oceny),
  oceny (agregacja + filtry), notatki, pola meal prep, upload/usuwanie obrazka, meta tagów.
- **Ownership (multi-user)**: prywatne niewidoczne dla innych, udostępnianie do household,
  uprawnienia edycji (`can_edit`, twórca), re-pin tylko przez twórcę.
- **Plan / Zakupy / Szablony**: walidacja `week_start` (musi być poniedziałkiem) i nieznanych
  przepisów, idempotentny PUT, zachowanie właściciela wierszy przy zastępowaniu planu i regeneracji
  listy, konsolidacja i normalizacja jednostek (kg→g), trwałość odhaczeń, pozycje custom,
  szablony (apply, pomijanie usuniętych).
- **Agent AI**: status/limit zużycia, blokada quoty (403/429), CRUD konwersacji, `run` (mock),
  streaming SSE (mock providera).
- **Warstwa narzędzi**: rejestr jest jedynym źródłem prawdy (brak drugiej listy narzędzi),
  spójność nazw/schematów/flag, dispatch narzędzi i mapowanie błędów domenowych.
- **MCP**: bramka autoryzacji (401), zakres klucza API (`read` odrzuca narzędzia zapisujące),
  powiązanie POST `/mcp/messages` z właścicielem sesji SSE, limity liczby żądań,
  regresja naive-datetime klucza API.
- **Cloudflare Access**: brak/zły/poprawny token, ścieżki bypass, walidacja konfiguracji.
- **Migracje**: stara baza podnosi się na nowym schemacie bez utraty danych; idempotencja.

---

## Frontend — Playwright (smoke)

Wymaga **działającej instancji** (backend serwuje też zbudowany frontend pod tym samym originem):

**Samowystarczalny** — nie trzeba uruchamiać `docker compose` ani znać hasła. Playwright sam
startuje izolowaną instancję (świeża baza, admin `e2e_admin`, zbudowany frontend) na porcie 8765,
przez skrypt [`e2e/run-server.sh`](frontend/e2e/run-server.sh). Wymaga venva backendu
(`backend/env/bin/python3.12`) — tego samego, którym uruchamiasz pytest.

```bash
cd frontend
npm install
npm run test:e2e:install      # jednorazowo: pobierz przeglądarkę Chromium
npm run test:e2e              # buduje front, startuje backend, odpala testy
```

Zmienne (opcjonalne):
- `E2E_BASE_URL` — testuj zewnętrzną instancję zamiast startować własną (wtedy podaj `E2E_USERNAME`/`E2E_PASSWORD`).
- `E2E_SKIP_BUILD=1` — pomiń `vite build` (gdy `dist/` jest aktualny).
- `E2E_PORT` — zmień port (domyślnie 8765).

Pokrycie smoke: logowanie, nawigacja między ekranami, lista przepisów + otwarcie szczegółów,
brak twardych błędów JS przy starcie.

---

## Checklista przed tagiem wersji

- [ ] `pytest` zielony (`-m smoke` to minimum, najlepiej całość)
- [ ] `npm run test:e2e` zielony (zawiera też `vite build`)
- [ ] `version` w `config.yaml` == nowy wpis w `CHANGELOG.md`
- [ ] obraz buduje się dla `amd64` i `aarch64`
- [ ] ręczny test upgrade'u: baza z poprzedniej wersji startuje na nowym obrazie
