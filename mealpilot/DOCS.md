# MealPilot

Add-on serwuje aplikację MealPilot (FastAPI + React) na porcie 8000, dostępną przez ingress
Home Assistanta (panel boczny "MealPilot") oraz bezpośrednio na `http://<host>:8000`.

## Opcje

- `cors_origins` – lista originów CORS oddzielona przecinkami (`*` = wszystkie). Pod ingress można zostawić `*`.
- `require_cf_access` – wymuszanie nagłówka Cloudflare Access JWT (zostaw `false` dla użytku lokalnego/ingress).
- `cookie_secure` – ustawia ciasteczko sesyjne jako Secure (włącz, jeśli wystawiasz aplikację po HTTPS).

## Trwałe dane

Baza SQLite oraz obrazki cache są zapisywane w `/data` (montowane przez Home Assistanta).

## Klucze API

W aplikacji, w **Ustawienia → Klucze API**, każdy użytkownik może wygenerować długo żyjący
klucz (nagłówek `X-MealPilot-Token`) do automatyzacji i integracji zewnętrznych.
Klucz ma jeden z dwóch zakresów:

- **Odczyt i zapis** (`write`, domyślny) — pełny dostęp do danych użytkownika.
- **Tylko odczyt** (`read`) — API odrzuca nim każdą metodę HTTP inną niż `GET`/`HEAD`/`OPTIONS`,
  a serwer MCP odrzuca każde narzędzie zapisujące. Dobry wybór dla dashboardów i automatyzacji,
  które mają wyłącznie czytać.

Wartość klucza pokazywana jest tylko raz, zaraz po wygenerowaniu.

## Serwer MCP

Add-on wystawia serwer MCP po HTTP — można podłączyć klienta (np. Claude Desktop)
bez instalowania czegokolwiek lokalnie. Dostępne są dwa transporty:

- **`/mcp`** (Streamable HTTP) — zalecany dla nowych konfiguracji.
- **`GET /mcp/sse`** — starszy transport, nadal w pełni obsługiwany; istniejące konfiguracje
  klientów działają bez zmian. Gotowy fragment konfiguracji znajdziesz w aplikacji,
  w sekcji **Ustawienia → Klucze API**.

Autoryzacja odbywa się nagłówkiem `X-MealPilot-Token` z wygenerowanym kluczem;
endpointy MCP są limitowane liczbą żądań na adres IP.

Narzędzia (29) wykonują się w procesie add-onu, bezpośrednio na bazie, i respektują te same
uprawnienia co interfejs — w household widzisz to samo i możesz zmieniać to samo, co w UI.
Pełna specyfikacja: `AGENT_MCP_SPEC.md` w repozytorium.

Lokalny tryb stdio (`backend/mcp_server.py`) wymaga bezpośredniego dostępu do pliku bazy
(`MEALPILOT_DB`), więc działa tylko na hoście, na którym leży baza. Wewnątrz add-onu
wspieraną ścieżką jest HTTP (`/mcp` lub `/mcp/sse`).
