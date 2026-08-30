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

Autoryzacja działa na dwa sposoby — wybór zależy od tego, czy sam piszesz konfigurację klienta:

- **Klucz API** (nagłówek `X-MealPilot-Token`) — dla klientów z własnym plikiem konfiguracyjnym,
  czyli Claude Desktop i Claude Code. Gotowy fragment znajdziesz w **Ustawienia → Klucze API**.
- **OAuth 2.1** — dla zdalnych konektorów dodawanych w przeglądarce (claude.ai → Connectors),
  które dostają wyłącznie URL i nie mają jak wysłać własnego nagłówka. Klient sam wykrywa
  serwer autoryzacji, rejestruje się i przeprowadza logowanie; nic nie trzeba konfigurować
  poza podaniem adresu add-onu. Szczegóły niżej.

Endpointy MCP są limitowane liczbą żądań na adres IP.

### Podłączenie przez OAuth (claude.ai → Connectors)

Podaj adres serwera MCP (`https://twój-adres/mcp`). Klient odczyta metadane
z `/.well-known/oauth-protected-resource/mcp`, zarejestruje się sam, a Ciebie przekieruje
na ekran zgody MealPilota — tam logujesz się nazwą użytkownika i hasłem i zatwierdzasz dostęp.
Hasło jest wymagane nawet przy aktywnej sesji w przeglądarce: przepływ startuje strona trzecia,
a wydawany token żyje dłużej niż sesja.

Dwie rzeczy warto ustawić po stronie add-onu:

- **`public_url`** (Ustawienia → Dodatki → MealPilot → Konfiguracja) — publiczny adres add-onu,
  np. `https://mealpilot.example`, bez ukośnika na końcu. Za reverse proxy adres widziany przez
  aplikację nie jest tym, który wpisuje przeglądarka, a metadane OAuth muszą podawać ten drugi —
  inaczej przekierowanie po zgodzie trafia w próżnię. Puste pole = adres z samego żądania,
  co wystarcza tylko przy bezpośrednim dostępie po IP i porcie.
- **Cloudflare Access** — jeśli go używasz, ścieżki `/.well-known` i `/oauth` są domyślnie
  pominięte w `MEALPILOT_CF_ACCESS_BYPASS`. Konektor woła `/oauth/register` i `/oauth/token`
  serwer-do-serwera, więc nie ma jak dołożyć asercji CF Access; z włączonym checkiem cały
  przepływ kończyłby się 401.

Tokeny dostępu żyją godzinę, odświeżające 30 dni i są rotowane przy każdym użyciu. Zmiana hasła
oraz usunięcie konta unieważniają wszystkie wydane granty natychmiast.

Narzędzia (29) wykonują się w procesie add-onu, bezpośrednio na bazie, i respektują te same
uprawnienia co interfejs — w household widzisz to samo i możesz zmieniać to samo, co w UI.
Pełna specyfikacja: `AGENT_MCP_SPEC.md` w repozytorium.

Lokalny tryb stdio (`backend/mcp_server.py`) wymaga bezpośredniego dostępu do pliku bazy
(`MEALPILOT_DB`), więc działa tylko na hoście, na którym leży baza. Wewnątrz add-onu
wspieraną ścieżką jest HTTP (`/mcp` lub `/mcp/sse`).
