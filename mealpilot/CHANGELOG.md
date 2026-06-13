## 0.2.18
- Aktualizacja konfiguracji MCP w ustawieniach kluczy API — przejście z trybu natywnego URL/headers na `mcp-remote` przez `npx` dla lepszej kompatybilności z Claude Desktop
- Klucz API przekazywany przez zmienną środowiskową `MEALPILOT_TOKEN` zamiast nagłówka HTTP

## 0.2.17
- Kompletna pokrycia testami backend — testy jednostkowe (security, ownership, rate limiting, AI usage) i integracyjne (auth, recipes, admin, agent, MCP, plan/shopping, templates, CF Access, migracje)
- Testy e2e frontendu z Playwright — smoke testy UI (logowanie, lista przepisów, plan tygodnia, lista zakupów, ustawienia)

## 0.2.16
- Naprawa błędu 500 przy walidacji klucza API — SQLite zwraca naive datetime, teraz normalizowany do UTC przed porównaniem
- Aktualizacja instrukcji podłączania MCP w ustawieniach kluczy API — zmiana z trybu stdio na HTTP/SSE (`/mcp/sse`)

## 0.2.15
- Bug fix - Dockerfile

## 0.2.14
- Serwer MCP dostępny przez HTTP/SSE bezpośrednio z add-onu — endpoint `GET /mcp/sse`
- Integracja z Claude Desktop bez instalowania czegokolwiek lokalnie — wystarczy adres HA i klucz API
- Autoryzacja MCP przez nagłówek `X-MealPilot-Token`

## 0.2.13
- Wsparcie dla meal prep — oznaczanie przepisów jako nadających się do batch-cooking
- Nowe pola przepisu: liczba dni (batch na ile dni) i osobne kroki meal prep
- Zakładki „Gotowanie" / „Meal prep" w edytorze kroków oraz w widoku szczegółów przepisu
- Tryb gotowania dla meal prep — składniki i porcje skalowane automatycznie przez liczbę dni
- Odznaka „Meal prep" widoczna na karcie przepisu na liście

## 0.2.12
- Naprawa błędu: dodanie przepisu do planu tygodnia nie było zapisywane — zmiany znikały po odświeżeniu strony

## 0.2.11
- Streaming odpowiedzi agenta AI — tekst pojawia się na żywo, a wywołania narzędzi są widoczne w trakcie generowania
- Nowy tryb gotowania — ekran krok-po-kroku z timerami na każdy etap i sygnałem dźwiękowym po zakończeniu odliczania

## 0.2.10
- Przeciąganie składników i kroków (drag & drop + touch) w edytorze przepisów
- Responsywny pasek akcji na detalu przepisu — ikony zamiast tekstu na małych ekranach
- Responsywna siatka makroskładników w edycji (2 kolumny na mobile)
- Nowa ikona `trash` dla przycisku usuwania przepisu

## 0.2.9
- Ocenianie przepisów (1–5 gwiazdek) — każdy użytkownik może wystawić swoją ocenę
- Notatki do przepisów — możliwość dodania prywatnej notatki do każdego przepisu
- Widoczność ocen na liście przepisów (średnia, liczba ocen, własna ocena)
- Filtrowanie przepisów po minimalnej własnej ocenie lub średniej ocenie
- Agent AI uwzględnia oceny przy rekomendowaniu i planowaniu posiłków
- Nowe narzędzie agenta: `rate_recipe` do wystawiania i usuwania ocen

## 0.2.8
- Naprawa błędu odświezania strony po kliknięciu przycisku "Biezacy tydzien"
- Naprawa błędu z estymacją makroskładników przez AI