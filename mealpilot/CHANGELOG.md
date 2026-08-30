## 0.2.20
- Aktualizacje add-onu same doprowadzają bazę do właściwego kształtu — zmiany schematu są od teraz wersjonowane (Alembic), a przed każdą taką aktualizacją powstaje obok pliku bazy kopia `.pre-upgrade.bak`, do której można wrócić, gdyby coś poszło nie tak. Istniejące bazy przechodzą na nowy mechanizm same, przy pierwszym starcie, bez utraty danych
- Ustawienia asystenta nie trzymają już w bazie pustych pól „endpoint" i „klucz API" (i tak nieużywanych — jedno i drugie pochodzi z konfiguracji add-onu), a lista zakupów przestała blokować sytuację, w której dwoje domowników ma w tym samym tygodniu tę samą pozycję: jedną prywatną, drugą wspólną
- Jeden wspólny rejestr narzędzi dla agenta AI, serwera MCP i API — te same 29 narzędzi, te same opisy i te same uprawnienia w czacie, w Claude Desktop i w interfejsie
- Nowe narzędzia: wyszukiwanie przepisów po tytule/tagach/składnikach (`search_recipes`, odporne na wielkość liter i polskie znaki), notatki do przepisu, udostępnianie przepisu domownikom oraz pełna obsługa szablonów tygodnia (lista, zapis, zastosowanie, usunięcie). Agent w aplikacji dostał też ocenianie przepisów i szacowanie makro
- Pola meal prep (przepis na zapas, liczba dni, osobne kroki) są wreszcie dostępne dla agenta; doszły filtry po łącznym czasie przygotowania i po meal prepie
- Naprawa uprawnień w household: domownik bez prawa edycji nie zmieni już cudzego przepisu przez czat
- Naprawa gubienia współdzielenia: zastąpienie planu tygodnia lub przegenerowanie listy zakupów nie zamienia już wspólnych wpisów na prywatne; odhaczone pozycje listy przeżywają regenerację
- Plan tygodnia i lista zakupów pokazują w czacie dokładnie to samo, co ekran aplikacji (wcześniej w household potrafiły się różnić)
- Serwer MCP działa teraz w procesie add-onu, bez wewnętrznych zapytań HTTP — szybciej i bez drugiego modelu uprawnień. Błędy są zgłaszane jako prawdziwe błędy protokołu, a narzędzia niosą adnotacje (tylko odczyt / destrukcyjne / idempotentne), więc klient sam może zapytać o potwierdzenie
- Nowy transport MCP: Streamable HTTP pod `/mcp` (następca SSE w specyfikacji MCP) — zalecany dla nowych konfiguracji klientów. Dotychczasowy `/mcp/sse` działa dalej bez zmian, nie trzeba niczego przepinać
- Naprawa błędu: każda wiadomość MCP po SSE kończyła się w logach wyjątkiem „response already completed” — endpoint `/mcp/messages` odsyłał drugą odpowiedź HTTP po tej wysłanej już przez transport
- Klucze API mają zakres: **odczyt i zapis** albo **tylko odczyt**. Klucz tylko-do-odczytu nie zmieni danych ani przez API, ani przez MCP
- Endpointy MCP są limitowane liczbą żądań na adres IP, a wiadomość na `/mcp/messages` musi pochodzić od klucza, który otworzył daną sesję SSE
- **Bezpieczeństwo — usunięcie użytkownika kasuje wszystkie jego dane.** Wcześniej zostawały klucze API, ustawienia i rozmowy z agentem, a ponieważ SQLite potrafi nadać nowemu kontu id po skasowanym, świeżo założony użytkownik mógł odziedziczyć cudzy klucz API (czyli pełny dostęp do konta) i cudzą historię czatu. Przepisy współdzielone z domownikami przechodzą teraz na innego domownika zamiast znikać razem z autorem
- **Klucz API otwiera tylko dane, nie całe konto.** Klucz uwierzytelnia na ścieżkach przepisów, planu, zakupów, szablonów, ustawień i MCP — ale nie pozwala już tworzyć kolejnych kluczy, zmieniać hasła, zarządzać użytkownikami ani czytać rozmów z agentem; to zostaje wyłącznie dla zalogowanej sesji w przeglądarce
- **Naprawa: aplikacja nie działała między północą a 2:00.** Tydzień był wyliczany z pomieszaniem czasu lokalnego i UTC, więc w tych godzinach aplikacja prosiła serwer o niedzielę i dostawała błąd. Przy okazji poprawione podświetlanie „dziś" w planie i zakres dat w nagłówku tygodnia dla stref na zachód od UTC
- **Naprawa: ustawienia asystenta nie psują już jego instrukcji.** Ekran ustawień trzymał własną, nieaktualną kopię promptu systemowego i zapisywał ją do bazy przy każdym „Zapisz" — wystarczyło zmienić model, żeby na stałe przypiąć starą wersję (m.in. każącą agentowi szacować makro samodzielnie zamiast narzędziem). Prompt ma teraz jedno źródło w backendzie, a zapisywany jest tylko wtedy, gdy faktycznie go zmienisz
- **Lista zakupów jest wspólna dla domowników.** Pozycja wynikająca ze wspólnego planu trafia na wspólną listę, więc domownicy nie prowadzą już równoległych, prywatnych kopii tych samych zakupów. Odhaczyć zakup może każdy domownik, także bez prawa edycji (dopisywanie i usuwanie pozycji nadal go wymaga)
- **Naprawa: ręcznie dopisana ilość znikała po przeliczeniu listy.** Dopisanie do pozycji pochodzącej z planu było kasowane przy najbliższym generowaniu; teraz taka pozycja przestaje być przeliczana i dopisek zostaje. Naprawiony też błąd 500 przy generowaniu listy, gdy ręczna pozycja miała tę samą nazwę i jednostkę co składnik z planu
- **Naprawa: kasowanie rozmowy z asystentem faktycznie ją kasuje** — wcześniej wiadomości i wywołania narzędzi zostawały w bazie na zawsze, bo klucze obce nie były w SQLite egzekwowane. Włączony też tryb WAL i czekanie na zajętą bazę, co usuwa błędy „database is locked" przy równoległych zapisach
- **Naprawa: przerwana rozmowa nie zostawia już otwartego połączenia do bazy** (po kilkunastu przerwanych streamach serwer potrafił się zawiesić), a pojedyncza uszkodzona ramka odpowiedzi nie ucina już wiadomości asystenta — błędy zgłoszone przez API modelu są pokazywane zamiast cichego „(brak odpowiedzi)"
- Wywołania narzędzi (czat i MCP) nie blokują już całego serwera na czas zapytania do bazy; kopia bazy sprzed migracji id przepisów jest robiona tak, by w trybie WAL była kompletna; preflight CORS przechodzi przy włączonym Cloudflare Access; pierwsze uruchomienie z ustawionym `MEALPILOT_SETUP_TOKEN` da się dokończyć z poziomu UI
- **Id przepisu to teraz liczba, nie slug z tytułu.** Wcześniej id było wyprowadzane z tytułu, więc po zmianie nazwy przepisu przestawało do niego pasować — a poprawić się go nie dało bez zerwania powiązań z planami, listami zakupów, ocenami i notatkami. Przy pierwszym starcie add-on przenumerowuje przepisy i przepina wszystkie powiązania (także szablony tygodnia i ulubione), a kopię bazy sprzed migracji zostawia obok pliku bazy jako `.pre-int-ids.bak`. Integracje MCP/API muszą przekazywać `recipe_id` jako liczbę z odpowiedzi narzędzi — stare, zapisane na sztywno slugi przestaną działać
- **Zmiany łamiące dla integracji po MCP/agencie**: `list_recipes` i `filter_recipes` zwracają teraz stronicowane skróty (`{items, total, limit, offset, has_more}`) zamiast całej biblioteki; `get_shopping_list` i `get_week_nutrition_summary` zwracają obiekt z `week_start` zamiast gołej listy/mapy dni; `create_recipe` nie przyjmuje już `id` (nadaje je serwer); `week_start` musi być poniedziałkiem — inny dzień to teraz czytelny błąd z podpowiedzią właściwej daty zamiast pustej odpowiedzi. Narzędzie usuwania pozycji zakupów nazywa się `delete_shopping_item` (stara nazwa `remove_shopping_item` nadal działa)

## 0.2.19
- Porządki w kodzie backendu — konfiguracja i wdrożenie ruff (formatowanie, sortowanie importów, nowoczesne type hinty), naprawa wykrytych ostrzeżeń lintera; bez zmian funkcjonalnych

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