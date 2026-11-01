# Redesign Budget Buddy: raport końcowy

Gałąź `redesign/ui-foundation`. Logika budżetu, API, model danych, migracje i importery bez zmian w commitach redesignu. Decyzje poza briefem opisuje [DESIGN_DECISIONS.md](DESIGN_DECISIONS.md).

## Kryteria akceptacji

| Kryterium | Stan | Dowód lub zastrzeżenie |
|---|---|---|
| Brak kolorów hex/rgb poza tokenami | Spełnione z wyjątkiem | grep czysty poza `TagColorPicker` (tory suwaków widma) i komentarzem w `tagColors.ts` |
| Brak fontu monospace | Spełnione | jedyne trafienie to mapowanie zmiennej Tablera na Inter |
| Brak angielskich etykiet | Częściowo | grep po znanych terminach czysty, ale to nie pełny przegląd. Przycisk wyboru pliku („Choose file”) pochodzi z przeglądarki |
| Ucięty tekst ma tooltip | Częściowo | Budżet, Start, Transakcje, Konta tak. Na telefonie w Budżecie nazwy się zawijają. Pozostałe listy mają natywne `title` w części komórek, bez audytu każdej |
| Płynne przewijanie ~3400 wierszy | Spełnione na danych podstawionych | 3400 wygenerowanych wierszy w przeglądarce: p50 16,7 ms, p95 17,6 ms, maks. 49,9 ms na klatkę, 27 wierszy w DOM. Nie mierzono na prawdziwych danych |
| Edycja „Przypisane” z klawiatury | Spełnione | Enter, Esc, Tab i Shift+Tab sprawdzone w przeglądarce |
| Kontrast tekstu ≥ 4,5:1 | Z wyjątkiem | `--text-faint` ma 2,3–2,7:1 (świadomy wyjątek dla zer i placeholderów). Sprawdzono pary tokenów na powierzchniach, nie każdy ekran |
| Zrzuty każdego ekranu | Częściowo | patrz niżej |
| Logika, API, importery bez zmian; testy przechodzą | Bez testów | repo nie ma żadnych testów. Build i TypeScript przechodzą |
| `DESIGN_DECISIONS.md` | Spełnione | |

## Czego brief wymagał, a nie jest zrobione

- **Ekrany przebudowane tylko stylami globalnymi:** Tagi, Paragony, Panel użytkownika, szczegóły transakcji, kolejka do otagowania. Mają nowe kolory i typografię, ale dawny układ. Ekran powitalny i logowanie nie były oglądane.
- **Szerokości kolumn zapamiętywane per ekran** działają tylko w Transakcjach.
- **„Pusty stan” i szkielety** są na przebudowanych ekranach; Przedmioty pokazują przy pustej liście komunikat o filtrze.
- **Dziennik: „Dziś/Wczoraj” i sumy dnia** liczone są z wczytanych stron.
- **„Cofnij” w toastach** nie istnieje.
- **Martwy kod:** `useAccountsEditing.tsx` wciąż zawiera nieużywane renderery komórek, a `tokens.css` sekcję aliasów, z której korzystają nieprzerobione ekrany.
- **Struktura wydatków na liście transakcji kategorii** w Raportach ma nadal dwie kolumny (Wydatek, Wpływ).
- **Stan „Napraw” w Budżecie** nie był oglądany, bo dane demo mają dodatnie „Do przypisania”. Akcje zbiorcze w Transakcjach sprawdzono do pojawienia się paska, bez zapisu. Przejścia kreatora importu nie sprawdzono na prawdziwym pliku CSV.

## Zrzuty

Paczka `redesign-screens.zip`: ciemny motyw 1440 px dla wszystkich głównych ekranów oraz część wariantów jasnych i 390 px. Komplet 4 wariantów dla każdego ekranu nie powstał. Poziome przewijanie przy 390 px sprawdzono skryptem na 10 ekranach; po poprawce w Paragonach trzeba je potwierdzić przy następnym uruchomieniu. Zrzuty pochodzą z danych demo (17 transakcji), a obrazy 1440 px mają 800 px szerokości.

## Problemy z danymi (nienaprawiane)

Zgłoszone w briefie, niezweryfikowane na prawdziwych danych, bo baza deweloperska ma tylko dane demo:

- Duplikaty sklepów („Action” ×2, „13 Balice Manual” ×2).
- W strukturze wydatków „Przychód (Do przypisania)” liczy się jako wydatek.
- Jednorazowy wydatek 86 000 zł zawyża kategorię Czynsz do 91%.

## Ważne przy wdrożeniu

- **Zmiany backendu właściciela nie są w tych commitach:** `services/home.py` i `helpers.py` (limit 8 ostatnich transakcji, ukrycie korekt salda) leżą w drzewie roboczym. Zmiany właściciela w `HomePage.tsx` i `home.css` są za to wchłonięte w commit Startu.
- **`Dockerfile.dev`, `docker-compose.dev.yml`, `CHANGELOG.md`, `config.yaml`** pozostają niezacommitowane.
