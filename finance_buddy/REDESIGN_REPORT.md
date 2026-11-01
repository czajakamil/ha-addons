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
| Zrzuty każdego ekranu | Spełnione z zastrzeżeniem | patrz niżej |
| Logika, API, importery bez zmian; testy przechodzą | Patrz niżej | backend nietknięty w tej rundzie. 8 testów jednostkowych, TypeScript i build przechodzą, lint bez błędów (same ostrzeżenia) |
| `DESIGN_DECISIONS.md` | Spełnione | |

## Czego brief wymagał, a nie jest zrobione

- **Panel użytkownika** ma nadal dawny układ, tylko z nowymi stylami. Nie był częścią tej rundy.
- **„Cofnij” w toastach** nie istnieje.
- **Dziennik: „Dziś/Wczoraj” i sumy dnia** liczone są z wczytanych stron.
- **Struktura wydatków na liście transakcji kategorii** w Raportach ma nadal dwie kolumny (Wydatek, Wpływ), więc nie jest spójna z nowym dialogiem.
- **Edytor paragonu** ma tabelę pozycji szerszą niż jej kolumna przy 1440 px (pole produktu ucięte, trzeba przewijać). Układ nie był w tej rundzie zmieniany.

## Sprawdzone w tej rundzie

Skrypt w prawdziwej przeglądarce na backendzie deweloperskim:

- **Akcje zbiorcze:** zapisy podstawione, baza nietknięta. Z 5 transakcji 2 z błędem 500: „udało się 3 z 5”, obie na liście, zaznaczone zostają 2. Ponowienie wysłało tylko 2 żądania, następne tylko 1.
- **Dialog:** zapis podstawiony. „50,5” po prefiksie daje −5050 groszy, „120” daje +12000, sam „-” jest odrzucany.
- **Szerokości:** przeciągnięcie, zapis i powrót po przeładowaniu na Budżecie, Kontach, Sklepach (kolumna elastyczna), Transakcjach i kolejce. Pozostałych tabel nie przeciągano; korzystają z tego samego kodu, a Przedmioty, Paragony i Struktura wydatków nie miały w danych demo tabeli do testu. Testowe zapisy wyczyszczone.
- **Tagi:** rozwijanie i wejście w edycję nazwy.

Nie sprawdzono:

- **Prawdziwych niepowodzeń serwera**: błędy akcji zbiorczych były podstawione.
- **Wgrywania paragonu** od wyboru pliku po OCR, zmiany koloru i usuwania tagu, dodawania pozycji i załącznika po przebudowie: kod zapisu jest niezmieniony, ale nowego układu nie przeklikano do końca.
- **Logowania prawdziwym passkey i tworzenia pierwszego konta:** tylko wygląd, stany wymuszone podstawioną odpowiedzią API.
- **Stanu „Napraw” w Budżecie** i kreatora importu na prawdziwym CSV, jak wcześniej.

## Zrzuty

Katalog `redesign-screens/` (poza gitem) i paczka `redesign-screens.zip`: 28 widoków × 4 warianty (ciemny/jasny × 1440/390) = 112 plików w pełnej rozdzielczości, szerokość obrazu równa szerokości okna, wysokość do końca strony (Transakcje są wirtualizowane, więc ekran pokazuje wczytany fragment).

Dane to baza demo (17 transakcji). **Odpowiedzi API podstawione tylko do zrzutów:** `paragony`, `paragon-edytor`, `przedmioty`, `raporty-przeplywy-deficyt`, `logowanie`, `logowanie-pierwsze`, `powitanie`, bo baza demo nie ma tych stanów. Wersje `-pusto` pokazują prawdziwe dane.

Skrypt przy każdym zrzucie mierzył poziome przewijanie strony: zero na wszystkich 112, bez błędów 404/5xx. Mierzy całą stronę, nie wewnętrzne kontenery, a całość obejrzano na arkuszach dwóch wariantów (ciemny 1440, jasny 390) plus wybrane ekrany w pełnym rozmiarze, nie każdy plik z osobna. Na zrzutach importu z banku widać jeszcze natywne „Choose file”.

## Problemy z danymi (nienaprawiane)

Zgłoszone w briefie, niezweryfikowane na prawdziwych danych, bo baza deweloperska ma tylko dane demo:

- Duplikaty sklepów („Action” ×2, „13 Balice Manual” ×2).
- W strukturze wydatków „Przychód (Do przypisania)” liczy się jako wydatek.
- Jednorazowy wydatek 86 000 zł zawyża kategorię Czynsz do 91%.

## Ważne przy wdrożeniu

- **Zmiany właściciela są w osobnym commicie:** `services/home.py` i `helpers.py` (limit 8 ostatnich transakcji, ukrycie korekt salda), `Dockerfile.dev`, `docker-compose.dev.yml`, `CHANGELOG.md` i `config.yaml`. Zmiany właściciela w `HomePage.tsx` i `home.css` są wchłonięte w commit Startu.
