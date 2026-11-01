# Decyzje projektowe: redesign Budget Buddy

Decyzje, których nie rozstrzyga brief redesignu. Przy wątpliwościach wybieramy rozwiązanie prostsze.

## Stack

- **Bez Tailwinda i shadcn/ui.** Aplikacja używa czystego CSS w warstwach na Tablerze, a shadcn wymaga Tailwinda. Dostępne komponenty dają bezpośrednio prymitywy Radix (Tooltip, Dropdown Menu), stylowane tokenami.
- **Nivo zamiast Recharts.** Nivo jest już w projekcie i ma słupki oraz Sankeya. Recharts stałby obok niego, zamiast go zastąpić.
- **Bez Motion.** Rozwijanie grup i zmianę miesiąca obsługują przejścia CSS.
- **TanStack Table i Virtual dopiero przy Transakcjach.** Budżet ma nieco ponad 100 wierszy i wystarcza mu zwykła tabela.
- **Inter hostowany lokalnie** (`@fontsource-variable/inter`), bez Google Fonts: aplikacja działa przez Ingress Home Assistanta także bez dostępu do sieci. Usunięty też Tabler `@latest` z unpkg w `index.html`, który omijał warstwy CSS.
- **Lucide wchodzi stopniowo.** Nowe komponenty używają Lucide, a ikony Tablera znikają w fazie porządków.

## Tokeny

- **Aliasy dawnych nazw** (`--primary`, `--muted`, `--r`, `--space-*`) wskazują na nowe tokeny, dzięki czemu nieprzerobione ekrany od razu dziedziczą nowy wygląd. Sekcja aliasów znika w fazie porządków.
- **`--primary` to `--text`**, a nie akcent: dawne wypełnione elementy stają się odwrócone, zgodnie z zasadą, że akcent nie wypełnia powierzchni.
- **`--pill` ma promień kontrolki.** Promień 999 px mają tylko kropki statusu (`--radius-dot`).
- **Dodatkowe tokeny poza briefem:** `--overlay` (tło pod dialogiem), `--on-color` (znacznik na kolorowym polu), tinty stanów 12%.

## Wyjątki od zakazu kolorów na sztywno

- **`TagColorPicker`:** tory suwaków odcienia i jasności to widmo, a nie kolor interfejsu.
- **Kolory tagów ustawione przez użytkownika** to dane, nie styl.
- **Kolory w kodzie wykresów** (`reports/model.ts`, `useChartTheme.ts`, `debtPresentation.ts`) znikają w fazie 5 razem z wymianą wykresów.

## Sidebar

- **Nazwa „Projekt Sztefan” usunięta z nagłówka** — zostaje sama nazwa „Budget Buddy”.
- **Motyw przełącza się pozycją w menu użytkownika**, bez zamykania menu, żeby od razu było widać efekt.

## Terminologia

- **„Kategoria”, nie „Koperta”.** To słowo pasuje do kolumny z briefu i do modelu danych. Budżet używał dotąd „Tag”. Ekran „Tagi” zostaje pod tą nazwą, zgodnie z briefem.

## Budżet

- **„+ Grupa” prowadzi do ekranu Tagi.** Dawny przycisk „+ Category Group” nie miał żadnej akcji, a grupy tworzy się tam, podając ścieżkę tagu. Własny dialog wykraczałby poza warstwę prezentacji.
- **Podsumowanie miesiąca bez pasków procentowych i bez notatki.** Zostały same liczby. „Do przypisania” jest już główną liczbą ekranu, więc nie powtarza się w panelu.
- **Przypisz automatycznie zostaje w panelu bocznym** jako druga zwijana sekcja, bo brief nie wskazuje dla niej innego miejsca.
- **Pasek koperty:** procent to wydane / (wydane + dostępne). Pokazujemy go, gdy kategoria ma przypisanie, aktywność lub saldo; API nie zwraca celów. Grupy nie mają paska.
- **Do 700 px** znika kolumna Aktywność (jest w podsumowaniu), a nazwy kategorii zawijają się zamiast ucinać, bo tooltip nie działa na dotyku. Przełącznik filtrów też się zawija.
- **Enter i Esc oddają fokus komórce**, a Tab i Shift+Tab przechodzą po widocznych kategoriach, z pominięciem zwiniętych grup. Niezmieniona kwota nie wysyła żądania do API.
- **Kwoty zawsze grupowane spacją.** Domyślnie `pl-PL` grupuje dopiero od 10 000, a brief wymaga separatora tysięcy. Zmiana w `formatMoney` działa w całej aplikacji.

## Kontrast

- **`--text-faint` zostaje z briefu jako świadomy wyjątek** (2,3–2,7:1), tylko dla zer i placeholderów. Przy 4,5:1 zlewałby się z `--text-muted` i zera przestałyby ustępować kwotom.
- **`--negative` w jasnym motywie to `#CF2020`, nie `#DC2626`:** wartość z briefu miała 4,39:1 na `--surface-2`, nowa ma 4,92:1.

## Start

- **„Możesz jeszcze wydać” to `available_cents` z podsumowania miesiąca Budżetu**, czyli suma dostępnych środków we wszystkich kategoriach. Brief nie definiuje tej liczby.
- **„Kategorie blisko limitu”, nie „Koperty”**, dla spójności z rozstrzygniętym terminem. Dotyczy też alertu „Kategoria ponad budżet”.
- **Alerty budżetowe otwierają Budżet z filtrem** (`/budget?filter=overspent`).
- **Korekty salda znikają ze Startu** przez zmianę w `services/home.py`, którą właściciel ma w drzewie roboczym. Etykieta „Korekta salda” działa też bez niej.

## Transakcje

- **react-data-grid zamiast TanStack Table.** Siatka jest już w projekcie, wirtualizuje wiersze, zapamiętuje szerokości kolumn i ma działającą edycję w komórkach. Pomiar na 3400 podstawionych wierszach: 17,6 ms na klatkę w p95. TanStack wymagałby przepisania edycji bez zysku dla użytkownika.
- **Bez kolumny daty.** Datę niesie nagłówek dnia, a zmienia się ją w szczegółach transakcji.
- **Kwota edytowana ze znakiem:** wydatek z minusem, wpływ bez znaku.
- **Suma dnia dotyczy wczytanych wierszy.** Na granicy strony może być chwilowo niepełna; kolejne strony doładowują się same przed końcem listy.
- **Filtr kategorii idzie przez `/transactions`,** bo `/transactions/page` go nie obsługuje, a API zostaje bez zmian. Liczba wyników jest wtedy znana dopiero po ostatniej stronie, więc do tego czasu nie pokazujemy łącznej liczby.
- **Status:** niezatwierdzona ma stałą ikonę, a zatwierdzona i uzgodniona tylko na hover. „Cleared” to „Zatwierdzona”, „Reconciled” to „Uzgodniona”.
- **Akcje zbiorcze idą pojedynczymi żądaniami, po 5 naraz,** bo API nie ma zapisu zbiorczego. Zmiana kategorii pomija przelewy.
- **Telefon (do 700 px):** sklep z kontem pod spodem i kwota, a dotknięcie otwiera szczegóły. Edycja w komórkach i zaznaczanie są tylko na większych ekranach.
