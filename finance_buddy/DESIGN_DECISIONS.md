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

- **Sekcja aliasów usunięta.** Wszystkie użycia dawnych nazw zamieniono mechanicznie na ich dotychczasowe cele, więc wygląd się nie zmienił. Dawne wypełnione elementy korzystają z `--text` (akcent nie wypełnia powierzchni), a promień 999 px mają tylko kropki statusu (`--radius-dot`).
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
- **Akcje zbiorcze idą pojedynczymi żądaniami, po 5 naraz,** bo API nie ma zapisu zbiorczego. Zmiana kategorii pomija przelewy, a zatwierdzanie transakcje już zatwierdzone; obie liczby są w podsumowaniu.
- **Podsumowanie akcji zbiorczej** zostaje do zamknięcia i pokazuje liczbę udanych, listę nieudanych z powodem oraz „Ponów nieudane”. Po akcji zaznaczone zostają tylko nieudane, a ich wiersze mają czerwony tint. Ponowienie działa też po zmianie filtrów, bo podsumowanie pamięta transakcje.
- **Powód błędu to polski opis statusu** (brak połączenia, transakcja nie istnieje, błąd serwera, odrzucona zmiana), bez surowej odpowiedzi API, która jest po angielsku.
- **Zaznaczenie obejmuje tylko wczytane wiersze.** Po wyszukaniu ukryte transakcje wypadają z zaznaczenia; wcześniej licznik obiecywał więcej, niż akcja zmieniała.
- **Telefon (do 700 px):** sklep z kontem pod spodem i kwota, a dotknięcie otwiera szczegóły. Edycja w komórkach i zaznaczanie są tylko na większych ekranach.

## Konta

- **Edycja na stronie konta, nie w modalu.** Modal zostaje tylko dla „Nowe konto”. „Uzgodnij saldo” to dymek, a potwierdzenia zmian i usunięcia rozwijają się na stronie.
- **Autozapis tylko dla nazwy, ikony, koloru i tagu,** z „Cofnij” po stronie frontendu (poprzednia wartość i ponowny PATCH). Status, waluta i typ wymagają potwierdzenia ze skutkiem i nie mają „Cofnij”, bo cofnięcie zmieniłoby budżet bez pokazania skutku.
- **Skutek zmiany statusu podaje tylko pewną część kwoty.** Do przypisania to saldo kont budżetowych minus dostępne środki. Frontend zna saldo, a przesunięcia dostępnych środków nie policzy bez API, więc tekst mówi, że kwota może się dodatkowo zmienić.
- **Dwuklik na nazwie na liście:** pojedynczy klik otwiera konto po 250 ms, inaczej dwuklik nigdy by nie zadziałał. Klawiatura otwiera od razu.
- **„Saldo po” liczymy wstecz od bieżącego salda, w kolejności listy** (od najnowszego wpisu według `recorded_at`). Przy wyszukiwaniu i filtrach kolumna znika, bo brakujące wiersze dałyby złe kwoty.
- **Kolumna Komentarz na stronie konta znika tylko, gdy żadna wczytana transakcja nie ma komentarza użytkownika.** Nazwy pozycji podziału trafiają wtedy do kolumny Sklep.
- **Wykres zaczyna się od salda otwarcia,** czyli miesiąca przed pierwszą transakcją. Konto z jednego miesiąca dałoby inaczej pojedynczy punkt. Poniżej 700 px widać 6 ostatnich miesięcy. `@nivo/line` dochodzi jako zależność, zgodnie z decyzją o Nivo.
- **Kolory konta:** szarość i 7 kolorów z `--chart-*`, bez `--chart-2`. Id `indigo` zostaje dla szarości, bo jest zapisane w bazie.
- **Korekta salda:** API zapisuje komentarz „Wyrównanie salda konta”. Frontend pokazuje go jako „Korekta salda” i nie traktuje jako komentarza użytkownika.
- **Waluta PLN przy kontach walutowych z YNAB pochodzi z importu,** który wpisuje PLN każdemu nowemu kontu (`scripts/import_ynab_data.py`). To nie błąd prezentacji; danych nie poprawiamy automatycznie.
- **Szczegóły konta to osobny adres** (`/accounts/:id`) z wykresem salda na koniec miesiąca z ostatnich 12 miesięcy, odtworzonym wstecz z transakcji konta. API nie ma historii sald.
- **Ostatnia aktywność to jedno żądanie na konto** (`/transactions?limit=1`), bo lista kont jej nie zwraca.
- **Suma grupy obejmuje tylko konta w PLN.** Bez kursów nie da się uczciwie zsumować innych walut.
- **Kolumna Komentarz i Tag zniknęły z listy.** Tag ustawia się w zakładce Ustawienia. Komentarz konta nie ma już pola edycji, a wyszukiwarka pomija automatyczne komentarze z importu.

## Sklepy

- **Zmiana nazwy zapisuje `group_name` we wszystkich sklepach grupy, a `name` zostaje.** Importer CSV dopasowuje sklepy po `Store.name`, więc zmiana `name` odcięłaby je od kolejnych importów. Skutek uboczny: nazwa identyczna z inną grupą łączy obie na liście.
- **Grupa to ta sama nazwa grupowa po zmianie wielkości liter i spacji,** a przy jej braku `name`. Sklep zarchiwizowany nie dokleja się do aktywnej grupy, bo importer już go nie dopasowuje.
- **`/payees/:id` przyjmuje id dowolnego sklepu z grupy,** bo grupa nie ma własnego id. Lista zostaje pod `/stores`, a stare adresy dalej działają.
- **Statystyki to jedno przejście po wszystkich transakcjach,** a nie żądanie na sklep. Koszt i propozycja endpointu są w `docs/design/sklepy/ETAP2.md`.
- **Transakcje i Ostatnia liczą całą historię, Wydano ostatnie 12 miesięcy kalendarzowych z bieżącym,** tak samo jak wykres. Średnia to zawsze suma / 12. Wydano pomija przelewy, korekty salda i inne waluty niż PLN, a zwrot pomniejsza sumę.
- **Do wczytania statystyk lista jest alfabetyczna i pokazuje wszystkie grupy.** Inaczej wiersze przeskakiwałyby, a bez dat nie wiadomo, co jest nieaktywne.
- **Domyślna kategoria grupy** to kategoria pierwszego sklepu, który ją ma, licząc od największej liczby transakcji. Zapis ustawia ją we wszystkich sklepach grupy i włącza auto-kategoryzację, bez której importer jej nie użyje.
- **NIP i adres są na jednym sklepie grupy** (tym, który już je ma, inaczej tym z największą liczbą transakcji), bo NIP jest unikalny dla użytkownika.
- **Każda pozycja „Do uporządkowania” ma jeden powód,** żeby liczniki chipów sumowały się do całości, jak w makiecie. Kolejność: przelew własny, duplikat, surowa nazwa, bez kategorii. Reguły działają na pojedynczych sklepach, nie grupach, bo kategorię i scalanie ustawia się per sklep.
- **Właściciel w regule przelewu własnego** to `display_name`, a przy jego braku `username` z sesji. Wszystkie części nazwy muszą wystąpić w nazwie sklepu.
- **Surowa nazwa obejmuje też holenderski kod pocztowy i `straat`/`strasse`/`street`,** bo tak wygląda przykład Adyen z makiety. Krajowy numer konta (26 cyfr) też jest maskowany. Propozycja nazwy to tekst przed formą prawną, przecinkiem lub cyfrą.
- **Usuwanie tylko przy pewnej liczbie 0 transakcji.** Backend nie blokuje usunięcia sklepu z transakcjami i zeruje im `store_id`, więc błąd wczytania też blokuje przycisk. Etap 2 proponuje blokadę na backendzie. Grupa bez transakcji usuwa wszystkie swoje sklepy.
- **Archiwizacja zostaje,** choć brief jej nie wymienia: w „⋯”, na stronie sklepu i jako „Pokaż zarchiwizowane”, jak w Kontach. Usunięty komentarz sklepu nie ma już pola edycji; dane zostają w bazie.
- **Wykres to CSS, nie Nivo.** Dwanaście słupków z wartościami nie potrzebuje biblioteki; precedens to lista z paskami w Raportach. Poniżej 700 px widać 6 miesięcy, jak na stronie konta.
- **Filtr po sklepie w Transakcjach** (`?stores=a,b`) pobiera wszystkie transakcje każdego sklepu i scala je w przeglądarce, bo API filtruje po jednym sklepie, a API zostaje bez zmian. Nazwa filtra to chip z „×”.
- **„Nazwa z banku” to na razie `Store.name`,** bo API nie zwraca `bank_description`.
- **Telefon (do 700 px):** lista pokazuje nazwę i Wydano, a porządki nazwę z powodem pod spodem. Sortowanie to pole wyboru jak w Kontach, a nie klik w nagłówek.

## Raporty

- **Struktura wydatków: lista z paskami pełni rolę poziomego wykresu.** Top 10 plus „Pozostałe”, posortowane malejąco, paski neutralne, klikalne jak wcześniej. Biblioteka wykresów nie dawałaby tu nic ponad to, a tekst pozostaje czytelny i dostępny.
- **Donut w „Tagi kont” pojawia się tylko przy najwyżej 5 pozycjach.**
- **Paleta wykresów `--chart-1…8`** to sprawdzona paleta kategorialna, osobno dla obu motywów. Nivo nie rozumie `var()`, więc kolory czytamy z tokenów w `utils/tokens.ts`, a zmiana motywu przebudowuje zakładkę.
- **Przepływy:** Przychody → grupy → kategorie, 8 największych kategorii, reszta jako „Pozostałe wydatki”. Grupy biorą się ze ścieżki kategorii. Przelewy między kontami nie są już częścią diagramu. Gdy przychody przewyższają wydatki, nadwyżka to osobna gałąź.
- **Deficyt w Przepływach:** „Przychody” i „Z oszczędności” spływają do węzła „Wydatki”, a dopiero z niego do grup. Bezpośrednie łącza wymagałyby wymyślenia, które źródło opłaciło którą grupę. „Z oszczędności” to matematyczna różnica wydatków i przychodów: dane nie mówią, czy deficyt pokryły oszczędności, czy np. karta kredytowa.
- **Przepływy na telefonie przewijają się w bok** (diagram ma min. 720 px), bo pięć poziomów etykiet nie mieści się w 390 px.

## Pozostałe ekrany i porządki

- **Tabler CSS zostaje.** Okna dialogowe, formularze i układ pozostałych ekranów nadal korzystają z jego klas, a tokeny sprowadzają je do jednego wyglądu. Pakiet `@tabler/icons-react` jest usunięty.
- **Panel użytkownika ma nowy wygląd przez style globalne, bez przepisywania układu.** Sklepy i Przedmioty mają dodatkowo menu „Kolumny”, a Import z banku kreator w 3 krokach.
- **Paleta Cmd+K:** dodanie wydatku, ekrany, konta i kategorie. Wybór kategorii otwiera Budżet. Skrót N działa poza polami tekstowymi.
- **Dialog wydatku ma jedno pole kwoty ze znakiem,** jak Transakcje: wydatek z minusem, wpływ bez znaku. Pole startuje od „-”, więc zwykłe wpisanie kwoty daje wydatek, a wpływ wymaga skasowania znaku. Klawiatura dziesiętna iPhone'a nie ma minusa, więc inaczej nie dałoby się na niej wpisać wydatku. Pod polem na bieżąco widać „Wydatek: …” albo „Wpływ: …”. Kwota jest na górze z autofokusem. Podpowiedź kategorii to domyślna kategoria sklepu.
- **Toasty (Sonner)** są na dole pośrodku. Potwierdzają dodanie transakcji z palety i zapisy na stronie konta, gdzie mają „Cofnij”.

## Układ przebudowanych ekranów

- **Tagi:** przełącznik zakresu i dodawanie w jednym pasku, nad płaską listą bez karty w karcie. Kosz pojawia się po najechaniu, a na dotyku jest stale widoczny. Liczba przy grupie to liczba bezpośrednich podtagów.
- **Paragony:** przycisk „Wgraj paragony” otwiera wybór plików i od razu startuje rozpoznawanie, bez drugiego kliknięcia. Szkic nie jest jeszcze transakcją i można go usunąć. Znika przy okazji angielskie „Choose file” przeglądarki. Plik i liczba pozycji są podpisem pod sklepem, a nie osobnymi kolumnami.
- **Szczegóły transakcji:** nagłówek z nazwą, kwotą i statusem, pozycje i załączniki w głównej kolumnie, formularz w przyklejonej kolumnie po prawej (na węższych ekranach pod spodem). Formularz nowej pozycji otwiera się na żądanie. Usunięto „Odśwież” i nieużywany tryb `embedded`.
- **Kolejka do otagowania:** karta decyzji po lewej, lista po prawej (na węższych ekranach pod spodem). Lista pokazuje sklep z podpowiedzią, datę i kwotę; konto widać na karcie. Przycisk „Inny tag” zniknął, bo pole tagu jest zawsze widoczne, a skrót T działa dalej. Na telefonie nie ma podpowiedzi klawiatury.
- **Logowanie:** jedna kolumna z jedną główną akcją. Dodawanie passkey z zaproszenia jest rozwijane; przy pierwszym uruchomieniu formularz zastępuje logowanie. Pole loginu pojawia się tylko przy tworzeniu pierwszego admina, a kodu zaproszenia poza tym przypadkiem.

## Szerokości kolumn

- **Dwie ścieżki, jeden zapis.** `DataTable` dostaje `tableId`, a zwykłe tabele HTML komponent `ResizableTable`. Oba korzystają z `useColumnWidths`, czyli istniejącego API `/preferences/table-columns`, więc szerokości są na koncie, nie w przeglądarce.
- **Stan jest wspólny dla tabeli,** bo API zastępuje cały zapis, a Konta czy Sklepy pokazują tę samą tabelę kilka razy na ekranie. Osobne kopie kasowałyby sobie zmiany.
- **Zwykłe tabele rozpoznają kolumnę po tekście nagłówka** (lub `data-col`). Kolumny bez nazwy nie mają uchwytu. Zmiana tekstu nagłówka oznacza powrót do domyślnej szerokości.
- **Na telefonie (do 700 px) szerokości się nie stosują ani nie zapisują.** Te układy mają własne kolumny, a przeciąganie na dotyku nie działa.
- **Minimum to 48 px** w każdej tabeli (w Transakcjach było 10 px).
- **Przeciąganie obsługuje tylko mysz,** jak w dotychczasowej siatce. Skrótu klawiaturowego nie ma.

## Testy

- **Pierwsze testy jednostkowe** (`bulkUpdate.test.ts`, `flows.test.ts`) obejmują logikę, której błąd zmieniałby dane lub liczby: podział wyników akcji zbiorczej i węzły Przepływów.
