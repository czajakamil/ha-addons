# Changelog

## 0.1.20

- Transakcje: „Zmień kategorię” przy zaznaczonych wierszach to teraz drzewo z wyszukiwarką zamiast długiej
  listy ścieżek. Kliknięcie od razu wybiera kategorię, Enter wybiera pierwszą pasującą.
- Ścieżki kategorii wszędzie wyglądają tak samo: „Rachunki › Czynsz” zamiast „Rachunki/Czynsz”. Dotyczy podpowiedzi
  po najechaniu na kategorię, kolejki „Bez kategorii”, porządków w Sklepach, szczegółów transakcji, komunikatów
  po zmianie zbiorczej i palety poleceń.
- Podpowiedzi w polu kategorii pokazują grupę przygaszoną, a samą kategorię wyraźnie. Wpisywanie „grupa/kategoria”
  nadal tworzy nową podkategorię.
- Przedmioty: w kolumnie kategorii jest sama nazwa (jak w Transakcjach), pełna ścieżka w podpowiedzi.
- Paleta poleceń: kategoria otwiera swoją stronę zamiast Budżetu.

## 0.1.19

- Transakcje: nowy wybór kategorii zamiast długiej listy ścieżek. Kategorie są drzewem w kolejności z ekranu
  Kategorie, z wyszukiwarką, i można zaznaczyć kilka naraz. Zaznaczenie kategorii nadrzędnej obejmuje jej
  podkategorie; częściowo zaznaczona grupa ma kreskę w polu wyboru. Jest też „Bez kategorii”.
- Z filtrem kategorii lista od razu pokazuje liczbę wszystkich pasujących operacji.
- „Pokaż transakcje” z ekranu Kategorie obejmuje teraz także podkategorie.

## 0.1.18

- Kategorie: podkategorie z importu YNAB nazywają się już po prostu „Jedzenie”, a nie „Delegacje/Jedzenie”.
  Aktualizacja poprawia istniejące nazwy (nazwę zostawia, gdy pod tym samym rodzicem jest już kategoria
  o takiej nazwie), a kolejne importy zapisują same nazwy.
- Kategorie: sortowanie jak w Sklepach — kliknięcie nagłówka Kategoria, Transakcje, Wydano albo Ostatnio
  sortuje każdy poziom drzewa, drugie kliknięcie odwraca kierunek. Lista „Sortuj” wraca do własnej
  kolejności; przeciąganie działa tylko w niej.

## 0.1.17

- Kategorie: nowy ekran z drzewem w tabeli. Każda kategoria pokazuje liczbę transakcji, wydatki z 12 miesięcy,
  datę ostatniej transakcji i mini-wykres trendu; kwoty kategorii nadrzędnej obejmują podkategorie. Jest
  wyszukiwarka, „Rozwiń/Zwiń wszystko”, a kategorie bez transakcji od 12 miesięcy chowają się za „Pokaż nieużywane”.
- Kolejność kategorii ustawisz przeciąganiem wiersza: nad, pod albo do środka innej kategorii. To samo zrobisz
  z menu „⋯”: „Przesuń wyżej/niżej” i „Przenieś do…”. Nowe i przeniesione kategorie trafiają na koniec listy.
- Kliknięcie kategorii otwiera jej stronę: średnia i suma wydatków, wykres miesięcy, ostatnie transakcje razem
  z podkategoriami, najczęstsze sklepy oraz ustawienia kategorii nadrzędnej i koloru.
- Usunąć można już tylko kategorię bez transakcji (także w podkategoriach), żeby transakcje nie traciły
  kategorii bez ostrzeżenia.

## 0.1.16

- Raporty → Wydatki: wykres ma przełącznik „Słupki | Koło”. Koło to pierścień z podziałem wydatków za cały
  wybrany okres (5 największych kategorii i „Pozostałe”, te same kolory co na słupkach), z sumą w środku
  i listą kwot oraz udziałów obok. Najechanie podświetla kategorię, a kliknięcie otwiera jej transakcje.
  Wybrany widok zostaje zapamiętany w przeglądarce.

## 0.1.15

- Raporty: w menu wyboru kont i kategorii checkbox stoi w jednym wierszu z nazwą, a samo menu nie chowa się
  już pod paskiem bocznym. Pozycje na liście są niższe.
- Transakcje: filtr kont to teraz to samo menu co w Raportach, z wyszukiwarką i wyborem kilku kont naraz.
  Zaznaczenie wszystkich kont działa jak brak filtra.

## 0.1.14

- W całym interfejsie „Tag” nazywa się teraz „Kategoria”: ekran „Kategorie”, kolejka „Bez kategorii”,
  konta, raporty, przedmioty, paragony i komunikaty błędów. Stare adresy `/tags`
  i `/transactions/untagged` przekierowują na nowe.
- Przedmioty: lista ma kolumny „Zakupy”, „Wydano (12 mies.)” i „Ostatni zakup” oraz sortowanie
  po nich. Okno przedmiotu pokazuje podsumowanie zakupów i wykres wydatków miesięcznie. Liczymy tylko
  PLN, bez przelewów i korekt, a zwroty pomniejszają sumę.
- Transakcje: ikona statusu jest zawsze widoczna i każdy stan ma inny kształt: pusty okrąg
  (niezatwierdzona), okrąg z ptaszkiem (zatwierdzona), kłódka (uzgodniona). Kliknięcie przełącza tylko
  niezatwierdzona ↔ zatwierdzona. Uzgodnienie ustawia się z menu kontekstowego, a jego cofnięcie wymaga
  potwierdzenia i zostawia transakcję zatwierdzoną.

## 0.1.13

- Sklepy: „Do uporządkowania” ma przycisk „Usuń sklepy bez transakcji”, który jednym potwierdzeniem usuwa
  wszystkie aktywne sklepy bez żadnej transakcji, także te już uporządkowane.
- Sklepy: surową nazwę z banku, która jest jednak poprawna, można oznaczyć jako zweryfikowaną, pojedynczo
  albo dla zaznaczonych. Potwierdzenie dotyczy tej konkretnej nazwy, więc po zmianie nazwy grupy na inną
  podejrzaną reguła znów działa.

## 0.1.12

- Sklepy: „Do uporządkowania” podpowiada domyślną kategorię sklepu z historii jego transakcji, gdy jedna
  kategoria ma co najmniej 60% z nich. Podpowiedzi stosuje się jednym przyciskiem dla wszystkich, dla
  zaznaczonych albo pojedynczo z menu, z możliwością cofnięcia.
- Sklepy: „Scal wszystkie duplikaty” scala każdy duplikat osobno, do sklepu z największą liczbą transakcji,
  zamiast łączyć wszystko w jeden sklep. Przed scaleniem widać listę scaleń, a te z konfliktem są pomijane.
  Każde da się cofnąć na stronie sklepu.
- Import z YNAB: pierwszy import budżetu sam ustawia sklepom bez kategorii kategorię podpowiedzianą
  z historii transakcji. Kolejne importy niczego nie nadpisują. Podsumowanie importu pokazuje,
  ile sklepów ją dostało.

## 0.1.11

- Przedmioty: duplikaty można scalić. W oknie przedmiotu wskazujesz właściwy, a pozycje transakcji
  i nazwy z paragonów przechodzą do niego. Stara nazwa dalej rozpoznaje się na paragonach.
- Przedmioty: okno przedmiotu pokazuje nazwy z paragonów, które na niego wskazują, z podziałem
  na sklepy. Błędnie przypiętą nazwę da się odpiąć.
- Paragony: propozycje podobnych produktów biorą pod uwagę także nazwy z paragonów innych sklepów.
  Przy nowym produkcie jest podpowiedź, żeby najpierw poszukać istniejącego.
- Transakcje: pozycje z paragonu zapamiętują oryginalną nazwę z wydruku i pokazują ją w szczegółach.
- Transakcje: godzinę i tag pozycji z paragonu można zmienić w tabeli podwójnym kliknięciem. Tag pozycji
  zmienia się na przedmiocie, więc we wszystkich jego pozycjach. Godzinę da się wpisać skrótem („915”, „9”).
- Paragony: przy linku w pasku bocznym jest licznik paragonów do przejrzenia.
- Sklepy: tabelę sortuje się kliknięciem w nagłówek kolumny, drugie kliknięcie odwraca kierunek.
  Domyślnie lista jest alfabetyczna.
- Sklepy: „Do uporządkowania” pokazuje grupy sklepów, tak jak widok „Wszystkie”. Grupę z powtarzającymi się
  nazwami z banku da się scalić w jeden sklep, a sklepy bez transakcji usunąć, także hurtem.
  Przelew do siebie rozpoznaje się tylko po pełnym imieniu i nazwisku.
- Import z YNAB: nowy sklep powstaje tylko dla payee, który ma transakcje, więc usunięte puste sklepy
  nie wracają. Payee o nazwie, którą ma już inny sklep, trafia do tego sklepu zamiast tworzyć duplikat.

## 0.1.10

- Paragony: na telefonie i tablecie jest przycisk „Zrób zdjęcie”, który od razu otwiera tylny aparat.
  Zdjęcie trafia do skrzynki jako szkic, tak jak wgrany plik.
- Paragony: przed wysłaniem duże zdjęcia są zmniejszane do 3200 px na dłuższym boku i zapisywane jako JPEG,
  także te wybrane z galerii. Wysyłka przez komórkowy internet jest dzięki temu kilka razy szybsza,
  a zdjęcia HEIC z iPhone'a dają się odczytać. PDF-y i małe pliki idą bez zmian.

## 0.1.9

- Raporty: okres wybiera się rządkiem przycisków nad wykresami (na telefonie z listy), a nagłówek
  „Raporty” wrócił na górę. Filtry kont i kategorii mają prostsze podsumowanie wybranych pozycji.
- Raporty: w zakładce Wydatki „Pozostałe” rozwijają się do poszczególnych kategorii. Najechanie
  na słupek pokazuje dymek z podziałem miesiąca na kategorie.
- Daty i godziny w nowej transakcji, szczegółach transakcji i filtrach listy można wpisać z klawiatury
  (np. „1.11.2026”, „9:15”). W aplikacji Home Assistant na Macu natywny wybierak daty powodował awarię.

## 0.1.8

- Raporty: zakładka Wydatki ma nowy układ. Na górze łączna kwota, średnia miesięczna i bieżący miesiąc w toku,
  niżej słupki miesięczne ułożone z największych kategorii (z linią średniej), a pod nimi tabela z udziałem,
  porównaniem ostatniego miesiąca ze średnią i mini-wykresem. Kategorie i Grupy przełącza się nad tabelą.
  Zakładka Trendy jest w niej zawarta, więc zniknęła. Na telefonie słupki pokazują ostatnie pół roku,
  a wiersze mają układ kart.
- Raporty: zakładka Tagi weszła do zakładki Majątek jako „Skład majątku” z przełącznikiem Typy/Tagi.
  Skład liczy się teraz na koniec wybranego okresu, więc zmiana okresu go zmienia, a suma listy
  zgadza się z „Netto”. Obejmuje też zamknięte konta, które mają saldo.
- Raporty: wykres majątku jest słupkowy. Każdy miesiąc ma parę słupków, aktywa i zobowiązania, a nad nią netto. Linia z kropkami pokazuje zmianę netto z miesiąca na miesiąc.
- Transakcje: lista i szczegóły pokazują godzinę, jeśli źródło ją podało (paragon, CSV z banku z godziną).
  Można ją też wpisać ręcznie, opcjonalnie, przy nowej transakcji i w szczegółach.

## 0.1.7

- Konta: lista nie miga już przy wejściu na ekran. Puste konta od razu są ukryte pod „Pokaż puste”,
  bo do wczytania ostatniej aktywności kont widać szkielet wierszy.

## 0.1.6

- Zarchiwizowane konta nie pojawiają się już przy wyborze konta: w przelewach na liście Transakcji,
  w szczegółach transakcji i w edytorze paragonu. Stare przelewy dalej pokazują ich nazwę, a konto
  już ustawione w szczegółach transakcji lub na paragonie zostaje na liście.

## 0.1.5

- Transakcje: poza telefonem osobne kolumny Wydatek i Wpływ. Kwotę wpisuje się bez znaku, a wpisanie jej
  po drugiej stronie zamienia wydatek na wpływ i odwrotnie.
- Lista wyboru w komórce ma sekcje „Sklepy” i „Przelew na konto” i zapisuje wybraną pozycję,
  a nie jej nazwę.
- Panel ma układ wierszy ustawień. Nowy użytkownik, reset dostępu i usunięcie konta idą przez okna,
  a usunięcie wymaga wpisania nazwy.
- Klucz `ynab_api_key` z konfiguracji ma pierwszeństwo przed zapisanym w aplikacji, a panel nie pokazuje
  wtedy pola na klucz.

## 0.1.4

- Nowa sekcja Sklepy: lista grupowana, strona sklepu i statystyki liczone z bazy.
- Aliasy nazw z banku: importer dopasowuje sklep po aliasie dokładnym albo prefiksie z gwiazdką,
  a niejednoznaczne pomija.
- Scalanie sklepów z podglądem skutków i cofaniem przez 30 dni.
- Przelewy własne zapisane jako sklep można zamienić na przelewy między kontami, z podglądem
  i cofaniem przez 30 dni.
- Raporty: neutralne słupki w Trendach, linia z obszarem w Majątku netto, lista z paskami
  zamiast donuta w Tagach kont. Przychody i wydatki mają parę słupków z bilansem.
- Raporty na telefonie: zakładki w jednym rzędzie, filtry w oknie od dołu, a Przepływy jako lista grup.
- Przed każdą migracją aplikacja robi kopię bazy. Ta wersja dodaje migracje dla aliasów,
  scaleń i zamian przelewów.

## 0.1.3

- Konto edytuje się na jego stronie: zakładki Transakcje i Ustawienia, zmiana nazwy w miejscu, „Uzgodnij saldo”,
  autozapis z „Cofnij”, a przy statusie, walucie i typie potwierdzenie ze skutkiem.
- Lista transakcji konta ma kolumnę „Saldo po”, a wykres salda to neutralna linia od pierwszej transakcji.
- Lista kont: dwuklik zmienia nazwę, a menu „⋯” ma „Ustawienia konta” i archiwizację.

## 0.1.2

- Ekran Start zajmuje całą szerokość, a na szerokim ekranie układa się w dwie kolumny.
- Kafelki prowadzą do Raportów, Budżetu i Kont; doszedł kafelek „Do przypisania".
- „Ostatnie transakcje" pokazują 8 pozycji i pomijają korekty salda.

## 0.1.1

- Ekran powitalny używa klucza `ynab_api_key` z konfiguracji: sam ładuje budżety i nie prosi
  o ponowne wklejenie klucza.

## 0.1.0

- Pierwsza wersja aplikacji Home Assistanta: Ingress, logowanie przez HA, kopie w `/share`,
  jednorazowy import danych z `/share/finance_buddy/import`.
