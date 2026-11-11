# Changelog

## 0.1.36

- Transakcje, które przychodzą z synchronizacji z bankiem lub z importu wyciągu CSV, mają teraz żółty
  status „do zatwierdzenia”, a nie zielony. Najpierw je kategoryzujesz i potwierdzasz. Dotyczy to też
  transakcji, którym kategorię podpowiedziała reguła sklepu.
- Gdy wiersz z banku scali się z transakcją, którą wpisałeś wcześniej sam, ta transakcja nadal staje
  się zatwierdzona.

## 0.1.35

- Gdy w Transakcjach wybierzesz sklep, a transakcja nie ma jeszcze kategorii, wpisze się ta z reguły
  sklepu. Jeśli sklep nie ma reguły, wpisze się kategoria z jego ostatniej transakcji. Komórka kategorii
  na chwilę się podświetli, żeby było widać, że wpisała się sama.
- Kategorii, którą już ustawiłeś, zmiana sklepu nie rusza. Transakcji podzielonych na pozycje też nie.

## 0.1.34

- Nowy ekran Start. Na górze duża kwota, którą możesz jeszcze wydać w tym miesiącu, i ile to wychodzi
  dziennie. Gdy przypiszesz więcej, niż masz, zamiast niej zobaczysz czerwone ostrzeżenie z nadwyżką,
  realną kwotą do wydania i przyciskiem „Napraw w Budżecie”.
- Pod spodem trzy liczby: Wydane, saldo kont budżetowych i ile transakcji czeka na kategorię.
- „Do zrobienia” ma przy każdej sprawie krótki opis i przycisk (Kategoryzuj, Przejrzyj, Importuj).
  Kategorie ponad budżet i blisko limitu są w jednym wierszu, który rozwijasz, żeby zobaczyć paski.
- Nowa karta „Tempo wydatków”: ile wydałeś od początku miesiąca na tle średniej z poprzednich miesięcy,
  z podpowiedzią, czy wydajesz szybciej, czy wolniej niż zwykle. Obok „Najwięcej w tym miesiącu”.
- „Wydane” na Start i wydatki w Raportach liczą też wydatki z nieznanym odbiorcą (np. BLIK do osoby),
  zanim nadasz im kategorię. Wcześniej pomijaliśmy je, więc kwoty mogły wyglądać na mniejsze.
- Korekta salda z „Wyrównaj saldo” nie trafia już do kolejki „Bez kategorii”.
- „Możesz jeszcze wydać” nie wlicza pieniędzy odłożonych na spłatę karty.
- Opłacone co do grosza rachunki nie pokazują się już jako „blisko limitu”.

## 0.1.33

- Kategoria w Transakcjach pokazuje pod nazwą, mniejszym tekstem, kategorię główną, np. „Zakupy Spożywcze”
  z „Jedzenie” pod spodem. Od razu widać, z której grupy jest podkategoria.
- Dwuklik na słowo w otwartym polu edycji (sklep, kategoria, konto, kwota, godzina) zaznacza je i zostawia
  zaznaczone. Wcześniej zaznaczenie od razu znikało, a w kwocie i godzinie kasował się też wpisany tekst.

## 0.1.32

- W Transakcjach sklep, którego jeszcze nie ma, tworzysz od razu w polu Sklep: wpisz nazwę i wybierz
  „Utwórz sklep „…””. Gdy nic innego nie pasuje, wystarczy Enter. Jeśli zrobisz literówkę i zaraz
  przepniesz transakcję na poprawny sklep, ten z literówką zniknie (o ile nie ma innych transakcji).
- Podpowiedzi kategorii w Transakcjach znów pokazują zagnieżdżenie, np. „Dom › Domena Internetowa”,
  a wpisanie nazwy grupy pokazuje też wszystkie jej podkategorie.
- Pola w tabeli Transakcji (sklep, kategoria, kwota, godzina, komentarz) wpisujesz wprost w komórce,
  bez dodatkowego, mniejszego pola z osobną ramką.
- Esc w komentarzu anuluje zmiany. Wcześniej wpisany tekst i tak się zapisywał.

## 0.1.31

- Płatności kartą z PKO mają nazwę sklepu zamiast sklejonego napisu z banku. Z „NOWY TARGKAUFLANDPL · CARD-PAYMENT”
  robi się sklep „KAUFLAND” z komentarzem „Nowy Targ”, a transakcja sama trafia do sklepu, który ma taką nazwę
  lub alias. Już pobrane transakcje poprawią się przy najbliższym pobraniu; komentarzy, które zmieniłeś, nie ruszamy.
- Komentarze z banku nie kończą się już kodami w rodzaju „TRANSFER” czy „CARD-PAYMENT”.
- Rachunki w Panelu są czytelniejsze: przy każdym widać saldo w banku i różnicę z aplikacją, a „Pobieraj od”
  przełączasz między „Ostatnie 90 dni” i „Wybranego dnia”. Powrót do 90 dni wreszcie kasuje wybraną datę.
- Komentarz w Transakcjach znów da się wygodnie edytować: po dwukliku pole otwiera się nad wierszem,
  zamiast ściskać się do cienkiego paska.
- PKO podaje przez bankowe API tylko datę zaksięgowania, więc płatność kartą widać 1–3 dni później niż w
  rzeczywistości. Jeśli dodasz ją wcześniej ręcznie albo z paragonu, połączy się z bankową i zachowa Twoją datę.

## 0.1.30

- Gdy bank nie udostępnia żadnego rachunku, aplikacja mówi, co zrobić: prywatna aplikacja w Enable Banking
  widzi tylko rachunki podpięte w swoim panelu. Dodaj tam rachunek (np. z Revoluta) i kliknij „Połącz ponownie”.

## 0.1.29

- Połączenie z Revolutem pokazuje rachunki. Wcześniej po zgodzie widniało „Bank nie udostępnił żadnego rachunku”,
  bo Revolut podaje listę rachunków później niż inne banki. Jeśli masz już takie połączenie, kliknij „Pobierz teraz”,
  a rachunki się pojawią, bez łączenia od nowa.

## 0.1.28

- Transakcje mogą spływać z banku same. W Panelu, w sekcji „Połączenia z bankiem”, wgrywasz klucz
  z Enable Banking (bezpłatnie dla własnych kont), logujesz się w banku i wybierasz, do którego konta
  trafia każdy rachunek. Instrukcja jest w dokumentacji dodatku.
- Aplikacja pobiera transakcje co 12 godzin, a „Pobierz teraz” od razu. Łączy je z tymi, które już masz
  (ręczne, z YNAB, z paragonów), i nie dubluje wyciągów CSV. Każde pobranie można cofnąć w historii importu.
- Przy rachunku widać saldo w banku obok salda w aplikacji. Gdy zgoda banku wygaśnie, kliknij „Połącz ponownie”.

## 0.1.27

- Przygotowanie do automatycznego pobierania transakcji z banku: dodany skrypt testowy Enable Banking,
  który podpina konto, pobiera transakcje i porównuje je z wyciągiem CSV. W aplikacji nic się jeszcze nie zmienia.

## 0.1.26

- Nowa transakcja na telefonie: pole „Komentarz” nie jest już ucięte, a formularz przewija się na niższych
  ekranach. Data i godzina na iPhonie stoją przy lewej krawędzi jak reszta wartości.

## 0.1.25

- Dodając transakcję na telefonie, możesz od razu dołączyć zdjęcie paragonu (z aparatu lub galerii) albo PDF.
  Pliki widać jako miniatury, a po zapisie są w szczegółach transakcji w sekcji „Zdjęcia”.
- Przycisk zdjęć nie zamyka już formularza, więc wpisane dane nie przepadają.
  Pozycje z paragonu nie rozpoznają się jeszcze automatycznie — do tego nadal służy skrzynka Paragony.

## 0.1.24

- Na telefonie na dole ekranu jest pasek kart: Start, Budżet, Transakcje, Raporty i „Więcej” (reszta menu).
- Nowe dodawanie transakcji na telefonie — pełny ekran z dużą kwotą. Stuknij „−”, żeby zmienić wydatek
  na wpływ. Sklep wybierasz z ostatnich albo wpisujesz nowy, a kategoria podpowiada się sama („Jak zwykle
  w Żabka”). Przelew to wybór konta zamiast sklepu.
- Nowe transakcje z telefonu są domyślnie zatwierdzone; przełącznik „Zatwierdzona” to zmienia.

## 0.1.23

- Transakcje na telefonie w nowym układzie: lista pogrupowana po dniach z przyklejonym nagłówkiem i sumą dnia.
  Wiersz pokazuje sklep, kategorię z kolorową kropką (przy paragonie „3 kategorie”), konto, komentarz, kwotę
  i godzinę. Niezatwierdzone mają pomarańczową kropkę, wydatki bez kategorii — pomarańczową etykietę.
- Przesuń wiersz w lewo, żeby zmienić kategorię albo zatwierdzić transakcję bez wchodzenia w szczegóły.
- Filtry na telefonie siedzą w panelu od dołu (okres, konto, kategoria, „Tylko bez kategorii”, „Do zatwierdzenia”),
  a aktywne widać jako chipy pod wyszukiwarką.
- Nad listą baner „Do zatwierdzenia” z liczbą niezatwierdzonych — „Przejrzyj” pokazuje tylko je.
  Nową transakcję dodaje okrągły przycisk „+”.

## 0.1.22

- Paragony z pozycjami liczą się teraz do kategorii produktów — w Budżecie, Raportach i na ekranie Kategorie.
  Przykład: zakupy w Kaufland za 60 zł z winem za 20 zł zdejmą 20 zł z koperty „Alkohol”, a resztę z kategorii
  transakcji.
- Produkt bez kategorii i niewpisana część paragonu liczą się do głównej kategorii transakcji. Rabat na cały
  paragon rozkłada się proporcjonalnie na pozycje.
- Szczegóły transakcji: kategoria główna jest znów widoczna także przy paragonach z pozycjami. Pozycje,
  które z niej dziedziczą, mają ją przygaszoną.
- Filtr kategorii w Raportach pokazuje tylko pasującą część paragonu.

## 0.1.21

- Szczegóły transakcji w nowym układzie. Nagłówek ma nazwę sklepu (link do jego transakcji), dużą kwotę
  ze statusem i menu „⋯” z usuwaniem transakcji.
- Transakcja z pozycjami: nowa karta „Podział na kategorie” z paskiem i kwotami, a w tabeli pozycji kategorię
  zmienia się prosto w wierszu (drzewo z wyszukiwarką). Kategoria należy do produktu, więc obowiązuje też przy
  kolejnych zakupach. „Jedna kategoria dla wszystkich…” ustawia ją całemu paragonowi naraz.
- Pod tabelą widać, czy suma pozycji zgadza się z kwotą transakcji, a jeśli nie — o ile się różni.
- Zdjęcia paragonu jako miniatury z kafelkiem „Dodaj zdjęcie”.
- Transakcja bez pozycji: zamiast pustej tabeli pasek „Dodaj zdjęcie paragonu / Dodaj pozycje” i lista
  poprzednich transakcji z tym samym sklepem.
- Pola po prawej zapisują się same — przycisk „Zapisz zmiany” zniknął.

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
