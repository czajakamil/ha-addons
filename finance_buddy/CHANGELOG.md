# Changelog

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
