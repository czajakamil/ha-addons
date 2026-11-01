# Changelog

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
