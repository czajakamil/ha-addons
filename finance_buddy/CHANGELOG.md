# Changelog

## Niewydane

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
