# Sklepy — brief dla agenta

Gałąź: `redesign/ui-foundation`. Zasady jak w briefie redesignu i `DESIGN_DECISIONS.md`.
Wzorzec: ekran **Konta** i **strona konta** — ta sama struktura, komponenty i tokeny.

Makieta referencyjna (źródła w `docs/design/sklepy/`):
- `Main.dc.html` — lista Sklepów, widoki „Wszystkie” i „Do uporządkowania”, okno scalania
- `Sklep.dc.html` — strona sklepu (nagłówek, wykres, zakładki Transakcje | Ustawienia)
- `*.png` — zrzuty makiety

Pliki `.dc.html` to format edytora makiet, nie działają samodzielnie. Traktuj je jako
specyfikację układu, odstępów, kolorów i tekstów. Implementuj na istniejących komponentach
i tokenach aplikacji, nie kopiuj markupu 1:1. Dane w makiecie są przykładowe.

## ETAP 1 — tylko frontend

1. **Lista grupowana po nazwie grupowej**: jeden wiersz na grupę, podpis „N nazwy z banku”,
   rozwinięcie (chevron) pokazuje surowe nazwy z liczbą transakcji. Kolumna „Nazwa grupowa” usunięta.
2. **Kolumny**: inicjał/ikona · Sklep · Domyślna kategoria (kropka + nazwa) · Transakcje ·
   Wydano (12 mies.) · Ostatnia · menu „⋯”. Domyślne sortowanie: Wydano malejąco.
3. Grupy bez transakcji z 12 mies. zwinięte pod „Pokaż nieaktywne (n)”.
4. **Bez ikony ołówka w wierszu**: klik nazwy → strona sklepu, dwuklik → zmiana nazwy w miejscu,
   rzadkie akcje w „⋯”. Nagłówek (tytuł + licznik, „Kolumny”, „Nowy sklep”) jak w Kontach.
5. **Przełącznik „Wszystkie · Do uporządkowania (n)”** + baner nad listą w widoku Wszystkie
   („N pozycji wymaga uporządkowania… [Przejrzyj]”).
6. **Do uporządkowania**: chipy filtrów (Duplikaty, Surowe nazwy z banku, Przelewy własne,
   Bez kategorii) z licznikami. Kolumny: checkbox · Nazwa · Powód (badge) · Sugestia · Transakcje · „⋯”.
   Reguły wykrywania:
   - Duplikat: ta sama nazwa po normalizacji (wielkość liter, spacje, formy prawne „Sp. z o.o.”, „S.A.”).
   - Surowa nazwa z banku: same cyfry; zawiera adres, kod pocztowy lub numer konta (IBAN).
   - Przelew własny: zawiera imię i nazwisko właściciela + „SENT FROM” / „Account:”.
   - Bez kategorii: brak domyślnej kategorii.
   Numery kont maskuj w UI (`LT13 •••• 1248`).
7. **Zaznaczanie** → pasek akcji zbiorczych (odwrócone kolory): „Zaznaczono: N”, „Scal w jeden sklep”,
   „Ustaw kategorię”, „Odznacz”. W etapie 1 „Scal” otwiera okno jak w makiecie, ale przycisk
   zatwierdzenia jest nieaktywny z dopiskiem „Wymaga etapu 2”. „Ustaw kategorię” działa,
   jeśli API pozwala ustawić domyślną kategorię sklepu.
8. **Strona sklepu** (`/payees/:id`):
   - nazwa edytowalna w miejscu (hover: ołówek + tło), podpis: kategoria · liczba transakcji · liczba nazw z banku;
   - po prawej „Wydano w 12 mies.” (32/600) i „średnio X zł miesięcznie”;
   - wykres słupkowy wydatków miesięcznie, słupki neutralne, bieżący miesiąc w `--text`, wartości nad słupkami;
   - zakładki Transakcje | Ustawienia. Transakcje: Data · Nazwa z banku · Konto · Kategoria · Kwota,
     ~20 ostatnich + link „Wszystkie transakcje sklepu w Transakcjach →” (filtr po sklepie);
   - Ustawienia: Domyślna kategoria (combobox), NIP i Adres (opcjonalne), autozapis z toastem „Zapisano · Cofnij”;
     sekcję „Nazwy z banku (aliasy)” pokaż tylko do odczytu (lista surowych nazw z grupy) — edycja w etapie 2;
   - „Scal z innym sklepem” w osobnej karcie (nieaktywne do etapu 2);
   - Strefa niebezpieczna: „Usuń sklep” aktywne tylko bez transakcji, inaczej wyjaśnienie dlaczego.
9. Statystyki (transakcje, wydano, ostatnia) licz z istniejących endpointów. Jeśli to wymaga
   N+1 zapytań przy 514 sklepach, nie implementuj na ślepo — opisz koszt i propozycję w raporcie.

## ETAP 2 — tylko propozycja, bez implementacji

10. Scalanie sklepów (zbiorczo i ze strony sklepu) oraz aliasy nazw z banku używane przez importer:
    zmiany API i modelu danych, migracja, wpływ na importer CSV/YNAB, obsługa cofania.
11. Czy „Przelewy własne” powinny być konwertowane na przelewy między kontami zamiast sklepów.
12. Aliasy z gwiazdką (`BIEDRONKA*`) vs dokładne dopasowanie — rekomendacja.
Opisz w `docs/design/sklepy/ETAP2.md` i czekaj na akceptację.

## Po zakończeniu etapu 1
Zrzuty: lista (oba widoki), strona sklepu (obie zakładki) — ciemny i jasny, 1440 i 390 px.
Decyzje poza tym briefem dopisz do `DESIGN_DECISIONS.md`.
