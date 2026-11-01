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
