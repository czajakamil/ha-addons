# Sklepy — ETAP 2 (propozycja, bez implementacji)

Status: zaakceptowany ze zmianami, wdrażany krokami. Poniższa lista ma pierwszeństwo przed resztą dokumentu.

## Zmiany z akceptacji

- **Kopia przed migracją:** plik z datą w nazwie, bez niego migracja nie rusza. Testy migracji na kopii prawdziwej bazy.
- **Opis z banku:** bez opisu kolumna „Nazwa z banku” jest ukryta. Nie podstawiamy nazwy sklepu.
- **Aliasy:** unikalne po normalizacji (wielkość liter, spacje, formy prawne). Kolizja z migracji daje alias niejednoznaczny: importer go nie używa, a sklepy trafiają do „Do uporządkowania”.
- **Importer:** dokładny alias po normalizacji, potem prefiks tylko dla aliasów z `*` na końcu (min. 4 znaki, najdłuższy wygrywa), inaczej bez sklepu.
- **UI aliasów:** podgląd „dopasuje N transakcji, w tym M z innych sklepów”.
- **Scalanie:** `POST /stores/merge` z `dry_run` zwraca liczby i konflikty, okno pokazuje podgląd, a 409 jest tylko zabezpieczeniem. Łańcuchy `merged_into_id` są spłaszczane. Cofanie przez 30 dni przywraca transakcje, aliasy i domyślną kategorię.
- **Przelewy własne:** pomijamy kandydatki, które już są przelewami. Różne waluty to wybór ręczny, bez przeliczania kursów.

## Decyzje do akceptacji

| # | Pytanie | Rekomendacja |
|---|---------|--------------|
| 1 | Model scalania | Tabela `store_aliases`, scalanie przepina transakcje, stary sklep zostaje jako nieaktywne „przekierowanie” przez 30 dni |
| 2 | Cofanie | Zapis operacji w `store_merges` i `POST /stores/merges/{id}/undo`, działa do 30 dni |
| 3 | Przelewy własne | Nie konwertować automatycznie. Osobna akcja z podglądem, parująca tylko pewne przypadki |
| 4 | Gwiazdka | Domyślnie dokładne dopasowanie. Gwiazdka tylko na końcu aliasu, jako jawny wybór |
| 5 | Statystyki | `GET /stores/stats` zastępuje przejście po wszystkich transakcjach |

## Stan dzisiaj (wnioski z kodu)

- **Sklep to jednocześnie marka i surowa nazwa.** `Store.name` trzyma nazwę z banku, a `group_name` markę. Etap 1 grupuje po `group_name` we frontendzie.
- **Importer CSV dopasowuje dokładnie po znormalizowanym `Store.name`** (`bank_import.py`, `_store_lookup`) i świadomie nie zakłada nowych sklepów. Niedopasowana transakcja zostaje bez sklepu, a opis trafia do `bank_description`.
- **`Transaction.bank_description` istnieje, ale API go nie zwraca.** Dlatego kolumna „Nazwa z banku” w etapie 1 pokazuje `Store.name`.
- **Import YNAB używa id payee jako `Store.id`** (`import_payees_as_stores`, `s.get(Store, pid)`). Usunięcie scalonego sklepu sprawi, że kolejny import założy go od nowa. To główne ograniczenie projektu.
- **`Store.renaming_rules` nie jest używane** przez żaden serwis ani router.
- **Część scalania już istnieje:** ta sama nazwa grupowa łączy sklepy na liście, w tagowaniu i w wyszukiwarce. Zmiana nazwy na istniejącą w etapie 1 daje więc „miękkie” scalenie bez przenoszenia transakcji.
- **`DELETE /stores/{id}` nie sprawdza transakcji** i zeruje im `store_id`. Blokada w etapie 1 jest tylko po stronie frontendu.

## 10. Scalanie i aliasy

### Model danych

```
store_aliases
  id, user_id, store_id → stores.id ON DELETE CASCADE
  pattern           TEXT      -- tekst aliasu, jak go podano
  normalized        TEXT      -- wynik _normalize_name, po nim dopasowujemy
  match_type        TEXT      -- 'exact' | 'prefix'
  source            TEXT      -- 'store_name' | 'merge' | 'manual'
  created_at
  UNIQUE (user_id, normalized, match_type)

store_merges
  id, user_id, target_store_id, created_at, undone_at
  payload JSON     -- stan sprzed scalenia, patrz „Cofanie”

stores
  + merged_into_id → stores.id ON DELETE SET NULL
```

`UNIQUE` sprawia, że alias należy do jednego sklepu, a konflikt jest jawnym błędem 409, a nie cichym wyborem.

### Migracja

1. Dla każdego sklepu tworzymy alias `exact` ze `Store.name` (`source='store_name'`). Przy kolizji znormalizowanych nazw (czyli przy dzisiejszych duplikatach) alias dostaje sklep z największą liczbą transakcji. To ten sam sklep, którego dziś często wybiera słownik w `_store_lookup`, ale teraz wybór jest stabilny.
2. Sklepów nie scalamy automatycznie: duplikaty dalej porządkuje użytkownik w widoku „Do uporządkowania”.
3. Migracja jest addytywna, a `downgrade` usuwa nowe tabele i kolumnę. Przed migracją działa istniejąca kopia zapasowa.

### API

| Metoda | Ścieżka | Działanie |
|--------|---------|-----------|
| `POST` | `/stores/merge` | `{source_ids, target_id \| null, name, category_id}`. Jedna transakcja bazy. Zwraca sklep i `merge_id`. |
| `POST` | `/stores/merges/{id}/undo` | Odtwarza stan z `payload`. 409, gdy minęło 30 dni albo sklep docelowy też był już scalany. |
| `GET` | `/stores/{id}/aliases` | Lista z liczbą transakcji. |
| `POST` | `/stores/{id}/aliases` | `{pattern, match_type}`. 409 przy konflikcie. |
| `DELETE` | `/stores/{id}/aliases/{alias_id}` | „Odepnij”. Transakcje zostają przy sklepie, działa to tylko na przyszłe importy. |
| `GET` | `/stores/stats` | `[{store_id, count, spent_cents_12m, last_date}]`, patrz niżej. |

Scalenie w jednej transakcji bazy:

1. przepina `transactions.store_id`, `receipt_drafts.matched_store_id` i `product_aliases.store_id`;
2. przenosi aliasy źródeł do sklepu docelowego (`source='merge'`);
3. ustawia źródłom `merged_into_id` oraz `is_deleted=true`.

**Do sprawdzenia przy implementacji:**
- `uq_tx_receipt (store_id, date, receipt_number)` może się zderzyć po przepięciu. Wtedy cała operacja zwraca 409 z listą kolidujących paragonów.
- `uq_stores_user_nip`: gdy źródła mają różne NIP-y, zostaje NIP celu, a pozostałe pokazujemy przed zatwierdzeniem.
- `uq_product_alias_store` może się zderzyć przy przepinaniu aliasów produktów. Duplikaty usuwamy i zapisujemy w `payload`.

### Importer CSV

`_store_lookup` zastępuje nowy `match_store(name)` działający w kolejności:

1. alias `exact` po `normalized`;
2. alias `prefix`, wygrywa najdłuższy;
3. brak dopasowania: bez sklepu, tak jak dziś.

Sklepy zarchiwizowane i scalone są pomijane. Podgląd importu pokazuje, który alias zadziałał. Zmiany aliasów nie ruszają już zaimportowanych transakcji.

### YNAB

`import_payees_as_stores` przy trafieniu w sklep z `merged_into_id` idzie po łańcuchu do sklepu docelowego (z ochroną przed cyklem) i mapuje `pid` na niego. Dzięki temu scalony sklep nie wraca, a transakcje z YNAB trafiają do właściwego sklepu. Dlatego źródłowych wierszy **nie kasujemy**, nawet po wygaśnięciu cofania.

### Cofanie

`payload` trzyma pola źródeł i celu oraz listę id przepiętych transakcji. Cofnięcie przepina z powrotem tylko te transakcje i usuwa aliasy dodane przez scalenie. Nowe transakcje, które po scaleniu przyszły pod alias źródła, zostają w sklepie docelowym, a odpowiedź podaje ich liczbę. Po 30 dniach czyścimy `payload`, ale wiersz `stores` z `merged_into_id` zostaje dla YNAB.

### UI po etapie 2

- Odblokowane „Scal” w oknie i „Scal z…” na stronie sklepu, toast „Cofnij” po scaleniu.
- Edycja aliasów z makiety: „Odepnij” i „Dodaj”.
- Zmiana nazwy w etapie 1 nie wymaga zmian: nazwa grupowa zostaje nazwą wyświetlaną.
- Blokada usuwania na backendzie: `DELETE` zwraca 409, gdy sklep ma transakcje.

## 11. Czy „Przelewy własne” konwertować na przelewy między kontami?

**Tak, ale tylko na żądanie, z podglądem i tylko gdy druga strona jest pewna. Bez konwersji automatycznej.**

- Przelew zmienia raporty i budżet: znika wydatek i kategoria, dochodzi drugi wpis lub powiązanie (`transfer_account_id`, `transfer_transaction_id`). Błędne rozpoznanie po nazwisku w opisie (np. zwrot od osoby o tym samym nazwisku) zafałszowałoby liczby bez śladu.
- **Reguła parowania:** ta sama kwota z przeciwnym znakiem, inne konto użytkownika, data w oknie ±3 dni, dokładnie jedna kandydatka.
- **Trzy wyniki w podglądzie:**
  - para znaleziona: łączymy istniejące wpisy;
  - brak drugiej strony (konto nieśledzone): do wyboru konto docelowe, wpis zostaje jednostronny;
  - kilka kandydatek: nic nie robimy, użytkownik wybiera.
- Sklep po konwersji bez transakcji można zarchiwizować. Operacja ma cofanie na tej samej zasadzie co scalanie.
- Importer dostaje później regułę, która oznacza takie wiersze w podglądzie importu, ale ich nie konwertuje.

## 12. Aliasy z gwiazdką czy dokładne dopasowanie?

**Rekomendacja: domyślnie dokładne dopasowanie, a gwiazdka tylko na końcu (`BIEDRONKA*`), dodawana świadomie.**

| | Dokładne | Gwiazdka na końcu | Gwiazdka gdziekolwiek |
|---|---|---|---|
| Fałszywe trafienia | brak | rzadkie, widoczne w podglądzie | realne (`*ORLEN*`) |
| Warianty z numerem placówki | każdy osobno | jeden alias | jeden alias |
| Rozstrzyganie konfliktów | niepotrzebne | najdłuższy prefiks wygrywa | wymaga priorytetów |
| Wydajność | słownik | posortowane prefiksy | skan po wszystkich |

- Dokładne dopasowanie jest dziś zasadą importera („bez zgadywania”), a błędny sklep to błędna automatyczna kategoria.
- Same dokładne aliasy nie wystarczą, bo `BIEDRONKA 4521 KRAKOW` ma numer placówki, który się zmienia.
- Prefiks pokrywa ten przypadek bez kosztu składni ogólnej. Przypadki typu `JMP S.A. BIEDRONKA 1123` obsługuje osobny alias z prefiksem `JMP S.A. BIEDRONKA*`.
- **Zabezpieczenia:** prefiks ma mieć co najmniej 4 znaki bez gwiazdki, a przed zapisaniem UI pokazuje, które istniejące nazwy pasują („dopasuje 88 transakcji z 3 sklepów”).
- Makieta mówi dziś „Gwiazdka dopasowuje dowolny ciąg”. Tekst trzeba zmienić na „Gwiazdka na końcu dopasowuje dalszy ciąg nazwy”.

## Koszt statystyk i `GET /stores/stats`

Etap 1 pobiera całą historię, bo API nie ma agregatów.

| Pomiar | 514 sklepów, 3417 transakcji, backend lokalny |
|---|---|
| Żądania | 18 (1 + 17, po 4 naraz) |
| Dane | ok. 1,5 MB JSON, transakcje bez pozycji |
| Czas | ok. 0,55 s |

Koszt rośnie liniowo z liczbą transakcji i powtarza się przy każdym wejściu na listę. Na Home Assistancie (Raspberry Pi, Ingress) i z pozycjami paragonów będzie zauważalnie wyższy; tego nie mierzyłem.

Proponowany endpoint to jedno zapytanie `GROUP BY store_id`, z tymi samymi wykluczeniami co `_is_internal_report_transaction` i oknem liczonym po stronie serwera. Odpowiedź ma ok. 40 KB. Zmiana we frontendzie dotyczy tylko `useStoreStats.ts`. Przy okazji warto dodać do `/transactions` parametr `store_ids` i pole `bank_description`, co usuwa scalanie list po stronie przeglądarki oraz przybliżenie w kolumnie „Nazwa z banku”.
