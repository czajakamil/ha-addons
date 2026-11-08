# Enable Banking — synchronizacja z bankiem przez PSD2

Stan: **integracja w aplikacji (0.1.28)** — `services/enable_banking.py` (klient),
`services/bank_sync.py` (zgody, pobieranie), Panel → Połączenia z bankiem.
Narzędzie testowe: `backend/scripts/enable_banking_spike.py` (instrukcja w docstringu).

## 1. Dlaczego Enable Banking

- Darmowy do użytku prywatnego: produkcja tylko dla kont, które sam podpinasz
  w panelu Enable Banking. Udostępnienie innym wymaga umowy i KYB.
- GoCardless Bank Account Data (dawniej Nordigen) nie przyjmuje nowych
  rejestracji od lipca 2025 — praktycznie nie ma darmowej alternatywy.
- Ten sam wybór zrobiły Actual Budget (PR actualbudget/actual#7345, maj 2026)
  i Firefly III Data Importer (v2.1.0). Kod Actual
  (`packages/sync-server/src/app-enablebanking/`) to nasz wzorzec.

## 2. API w skrócie

| Krok | Wywołanie |
|---|---|
| Uwierzytelnienie | JWT RS256, `kid` = Application ID, `iss: enablebanking.com`, `aud: api.enablebanking.com`, ważny 1 h |
| Weryfikacja klucza | `GET /application` |
| Banki | `GET /aspsps?country=PL` (z `maximum_consent_validity`) |
| Zgoda | `POST /auth` → URL do banku → powrót na `redirect_url?code&state` → `POST /sessions {code}` |
| Dane | `GET /accounts/{uid}/transactions?date_from&date_to` (+ `continuation_key`), `GET /accounts/{uid}/balances` |

## 3. Wyniki testu na PKO BP (listopad 2026)

| Pytanie | Wynik |
|---|---|
| Identyfikator | `entry_reference` = `O;N`, numer kolejnej operacji na koncie. `transaction_id` to jego base64 — nie niezależny id |
| Stały przy innym `date_from` | **tak** (zrzut 90 i 30 dni: 45/45 tych samych id) |
| Stały w czasie / po ponownej zgodzie | **do potwierdzenia** (`stability` na zrzutach z różnych dni; po ~90 dniach po reautoryzacji) |
| Oczekujące (`PDNG`) | nie są zwracane — mimo blokad widocznych jako różnica `ITBD` − `ITAV` |
| Daty | tylko `booking_date`; `value_date` i `transaction_date` puste |
| Salda | `ITBD`, `ITAV` (bez `CLBD`) |
| Dodatkowe pola | `balance_after_transaction` przy każdej operacji, `merchant_category_code`, `reference_number` (pusty) |
| Historia | od założenia konta (konto młodsze niż 90 dni, więc limit historii nieznany) |

## 4. Decyzje dla integracji

- **Tożsamość transakcji:** `external_id = "eb:" + entry_reference`. Gdy bank
  nie poda id — odcisk z surowych pól jak w Actual (bez `status` i
  `booking_date`), wzbogacony o `balance_after_transaction`. Jeśli test w czasie
  wykaże przesuwanie numeracji PKO, odcisk staje się podstawą także dla PKO.
- **Import przez istniejące sita** (`services/bank_import.py`): wiersz z API →
  `ParsedRow`; `analyze()` trzeba rozdzielić na parsowanie CSV i sita.
- **Pierwsze pobranie** sięga 90 dni wstecz, ale nie wcześniej niż `sync_from`
  rachunku — domyślnie dzień po ostatniej transakcji z CSV na koncie. Wiersze
  z CSV mają inne `external_id`, a sito 2 celowo pomija transakcje z banku,
  więc bez tej granicy pierwsze pobranie zdublowałoby wyciąg.
- **Tylko zaksięgowane.** Okno pobierania: od ostatniej zaksięgowanej daty
  minus ~7 dni; duplikaty wycina sito 1.
- **Kompletność:** porównanie `balance_after_transaction` i salda `ITBD`
  z saldem konta w aplikacji.
- **Powrót z banku przy Ingress:** dodatek nie ma publicznego adresu i mieć nie
  będzie. Redirect URL aplikacji to `https://czajakamil.github.io/ha-addons/eb-callback`
  (strona nie musi istnieć); użytkownik wkleja adres z paska. Sam `code` jest
  bezużyteczny bez klucza prywatnego.
- **Klucz `.pem` i App ID:** wgrywane w Ustawieniach, sprawdzane przez
  `GET /application`, zapisywane przez `keyring` (`/data/.keyring`, jak klucz
  YNAB). Nie w `/share`, nie w opcjach dodatku. Kopie aplikacji w `/share`
  klucza nie zawierają; kopie HA (z `/data`) — tak.
- **Limit PSD2:** 4 pobrania / 24 h na rachunek bez obecności użytkownika
  (RTS 2018/389 art. 36 ust. 5), liczony przez każdy bank osobno. Synchronizacja
  w tle 1–2×/dobę, saldo raz dziennie. Nagłówki `Psu-Ip-Address` /
  `Psu-User-Agent` tylko przy akcji użytkownika („Pobierz teraz”) — nigdy w tle.
- **Zgoda:** min(180 dni, `maximum_consent_validity` banku); błędy 401/403,
  `expired_session`, `closed_session` → stan „połącz ponownie”. Nowa sesja daje
  nowe `uid` kont — powiązanie konta musi dać się przepiąć.

## 5. Lekcje z Actual

1. Brak id → usunięte transakcje wracają przy każdej synchronizacji; stąd odcisk.
2. Oczekujące bez daty pomijać zamiast przerywać synchronizację.
3. Znak kwoty z `credit_debit_indicator`; odbiorca: `creditor` przy `DBIT`,
   `debtor` przy `CRDT`, w ostateczności pierwsza linia opisu.
4. Saldo bywa odrzucane — transakcje pobierać mimo to.
5. Część banków odrzuca nagłówki PSU przy pobieraniu danych; prywatnych IP nie wysyłać.
6. Pętla stronicowania z zabezpieczeniem na powtórzony `continuation_key`.
