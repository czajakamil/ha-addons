r"""Spike Enable Banking: podpięcie własnego konta, zrzut surowych transakcji i analiza.

Jednorazowe narzędzie badawcze, nie część aplikacji. Ma odpowiedzieć na trzy
pytania przed wpięciem API do importu (patrz rozmowa o Enable Banking):

1. czy bank podaje `entry_reference` / `transaction_id` i czy są stałe,
2. czy te identyfikatory pokrywają się z czymkolwiek w CSV z tego samego banku,
3. która data z API (`booking_date` / `value_date` / `transaction_date`)
   odpowiada dacie z CSV.

Przebieg:

    export EB_APP_ID=...                      # Application ID z panelu Enable Banking
    export EB_KEY_PATH=~/eb/<app-id>.pem      # klucz prywatny pobrany przy rejestracji

    uv run python -m backend.scripts.enable_banking_spike check
    uv run python -m backend.scripts.enable_banking_spike banks
    uv run python -m backend.scripts.enable_banking_spike auth --bank "mBank"
    #   → otwórz URL, zaloguj się w banku, skopiuj adres z paska po przekierowaniu
    uv run python -m backend.scripts.enable_banking_spike session "https://…?code=…&state=…"
    uv run python -m backend.scripts.enable_banking_spike dump --days 90
    uv run python -m backend.scripts.enable_banking_spike analyze <zrzut.json>
    uv run python -m backend.scripts.enable_banking_spike compare <zrzut.json> wyciag.csv \\
        --date-col "Data operacji" --amount-col "Kwota" --id-col "Numer referencyjny"
    #   po dniu albo dwóch: drugi `dump` i
    uv run python -m backend.scripts.enable_banking_spike stability <stary.json> <nowy.json>

Stan (sesja, zrzuty) ląduje w `finance_buddy/data/enable_banking/` — katalog
jest w .gitignore. Zrzuty zawierają prawdziwe transakcje: nie wrzucaj ich do repo.

Limit PSD2: bez nagłówków PSU bank wpuszcza ok. 4 pobrania dziennie na konto.
Każdy `dump` to jedno zapytanie o saldo (chyba że `--no-balances`) i co najmniej
jedno o transakcje — nie odpalaj go w pętli.
"""

from __future__ import annotations

import argparse
import base64
import datetime as dt
import hashlib
import json
import os
import secrets
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter, defaultdict
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa

from ..finance_app.services import bank_csv

BASE_URL = "https://api.enablebanking.com"
JWT_TTL_SECONDS = 3600
REQUEST_TIMEOUT_SECONDS = 30
MAX_PAGES = 100
DEFAULT_CONSENT_DAYS = 90
# Ta sama tolerancja co w transaction_matching.MATCH_WINDOW_DAYS.
MATCH_WINDOW_DAYS = 4

DEFAULT_STATE_DIR = Path(__file__).resolve().parents[2] / "data" / "enable_banking"


class SpikeError(RuntimeError):
    pass


# --- JWT ---------------------------------------------------------------------


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def load_private_key(path: Path) -> rsa.RSAPrivateKey:
    key = serialization.load_pem_private_key(path.expanduser().read_bytes(), password=None)
    if not isinstance(key, rsa.RSAPrivateKey):
        raise SpikeError(f"{path} nie jest kluczem RSA.")
    return key


def make_jwt(app_id: str, key: rsa.RSAPrivateKey, now: int | None = None) -> str:
    """JWT RS256, jakiego oczekuje Enable Banking: `kid` = Application ID."""
    issued = int(time.time()) if now is None else now
    header = {"typ": "JWT", "alg": "RS256", "kid": app_id}
    payload = {
        "iss": "enablebanking.com",
        "aud": "api.enablebanking.com",
        "iat": issued,
        "exp": issued + JWT_TTL_SECONDS,
    }
    signing_input = ".".join(
        _b64url(json.dumps(part, separators=(",", ":")).encode()) for part in (header, payload)
    )
    signature = key.sign(signing_input.encode("ascii"), padding.PKCS1v15(), hashes.SHA256())
    return f"{signing_input}.{_b64url(signature)}"


# --- HTTP --------------------------------------------------------------------


@dataclass
class Client:
    app_id: str
    key: rsa.RSAPrivateKey
    psu_headers: dict[str, str]

    def request(
        self,
        method: str,
        path: str,
        body: dict | None = None,
        *,
        psu: bool = False,
    ) -> Any:
        headers = {
            "Authorization": f"Bearer {make_jwt(self.app_id, self.key)}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        if psu:
            headers.update(self.psu_headers)
        data = json.dumps(body).encode() if body is not None else None
        # Stały host z BASE_URL, nie adres od użytkownika.
        req = urllib.request.Request(  # noqa: S310
            BASE_URL + path, data=data, headers=headers, method=method
        )
        try:
            with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT_SECONDS) as resp:  # noqa: S310
                return json.loads(resp.read() or b"null")
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", "replace")
            raise SpikeError(f"{method} {path} → HTTP {exc.code}: {detail}") from exc

    def transactions(
        self, account_uid: str, date_from: dt.date, date_to: dt.date
    ) -> tuple[list[dict], int]:
        """Wszystkie strony transakcji. Zwraca (transakcje, liczba zapytań)."""
        found: list[dict] = []
        key: str | None = None
        pages = 0
        while True:
            query = {"date_from": date_from.isoformat(), "date_to": date_to.isoformat()}
            if key:
                query["continuation_key"] = key
            path = (
                f"/accounts/{urllib.parse.quote(account_uid)}/transactions?"
                f"{urllib.parse.urlencode(query)}"
            )
            page = self.request("GET", path, psu=True)
            pages += 1
            found.extend(page.get("transactions") or [])
            next_key = page.get("continuation_key")
            # Zabezpieczenie z Actual: część banków potrafi oddać ten sam klucz.
            if not next_key or next_key == key or pages >= MAX_PAGES:
                return found, pages
            key = next_key


def client_from_args(args: argparse.Namespace) -> Client:
    app_id = args.app_id or os.environ.get("EB_APP_ID")
    key_path = args.key or os.environ.get("EB_KEY_PATH")
    if not app_id or not key_path:
        raise SpikeError("Podaj --app-id i --key albo ustaw EB_APP_ID i EB_KEY_PATH.")
    psu: dict[str, str] = {}
    if args.psu_ip:
        psu["Psu-Ip-Address"] = args.psu_ip
        psu["Psu-User-Agent"] = args.psu_user_agent
    return Client(app_id=app_id, key=load_private_key(Path(key_path)), psu_headers=psu)


# --- Stan na dysku -----------------------------------------------------------


def _write_json(path: Path, data: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    path.chmod(0o600)


def _read_json(path: Path) -> Any:
    if not path.exists():
        raise SpikeError(f"Brak pliku {path}.")
    return json.loads(path.read_text(encoding="utf-8"))


def parse_redirect(value: str) -> tuple[str, str | None]:
    """Wklejony adres po przekierowaniu (albo sam kod) → (code, state)."""
    value = value.strip()
    if "code=" not in value:
        return value, None
    query = urllib.parse.parse_qs(urllib.parse.urlsplit(value).query)
    if "error" in query:
        raise SpikeError(f"Bank zwrócił błąd: {query['error'][0]} {query.get('error_description')}")
    codes = query.get("code")
    if not codes:
        raise SpikeError("W adresie nie ma parametru code.")
    states = query.get("state")
    return codes[0], states[0] if states else None


# --- Normalizacja (jak w Actual, ale do groszy i ParsedRow-podobnej postaci) --


@dataclass
class Normalized:
    raw: dict
    tx_id: str | None  # entry_reference albo transaction_id, gdy bank podał
    derived_id: str | None  # odcisk, gdy bank nie podał żadnego
    status: str
    date: dt.date | None
    amount_cents: int | None
    currency: str | None
    payee: str | None
    memo: str | None
    problems: list[str]

    @property
    def external_id(self) -> str | None:
        if self.tx_id:
            return f"eb:{self.tx_id}"
        return f"eb-fp:{self.derived_id}" if self.derived_id else None


def _iso_date(raw: object) -> dt.date | None:
    if not isinstance(raw, str):
        return None
    try:
        return dt.date.fromisoformat(raw[:10])
    except ValueError:
        return None


def _cents(tx: dict) -> int | None:
    amount = (tx.get("transaction_amount") or {}).get("amount")
    if amount is None:
        return None
    try:
        value = Decimal(str(amount).strip())
    except InvalidOperation:
        return None
    cents = int((abs(value) * 100).to_integral_value())
    indicator = tx.get("credit_debit_indicator")
    if indicator == "DBIT":
        return -cents
    if indicator == "CRDT":
        return cents
    return cents if value >= 0 else -cents


def _counterparty(tx: dict) -> str | None:
    creditor = (tx.get("creditor") or {}).get("name")
    debtor = (tx.get("debtor") or {}).get("name")
    indicator = tx.get("credit_debit_indicator")
    if indicator == "DBIT":
        return creditor or None
    if indicator == "CRDT":
        return debtor or None
    return creditor or debtor or None


def _fingerprint(tx: dict) -> str:
    # Tylko surowe pola i bez `status` / `booking_date` — inaczej odcisk
    # zmieniałby się przy zaksięgowaniu (lekcja z Actual).
    content = json.dumps(
        [
            tx.get("transaction_date") or tx.get("value_date") or tx.get("booking_date") or "",
            (tx.get("transaction_amount") or {}).get("amount"),
            (tx.get("transaction_amount") or {}).get("currency"),
            tx.get("credit_debit_indicator") or "",
            (tx.get("creditor") or {}).get("name") or "",
            (tx.get("debtor") or {}).get("name") or "",
            tx.get("remittance_information") or [],
        ],
        ensure_ascii=False,
    )
    return hashlib.sha256(content.encode("utf-8")).hexdigest()[:32]


def normalize(transactions: list[dict]) -> list[Normalized]:
    result: list[Normalized] = []
    for tx in transactions:
        problems: list[str] = []
        date = _iso_date(
            tx.get("booking_date") or tx.get("value_date") or tx.get("transaction_date")
        )
        if date is None:
            problems.append("brak daty")
        cents = _cents(tx)
        if cents is None:
            problems.append("brak lub nieczytelna kwota")
        indicator = tx.get("credit_debit_indicator")
        if indicator not in ("CRDT", "DBIT"):
            problems.append("brak credit_debit_indicator")
        raw_amount = str((tx.get("transaction_amount") or {}).get("amount", "")).strip()
        if indicator == "CRDT" and raw_amount.startswith("-"):
            problems.append("CRDT z ujemną kwotą")
        remittance = [s.strip() for s in tx.get("remittance_information") or [] if s and s.strip()]
        payee = _counterparty(tx) or (remittance[0] if remittance else None)
        result.append(
            Normalized(
                raw=tx,
                tx_id=tx.get("entry_reference") or tx.get("transaction_id") or None,
                derived_id=None,
                status=tx.get("status") or "?",
                date=date,
                amount_cents=cents,
                currency=(tx.get("transaction_amount") or {}).get("currency"),
                payee=payee,
                memo=" · ".join(remittance) or None,
                problems=problems,
            )
        )
    # Zaksięgowane numerowane przed oczekującymi, powtórki dostają licznik.
    counts: Counter[str] = Counter()
    for booked in (True, False):
        for row in result:
            if row.tx_id or (row.status != "PDNG") != booked:
                continue
            base = _fingerprint(row.raw)
            row.derived_id = base if counts[base] == 0 else f"{base}-{counts[base]}"
            counts[base] += 1
    return result


# --- Analiza -----------------------------------------------------------------


def _pct(part: int, whole: int) -> str:
    return f"{part}/{whole} ({100 * part / whole:.0f}%)" if whole else "0/0"


def analyze_account(account: dict) -> list[str]:
    txs: list[dict] = account.get("transactions") or []
    rows = normalize(txs)
    lines = [
        (
            f"Konto {account.get('name') or account.get('uid')} "
            f"({account.get('iban') or '?'}, {account.get('currency') or '?'})"
        ),
        f"  transakcji: {len(rows)}, zapytań o transakcje: {account.get('pages', '?')}",
    ]
    if not rows:
        return [*lines, "  (pusto — sprawdź zakres dat albo zgodę)"]

    statuses = Counter(r.status for r in rows)
    lines.append(f"  status: {dict(statuses)}")
    with_entry = sum(1 for r in rows if r.raw.get("entry_reference"))
    with_txid = sum(1 for r in rows if r.raw.get("transaction_id"))
    without = sum(1 for r in rows if not r.tx_id)
    lines.append(f"  entry_reference: {_pct(with_entry, len(rows))}")
    lines.append(f"  transaction_id:  {_pct(with_txid, len(rows))}")
    lines.append(f"  bez żadnego id:  {_pct(without, len(rows))}")
    both = [r for r in rows if r.raw.get("entry_reference") and r.raw.get("transaction_id")]
    if both:
        same = sum(1 for r in both if r.raw["entry_reference"] == r.raw["transaction_id"])
        lines.append(f"  entry_reference == transaction_id: {_pct(same, len(both))}")

    id_counts = Counter(r.tx_id for r in rows if r.tx_id)
    dupes = {k: v for k, v in id_counts.items() if v > 1}
    if dupes:
        lines.append(
            f"  UWAGA: powtórzone id w jednej odpowiedzi: {len(dupes)} (np. {next(iter(dupes))})"
        )
    pdng_with_id = sum(1 for r in rows if r.status == "PDNG" and r.tx_id)
    if statuses.get("PDNG"):
        lines.append(f"  oczekujące z id: {_pct(pdng_with_id, statuses['PDNG'])}")

    for field_name in ("booking_date", "value_date", "transaction_date"):
        present = sum(1 for r in rows if r.raw.get(field_name))
        lines.append(f"  {field_name}: {_pct(present, len(rows))}")
    differ = sum(
        1
        for r in rows
        if r.raw.get("booking_date")
        and r.raw.get("transaction_date")
        and r.raw["booking_date"] != r.raw["transaction_date"]
    )
    if differ:
        lines.append(f"  booking_date ≠ transaction_date: {differ}")

    problems = Counter(p for r in rows for p in r.problems)
    if problems:
        lines.append(f"  problemy: {dict(problems)}")
    currencies = Counter(r.currency for r in rows)
    if len(currencies) > 1 or (account.get("currency") and account["currency"] not in currencies):
        lines.append(f"  waluty: {dict(currencies)}")
    dates = sorted(r.date for r in rows if r.date)
    if dates:
        lines.append(f"  zakres dat: {dates[0]} … {dates[-1]}")
    no_payee = sum(1 for r in rows if not r.payee)
    lines.append(f"  bez odbiorcy (ani w opisie): {_pct(no_payee, len(rows))}")

    keys = Counter(k for tx in txs for k in tx)
    lines.append(f"  pola w odpowiedzi: {', '.join(f'{k}({v})' for k, v in sorted(keys.items()))}")

    balances = account.get("balances")
    if isinstance(balances, list):
        for bal in balances:
            amount = (bal.get("balance_amount") or {}).get("amount")
            lines.append(
                f"  saldo {bal.get('balance_type')}: {amount} "
                f"({bal.get('reference_date') or bal.get('last_change_date_time') or '-'})"
            )
    elif account.get("balances_error"):
        lines.append(f"  saldo: błąd — {account['balances_error']}")
    return lines


# --- Porównanie z CSV --------------------------------------------------------


@dataclass
class CsvRow:
    parsed: bank_csv.ParsedRow
    cells: list[str]


def read_csv(path: Path, args: argparse.Namespace) -> list[CsvRow]:
    overrides = {
        "encoding": args.encoding,
        "delimiter": args.delimiter,
        "header_row": args.header_row,
    }
    table = bank_csv.read_table(
        path.read_bytes(), {k: v for k, v in overrides.items() if v is not None}
    )
    header = [bank_csv.normalize_header_name(h) for h in table.header]

    def index_of(name: str) -> int:
        wanted = bank_csv.normalize_header_name(name)
        if wanted not in header:
            raise SpikeError(f"Brak kolumny {name!r}. Kolumny w pliku: {table.header}")
        return header.index(wanted)

    columns: dict[int, str] = {index_of(args.date_col): bank_csv.ROLE_DATE}
    if args.amount_col:
        columns[index_of(args.amount_col)] = bank_csv.ROLE_AMOUNT
    if args.credit_col:
        columns[index_of(args.credit_col)] = bank_csv.ROLE_CREDIT
    if args.debit_col:
        columns[index_of(args.debit_col)] = bank_csv.ROLE_DEBIT
    if args.id_col:
        columns[index_of(args.id_col)] = bank_csv.ROLE_EXTERNAL_ID
    if args.payee_col:
        columns[index_of(args.payee_col)] = bank_csv.ROLE_PAYEE
    mapping = bank_csv.Mapping.from_dict(
        {
            "columns": columns,
            "amount_mode": bank_csv.AMOUNT_SPLIT
            if (args.credit_col or args.debit_col)
            else bank_csv.AMOUNT_SIGNED,
        }
    )
    errors = mapping.errors(table.width)
    if errors:
        raise SpikeError("; ".join(errors))
    options = bank_csv.resolve_value_formats(table, mapping)
    cells_by_line = dict(table.rows)
    return [
        CsvRow(parsed=row, cells=cells_by_line.get(row.line, []))
        for row in bank_csv.apply_mapping(table, mapping, options)
        if row.status == bank_csv.ROW_OK
    ]


def compare(api_rows: list[Normalized], csv_rows: list[CsvRow]) -> list[str]:
    """Paruje po kwocie i najbliższej dacie (±4 dni), potem sprawdza identyfikatory."""
    booked = [r for r in api_rows if r.status != "PDNG" and r.date and r.amount_cents is not None]
    if not booked or not csv_rows:
        return ["Brak danych do porównania (zaksięgowane z API albo poprawne wiersze CSV)."]
    lo = min(r.date for r in booked if r.date)
    hi = max(r.date for r in booked if r.date)
    # Margines jak przy parowaniu: CSV może mieć datę transakcji, a API księgowania.
    pad = dt.timedelta(days=MATCH_WINDOW_DAYS)
    in_range = [c for c in csv_rows if c.parsed.date and lo - pad <= c.parsed.date <= hi + pad]

    by_amount: dict[int, list[CsvRow]] = defaultdict(list)
    for c in in_range:
        by_amount[c.parsed.amount_cents or 0].append(c)
    taken: set[int] = set()
    pairs: list[tuple[Normalized, CsvRow]] = []
    for r in sorted(booked, key=lambda x: x.date or dt.date.min):
        best: CsvRow | None = None
        best_gap: int | None = None
        for c in by_amount.get(r.amount_cents or 0, []):
            if id(c) in taken or c.parsed.date is None or r.date is None:
                continue
            gap = abs((c.parsed.date - r.date).days)
            if gap <= MATCH_WINDOW_DAYS and (best_gap is None or gap < best_gap):
                best, best_gap = c, gap
        if best is not None:
            taken.add(id(best))
            pairs.append((r, best))

    lines = [
        f"Okres API (zaksięgowane): {lo} … {hi}",
        f"  z API: {len(booked)}, z CSV w tym okresie: {len(in_range)}, sparowanych: {len(pairs)}",
        f"  tylko w API: {len(booked) - len(pairs)}, tylko w CSV: {len(in_range) - len(pairs)}",
    ]
    if not pairs:
        return lines

    for field_name in ("booking_date", "value_date", "transaction_date"):
        hits = sum(1 for r, c in pairs if _iso_date(r.raw.get(field_name)) == c.parsed.date)
        lines.append(f"  data CSV == {field_name}: {_pct(hits, len(pairs))}")

    with_csv_id = [
        (r, c) for r, c in pairs if c.parsed.external_id and c.parsed.external_id.startswith("id:")
    ]
    if with_csv_id:
        for field_name in ("entry_reference", "transaction_id"):
            hits = sum(
                1
                for r, c in with_csv_id
                if r.raw.get(field_name) and f"id:{r.raw[field_name]}" == c.parsed.external_id
            )
            lines.append(f"  id z CSV == {field_name}: {_pct(hits, len(with_csv_id))}")
    else:
        lines.append("  (bez --id-col nie porównuję identyfikatorów 1:1)")

    # Identyfikator banku może siedzieć w innej kolumnie albo w opisie.
    anywhere = 0
    for r, c in pairs:
        ids = [str(r.raw[f]) for f in ("entry_reference", "transaction_id") if r.raw.get(f)]
        if any(i and any(i in cell for cell in c.cells) for i in ids):
            anywhere += 1
    with_any_id = sum(1 for r, _ in pairs if r.tx_id)
    lines.append(f"  id z API występuje gdziekolwiek w wierszu CSV: {_pct(anywhere, with_any_id)}")

    sample = next(((r, c) for r, c in pairs if r.tx_id), pairs[0])
    lines.append("  przykład pary (sprawdź, czy to ta sama transakcja):")
    api, row = sample
    lines.append(f"    API: {api.date} {api.amount_cents} id={api.tx_id} {api.payee!r}")
    lines.append(f"    CSV: {row.parsed.date} {row.parsed.amount_cents} {row.cells}")
    return lines


# --- Stabilność między zrzutami ---------------------------------------------


def stability(old: dict, new: dict) -> list[str]:
    lines: list[str] = []
    new_accounts = {a["uid"]: a for a in new.get("accounts", [])}
    for old_acc in old.get("accounts", []):
        new_acc = new_accounts.get(old_acc["uid"])
        label = old_acc.get("name") or old_acc["uid"]
        if new_acc is None:
            lines.append(f"Konto {label}: brak w nowym zrzucie (inna sesja → inne uid).")
            continue
        old_rows = normalize(old_acc.get("transactions") or [])
        new_rows = normalize(new_acc.get("transactions") or [])
        # Wspólny okres, żeby nie liczyć jako „zniknięte" tego, co wypadło z zakresu dat.
        new_dates = [r.date for r in new_rows if r.date]
        if not new_dates:
            lines.append(f"Konto {label}: nowy zrzut pusty.")
            continue
        start = min(new_dates)
        old_in = [r for r in old_rows if r.date and r.date >= start]
        new_ids = {r.external_id for r in new_rows}
        kept = [r for r in old_in if r.external_id in new_ids]
        lost = [r for r in old_in if r.external_id not in new_ids]
        lines.append(f"Konto {label}: wspólny okres od {start}")
        lines.append(f"  stare id obecne w nowym zrzucie: {_pct(len(kept), len(old_in))}")
        by_status = Counter(r.status for r in lost)
        if lost:
            lines.append(
                f"  zniknęły / zmieniły id: {len(lost)} według starego statusu {dict(by_status)}"
            )
            # Odpowiednik to tylko NOWE id — nie transakcja, która już była w starym zrzucie.
            old_ids = {r.external_id for r in old_rows}
            new_by_key = defaultdict(list)
            for r in (n for n in new_rows if n.external_id not in old_ids):
                new_by_key[(r.amount_cents, r.payee)].append(r)
            for r in lost[:5]:
                twins = [
                    n
                    for n in new_by_key[(r.amount_cents, r.payee)]
                    if n.date and r.date and abs((n.date - r.date).days) <= MATCH_WINDOW_DAYS
                ]
                twin = twins[0] if twins else None
                after = f"→ {twin.status} {twin.external_id}" if twin else "→ brak odpowiednika"
                lines.append(f"    {r.status} {r.date} {r.amount_cents} {r.external_id} {after}")
    return lines or ["Brak kont do porównania."]


# --- Komendy -----------------------------------------------------------------


def cmd_check(args: argparse.Namespace) -> None:
    app = client_from_args(args).request("GET", "/application")
    print(json.dumps(app, ensure_ascii=False, indent=2))


def cmd_banks(args: argparse.Namespace) -> None:
    data = client_from_args(args).request("GET", f"/aspsps?country={args.country}")
    for aspsp in data.get("aspsps", []):
        days = (aspsp.get("maximum_consent_validity") or 0) // 86400
        psu = ",".join(aspsp.get("psu_types") or [])
        print(f"{aspsp['name']:<40} zgoda max {days:>3} dni  psu={psu}")


def cmd_auth(args: argparse.Namespace) -> None:
    client = client_from_args(args)
    redirect = args.redirect
    if not redirect:
        urls = client.request("GET", "/application").get("redirect_urls") or []
        if not urls:
            raise SpikeError("Aplikacja nie ma redirect URL — podaj --redirect.")
        redirect = urls[0]
    aspsps = client.request("GET", f"/aspsps?country={args.country}").get("aspsps", [])
    aspsp = next((a for a in aspsps if a["name"].casefold() == args.bank.casefold()), None)
    if aspsp is None:
        raise SpikeError(f"Nie ma banku {args.bank!r} w {args.country}. Zobacz komendę `banks`.")
    max_seconds = aspsp.get("maximum_consent_validity") or DEFAULT_CONSENT_DAYS * 86400
    days = min(args.consent_days or max_seconds // 86400, max_seconds // 86400)
    valid_until = dt.datetime.now(dt.UTC) + dt.timedelta(days=days)
    state = secrets.token_urlsafe(16)
    resp = client.request(
        "POST",
        "/auth",
        {
            "access": {"valid_until": valid_until.isoformat()},
            "aspsp": {"name": aspsp["name"], "country": args.country},
            "state": state,
            "redirect_url": redirect,
            "psu_type": args.psu_type,
        },
        psu=True,
    )
    _write_json(
        args.state_dir / "pending_auth.json",
        {"state": state, "aspsp": aspsp["name"], "valid_until": valid_until.isoformat()},
    )
    print(f"Zgoda na {days} dni (do {valid_until:%Y-%m-%d}), powrót na {redirect}")
    print("Otwórz w przeglądarce i zaloguj się w banku:\n")
    print(resp["url"])
    print('\nPotem skopiuj CAŁY adres z paska i uruchom: session "<adres>"')


def cmd_session(args: argparse.Namespace) -> None:
    code, state = parse_redirect(args.redirect)
    pending_path = args.state_dir / "pending_auth.json"
    pending = _read_json(pending_path) if pending_path.exists() else {}
    if state and pending.get("state") and state != pending["state"]:
        raise SpikeError("state z adresu nie zgadza się z ostatnim `auth` — to nie ta autoryzacja.")
    session = client_from_args(args).request("POST", "/sessions", {"code": code})
    session["aspsp_name"] = pending.get("aspsp")
    _write_json(args.state_dir / "session.json", session)
    pending_path.unlink(missing_ok=True)
    print(f"Sesja zapisana w {args.state_dir / 'session.json'}")
    for acc in session.get("accounts", []):
        iban = (acc.get("account_id") or {}).get("iban")
        print(
            f"  {acc['uid']}  {iban or '-'}  {acc.get('name') or '-'}  {acc.get('currency') or '-'}"
        )


def cmd_dump(args: argparse.Namespace) -> None:
    client = client_from_args(args)
    session = _read_json(args.state_dir / "session.json")
    date_to = dt.datetime.now(dt.UTC).date()
    date_from = date_to - dt.timedelta(days=args.days)
    dump: dict = {
        "fetched_at": dt.datetime.now(dt.UTC).isoformat(),
        "aspsp": session.get("aspsp_name") or (session.get("aspsp") or {}).get("name"),
        "date_from": date_from.isoformat(),
        "date_to": date_to.isoformat(),
        "psu_headers": bool(client.psu_headers),
        "accounts": [],
    }
    for acc in session.get("accounts", []):
        if args.account and acc["uid"] != args.account:
            continue
        entry: dict = {
            "uid": acc["uid"],
            "iban": (acc.get("account_id") or {}).get("iban"),
            "name": acc.get("name"),
            "currency": acc.get("currency"),
        }
        txs, pages = client.transactions(acc["uid"], date_from, date_to)
        entry["transactions"], entry["pages"] = txs, pages
        if not args.no_balances:
            try:
                path = f"/accounts/{urllib.parse.quote(acc['uid'])}/balances"
                entry["balances"] = client.request("GET", path, psu=True).get("balances")
            except SpikeError as exc:
                # Część banków odrzuca saldo — transakcje i tak chcemy mieć.
                entry["balances_error"] = str(exc)
        dump["accounts"].append(entry)

    stamp = dt.datetime.now(dt.UTC).strftime("%Y%m%dT%H%M%SZ")
    out = args.state_dir / "dumps" / f"{stamp}.json"
    _write_json(out, dump)
    print(f"Zrzut: {out}\n")
    for acc in dump["accounts"]:
        print("\n".join(analyze_account(acc)))
        print()


def cmd_analyze(args: argparse.Namespace) -> None:
    dump = _read_json(Path(args.dump))
    period = f"{dump.get('date_from')} … {dump.get('date_to')}"
    print(f"{dump.get('aspsp')}: {period}, pobrano {dump.get('fetched_at')}\n")
    for acc in dump.get("accounts", []):
        print("\n".join(analyze_account(acc)))
        print()


def cmd_compare(args: argparse.Namespace) -> None:
    if not args.amount_col and not (args.credit_col or args.debit_col):
        raise SpikeError("Podaj --amount-col albo --credit-col/--debit-col.")
    dump = _read_json(Path(args.dump))
    accounts = dump.get("accounts", [])
    if args.account:
        accounts = [a for a in accounts if args.account in (a["uid"], a.get("iban"))]
    if len(accounts) != 1:
        raise SpikeError("Zrzut ma kilka kont — wskaż jedno przez --account (uid albo IBAN).")
    csv_rows = read_csv(Path(args.csv), args)
    print("\n".join(compare(normalize(accounts[0].get("transactions") or []), csv_rows)))


def cmd_stability(args: argparse.Namespace) -> None:
    print("\n".join(stability(_read_json(Path(args.old)), _read_json(Path(args.new)))))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--app-id", help="Application ID (domyślnie $EB_APP_ID)")
    parser.add_argument("--key", help="Ścieżka do klucza .pem (domyślnie $EB_KEY_PATH)")
    parser.add_argument("--state-dir", type=Path, default=DEFAULT_STATE_DIR)
    parser.add_argument("--country", default="PL")
    parser.add_argument("--psu-ip", help="Twój publiczny IP — zdejmuje limit pobrań w tle")
    parser.add_argument("--psu-user-agent", default="Mozilla/5.0 finance-buddy-spike")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("check", help="Sprawdź klucz i pokaż ustawienia aplikacji").set_defaults(
        func=cmd_check
    )
    sub.add_parser("banks", help="Lista banków w kraju").set_defaults(func=cmd_banks)

    auth = sub.add_parser("auth", help="Zacznij zgodę w banku")
    auth.add_argument("--bank", required=True, help="Nazwa z komendy `banks`")
    auth.add_argument("--redirect", help="Domyślnie pierwszy redirect URL aplikacji")
    auth.add_argument("--consent-days", type=int, help="Domyślnie maksimum banku")
    auth.add_argument("--psu-type", default="personal", choices=("personal", "business"))
    auth.set_defaults(func=cmd_auth)

    session = sub.add_parser("session", help="Dokończ zgodę wklejonym adresem")
    session.add_argument("redirect", help="Pełny adres po przekierowaniu albo sam code")
    session.set_defaults(func=cmd_session)

    dump = sub.add_parser("dump", help="Pobierz transakcje i salda do pliku")
    dump.add_argument("--days", type=int, default=90)
    dump.add_argument("--account", help="Tylko to uid")
    dump.add_argument("--no-balances", action="store_true")
    dump.set_defaults(func=cmd_dump)

    analyze = sub.add_parser("analyze", help="Statystyki zrzutu")
    analyze.add_argument("dump")
    analyze.set_defaults(func=cmd_analyze)

    comp = sub.add_parser("compare", help="Porównaj zrzut z CSV z banku")
    comp.add_argument("dump")
    comp.add_argument("csv")
    comp.add_argument("--account", help="uid albo IBAN, gdy w zrzucie jest kilka kont")
    comp.add_argument("--date-col", required=True)
    comp.add_argument("--amount-col")
    comp.add_argument("--credit-col")
    comp.add_argument("--debit-col")
    comp.add_argument("--id-col")
    comp.add_argument("--payee-col")
    comp.add_argument("--encoding")
    comp.add_argument("--delimiter")
    comp.add_argument("--header-row", type=int)
    comp.set_defaults(func=cmd_compare)

    stab = sub.add_parser("stability", help="Czy id są te same w dwóch zrzutach")
    stab.add_argument("old")
    stab.add_argument("new")
    stab.set_defaults(func=cmd_stability)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        args.func(args)
    except (SpikeError, bank_csv.BankCsvError) as exc:
        print(f"Błąd: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
