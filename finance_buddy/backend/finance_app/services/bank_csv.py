"""Parser wyciągów bankowych CSV — czyste funkcje, bez bazy.

Jedyne miejsce, które zamienia tekst z pliku na daty i grosze. Frontend niczego
tu nie liczy, tylko pokazuje wynik: dwie implementacje (Python i TS) mogłyby
pokazać w podglądzie co innego, niż trafi do bazy, a przy pieniądzach to
niedopuszczalne. Projekt: docs/design/import-csv.md §3.

Przepływ: `read_table` (bajty → nagłówek + wiersze, z wykrywaniem kodowania,
separatora i wiersza nagłówka), potem `apply_mapping` (wiersze → `ParsedRow`
według ról kolumn). Każde wykrycie da się nadpisać — wartości trafiają do
profilu, żeby kolejny import z tego banku nie zgadywał od nowa.
"""

from __future__ import annotations

import csv
import datetime as dt
import hashlib
import io
import re
from collections import Counter
from dataclasses import dataclass, field
from decimal import Decimal

# --- Stałe -------------------------------------------------------------------

DELIMITERS = (";", ",", "\t", "|")
ENCODINGS = ("utf-8", "cp1250", "iso-8859-2")

# Kolejność ma znaczenie: przy niejednoznaczności (żaden dzień > 12) wygrywa
# pierwszy pasujący, a polskie banki piszą dzień przed miesiącem.
DATE_FORMATS = (
    "%Y-%m-%d",
    "%d.%m.%Y",
    "%d-%m-%Y",
    "%d/%m/%Y",
    "%m/%d/%Y",
    "%Y/%m/%d",
    "%Y.%m.%d",
)
TIME_FORMATS = ("%H:%M:%S", "%H:%M")

ROLE_DATE = "date"
ROLE_AMOUNT = "amount"
ROLE_CREDIT = "credit"
ROLE_DEBIT = "debit"
ROLE_PAYEE = "payee"
ROLE_MEMO = "memo"
ROLE_CURRENCY = "currency"
ROLE_FEE = "fee"
ROLE_STATUS = "status"
ROLE_EXTERNAL_ID = "external_id"
ROLE_IGNORE = "ignore"
ROLES = (
    ROLE_DATE,
    ROLE_AMOUNT,
    ROLE_CREDIT,
    ROLE_DEBIT,
    ROLE_PAYEE,
    ROLE_MEMO,
    ROLE_CURRENCY,
    ROLE_FEE,
    ROLE_STATUS,
    ROLE_EXTERNAL_ID,
    ROLE_IGNORE,
)
# Tylko opis może się składać z kilku kolumn (ING: „Tytuł" + „Szczegóły").
MULTI_ROLES = frozenset({ROLE_MEMO, ROLE_IGNORE})

AMOUNT_SIGNED = "signed"
AMOUNT_SIGNED_INVERTED = "signed_inverted"
AMOUNT_SPLIT = "split"
AMOUNT_MODES = (AMOUNT_SIGNED, AMOUNT_SIGNED_INVERTED, AMOUNT_SPLIT)

MAX_ROWS = 20_000

# Typograficzny minus (U+2212) — część banków i arkuszy wstawia go zamiast `-`.
MINUS_SIGN = "\N{MINUS SIGN}"

ROW_OK = "ok"
ROW_SKIPPED = "skipped"
ROW_INVALID = "invalid"


class BankCsvError(ValueError):
    """Pliku nie da się w ogóle odczytać jako tabeli — komunikat dla użytkownika."""


# --- Kodowanie, separator, nagłówek -----------------------------------------


def decode(content: bytes, encoding: str | None = None) -> tuple[str, str]:
    """Tekst pliku i użyte kodowanie.

    BOM albo poprawne UTF-8 → UTF-8; w przeciwnym razie cp1250, bo to jedyne
    inne kodowanie, w jakim polskie banki (ING) faktycznie eksportują. Ścisłe
    UTF-8 na pliku cp1250 z polskimi znakami praktycznie zawsze się wywraca,
    więc test jest wiarygodny.
    """
    if encoding:
        if encoding not in ENCODINGS:
            raise BankCsvError(f"Nieobsługiwane kodowanie: {encoding}")
        codec = "utf-8-sig" if encoding == "utf-8" else encoding
        return content.decode(codec, errors="replace"), encoding
    try:
        return content.decode("utf-8-sig"), "utf-8"
    except UnicodeDecodeError:
        return content.decode("cp1250", errors="replace"), "cp1250"


def _records(text: str, delimiter: str) -> list[list[str]]:
    reader = csv.reader(io.StringIO(text), delimiter=delimiter)
    return list(reader)


def _modal_width(records: list[list[str]]) -> tuple[int, int]:
    """Najczęstsza liczba pól wśród wierszy z ≥2 polami i ile wierszy ją ma."""
    widths = Counter(len(r) for r in records if len(r) >= 2)
    if not widths:
        return 0, 0
    # Przy remisie liczności wygrywa szersza tabela.
    width, count = max(widths.items(), key=lambda kv: (kv[1], kv[0]))
    return width, count


def detect_delimiter(text: str) -> str:
    """Separator, przy którym najwięcej wierszy ma tę samą, ≥2 liczbę pól.

    Preambuła i stopka (ING) mają po jednym polu przy każdym separatorze, więc
    nie przeszkadzają. Przecinek dziesiętny w nieujętych w cudzysłów kwotach ING
    rozjeżdża liczbę pól przy `,`, więc `;` wygrywa uczciwie.
    """
    best = DELIMITERS[0]
    best_score = (-1, -1)
    for delimiter in DELIMITERS:
        width, count = _modal_width(_records(text, delimiter))
        score = (count, width) if width >= 2 else (0, 0)
        if score > best_score:
            best, best_score = delimiter, score
    return best


def _looks_like_data(cells: list[str]) -> bool:
    """Wiersz z datą w którejś komórce to dane, nie nagłówek."""
    return any(_date_format_for(c) for c in cells if c.strip())


def detect_header_row(records: list[list[str]]) -> int:
    """Indeks wiersza nagłówka albo -1, gdy plik zaczyna się od razu od danych.

    Nagłówek to pierwszy wiersz o modalnej szerokości tabeli. Wiersze
    preambuły (ING: „Lista transakcji", „Za okres…") są węższe.
    """
    width, _ = _modal_width(records)
    if width < 2:
        raise BankCsvError("Plik nie wygląda na tabelę CSV — nie znalazłem kolumn.")
    for index, row in enumerate(records):
        if len(row) == width:
            return -1 if _looks_like_data(row) else index
    return -1


def normalize_header_name(name: str) -> str:
    return " ".join(name.strip().lower().split())


def header_signature(header: list[str]) -> str:
    """Odcisk nagłówka do rozpoznania profilu — nazwy po normalizacji."""
    return "\x1f".join(normalize_header_name(h) for h in header)


@dataclass(frozen=True)
class ParseOptions:
    encoding: str
    delimiter: str
    header_row: int
    date_format: str | None = None
    decimal_separator: str | None = None

    def as_dict(self) -> dict:
        return {
            "encoding": self.encoding,
            "delimiter": self.delimiter,
            "header_row": self.header_row,
            "date_format": self.date_format,
            "decimal_separator": self.decimal_separator,
        }


@dataclass
class CsvTable:
    options: ParseOptions
    header: list[str]
    # (numer wiersza w pliku liczony od 1, komórki)
    rows: list[tuple[int, list[str]]]
    sha256: str

    @property
    def width(self) -> int:
        return len(self.header)

    def column_values(self, index: int) -> list[str]:
        return [cells[index].strip() for _, cells in self.rows if index < len(cells)]


def read_table(content: bytes, overrides: dict | None = None) -> CsvTable:
    """Bajty pliku → nagłówek i wiersze danych, z wykrytymi ustawieniami.

    `overrides` to ustawienia z profilu albo z UI; brak klucza (lub `None`)
    oznacza „wykryj". Format daty i separator dziesiętny wykrywa dopiero
    `resolve_value_formats`, bo potrzebuje wiedzieć, które kolumny to daty i
    kwoty.
    """
    overrides = overrides or {}
    if not content.strip():
        raise BankCsvError("Plik jest pusty.")
    text, encoding = decode(content, overrides.get("encoding"))
    delimiter = overrides.get("delimiter") or detect_delimiter(text)
    if delimiter not in DELIMITERS:
        raise BankCsvError(f"Nieobsługiwany separator: {delimiter!r}")
    records = _records(text, delimiter)

    header_row = overrides.get("header_row")
    if header_row is None:
        header_row = detect_header_row(records)
    if header_row >= len(records):
        raise BankCsvError("Wskazany wiersz nagłówka jest poza plikiem.")

    if header_row >= 0:
        header = [h.strip() for h in records[header_row]]
        start = header_row + 1
    else:
        width, _ = _modal_width(records)
        header = [f"Kolumna {i + 1}" for i in range(width)]
        start = 0

    rows = [
        (line + 1, cells)
        for line, cells in enumerate(records)
        if line >= start and any(c.strip() for c in cells)
    ]
    if len(rows) > MAX_ROWS:
        raise BankCsvError(f"Plik ma ponad {MAX_ROWS} wierszy — podziel go na mniejsze.")

    return CsvTable(
        options=ParseOptions(
            encoding=encoding,
            delimiter=delimiter,
            header_row=header_row,
            date_format=overrides.get("date_format"),
            decimal_separator=overrides.get("decimal_separator"),
        ),
        header=header,
        rows=rows,
        sha256=hashlib.sha256(content).hexdigest(),
    )


# --- Daty --------------------------------------------------------------------


def _split_date_time(raw: str) -> tuple[str, str | None]:
    text = raw.strip()
    for sep in ("T", " "):
        if sep in text:
            head, _, tail = text.partition(sep)
            return head, tail.strip() or None
    return text, None


def _date_format_for(raw: str) -> str | None:
    head, _ = _split_date_time(raw)
    for fmt in DATE_FORMATS:
        try:
            dt.datetime.strptime(head, fmt)  # noqa: DTZ007 — bierzemy tylko datę
        except ValueError:
            continue
        return fmt
    return None


def parse_date(raw: str, date_format: str) -> tuple[dt.date, dt.time | None]:
    """Data i (jeśli jest) godzina. Godzinę, której nie umiemy odczytać, pomijamy."""
    head, tail = _split_date_time(raw)
    day = dt.datetime.strptime(head, date_format).date()  # noqa: DTZ007 — tylko data
    moment = None
    if tail:
        for fmt in TIME_FORMATS:
            try:
                moment = dt.datetime.strptime(tail, fmt).time()  # noqa: DTZ007 — tylko godzina
                break
            except ValueError:
                continue
    return day, moment


def detect_date_format(values: list[str]) -> str | None:
    """Pierwszy format, który parsuje WSZYSTKIE niepuste wartości kolumny.

    Stopka czy wiersz spoza tabeli nie może zepsuć wykrycia, więc liczymy
    tylko wartości, które w ogóle wyglądają na datę w jakimś formacie.
    """
    candidates = [v for v in values if v and _date_format_for(v)]
    if not candidates:
        return None
    for fmt in DATE_FORMATS:
        try:
            for value in candidates:
                parse_date(value, fmt)
        except ValueError:
            continue
        return fmt
    return None


# --- Kwoty -------------------------------------------------------------------

_AMOUNT_NOISE = re.compile(r"[^\d.,()+\-]")


def detect_decimal_separator(values: list[str]) -> str:
    """Separator dziesiętny z próbek: ostatni `,`/`.` z 1–2 cyframi po nim.

    Trzy cyfry po separatorze to raczej tysiące (`1.234`), więc nie głosują.
    Brak głosów → przecinek, bo to polska konwencja.
    """
    votes: Counter[str] = Counter()
    for value in values:
        cleaned = _AMOUNT_NOISE.sub("", value.replace(MINUS_SIGN, "-"))
        match = re.search(r"([.,])(\d+)\)?-?$", cleaned)
        if match and len(match.group(2)) in (1, 2):
            votes[match.group(1)] += 1
    if not votes:
        return ","
    return votes.most_common(1)[0][0]


def parse_amount(raw: str, decimal_separator: str) -> int:
    """Kwota w groszach. Pusta wartość lub śmieci → `ValueError`.

    Rozumie: separator tysięcy (spacja, NBSP, apostrof lub przeciwny do
    dziesiętnego, ale tylko w poprawnych grupach po 3 — `12.5` przy przecinku
    dziesiętnym to błąd, a nie 125 zł), minus jako `-` lub U+2212, minus na końcu
    (`45,99-`), nawiasy księgowe i dopisany kod waluty. Więcej niż dwa miejsca
    po przecinku to błąd, nie zaokrąglenie.
    """
    text = raw.strip().replace(MINUS_SIGN, "-")
    if not text:
        raise ValueError("pusta kwota")
    negative = False
    text = re.sub(r"[\s\u00a0'\u2019]", "", text)
    text = re.sub(r"[A-Za-zł€$£zł]+$", "", text)
    text = re.sub(r"^[A-Za-zł€$£]+", "", text)
    if text.startswith("(") and text.endswith(")"):
        negative = True
        text = text[1:-1]
    if text.endswith("-"):
        negative = not negative
        text = text[:-1]
    if text.startswith("-"):
        negative = not negative
        text = text[1:]
    elif text.startswith("+"):
        text = text[1:]

    dec = re.escape(decimal_separator)
    thousands = re.escape("." if decimal_separator == "," else ",")
    pattern = rf"^(?:\d{{1,3}}(?:{thousands}\d{{3}})+|\d+)(?:{dec}(\d{{1,2}}))?$"
    match = re.match(pattern, text)
    if not match:
        raise ValueError(f"nieczytelna kwota: {raw.strip()!r}")
    integer_part = re.split(dec, text)[0]
    integer_part = re.sub(thousands, "", integer_part)
    fraction = (match.group(1) or "").ljust(2, "0")
    cents = int(Decimal(integer_part) * 100) + int(fraction or 0)
    return -cents if negative else cents


# --- Mapowanie -----------------------------------------------------------------


@dataclass
class Mapping:
    columns: dict[int, str]
    amount_mode: str = AMOUNT_SIGNED
    status_accepted: list[str] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: dict) -> Mapping:
        columns = {
            int(k): v for k, v in (data.get("columns") or {}).items() if v and v != ROLE_IGNORE
        }
        return cls(
            columns=columns,
            amount_mode=data.get("amount_mode") or AMOUNT_SIGNED,
            status_accepted=[s.strip().upper() for s in data.get("status_accepted") or [] if s],
        )

    def columns_for(self, role: str) -> list[int]:
        return sorted(i for i, r in self.columns.items() if r == role)

    def column_for(self, role: str) -> int | None:
        found = self.columns_for(role)
        return found[0] if found else None

    def errors(self, width: int) -> list[str]:
        """Błędy mapowania po polsku. Pusta lista = mapowanie kompletne."""
        problems: list[str] = []
        for index, role in self.columns.items():
            if role not in ROLES:
                problems.append(f"Nieznana rola kolumny: {role}")
            if index < 0 or index >= width:
                problems.append(f"Kolumna {index + 1} nie istnieje w pliku.")
        counts = Counter(self.columns.values())
        problems.extend(
            f"Rola „{role}” przypisana do więcej niż jednej kolumny."
            for role, count in counts.items()
            if count > 1 and role not in MULTI_ROLES
        )
        if self.amount_mode not in AMOUNT_MODES:
            problems.append(f"Nieznany tryb kwoty: {self.amount_mode}")
        if ROLE_DATE not in counts:
            problems.append("Wskaż kolumnę z datą.")
        if self.amount_mode == AMOUNT_SPLIT:
            if ROLE_CREDIT not in counts and ROLE_DEBIT not in counts:
                problems.append("Wskaż kolumny uznań i obciążeń.")
        elif ROLE_AMOUNT not in counts:
            problems.append("Wskaż kolumnę z kwotą.")
        return problems


def resolve_value_formats(table: CsvTable, mapping: Mapping) -> ParseOptions:
    """Uzupełnia format daty i separator dziesiętny, jeśli nie są narzucone."""
    options = table.options
    date_format = options.date_format
    date_col = mapping.column_for(ROLE_DATE)
    if not date_format and date_col is not None:
        date_format = detect_date_format(table.column_values(date_col))
    decimal_separator = options.decimal_separator
    if not decimal_separator:
        amount_cols = [
            i
            for role in (ROLE_AMOUNT, ROLE_CREDIT, ROLE_DEBIT, ROLE_FEE)
            for i in mapping.columns_for(role)
        ]
        samples = [v for i in amount_cols for v in table.column_values(i) if v]
        decimal_separator = detect_decimal_separator(samples)
    return ParseOptions(
        encoding=options.encoding,
        delimiter=options.delimiter,
        header_row=options.header_row,
        date_format=date_format,
        decimal_separator=decimal_separator,
    )


@dataclass
class ParsedRow:
    index: int  # pozycja wśród wierszy danych — identyfikator wiersza w decyzjach
    line: int  # numer wiersza w pliku, dla komunikatów
    status: str
    reason: str | None = None
    date: dt.date | None = None
    time: dt.time | None = None
    amount_cents: int | None = None
    currency: str | None = None
    payee: str | None = None
    memo: str | None = None
    external_id: str | None = None
    raw: list[str] = field(default_factory=list)

    @property
    def description(self) -> str:
        return " · ".join(part for part in (self.payee, self.memo) if part)


def _cell(cells: list[str], index: int | None) -> str:
    if index is None or index >= len(cells):
        return ""
    return cells[index].strip()


def row_fingerprint(cells: list[str]) -> str:
    """Odcisk surowego wiersza — niezależny od mapowania.

    Zmiana mapowania (np. dołożenie kolumny do opisu) nie może sprawić, że
    stare importy przestaną być rozpoznawane, dlatego liczymy z całego wiersza,
    a nie ze zmapowanych pól.
    """
    joined = "\x1f".join(c.strip() for c in cells)
    # Identyfikator, nie zabezpieczenie — sha1 wystarcza i daje krótszy klucz.
    return hashlib.sha1(joined.encode("utf-8"), usedforsecurity=False).hexdigest()


def apply_mapping(table: CsvTable, mapping: Mapping, options: ParseOptions) -> list[ParsedRow]:
    """Wiersze danych → `ParsedRow` ze statusem ok / skipped / invalid.

    `skipped` to wiersze, które świadomie nie są transakcjami (stopka, blokada
    bez kwoty, status spoza listy); `invalid` — takie, które powinny nimi być,
    a nie dają się odczytać.
    """
    date_col = mapping.column_for(ROLE_DATE)
    status_col = mapping.column_for(ROLE_STATUS)
    currency_col = mapping.column_for(ROLE_CURRENCY)
    fee_col = mapping.column_for(ROLE_FEE)
    payee_col = mapping.column_for(ROLE_PAYEE)
    memo_cols = mapping.columns_for(ROLE_MEMO)
    ext_col = mapping.column_for(ROLE_EXTERNAL_ID)
    needed_width = max(mapping.columns, default=-1) + 1
    decimal = options.decimal_separator or ","

    occurrences: Counter[str] = Counter()
    result: list[ParsedRow] = []
    for index, (line, cells) in enumerate(table.rows):
        row = ParsedRow(index=index, line=line, status=ROW_OK, raw=cells)
        result.append(row)

        if len(cells) < needed_width:
            row.status, row.reason = ROW_SKIPPED, "Wiersz spoza tabeli."
            continue

        raw_date = _cell(cells, date_col)
        if not raw_date:
            row.status, row.reason = ROW_SKIPPED, "Brak daty — wiersz spoza tabeli."
            continue

        if status_col is not None and mapping.status_accepted:
            state = _cell(cells, status_col).upper()
            if state not in mapping.status_accepted:
                row.status, row.reason = ROW_SKIPPED, f"Status {state or '(pusty)'}."
                continue

        if not options.date_format:
            row.status, row.reason = ROW_INVALID, "Nie rozpoznałem formatu daty."
            continue
        try:
            row.date, row.time = parse_date(raw_date, options.date_format)
        except ValueError:
            row.status, row.reason = ROW_INVALID, f"Nieczytelna data: {raw_date!r}."
            continue

        try:
            amount = _row_amount(cells, mapping, decimal)
        except ValueError as exc:
            row.status, row.reason = ROW_INVALID, f"Kwota: {exc}."
            continue
        if amount is None:
            row.status, row.reason = ROW_SKIPPED, "Brak kwoty (np. blokada środków)."
            continue
        raw_fee = _cell(cells, fee_col)
        if raw_fee:
            try:
                amount -= abs(parse_amount(raw_fee, decimal))
            except ValueError as exc:
                row.status, row.reason = ROW_INVALID, f"Opłata: {exc}."
                continue
        row.amount_cents = amount

        row.currency = _cell(cells, currency_col).upper() or None
        row.payee = _cell(cells, payee_col) or None
        memo = " · ".join(v for v in (_cell(cells, i) for i in memo_cols) if v)
        row.memo = memo or None

        bank_id = _cell(cells, ext_col)
        if bank_id:
            row.external_id = f"id:{bank_id}"
        else:
            fingerprint = row_fingerprint(cells)
            row.external_id = f"fp:{fingerprint}#{occurrences[fingerprint]}"
            occurrences[fingerprint] += 1
    return result


def _row_amount(cells: list[str], mapping: Mapping, decimal: str) -> int | None:
    """Kwota wiersza ze znakiem (wydatek < 0) albo None, gdy pusta."""
    if mapping.amount_mode == AMOUNT_SPLIT:
        raw_credit = _cell(cells, mapping.column_for(ROLE_CREDIT))
        raw_debit = _cell(cells, mapping.column_for(ROLE_DEBIT))
        if not raw_credit and not raw_debit:
            return None
        credit = abs(parse_amount(raw_credit, decimal)) if raw_credit else 0
        debit = abs(parse_amount(raw_debit, decimal)) if raw_debit else 0
        return credit - debit
    raw = _cell(cells, mapping.column_for(ROLE_AMOUNT))
    if not raw:
        return None
    value = parse_amount(raw, decimal)
    return -value if mapping.amount_mode == AMOUNT_SIGNED_INVERTED else value
