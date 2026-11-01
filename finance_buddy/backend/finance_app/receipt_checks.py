"""Kontrole spójności etykiety paragonu — jedna reguła dla wszystkich odbiorców.

Odbiorcy: skrzynka paragonów w aplikacji (ostrzega przy wpisywaniu, blokuje
zatwierdzenie), labeler datasetu przez `tools/dataset_checks.py`,
`receipt_eval/fix_labels.py` (naprawa już zapisanych etykiet) i pomiar
(wykluczanie skażonych pól z oceny).

Reguła jest **niezależna od OCR** — patrzy wyłącznie na etykietę i na datę
skanu. Gdyby zaglądała do odczytu silnika, „wartość jest zła" znaczyłoby tylko
tyle, co „wartość nie zgadza się z modelem", i przestałaby być kontrolą.

Skąd to się wzięło: formularz podsuwa wartości odczytane z obrazu, a
zatwierdzenie podpowiedzi wygląda tak samo jak wpisanie własnej wartości. Tak
do datasetu trafiły daty typu `7024-06-19` i zamienione dzień z miesiącem
(`dayfirst` narzucony na datę ISO).

Moduł jest czysto stdlib-owy i nie importuje niczego z `finance_app` — dzięki
temu narzędzia spoza aplikacji wczytują go po ścieżce, bez ciągnięcia całego
backendu. Bliźniacza implementacja w TypeScripcie (`ui/src/utils/receiptChecks.ts`)
odpowiada na te same pytania po stronie formularza; obie sprawdza ten sam
zestaw przypadków w `shared/receipt_checks_cases.json`.
"""

from __future__ import annotations

import datetime as dt
import re
from dataclasses import dataclass

# Skan nie może powstać przed zakupem, a paragony trafiają pod skaner w ciągu
# tygodni — nie miesięcy.
SCAN_WINDOW_DAYS = 60

# Tolerancja domknięcia pozycji z sumą paragonu. Luźniejsza niż grosz z
# podglądu na żywo: tu pytamy „czy etykieta jest skażona", a nie „czy właśnie
# skończyłeś przepisywać".
ITEM_SUM_TOLERANCE_RATIO = 0.01
ITEM_SUM_TOLERANCE_MIN_CENTS = 2

_SCAN_DATE_RE = re.compile(r"(20\d{2})[_-](\d{2})[_-](\d{2})")
_DATE_RE = re.compile(r"\d{4}-\d{2}-\d{2}")


@dataclass(frozen=True)
class LabelIssue:
    """Zastrzeżenie do pola etykiety.

    `level` "bad" znaczy, że wartość jest przy tych danych **niemożliwa** (data
    późniejsza niż skan, NIP bez sumy kontrolnej) — takiego zapisu formularz nie
    przepuszcza bez świadomego potwierdzenia. "warn" to wartość możliwa, ale
    podejrzana.

    `code` jest stabilnym identyfikatorem reguły: tym samym posługuje się
    formularz, potwierdzając blokadę imiennie (`acknowledged_issues`), i ten sam
    trzymają fikstury kontraktowe. Zmiana kodu to zmiana kontraktu API.

    `suggestion` to gotowa wartość zastępcza; wypełniana tylko tam, gdzie
    poprawka wynika z reguły, a nie ze zgadywania (patrz `inspect_nip`).
    """

    level: str
    code: str
    message: str
    suggestion: str | None = None

    def as_dict(self, field: str | None = None) -> dict:
        payload = {
            "level": self.level,
            "code": self.code,
            "message": self.message,
            "suggestion": self.suggestion,
        }
        if field is not None:
            payload["field"] = field
        return payload


def date_part(value: str | None) -> str | None:
    """Sama data z `happened_at` (`2024-06-19T20:44` → `2024-06-19`)."""
    if not value:
        return None
    head = value.strip()[:10]
    return head if _DATE_RE.fullmatch(head) else None


def scan_date(source: str | None) -> dt.date | None:
    """Data skanu z nazwy pliku źródłowego (`PDFs/Converted_2024_06_11.pdf`)."""
    match = _SCAN_DATE_RE.search(source or "")
    if not match:
        return None
    try:
        return dt.date(*(int(part) for part in match.groups()))
    except ValueError:
        return None


def _as_date(day_text: str) -> dt.date | None:
    try:
        return dt.date(int(day_text[:4]), int(day_text[5:7]), int(day_text[8:10]))
    except ValueError:
        return None


def _swap_day_and_month(day: dt.date) -> dt.date | None:
    try:
        return dt.date(day.year, day.day, day.month)
    except ValueError:
        return None


def _in_scan_window(day: dt.date, scanned: dt.date) -> bool:
    return day <= scanned and (scanned - day).days <= SCAN_WINDOW_DAYS


def _with_date(timestamp: str, day: dt.date) -> str:
    """Podmienia część datową, zostawiając godzinę bez zmian."""
    return f"{day.isoformat()}{timestamp[10:]}"


def inspect_date_against(happened_at: str | None, scanned: dt.date | None) -> LabelIssue | None:
    """Ocena daty etykiety wobec znanej daty skanu; `None`, gdy bez zastrzeżeń.

    To jest właściwa reguła; `inspect_date` to wygodna nakładka wyciągająca datę
    skanu z nazwy pliku. Aplikacja podaje datę wprost, bo zna ją lepiej: gdy w
    nazwie pliku jej nie ma, granicą jest moment wgrania paragonu.
    """
    day_text = date_part(happened_at)
    if not day_text:
        return None
    current = _as_date(day_text)
    if current is None:
        return None

    if scanned is None:
        # Bez daty skanu zostaje sam zdrowy rozsądek co do roku.
        if not 2000 <= current.year <= dt.datetime.now(dt.UTC).year + 1:
            return LabelIssue(
                "bad", "date_year_out_of_range", f"rok {current.year} jest niemożliwy"
            )
        return None

    assert happened_at is not None  # noqa: S101 - date_part zwróciło wartość
    if _in_scan_window(current, scanned):
        swapped = _swap_day_and_month(current)
        if (
            swapped
            and _in_scan_window(swapped, scanned)
            and (scanned - swapped).days < (scanned - current).days
        ):
            return LabelIssue(
                "warn",
                "date_day_month_ambiguous",
                f"obie daty możliwe, ale {swapped} leży bliżej skanu ({scanned}) niż {current}",
                _with_date(happened_at, swapped),
            )
        return None

    reason = (
        f"{current} jest po dacie skanu ({scanned})"
        if current > scanned
        else f"{(scanned - current).days} dni różnicy między podaną datą ({current}) "
        f"a datą skanu ({scanned})"
    )

    # Najpierw zamiana dnia z miesiącem (najczęstsza przyczyna), potem podmiana
    # roku. Kandydatów jest garść i przyjmujemy tylko tego, który wpada w okno.
    swapped = _swap_day_and_month(current)
    if swapped and _in_scan_window(swapped, scanned):
        return LabelIssue(
            "bad",
            "date_outside_scan_window",
            f"{reason} — wygląda na zamienione dzień z miesiącem",
            _with_date(happened_at, swapped),
        )
    for candidate_year in (scanned.year, scanned.year - 1):
        try:
            fixed = dt.date(candidate_year, current.month, current.day)
        except ValueError:
            continue
        if _in_scan_window(fixed, scanned):
            return LabelIssue(
                "bad",
                "date_outside_scan_window",
                f"{reason} — wygląda na przekłamany rok",
                _with_date(happened_at, fixed),
            )
    return LabelIssue("bad", "date_outside_scan_window", reason)


def inspect_date(happened_at: str | None, source: str | None) -> LabelIssue | None:
    """Ocena daty, gdy datę skanu trzeba dopiero wyczytać z nazwy pliku."""
    return inspect_date_against(happened_at, scan_date(source))


# Wagi sumy kontrolnej NIP-u (zduplikowane z parsera świadomie: kontrola
# etykiet ma nie zależeć od kodu silnika, który sama ocenia).
_NIP_WEIGHTS = (6, 5, 7, 2, 3, 4, 5, 6, 7)


def inspect_nip(nip: str | None) -> LabelIssue | None:
    """Zastrzeżenie, gdy NIP nie przechodzi sumy kontrolnej.

    Suma kontrolna to własność samego numeru — jeśli się nie zgadza, wartość
    ma przekłamaną cyfrę (przepisaną ręcznie albo z zatwierdzonego pre-fillu),
    niezależnie od tego, co odczytał jakikolwiek silnik. Dlatego poziom to
    "bad": prawidłowy polski NIP zawsze ją spełnia.

    **Świadomie bez `suggestion`**, choć parser potrafi naprawić pojedynczą
    cyfrę (`repair_nip`). Tam naprawa jest ratunkiem dla odczytu maszynowego,
    który i tak trafia do przeglądu; tutaj podsunięcie numeru z sufitu byłoby
    gorsze niż brak podpowiedzi — poprawek dających poprawną sumę kontrolną
    jest zwykle kilka, a numer wygląda potem na zweryfikowany i nikt do niego
    nie wróci. Właściwą poprawką jest odczytanie NIP-u z obrazu.
    """
    digits = re.sub(r"\D", "", nip or "")
    if not digits:
        return None
    if len(digits) != 10:
        return LabelIssue("bad", "nip_length", f"NIP ma {len(digits)} cyfr zamiast 10")
    checksum = sum(w * int(d) for w, d in zip(_NIP_WEIGHTS, digits, strict=False)) % 11
    if checksum != 10 and checksum == int(digits[9]):
        return None
    return LabelIssue(
        "bad",
        "nip_checksum",
        f"NIP {digits} nie przechodzi sumy kontrolnej — sprawdź cyfry na paragonie",
    )


def items_balance_cents(label: dict) -> int | None:
    """Do czego sumują się pozycje etykiety, razem z rabatami; `None`, gdy nie da się policzyć.

    Niezmiennik z dokumentacji labelera::

        Σ(wartość pozycji + rabat pozycji) + rabat na paragon = suma

    Rabaty trzymamy ujemne, więc wystarczy dodawanie. `discounts_declared_cents`
    („udzielono łącznie rabatów") **nie wchodzi do rachunku** — to suma
    kontrolna rabatów już przypisanych do pozycji, a nie kolejny rabat.
    """
    items = label.get("items") or []
    values = [item.get("line_total_cents") for item in items]
    if not values or any(value is None for value in values):
        return None
    balance = sum(values)
    for item in items:
        discount = item.get("discount_cents")
        if discount:
            balance -= abs(discount)
    receipt_discount = label.get("receipt_discount_cents")
    if receipt_discount:
        balance -= abs(receipt_discount)
    return balance


def inspect_item_sum(label: dict) -> LabelIssue | None:
    """Zastrzeżenie, gdy suma pozycji etykiety nie domyka się z jej sumą.

    Reguła czysto wewnętrzna (arytmetyka samej etykiety, zero OCR). Rozjazd
    oznacza zwykle zatwierdzony pre-fill z przesuniętymi wierszami — ten sam
    mechanizm skażenia, który wcześniej dotknął daty.

    Poziom "warn", nie "bad": rozjazd bywa też legalny (rabat, którego nie da
    się przypisać do pozycji), więc zapisu nie blokujemy — w odróżnieniu od
    NIP-u, gdzie zła suma kontrolna nie ma legalnego wytłumaczenia. Stopka
    formularza i tak pokazuje różnicę na bieżąco.

    Uwaga na ograniczenie: przesunięcie wartości między pozycjami (permutacja
    z pre-fillu) nie zmienia sumy i tędy nie wyjdzie — tę klasę skażenia widać
    dopiero okiem na obrazie.
    """
    total = label.get("total_cents")
    balance = items_balance_cents(label)
    if total is None or balance is None:
        return None
    # Bez zaokrąglania: `round()` w Pythonie idzie do parzystej, a `Math.round`
    # w JS w górę, więc na połówce grosza (suma 2,50 zł → tolerancja 2,5) obie
    # implementacje odpowiadałyby inaczej na ten sam paragon.
    tolerance = max(float(ITEM_SUM_TOLERANCE_MIN_CENTS), abs(total) * ITEM_SUM_TOLERANCE_RATIO)
    difference = balance - total
    if abs(difference) <= tolerance:
        return None
    return LabelIssue(
        "warn",
        "items_sum_mismatch",
        f"pozycje sumują się do {balance / 100:.2f}, a suma etykiety to "
        f"{total / 100:.2f} (różnica {difference / 100:+.2f}) — rabat "
        "niezapisany w polach rabatowych albo błędna wartość którejś pozycji",
    )


# Braki, których żadna reguła nie wyliczy — trzeba zajrzeć w obraz.
_MISSING_CHECKS: tuple[tuple[str, str, str], ...] = (
    ("store_name", "missing_store", "brak nazwy sklepu"),
    ("happened_at", "missing_date", "brak daty"),
    ("total_cents", "missing_total", "brak sumy"),
    ("items", "missing_items", "brak pozycji"),
)


def _is_missing(label: dict, field: str) -> bool:
    if field == "store_name":
        return not (label.get("store_name") or "").strip()
    if field == "happened_at":
        return not date_part(label.get("happened_at"))
    if field == "total_cents":
        return label.get("total_cents") is None
    return not (label.get("items") or [])


def missing_fields(label: dict) -> list[str]:
    """Komunikaty o brakach (zgodność wsteczna dla narzędzi datasetu)."""
    return [text for field, _code, text in _MISSING_CHECKS if _is_missing(label, field)]


def label_issues(label: dict) -> list[dict]:
    """Wszystkie zastrzeżenia do etykiety, w formie gotowej dla UI i API.

    Datę skanu bierze z `scan_date` (aplikacja podaje ją wprost), a gdy jej nie
    ma — z nazwy pliku w `source`.
    """
    issues = [
        LabelIssue("warn", code, text).as_dict(field)
        for field, code, text in _MISSING_CHECKS
        if _is_missing(label, field)
    ]

    scanned = label.get("scan_date")
    if isinstance(scanned, str):
        scanned = _as_date(scanned) if _DATE_RE.fullmatch(scanned[:10] or "") else None
    if scanned is None:
        scanned = scan_date(label.get("source"))

    checks = (
        ("happened_at", inspect_date_against(label.get("happened_at"), scanned)),
        ("nip", inspect_nip(label.get("nip"))),
        ("items", inspect_item_sum(label)),
    )
    for field, issue in checks:
        if issue:
            issues.append(issue.as_dict(field))
    return issues


def blocking_issues(label: dict) -> list[dict]:
    """Same zastrzeżenia, które wymagają świadomej decyzji przed zapisem."""
    return [issue for issue in label_issues(label) if issue["level"] == "bad"]
