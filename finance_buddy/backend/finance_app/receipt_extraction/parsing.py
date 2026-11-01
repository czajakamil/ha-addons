"""Czyste parsowanie tekstu OCR polskiego paragonu fiskalnego.

Moduł nie dotyka Tesseracta ani obrazów — dostaje surowy tekst i zwraca
`ReceiptExtraction`. Dzięki temu całą logikę regexów testujemy bez OCR.

Obsługiwane formaty linii pozycji (ilość, cena jednostkowa, wartość, litera VAT):

    MLEKO UHT 3,2% 1L 2 x3,49 6,98A
    BANAN LUZ 0,455 * 6,99 3,18 C
    CHLEB WIEJSKI 1 szt. x 5,99 5,99A

Nazwa złamana do drugiej linii (linia bez ceny doklejana do następnej):

    SER GOUDA PLASTRY 150G
    1 x7,49 7,49A

Rabaty dopinane do pozycji, której dotyczą (`ExtractedItem.discount_cents`),
nie jako osobne wpisy — "Rabat -1,50" po linii towaru obniża jego pozycję,
a nie dodaje nową:

    CHLEB WIEJSKI 1 szt. x 5,99 5,99A
    Rabat -1,50
"""

from __future__ import annotations

import datetime as dt
import difflib
import re
from collections.abc import Sequence
from decimal import Decimal, InvalidOperation

from dateutil import parser as date_parser

from .base import ExtractedItem, ReceiptExtraction

# NIP bywa drukowany w dowolnym grupowaniu (777-10-11-327, 813-219-94-74),
# więc bierzemy 10 cyfr z dowolnymi separatorami między nimi. Wariant "po
# słowie NIP" ma pierwszeństwo nad dowolnym dziesięciocyfrowcem na paragonie,
# bo tym drugim bywa numer wydruku.
_NIP_DIGITS = r"(?:\d[-\s.]?){9}\d"
NIP_LABELLED_REGEX = re.compile(rf"(?i)nip[^0-9]{{0,5}}({_NIP_DIGITS})")
NIP_LOOSE_REGEX = re.compile(rf"(?<!\d)({_NIP_DIGITS})(?!\d)")

# Wagi sumy kontrolnej NIP-u (9 cyfr znaczących, dziesiąta to cyfra kontrolna).
NIP_WEIGHTS = (6, 5, 7, 2, 3, 4, 5, 6, 7)

# Pary cyfr, które Tesseract myli na wydrukach termicznych. Zaobserwowane na
# korpusie: 7→1, 7→2, 8→6, 8→3, 6→5. Reszta to klasyka OCR.
_DIGIT_CONFUSIONS: dict[str, tuple[str, ...]] = {
    "0": ("8", "9", "6"),
    "1": ("7", "4"),
    "2": ("7", "3"),
    "3": ("8", "9", "2"),
    "4": ("1", "9"),
    "5": ("6", "8"),
    "6": ("8", "5", "0"),
    "7": ("1", "2"),
    "8": ("6", "3", "0", "5"),
    "9": ("4", "3", "0"),
}
# `(?!\d)` za rokiem: "20.01.20125" (rok z doklejoną cyfrą) nie może ujść za
# "20.01.2012" — lepszy brak daty niż pewnie wyglądająca nieprawda.
DATETIME_REGEX = re.compile(
    r"(?P<date>\d{4}[./-]\d{2}[./-]\d{2}|\d{2}[./-]\d{2}[./-]\d{4}(?!\d))"
    r"[ T]?(?P<time>\d{2}:\d{2}(?::\d{2})?)?"
)
NAME_BLACKLIST = (
    "paragon",
    "paragun",
    "fiskal",
    "sprzed",
    "nabyw",
    "kasa",
    "suma",
    "rachunek",
    "faktura",
    "numer",
)

# Dodatkowe słowa, które dyskwalifikują linię jako pozycję. Osobno od
# NAME_BLACKLIST, bo tamta lista filtruje też nazwy sklepów i nie chcemy jej
# obciążać słownictwem podsumowania.
ITEM_NAME_BLACKLIST = (
    *NAME_BLACKLIST,
    "zapłaty",
    "zaplaty",
    "zapłacono",
    "zaplacono",
    "ptacono",
    "gotówka",
    "gotowka",
    "karta",
    "reszta",
    "razem",
    "podsuma",
    "ptu",
    "nip",
    "rozliczenie",
    "cena",
    # Apteki drukują "Bez recepty" w linii cen pod nazwą leku — to nie nazwa.
    "recept",
)

# Wiersze podsumowania stawek i rabatów, które OCR zniekształca na tyle, że
# proste dopasowanie słowa ich nie łapie ("PTU"→"PIU", "OBNIŻEK"→"OONIŻEK",
# "SPRZEDAŻ OPODATKOWANA"→"SPRZECAŹ QPOUATKOWANA"). Prefiksy zamiast całych
# słów, bo końcówki rozjeżdżają się najczęściej.
SUMMARY_LINE_REGEX = re.compile(
    r"(?i)\b(kwota|p[itl]u|opust|obni|o[a-z]?od[ae]tk|opodatk|uwzgl|uuzgl|podsu|sp\.?op)"
)

# Kwota w formacie paragonowym: przecinek lub kropka jako separator groszy.
# OCR bywa wstawia spację za separatorem ("3, 99"), więc jest tolerowana.
_MONEY = r"\d+(?:[.,]\s?\d{1,2})"

# Za kwotą stoi litera stawki PTU, którą OCR regularnie myli z cyfrą lub
# znakiem interpunkcyjnym ("4,99C" bywa czytane jako "4,990", "61.94л" —
# cyrylicą). Nie używamy tej litery do niczego, więc dopuszczamy dowolny
# pojedynczy znak niebędący częścią kwoty — inaczej cała pozycja przepada
# przez jeden przekłamany znak.
_VAT_SUFFIX = r"[^\s.,]?"

# Znaki, które OCR podstawia za symbol mnożenia między ilością a ceną.
_TIMES = r"[x*×«»]"

_UNITS = r"szt[.,]?|opak\.?|kg|g|l|ml"

# Linia pozycji: NAZWA [ilość] [jednostka] x/* cena wartość [litera VAT].
ITEM_LINE_REGEX = re.compile(
    rf"""
    ^(?P<name>.+?)\s+
    (?P<qty>\d+(?:[.,]\d+)?)\s*
    (?P<unit>{_UNITS})?\s*
    {_TIMES}\s*
    (?P<unit_price>{_MONEY})\s+
    (?P<total>-?{_MONEY})\s*
    (?P<vat>{_VAT_SUFFIX})\s*$
    """,
    re.IGNORECASE | re.VERBOSE,
)

# Warianty rozluźnione tego samego wzorca. Każdy brakujący element czyni
# dopasowanie mniej pewnym, więc wynik wariantu przechodzi dodatkową bramkę
# arytmetyczną (ilość × cena ≈ wartość) — regex bez tej bramki łapałby
# wiersze podsumowania. Kolejność prób: od najściślejszego do najluźniejszego.
#
# Ilość zgubiona przez OCR ("PIZZA GUSEPPE x22,00 22,00C") — odtwarzana
# z dzielenia wartości przez cenę.
ITEM_NO_QTY_REGEX = re.compile(
    rf"^(?P<name>.+?)\s*{_TIMES}\s*(?P<unit_price>{_MONEY})\s+"
    rf"(?P<total>-?{_MONEY})\s*(?P<vat>{_VAT_SUFFIX})\s*$",
    re.IGNORECASE,
)

# Symbol mnożenia zgubiony ("A.LOD W KOSTKACH 1KG 1szt 7,49 7,49 A").
ITEM_NO_TIMES_REGEX = re.compile(
    rf"^(?P<name>.+?)\s+(?P<qty>\d+(?:[.,]\d+)?)\s*(?P<unit>{_UNITS})?\s+"
    rf"(?P<unit_price>{_MONEY})\s+(?P<total>-?{_MONEY})\s*(?P<vat>{_VAT_SUFFIX})\s*$",
    re.IGNORECASE,
)

# Wartość zgubiona — liczona z ilości i ceny. Tak drukują dystrybutory paliwa
# ("EFECTA DIESEL … 39.87*6.45" = litry × cena/litr, wartość dalej na
# paragonie) i tak wychodzą linie z uciętym końcem.
ITEM_NO_TOTAL_REGEX = re.compile(
    rf"^(?P<name>.+?)\s+(?P<qty>\d+(?:[.,]\d+)?)\s*(?P<unit>{_UNITS})?\s*"
    rf"{_TIMES}\s*(?P<unit_price>{_MONEY})\s*$",
    re.IGNORECASE,
)

# Sama para kwot: cena i wartość bez ilości i bez symbolu mnożenia
# ("CIASTO MAŁE 22,00 44,00 B" — rachunki gastronomiczne).
ITEM_PRICE_PAIR_REGEX = re.compile(
    rf"^(?P<name>.+?)\s+(?P<unit_price>{_MONEY})\s+"
    rf"(?P<total>-?{_MONEY})\s*(?P<vat>{_VAT_SUFFIX})\s*$",
    re.IGNORECASE,
)

# Pozycja bez ceny jednostkowej: sama nazwa i wartość. Tak drukują rachunki
# gastronomiczne ("Mix salat z truskawk 42,00"). Wzorzec z konieczności łapie
# niemal każdą linię zakończoną kwotą, więc bramkuje go filtr nazwy poniżej.
BARE_ITEM_LINE_REGEX = re.compile(
    rf"^(?P<name>.+?)\s+(?P<total>-?{_MONEY})\s*(?P<vat>{_VAT_SUFFIX})\s*$"
)

# Rabat/opust/obniżka: kwota korygująca sumę pozycji, czasem z nazwą towaru
# w tej samej linii ("RABAT 330ML SCHWEPPES NAPO -1,00"). Trzecia etykieta
# ("OBNIŻKA") to słownictwo stacji paliw, gdzie rabat dotyczy litra, nie
# sztuki. `target` bierze wszystko między etykietą a kwotą przez leniwe
# `.*?`, nie klasę znaków bez cyfr — nazwy towarów regularnie mają cyfry
# ("330ML"), które blokowałyby dojście do kwoty. Minus jest opcjonalny i
# dopuszcza powielenie ("--0,80" — OCR bywa duplikuje znak) — samo słowo
# etykiety już przesądza znak, a wywołujący zawsze wymusza wartość ujemną,
# więc fałszywie dodatni odczyt nie wejdzie do rachunku jako plus.
DISCOUNT_REGEX = re.compile(
    rf"(?i)^\s*(?P<label>rabat|opust|obni[żz]k\w*)\b\s*(?P<target>.*?)\s*"
    rf"(?P<amount>-*\s?{_MONEY})\s*(?P<vat>{_VAT_SUFFIX})\s*$"
)

# Ile ostatnio dodanych pozycji bierzemy pod uwagę jako cel nazwanego rabatu —
# receipt bywa drukowany blokami (najpierw wszystkie towary, potem wszystkie
# rabaty), więc "ostatnia pozycja" bywa złym sąsiadem; okno kilku ostatnich
# to kompromis między czułością a ryzykiem trafienia w niepowiązany towar.
_DISCOUNT_TARGET_WINDOW = 4

# Próg podobieństwa (SequenceMatcher.ratio, stdlib) poniżej którego nazwa
# w linii rabatu jest uznawana za nic nie mówiącą. Zmierzone na dwóch realnych
# przypadkach z korpusu: właściwy cel dawał 0,50–0,95, niepowiązane pozycje
# 0,24–0,28 — próg 0,35 leży bezpiecznie między nimi.
_DISCOUNT_TARGET_MIN_SIMILARITY = 0.35

# Suma paragonu: preferujemy wariant z walutą ("SUMA PLN 22,14"), bo
# "SUMA PTU 1,28" też zaczyna się od "SUMA". Etykieta waluty bywa poszarpana
# przez OCR ("APLN", "P PLN", samo "LN"), więc dopuszczamy 1–2 luźne litery
# przed nią i wariant bez "P". Kwotę z ogona wyciąga `_total_amount`, bo jej
# przekłamania (spacja zamiast przecinka) są wspólne dla wszystkich źródeł.
TOTAL_WITH_CURRENCY_REGEX = re.compile(
    r"(?i)\bsuma\b[ :.]{0,3}(?:[a-ząćęłńóśźż]{1,2}[ .]{0,2})?"
    r"(?P<cur>pln|ln|z[łl])[ :.]*(?P<tail>.*)$"
)
TOTAL_FALLBACK_REGEX = re.compile(r"(?i)^\s*suma\b[ :.]*(?P<tail>\d.*)$")

# Zamknięcie rachunku innym słowem niż "SUMA": "Do zapłaty", "Zapłacono
# razem". Słowo "do" bywa gubione przez OCR, więc nie jest wymagane — kwota
# tuż za słowem i tak przesądza, że to podsumowanie, a nie opis płatności
# ("POTWIERDZENIE ZAPŁATY KARTĄ" nie ma po sobie kwoty).
TOTAL_DUE_REGEX = re.compile(r"(?i)\b(?:do\s*)?\w*zap[łl]aty\b[ :.]*(?:pln\b[ :.]*)?(?P<tail>.*)$")

# "Zapłacono razem" bywa sumą, ale przy gotówce to kwota wręczona ("Zapłacono
# 50,00" przy paragonie na 22,14). Dlatego trafia tylko do bramkowanego
# fallbacku, przyjmowanego wyłącznie po uzgodnieniu z pozycjami. Pierwsza
# litera bywa zjedzona ("Jpłacono"), a "ł" czytane jako "t" ("Wptacono").
TOTAL_PAID_REGEX = re.compile(
    r"(?i)\b\w*p[łlt]acono\b(?:[ :.]*(?:(?:razem|kart[ąa]?|pln|got[óo]wk\w*)\b|\([^)]*\)))*"
    r"[ :.]*(?P<tail>.*)$"
)

# "RAZEM PLN 20,00" — u części sieci to jedyna czytelna suma (TK Maxx, Media
# Markt). Bramkowane pozycjami, bo "RAZEM" bywa też kwotą wręczoną.
TOTAL_RAZEM_REGEX = re.compile(r"(?i)^\s*razem\b[ :.]*(?:pln\b[ :.]*)?(?P<tail>.*)$")

# "Podsuma: 40,97" (Żabka) — suma pozycji przed rabatami; bramkowana.
TOTAL_PODSUMA_REGEX = re.compile(r"(?i)^\s*podsu\w*\b[ :.]*(?P<tail>\d.*)$")

# "CENA PLN 33,00" — bilety autokarowe; bramkowana.
TOTAL_CENA_REGEX = re.compile(r"(?i)^\s*cena\b[ :.]*(?:pln\b[ :.]*)?(?P<tail>.*)$")

# Kwota z rozliczenia płatności kartą ("INNA Karta kred. 18,17PLN",
# "Karta karta: 4.28") — na paragonach Żabki/Lewiatana to jedyna pełna suma,
# gdy linia "SUMA PLN" wychodzi ucięta. Bramkowana jak kwota zapłacona.
TOTAL_CARD_REGEX = re.compile(r"(?i)\bkart[ąay]?\b[^0-9\n]*(?P<tail>\d.*)$")

# Ostatnia deska ratunku: suma sprzedaży w rozbiciu na stawki VAT z definicji
# równa się sumie paragonu. Używana tylko wtedy, gdy domyka się z pozycjami.
# Wzorzec zakotwiczony na "opod…", bo początek linii bywa ucięty ("zedaz
# opodatkowana B: 36.00") albo skrócony ("SP. OPOD. PTU B 33,00").
TAXED_SALES_REGEX = re.compile(
    r"(?i)\bop[il]?od\w*[^\w\s]?\s*(?:ptu\s*)?[A-G]?\s*:?\s*(?P<tail>\d[\d ,.]*)$"
)

# Przekłamania kwoty w linii sumy, w kolejności prób: zdublowana cyfra
# ("52,9 98" → 52,98), zabłąkane zero z kropką ("40 0.97" → 40,97),
# spacja zamiast przecinka ("41 18" → 41,18).
_TOTAL_DOUBLED_REGEX = re.compile(r"^(\d+)[.,](\d)\s+(\d)(\d{1,2})(?!\d)")
_TOTAL_STRAY_ZERO_REGEX = re.compile(r"^(\d+)\s+0[.,]\s?(\d{2})(?!\d)")
_TOTAL_SPACED_REGEX = re.compile(r"^(\d+)\s+(\d{2})(?!\d)")


def _total_amount(tail: str) -> int | None:
    """Kwota z ogona linii sumy, z naprawą typowych przekłamań OCR.

    Kwota bez separatora groszy ("SUMA PLN 249" przy prawdzie 249,99) jest
    odrzucana, nie zgadywana — lepszy brak sumy niż cicha nieprawda.
    """
    tail = tail.strip()
    match = _TOTAL_DOUBLED_REGEX.match(tail)
    if match and match.group(2) == match.group(3):
        tail = f"{match.group(1)},{match.group(2)}{match.group(4)}"
    else:
        tail = _TOTAL_STRAY_ZERO_REGEX.sub(r"\1,\2", tail)
        tail = _TOTAL_SPACED_REGEX.sub(r"\1,\2", tail)
    match = re.match(rf"{_MONEY}", tail)
    return amount_to_cents(match.group(0)) if match else None


# Granice sekcji pozycji na paragonie fiskalnym. "Podsuma" celowo nie kończy
# sekcji: Żabka drukuje ją PRZED pozycją ("Podsuma: 2 x4,89 9,78A"), więc jest
# ścinana jako prefiks (patrz `extract_items`), a nie traktowana jak granica.
ITEMS_START_REGEX = re.compile(r"(?i)paragon\s+fiskalny")
ITEMS_END_REGEX = re.compile(
    r"(?i)^\s*(sprzeda[żz]\s+opodatk|suma\b|ptu\b|razem\b|rozliczenie"
    r"|do\s*zap[łl]aty|zap[łl]acono|got[oó]wka|reszta\b)"
)

# Prefiks "Podsuma:" doklejany przez OCR do linii pozycji (Żabka).
_PODSUMA_PREFIX_REGEX = re.compile(r"(?i)^\s*podsu\w*\b[:\s]*")


# Glify, którymi OCR zastępuje samotną jedynkę w kolumnie ilości.
_LONELY_ONE_REGEX = re.compile(rf"(?<![0-9A-Za-z])[|lI!\[{{fi](?=\s*(?:{_UNITS})?\s*{_TIMES}\s*\d)")
_SPACED_DECIMAL_REGEX = re.compile(r"(\d)\s*([.,])\s*(\d{2})(?![\d])")

# "=" wciśnięty między cenę a wartość ("1szt*3,95=3,95A").
_EQ_BETWEEN_DIGITS_REGEX = re.compile(r"(?<=\d)=(?=\d)")

# Samotny token waluty między kwotami ("… * 5,99 zt 5,99 A") — usuwany,
# żeby nie rozdzielał ceny od wartości.
_CURRENCY_TOKEN_REGEX = re.compile(r"(?i)(?<=[\d,.])\s+z[łlt]\.?(?=\s|$)")

# Jednostka "szt" czytana jako "s2t" ("1s2t 7.49").
_S2T_REGEX = re.compile(r"(?i)(?<=\d)\s*s2t\b")

# Nazwa sklejona z ilością ("STRZYZENIE1SZt.*60.00", "KWIATOWCIE1*45.00",
# "GORALSKJ0,204x49,90") — rozdzielana tylko gdy za cyframi stoi mnożenie,
# żeby nie kaleczyć nazw z liczbami ("Mix0,32" zostaje).
_NAME_QTY_GLUE_REGEX = re.compile(
    r"(?i)(?<=[^\W\d_])(?=\d+(?:[.,]\d+)?\s*(?:s[z2]t[.,]?)?\s*[x*×]\d)"
)

# Dwie kwoty sklejone bez spacji ("1x14,9914,99B", "0,524x27,9014,62C").
# Rozdzielenie wymaga, żeby ogon sam wyglądał jak kwota — inaczej ilość
# wagowa "0,212" zostałaby rozcięta.
_GLUED_MONEY_REGEX = re.compile(r"(?<=[.,]\d\d)(?=\d+[.,]\d\d)")

# Samotna cyfra-śmieć zdublowana przed kwotą ("… 4 x2,49 9 9,96C").
_LONE_DOUBLED_DIGIT_REGEX = re.compile(r"(?<!\S)(\d)\s+(?=\1[.,]\d\d)")

# Cyfra-śmieć wklejona tuż przed literą stawki na końcu ("… 7,49 9A").
_TRAILING_JUNK_DIGIT_REGEX = re.compile(r"(?<=\s)\d(?=[A-Ga-g]\s*$)")

# Samotne cyfry-śmieci między ceną a wartością ("CIASTO 20,00 0 2 20,00 B").
_MID_JUNK_DIGITS_REGEX = re.compile(r"(?<=[.,]\d\d)\s+\d(?:\s+\d)?(?=\s+\d+[.,]\d\d)")

# Ujemny ogon-śmieć doklejony do ceny ("1szt*2,95-2 2,95 B").
_MINUS_JUNK_REGEX = re.compile(r"(?<=[.,]\d\d)-\d+(?=\s)")

# Separator groszy zjedzony w pierwszej z pary tych samych kwot
# ("1sztx129912,99A" = "12,99 12,99A").
_SEP_LOSS_REGEX = re.compile(r"(\d+)(\d{2})(?=\1[.,]\2)")

# Przecinek ceny czytany jako spacja tuż za symbolem mnożenia ("*0 99").
_TIMES_SPACED_DECIMAL_REGEX = re.compile(r"(?<=[*x×])(\d)\s+(\d{2})(?!\d)")

# Kwota z zgubionym przecinkiem na końcu linii ("1 x23.99 23 99A") —
# stosowana tylko na liniach z symbolem mnożenia, patrz normalize_ocr_line.
_EOL_SPACED_DECIMAL_REGEX = re.compile(r"(\d+)\s+(\d{2})(\D?)\s*$")
_HAS_PRICE_CONTEXT_REGEX = re.compile(rf"{_TIMES}\s*\d")


def normalize_ocr_line(line: str) -> str:
    """Prostuje typowe przekłamania OCR w liniach pozycji.

    Każda poprawka wzięta z realnych odczytów korpusu (Tesseract i RapidOCR
    mylą się inaczej — stąd ich liczba). Wszystkie są celowo wąskie: wymagają
    kontekstu kwoty albo symbolu mnożenia obok, żeby nie kaleczyć nazw
    towarów.
    """
    fixed = _SPACED_DECIMAL_REGEX.sub(r"\1\2\3", line)
    fixed = fixed.replace("«", "x").replace("»", "x").replace("×", "x")
    fixed = _LONELY_ONE_REGEX.sub("1", fixed)
    fixed = _MINUS_JUNK_REGEX.sub("", fixed)
    fixed = _EQ_BETWEEN_DIGITS_REGEX.sub(" ", fixed)
    fixed = _CURRENCY_TOKEN_REGEX.sub("", fixed)
    fixed = _S2T_REGEX.sub(" szt", fixed)
    fixed = _NAME_QTY_GLUE_REGEX.sub(" ", fixed)
    fixed = _TIMES_SPACED_DECIMAL_REGEX.sub(r"\1,\2", fixed)
    fixed = _SEP_LOSS_REGEX.sub(r"\1,\2 ", fixed)
    fixed = _GLUED_MONEY_REGEX.sub(" ", fixed)
    fixed = _LONE_DOUBLED_DIGIT_REGEX.sub("", fixed)
    fixed = _MID_JUNK_DIGITS_REGEX.sub(" ", fixed)
    fixed = _TRAILING_JUNK_DIGIT_REGEX.sub("", fixed)
    if _HAS_PRICE_CONTEXT_REGEX.search(fixed):
        fixed = _EOL_SPACED_DECIMAL_REGEX.sub(r"\1,\2\3", fixed)
    return fixed


def amount_to_cents(raw: str) -> int | None:
    """Zamienia kwotę z paragonu ("3,49", "-1,50", "1 234,56") na grosze."""
    cleaned = raw.replace(" ", "").replace(" ", "").replace(",", ".")
    # Przy kilku kropkach (np. separator tysięcy z OCR) zostawiamy ostatnią.
    if cleaned.count(".") > 1:
        head, _, tail = cleaned.rpartition(".")
        cleaned = head.replace(".", "") + "." + tail
    try:
        value = Decimal(cleaned)
    except InvalidOperation:
        return None
    return int((value * 100).to_integral_value())


def _quantity_to_float(raw: str) -> float | None:
    try:
        return float(raw.replace(",", "."))
    except ValueError:
        return None


def normalize_nip(raw: str | None) -> str | None:
    if not raw:
        return None
    digits = re.sub(r"\D", "", raw)
    return digits if len(digits) == 10 else None


def nip_checksum_valid(digits: str) -> bool:
    """Czy 10 cyfr spełnia sumę kontrolną NIP-u."""
    if len(digits) != 10 or not digits.isdigit():
        return False
    checksum = (
        sum(weight * int(digit) for weight, digit in zip(NIP_WEIGHTS, digits, strict=False)) % 11
    )
    return checksum != 10 and checksum == int(digits[9])


def repair_nip(digits: str) -> str | None:
    """Prostuje jedną cyfrę przekłamaną przez OCR, jeśli suma kontrolna wtedy siada.

    Poprawiamy wyłącznie pojedynczy znak i tylko na parach, które OCR realnie
    myli. Dwie poprawki naraz dawałyby już numer wzięty z sufitu — łatwo
    trafić w inny poprawny NIP niż ten z paragonu.
    """
    for index, digit in enumerate(digits):
        for alternative in _DIGIT_CONFUSIONS.get(digit, ()):
            candidate = f"{digits[:index]}{alternative}{digits[index + 1 :]}"
            if nip_checksum_valid(candidate):
                return candidate
    return None


def _nip_candidates(lines: Sequence[str]) -> list[tuple[str, int]]:
    """Kandydaci na NIP: najpierw te po słowie "NIP", potem dowolne 10 cyfr."""
    labelled: list[tuple[str, int]] = []
    loose: list[tuple[str, int]] = []
    for index, line in enumerate(lines):
        for match in NIP_LABELLED_REGEX.finditer(line):
            digits = normalize_nip(match.group(1))
            if digits:
                labelled.append((digits, index))
        for match in NIP_LOOSE_REGEX.finditer(line):
            digits = normalize_nip(match.group(1))
            if digits:
                loose.append((digits, index))
    return labelled + loose


def extract_nip(lines: Sequence[str]) -> tuple[str | None, int | None]:
    """NIP zweryfikowany sumą kontrolną, z próbą naprawy przekłamania OCR."""
    candidates = _nip_candidates(lines)

    for digits, index in candidates:
        if nip_checksum_valid(digits):
            return digits, index
    for digits, index in candidates:
        repaired = repair_nip(digits)
        if repaired:
            return repaired, index
    return candidates[0] if candidates else (None, None)


# Linia adresowa: kod pocztowy, prefiks ulicy albo telefon. Na paragonie stoi
# tuż pod nazwą sklepu i bywa mylona z nią przez heurystyki.
ADDRESS_LINE_REGEX = re.compile(r"(?i)^(ul|al|os|pl)[\s.]|\b\d{2}-\d{3}\b|\btel\b")

# Ile pierwszych linii uznajemy za „szyld” paragonu.
STORE_TOP_LINES = 4


def _looks_like_store_name(line: str) -> bool:
    cleaned = re.sub(r"[^\w\s\.\-&]", "", line).strip()
    if len(cleaned) < 3:
        return False
    lowered = cleaned.lower()
    if any(keyword in lowered for keyword in NAME_BLACKLIST):
        return False
    if re.fullmatch(r"\d{10,}", cleaned):
        return False
    if ADDRESS_LINE_REGEX.search(line):
        return False
    # Sam numer sklepu albo ciąg cyfr to nie nazwa — potrzebne są litery.
    return len(re.findall(r"[^\W\d_]", cleaned)) >= 3


def guess_store_name(lines: Sequence[str], nip_index: int | None) -> str | None:
    """Nazwa sklepu z szyldu na górze paragonu, nie z okolic NIP-u.

    Zmierzone na 34 zetykietowanych paragonach: nazwa, którą człowiek uznaje
    za nazwę sklepu, stoi w linii 0 (20 przypadków), 1 (8) albo 2 (2).
    W sąsiedztwie NIP-u stoi zwykle dane spółki albo adres punktu sprzedaży,
    dlatego to tylko awaryjne źródło.
    """
    for line in lines[:STORE_TOP_LINES]:
        candidate = line.strip(" :-")
        if _looks_like_store_name(candidate):
            return candidate

    if nip_index is not None:
        for delta in (-2, -1, 1, 2):
            index = nip_index + delta
            if 0 <= index < len(lines):
                candidate = lines[index].strip(" :-")
                if _looks_like_store_name(candidate):
                    return candidate
    return None


def _year_is_plausible(moment: dt.datetime) -> bool:
    """Odsiewa rok przekłamany przez OCR ("7024-06-19" zamiast "2024-06-19")."""
    return 2000 <= moment.year <= dt.datetime.now(dt.UTC).year + 1


def extract_datetime(text: str) -> dt.datetime | None:
    """Data i godzina zakupu z dowolnego miejsca paragonu.

    Data z niemożliwym rokiem jest pomijana, a nie zwracana — podsunięta
    w formularzu potrafi zostać zatwierdzona i zanieczyścić dane.
    """
    for match in DATETIME_REGEX.finditer(text):
        date_text = match.group("date")
        time_part = match.group("time") or ""
        raw_value = f"{date_text} {time_part}".strip()
        # Zapis z rokiem na początku jest jednoznaczny; wymuszanie tam
        # `dayfirst` zamieniało miesiąc z dniem (2024-06-10 → 6 października).
        year_first = bool(re.match(r"\d{4}[./-]", date_text))
        try:
            parsed = date_parser.parse(raw_value, dayfirst=not year_first, fuzzy=True)
        except (ValueError, TypeError):
            continue
        if _year_is_plausible(parsed):
            return parsed
    return None


def extract_total(lines: Sequence[str]) -> tuple[int | None, str | None]:
    """Zwraca (total_cents, waluta) z linii "SUMA ..." albo (None, None)."""
    for line in lines:
        match = TOTAL_WITH_CURRENCY_REGEX.search(line)
        if match:
            amount = _total_amount(match.group("tail"))
            if amount is not None:
                return amount, "PLN"
    for line in lines:
        match = TOTAL_FALLBACK_REGEX.match(line)
        if match:
            tail = match.group("tail")
            amount = _total_amount(tail)
            if amount is not None:
                currency = "PLN" if re.search(r"(?i)\b(pln|z[łl])\b", tail) else None
                return amount, currency
    for line in lines:
        match = TOTAL_DUE_REGEX.search(line)
        if match:
            amount = _total_amount(match.group("tail"))
            if amount is not None:
                return amount, None
    return None, None


def _first_total(lines: Sequence[str], regex: re.Pattern[str]) -> int | None:
    """Pierwsza kwota dopasowana wzorcem sumy w którejkolwiek linii."""
    for line in lines:
        match = regex.search(line)
        if match:
            amount = _total_amount(match.group("tail"))
            if amount is not None:
                return amount
    return None


def total_from_taxed_sales(lines: Sequence[str]) -> int | None:
    """Suma paragonu odtworzona ze sprzedaży w rozbiciu na stawki VAT.

    Ratunek dla paragonów, na których linia "SUMA PLN" nie przeszła przez OCR.
    Wynik jest wiarygodny tylko wtedy, gdy OCR złapał wszystkie stawki, więc
    wywołujący musi go skonfrontować z pozycjami przed użyciem.
    """
    amounts = [
        _total_amount(match.group("tail"))
        for match in (TAXED_SALES_REGEX.search(line) for line in lines)
        if match
    ]
    found = [amount for amount in amounts if amount is not None]
    return sum(found) if found else None


def total_from_amount_paid(lines: Sequence[str]) -> int | None:
    """Kwota z linii "Zapłacono razem" — kandydat, nie pewnik (patrz regex)."""
    return _first_total(lines, TOTAL_PAID_REGEX)


def gated_total_candidates(lines: Sequence[str]) -> list[int]:
    """Kandydaci na sumę przyjmowani wyłącznie po uzgodnieniu z pozycjami.

    Kolejność od najbardziej wiarygodnego: "RAZEM" (u części sieci jedyna
    czytelna suma), rozbicie VAT, "Podsuma", kwota zapłacona, "CENA" (bilety).
    """
    candidates = [
        _first_total(lines, TOTAL_RAZEM_REGEX),
        total_from_taxed_sales(lines),
        _first_total(lines, TOTAL_PODSUMA_REGEX),
        total_from_amount_paid(lines),
        _first_total(lines, TOTAL_CARD_REGEX),
        _first_total(lines, TOTAL_CENA_REGEX),
    ]
    return [candidate for candidate in candidates if candidate is not None]


def _looks_like_item_name(line: str) -> bool:
    """Kandydat na złamaną nazwę: ma litery i nie kończy się kwotą.

    Linia z symbolem mnożenia przy kwocie to nie nazwa, tylko pokiereszowana
    linia pozycji — doklejenie jej do następnej skleiłoby dwie różne pozycje
    w jedną nieczytelną nazwę.
    """
    if len(line.strip()) < 3:
        return False
    if not re.search(r"[a-ząćęłńóśźż]", line, re.IGNORECASE):
        return False
    if re.search(rf"-?{_MONEY}\s*{_VAT_SUFFIX}\s*$", line):
        return False
    if re.search(rf"{_TIMES}\s*\d+[.,]\d", line):
        return False
    lowered = line.lower()
    return not any(keyword in lowered for keyword in NAME_BLACKLIST)


# Litera stawki PTU bywa drukowana zaraz za nazwą, w osobnej kolumnie
# ("SOK Z GREJFRUTÓW V   D   1 x4,99 4,99C"), i wpada do nazwy jako samotny
# znak na końcu. Obcinamy go tylko wtedy, gdy nazwa ma jeszcze inne słowa.
_TRAILING_COLUMN_REGEX = re.compile(r"^(?P<name>.*\S.*?)\s+[A-Za-z0-9()\[\]{}|]$")

# Kod kreskowy doklejony do nazwy ("WKŁAD AROMA KING 5905311240303") oraz
# kod działu/produktu na początku ("102040 Artykuły kolonialne") — to nie
# część nazwy towaru.
_TRAILING_EAN_REGEX = re.compile(r"\s*\d{8,}$")
_LEADING_CODE_REGEX = re.compile(r"^\d[\d/]{3,}\s+")


def _strip_trailing_column(name: str) -> str:
    match = _TRAILING_COLUMN_REGEX.match(name)
    return match.group("name").strip() if match else name


def _clean_item_name(name: str) -> str:
    """Nazwa pozycji bez artefaktów kolumn: litery VAT, EAN-u, kodu działu."""
    cleaned = _strip_trailing_column(name.strip(" .:-"))
    cleaned = _TRAILING_EAN_REGEX.sub("", cleaned)
    return _LEADING_CODE_REGEX.sub("", cleaned).strip(" .:-") or name.strip(" .:-")


# Sama ilość z jednostką ("1szt", "SZT:") to kolumna cen, której OCR oderwał
# nazwę — prawdziwa nazwa stoi w linii obok i czeka jako `pending_name`.
_BARE_UNIT_NAME_REGEX = re.compile(r"(?i)^[\d\s.,:*-]*(s[z2]t|opak|kg|ml|g|l)[.,:]?$")


def _is_plausible_item_name(name: str) -> bool:
    """Filtr dla luźnego wzorca pozycji bez ceny jednostkowej.

    Nazwa musi mieć realne słowo i nie może być słownictwem podsumowania —
    inaczej "Do zapłaty: 97,00" wjechałoby na listę zakupów.
    """
    if len(re.findall(r"[^\W\d_]", name)) < 3:
        return False
    if _BARE_UNIT_NAME_REGEX.match(name):
        return False
    if SUMMARY_LINE_REGEX.search(name):
        return False
    lowered = name.lower()
    return not any(keyword in lowered for keyword in ITEM_NAME_BLACKLIST)


# Tolerancja zgodności ilość × cena = wartość: grosz na błąd zaokrąglenia.
_LINE_ARITHMETIC_TOLERANCE = 1


def _line_arithmetic_ok(quantity: float, price_cents: int, total_cents: int) -> bool:
    return abs(round(quantity * price_cents) - total_cents) <= _LINE_ARITHMETIC_TOLERANCE


def _infer_quantity(price_cents: int | None, total_cents: int | None) -> float | None:
    """Ilość odtworzona z dzielenia wartości przez cenę (dla linii bez ilości).

    Tylko dzielenie bez reszty i w rozsądnym zakresie — inaczej wynik byłby
    zgadywanką, a nie odtworzeniem kolumny.
    """
    if not price_cents or total_cents is None or total_cents <= 0:
        return None
    if price_cents == total_cents:
        return 1.0
    if total_cents % price_cents == 0 and 1 <= total_cents // price_cents <= 99:
        return float(total_cents // price_cents)
    return None


# Ilość wagowa z zjedzonym przecinkiem ("0560" zamiast "0,560") — wiodące
# zero zdradza, że to ułamek.
_ZERO_LED_QTY_REGEX = re.compile(r"^0\d{2,}$")

# Ilość, która utknęła na końcu nazwy, gdy kolumny się rozjechały
# ("CYTRYNY 0,706 6x7,99 5,64C" — prawdziwa ilość to 0,706, nie 6).
_NAME_TAIL_QTY_REGEX = re.compile(r"(?P<qty>\d+[.,]\d{1,3})\s*$")


def _repair_item_arithmetic(item: ExtractedItem) -> ExtractedItem:
    """Uzgadnia ilość × cena = wartość, gdy OCR przekłamał jedną z kolumn.

    Dwie naprawy, obie przyjmowane tylko przy dokładnej zgodności arytmetyki:
    ilość odzyskana z końca nazwy (rozjechane kolumny) oraz wartość odzyskana
    z iloczynu, gdy odczytana różni się od niego jedynie doklejoną cyfrą
    ("92,99" przy 1 × 2,99 — iloczyn "299" zawiera się w odczycie "9299").
    """
    quantity, price, total = item.quantity, item.unit_price_cents, item.line_total_cents
    if quantity is None or price is None or total is None or price <= 0:
        return item
    if _line_arithmetic_ok(quantity, price, total):
        return item

    # Cena równa wartości przesądza ilość 1 — odczytana "ilość" to w takim
    # razie odklejona końcówka nazwy ("PapierToalRegina16 x25,99 25,99A").
    if price == total and quantity == int(quantity):
        item.raw_name = f"{item.raw_name}{int(quantity)}"
        item.quantity = 1.0
        return item

    tail = _NAME_TAIL_QTY_REGEX.search(item.raw_name)
    if tail:
        alt_qty = _quantity_to_float(tail.group("qty"))
        if alt_qty is not None and _line_arithmetic_ok(alt_qty, price, total):
            item.raw_name = item.raw_name[: tail.start()].strip(" .:-")
            item.quantity = alt_qty
            return item

    computed = round(quantity * price)
    if computed > 0 and str(computed) in str(total):
        item.line_total_cents = computed
    return item


def _build_item(match: re.Match[str]) -> ExtractedItem:
    groups = match.groupdict()
    unit = groups.get("unit")
    quantity_raw = groups.get("qty")
    quantity = _quantity_to_float(quantity_raw) if quantity_raw else None
    if quantity_raw and _ZERO_LED_QTY_REGEX.match(quantity_raw):
        quantity = _quantity_to_float(f"0.{quantity_raw[1:]}")
    price = amount_to_cents(groups["unit_price"]) if groups.get("unit_price") else None
    total = amount_to_cents(groups["total"]) if groups.get("total") else None
    item = ExtractedItem(
        raw_name=_clean_item_name(groups["name"]),
        quantity=quantity,
        unit=unit.rstrip(".,").lower() if unit else None,
        unit_price_cents=price,
        line_total_cents=total,
    )
    return _repair_item_arithmetic(item)


def _match_item_line(line: str) -> ExtractedItem | None:
    """Próbuje dopasować linię pozycji, od wzorca ścisłego po rozluźnione.

    Wzorzec ścisły (pełna kolumna "ilość × cena wartość") jest wiarygodny sam
    z siebie. Każdy rozluźniony wymaga domknięcia arytmetyki — brakującą
    kolumnę wolno odtworzyć tylko wtedy, gdy pozostałe dwie ją potwierdzają.
    """
    match = ITEM_LINE_REGEX.match(line)
    if match:
        return _build_item(match)

    match = ITEM_NO_QTY_REGEX.match(line)
    if match:
        item = _build_item(match)
        quantity = _infer_quantity(item.unit_price_cents, item.line_total_cents)
        if quantity is not None:
            item.quantity = quantity
            return item
        # Wariant z cyfrą-śmieciem doklejoną do wartości ("x4,69 94,690").
        if (
            item.unit_price_cents
            and item.line_total_cents
            and str(item.unit_price_cents) in str(item.line_total_cents)
        ):
            item.quantity = 1.0
            item.line_total_cents = item.unit_price_cents
            return item
        return None

    match = ITEM_NO_TIMES_REGEX.match(line)
    if match:
        item = _build_item(match)
        if (
            item.quantity is not None
            and item.unit_price_cents
            and item.line_total_cents is not None
            and _line_arithmetic_ok(item.quantity, item.unit_price_cents, item.line_total_cents)
        ):
            return item
        return None

    match = ITEM_NO_TOTAL_REGEX.match(line)
    if match:
        item = _build_item(match)
        if item.quantity is not None and item.unit_price_cents:
            computed = round(item.quantity * item.unit_price_cents)
            if 0 < computed <= 10_000_00:
                item.line_total_cents = computed
                return item
        return None

    match = ITEM_PRICE_PAIR_REGEX.match(line)
    if match:
        item = _build_item(match)
        quantity = _infer_quantity(item.unit_price_cents, item.line_total_cents)
        if quantity is not None:
            item.quantity = quantity
            return item
        return None

    return None


def _digits_only_name(name: str) -> bool:
    """Nazwa bez liter — kod działu/numer, prawdziwa nazwa stoi obok."""
    return len(re.findall(r"[^\W\d_]", name)) < 3 and len(name) >= 4


def _find_discount_target(items: Sequence[ExtractedItem], target_text: str) -> ExtractedItem:
    """Pozycja, do której należy rabat z linii `DISCOUNT_REGEX`.

    Gdy linia wymienia nazwę towaru, dopasowujemy ją do kilku ostatnio
    dodanych pozycji przez podobieństwo napisów — potrzebne akurat wtedy, gdy
    receipt drukuje najpierw wszystkie towary, a rabaty osobnym blokiem na
    końcu, bo wtedy "ostatnia pozycja" jest zwykle złym sąsiadem (zmierzone:
    Delikatesy Centrum, dwie pizze pod rząd, potem dwa rabaty pod rząd — bez
    dopasowania po nazwie oba trafiłyby w drugą pizzę). Bez nazwy (gołe
    "OPUST -1,50") albo gdy nic nie przebija progu, bierzemy ostatnią pozycję
    — tak drukuje większość sieci: rabat tuż pod swoim towarem.
    """
    stripped = target_text.strip(" .:-")
    if len(stripped) >= 3:
        window = items[-_DISCOUNT_TARGET_WINDOW:]
        best_item: ExtractedItem | None = None
        best_score = _DISCOUNT_TARGET_MIN_SIMILARITY
        for candidate in window:
            score = difflib.SequenceMatcher(
                None, stripped.upper(), candidate.raw_name.upper()
            ).ratio()
            if score >= best_score:
                best_item, best_score = candidate, score
        if best_item is not None:
            return best_item
    return items[-1]


def extract_items(lines: Sequence[str], *, allow_bare: bool = False) -> list[ExtractedItem]:
    """Wyciąga pozycje paragonu z sekcji między "PARAGON FISKALNY" a podsumowaniem.

    `allow_bare` dopuszcza pozycje bez ceny jednostkowej (sama nazwa i kwota).
    Domyślnie wyłączone, bo taki wzorzec pasuje do niemal każdej linii
    zakończonej kwotą i na paragonie fiskalnym zbiera wiersze podsumowania
    zamiast towarów. Włączamy go dopiero wtedy, gdy ścisły wzorzec nie znalazł
    nic — czyli na rachunkach, które po prostu nie mają kolumny "ilość × cena".
    """
    start = 0
    for idx, line in enumerate(lines):
        if ITEMS_START_REGEX.search(line):
            start = idx + 1
            break

    items: list[ExtractedItem] = []
    pending_name: str | None = None
    pending_priced: ExtractedItem | None = None
    for raw_line in lines[start:]:
        # Normalizacja jest idempotentna, więc linie z `parse_receipt_text`
        # (już znormalizowane) nie ucierpią, a wywołanie z surowym tekstem
        # dostaje te same poprawki.
        line = _PODSUMA_PREFIX_REGEX.sub("", normalize_ocr_line(raw_line)).strip()
        if not line:
            continue
        if ITEMS_END_REGEX.match(line):
            break

        # Najpierw linia sama w sobie — doklejanie zawieszonej nazwy do linii,
        # która sama jest kompletną pozycją, sklejałoby dwie pozycje w jedną.
        alone = _match_item_line(line)
        if alone is not None and _is_plausible_item_name(alone.raw_name):
            items.append(alone)
            pending_name = pending_priced = None
            continue

        if pending_name:
            combined = _match_item_line(f"{pending_name} {line}")
            if combined is not None and _is_plausible_item_name(combined.raw_name):
                items.append(combined)
                pending_name = pending_priced = None
                continue

        # Kompletna linia cen bez nazwy (kod działu zamiast niej — TK Maxx,
        # Empik): kwoty czekają na nazwę z sąsiedniej linii.
        if alone is not None and _digits_only_name(alone.raw_name):
            pending_priced = alone
            pending_name = None
            continue

        discount = DISCOUNT_REGEX.match(line)
        if discount:
            # Znak nie ma znaczenia — poniżej i tak bierzemy abs() — a
            # `Decimal` odrzuca podwójny minus ("--0,80"), więc zdejmujemy
            # wszystkie wiodące minusy przed konwersją, zamiast zgadywać,
            # ile ich naprawdę było na paragonie.
            raw_amount = discount.group("amount").replace(" ", "").lstrip("-")
            amount = amount_to_cents(raw_amount)
            if amount is not None and items:
                # Dopięty do pozycji, nie osobną linią: rabat jako własny
                # wpis wygląda w szkicu paragonu jak kupiony produkt „Rabat"
                # — dokładnie ta niezgodność z `ReceiptDraftItem`, której ma
                # unikać `discount_cents` (patrz base.py).
                target = _find_discount_target(items, discount.group("target"))
                target.discount_cents = (target.discount_cents or 0) - abs(amount)
                target.discount_label = line.strip()
            pending_name = pending_priced = None
            continue

        bare = BARE_ITEM_LINE_REGEX.match(line) if allow_bare else None
        if bare:
            name = _clean_item_name(bare.group("name"))
            if _is_plausible_item_name(name):
                items.append(
                    ExtractedItem(
                        raw_name=name,
                        quantity=1.0,
                        line_total_cents=amount_to_cents(bare.group("total")),
                    )
                )
                pending_name = pending_priced = None
                continue

        if pending_priced is not None and _looks_like_item_name(line):
            pending_priced.raw_name = _clean_item_name(line)
            items.append(pending_priced)
            pending_name = pending_priced = None
            continue

        # Linia bez ceny — potencjalnie złamana nazwa doklejana do następnej.
        pending_name = line if _looks_like_item_name(line) else None

    return items


def check_needs_review(items: Sequence[ExtractedItem], total_cents: int | None) -> bool:
    """Walidacja krzyżowa: suma pozycji vs suma paragonu (tolerancja 1%).

    Niezmiennik zgodny z `receipt_checks.items_balance_cents` (produkcja):
    Σ(wartość pozycji + rabat pozycji) = suma. `line_total_cents` jest
    wartością sprzed rabatu, więc `discount_cents` trzeba odjąć osobno —
    bez tego każdy zrabatowany paragon fałszywie wyglądałby na niedomknięty.
    """
    if total_cents is None or not items:
        return True
    line_totals = [item.line_total_cents for item in items if item.line_total_cents is not None]
    if not line_totals:
        return True
    balance = sum(line_totals) - sum(
        abs(item.discount_cents) for item in items if item.discount_cents
    )
    tolerance = max(1, round(abs(total_cents) * 0.01))
    return abs(balance - total_cents) > tolerance


def _confidence(extraction: ReceiptExtraction) -> float:
    """Prosta heurystyka pewności: udział znalezionych pól + zgodność sumy."""
    score = 0.0
    if extraction.nip:
        score += 0.2
    if extraction.store_name:
        score += 0.15
    if extraction.happened_at:
        score += 0.15
    if extraction.total_cents is not None:
        score += 0.2
    if extraction.items:
        score += 0.2
    if not extraction.needs_review:
        score += 0.1
    return round(score, 2)


def merge_extractions(header: ReceiptExtraction, items: ReceiptExtraction) -> ReceiptExtraction:
    """Łączy wyniki dwóch przebiegów OCR tego samego paragonu.

    Tryby segmentacji Tesseracta wymieniają się zaletami: automatyczny lepiej
    czyta nagłówek (NIP, data), tryb jednolitego bloku — kolumnę pozycji.
    Zamiast wybierać mniejsze zło, bierzemy z każdego to, w czym jest lepszy:
    pola nagłówka z pierwszego przebiegu (z uzupełnieniem z drugiego), a
    pozycje z tego, który znalazł ich więcej.
    """
    chosen = items if len(items.items) >= len(header.items) else header
    merged = ReceiptExtraction(
        store_name=header.store_name or items.store_name,
        nip=header.nip or items.nip,
        happened_at=header.happened_at or items.happened_at,
        total_cents=_reconcilable_total(chosen.items, header, items),
        currency=header.currency or items.currency,
        items=list(chosen.items),
        raw_text=chosen.raw_text,
    )
    merged.needs_review = check_needs_review(merged.items, merged.total_cents)
    merged.confidence = _confidence(merged)
    return merged


def _reconcilable_total(
    items: Sequence[ExtractedItem], *candidates: ReceiptExtraction
) -> int | None:
    """Suma z tego przebiegu, który zgadza się z wybranymi pozycjami.

    Gdy przebiegi odczytały różne sumy, rozstrzyga je arytmetyka paragonu:
    wygrywa ta, która domyka się z pozycjami.
    """
    totals = [c.total_cents for c in candidates if c.total_cents is not None]
    for total in totals:
        if not check_needs_review(items, total):
            return total
    return totals[0] if totals else None


def parse_receipt_text(text: str) -> ReceiptExtraction:
    """Parsuje surowy tekst OCR paragonu na ustrukturyzowany wynik."""
    lines = [normalize_ocr_line(line.strip()) for line in text.splitlines() if line.strip()]

    nip, nip_index = extract_nip(lines)
    store_name = guess_store_name(lines, nip_index)
    happened_at = extract_datetime(text)
    total_cents, currency = extract_total(lines)
    items = extract_items(lines)
    if not items:
        # Rachunek bez kolumny "ilość × cena" — dopiero teraz warto ryzykować
        # luźniejszy wzorzec, bo alternatywą jest pusta lista.
        items = extract_items(lines, allow_bare=True)

    # Suma z linii "SUMA"/"Do zapłaty" bywa przekłamana albo nieobecna.
    # Rozstrzyga arytmetyka paragonu: jeśli odczytana suma nie domyka się
    # z pozycjami, a któryś z kandydatów rezerwowych ("RAZEM", rozbicie VAT,
    # "Podsuma", kwota zapłacona) domyka — wygrywa kandydat. Bez zgodności
    # z pozycjami kandydatów nie przyjmujemy wcale: przy gotówce "zapłacono"
    # bywa większe od sumy i wpisalibyśmy cichą nieprawdę.
    if check_needs_review(items, total_cents):
        for candidate in gated_total_candidates(lines):
            if not check_needs_review(items, candidate):
                total_cents = candidate
                break

    extraction = ReceiptExtraction(
        store_name=store_name,
        nip=nip,
        happened_at=happened_at,
        total_cents=total_cents,
        currency=currency or "PLN",
        items=items,
        raw_text=text,
        needs_review=check_needs_review(items, total_cents),
    )
    extraction.confidence = _confidence(extraction)
    return extraction
