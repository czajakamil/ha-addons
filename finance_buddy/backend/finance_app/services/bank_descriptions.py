"""Opisy transakcji z Enable Banking sprowadzone do nazwy sklepu i krótkiego komentarza.

PKO BP oddaje płatność kartą jako dwa pola tytułu: miejsce i typ operacji, np.
``["NOWY TARGKAUFLANDPL", "CARD-PAYMENT"]``. Miejsce to sklejone bez odstępów
miasto z terminala (najwyżej 13 znaków), nazwa akceptanta i kod kraju. Rozcinamy je,
żeby nazwa akceptanta trafiała do sklepu, a miasto do komentarza.

Miasto rozpoznajemy po zmianie wielkości liter („Nowy TargMcDonalds”), po liście
miast i po miastach rozpoznanych w tej samej paczce. Bez trafienia zostaje cały
napis — lepiej brzydki sklep niż ucięta nazwa.
"""

from __future__ import annotations

import re
import unicodedata
from collections.abc import Iterable, Sequence
from dataclasses import dataclass

CARD_PAYMENT = "CARD-PAYMENT"
# Pole miasta w danych terminala ma 13 znaków; dłuższe nazwy bank ucina.
CITY_FIELD_LENGTH = 13
SEPARATOR = " · "

# Typy operacji, które PKO dokleja jako ostatni element tytułu.
OPERATION_LABELS: dict[str, str | None] = {
    CARD_PAYMENT: None,
    "TRANSFER": None,
    "TRANSFER-IN": None,
    "TRANSFER-OUT": None,
    "CASH-IN": "Wpłata",
    "CASH-OUT": "Wypłata",
    "ATM": "Wypłata z bankomatu",
    "INSURANCE": "Ubezpieczenie",
    "DEBIT": "Obciążenie",
    "PAYCARD-TRANSFER": "Spłata karty",
    "BLIK": "BLIK",
}

_COUNTRIES = frozenset(
    """
    AD AE AF AG AI AL AM AO AQ AR AS AT AU AW AX AZ BA BB BD BE BF BG BH BI BJ BL BM BN
    BO BQ BR BS BT BV BW BY BZ CA CC CD CF CG CH CI CK CL CM CN CO CR CU CV CW CX CY CZ
    DE DJ DK DM DO DZ EC EE EG EH ER ES ET FI FJ FK FM FO FR GA GB GD GE GF GG GH GI GL
    GM GN GP GQ GR GS GT GU GW GY HK HM HN HR HT HU ID IE IL IM IN IO IQ IR IS IT JE JM
    JO JP KE KG KH KI KM KN KP KR KW KY KZ LA LB LC LI LK LR LS LT LU LV LY MA MC MD ME
    MF MG MH MK ML MM MN MO MP MQ MR MS MT MU MV MW MX MY MZ NA NC NE NF NG NI NL NO NP
    NR NU NZ OM PA PE PF PG PH PK PL PM PN PR PS PT PW PY QA RE RO RS RU RW SA SB SC SD
    SE SG SH SI SJ SK SL SM SN SO SR SS ST SV SX SY SZ TC TD TF TG TH TJ TK TL TM TN TO
    TR TT TV TW TZ UA UG UM US UY UZ VA VC VE VG VI VN VU WF WS YE YT ZA ZM ZW
    """.split()  # noqa: SIM905 — czytelniej niż 250 napisów w cudzysłowach
)

# Miasta wojewódzkie, największe miasta i okolice, z których przychodzą transakcje.
# Bez polskich znaków i wielkich liter — porównujemy po złożeniu `fold`.
_KNOWN_CITIES = """
Warszawa Krakow Lodz Wroclaw Poznan Gdansk Szczecin Bydgoszcz Lublin Bialystok Katowice
Gdynia Czestochowa Radom Torun Sosnowiec Rzeszow Kielce Gliwice Olsztyn Zabrze Bielsko-Biala
Bytom Zielona Gora Rybnik Ruda Slaska Opole Tychy Gorzow Wielkopolski Elblag Plock
Dabrowa Gornicza Walbrzych Wloclawek Tarnow Chorzow Koszalin Kalisz Legnica Grudziadz
Jaworzno Slupsk Jastrzebie-Zdroj Nowy Sacz Jelenia Gora Siedlce Myslowice Konin Pila
Piotrkow Trybunalski Inowroclaw Lubin Ostrow Wielkopolski Suwalki Stargard Gniezno
Ostrowiec Swietokrzyski Siemianowice Slaskie Glogow Pabianice Leszno Zamosc Lomza Zory
Pruszkow Elk Tomaszow Mazowiecki Chelm Mielec Kedzierzyn-Kozle Przemysl Stalowa Wola
Tczew Biala Podlaska Belchatow Swidnica Bedzin Zgierz Piekary Slaskie Raciborz Legionowo
Ostroleka Swietochlowice Wejherowo Zawiercie Starachowice Skierniewice Tarnowskie Gory
Pulawy Wodzislaw Slaski Starogard Gdanski Radomsko Krosno Otwock Skarzysko-Kamienna
Kolobrzeg Swinoujscie Sopot Malbork Zakopane Nowy Targ Bialy Dunajec Szaflary Poronin
Bukowina Tatrzanska Czarny Dunajec Rabka-Zdroj Koscielisko Szczawnica Kroscienko
Jablonka Limanowa Myslenice Wadowice Oswiecim Chrzanow Bochnia Wieliczka Skawina
Zywiec Andrychow Sucha Beskidzka Gorlice Jaslo Sanok Krynica-Zdroj Ustka Leba Hel
Wladyslawowo Jastarnia Mragowo Gizycko Augustow Szczytno Ilawa Ostroda Zlotow Raszyn
Janki Piaseczno Marki Zabki Zielonka Wolomin Grodzisk Mazowiecki Lomianki Konstancin-Jeziorna
Rumia Reda Pruszcz Gdanski Kartuzy Koscierzyna Chojnice Bytow Lebork Szczecinek Walcz
Swiecie Chelmno Brodnica Ciechanow Mlawa Plonsk Sochaczew Zyrardow Lowicz Kutno Sieradz
Zdunska Wola Wielun Lubliniec Kluczbork Nysa Brzeg Prudnik Klodzko Dzierzoniow Olawa
Olesnica Trzebnica Boleslawiec Zgorzelec Luban Lwowek Slaski Zary Zagan Nowa Sol Swiebodzin
Miedzyrzecz Sulechow Gubin Kostrzyn Mysliborz Pyrzyce Goleniow Police Nowogard Gryfice
Swidwin Bialogard Darlowo Miastko Czluchow Chodziez Wagrowiec Oborniki Szamotuly Wrzesnia
Sroda Wielkopolska Koscian Gostyn Krotoszyn Jarocin Pleszew Kepno Ostrzeszow Turek Kolo
Slupca Mogilno Znin Naklo nad Notecia Szubin Lipno Rypin Golub-Dobrzyn Wabrzezno Sierpc
Gostynin Pultusk Wyszkow Ostrow Mazowiecka Sokolow Podlaski Wegrow Minsk Mazowiecki
Garwolin Kozienice Zwolen Lipsko Opatow Sandomierz Staszow Busko-Zdroj Pinczow Jedrzejow
Wloszczowa Konskie Opoczno Przysucha Bialobrzegi Grojec Warka Gora Kalwaria Brzesko
Debica Ropczyce Lancut Przeworsk Jaroslaw Lubaczow Tomaszow Lubelski Hrubieszow
Krasnystaw Swidnik Leczna Lubartow Radzyn Podlaski Lukow Ryki Deblin Krasnik Janow Lubelski
Bilgoraj Lezajsk Nisko Tarnobrzeg Kolbuszowa Strzyzow Brzozow Lesko Ustrzyki Dolne
Hajnowka Bielsk Podlaski Siemiatycze Grajewo Kolno Zambrow Wysokie Mazowieckie Sokolka
Monki Sejny Olecko Goldap Wegorzewo Ketrzyn Bartoszyce Lidzbark Warminski Braniewo
Paslek Morag Nowe Miasto Lubawskie Dzialdowo Nidzica Pisz Gorowo Ilaweckie Kwidzyn
Sztum Prabuty Nowy Dwor Gdanski Puck Wolsztyn Grodzisk Wielkopolski Nowy Tomysl Lubon
Swarzedz Kornik Mosina Puszczykowo Tarnowo Podgorne Komorniki Dopiewo Suchy Las
Kobylnica Zabierzow Niepolomice Zielonki Michalowice Wegrzce
"""


def fold(value: str) -> str:
    """Wielkie litery bez polskich znaków i odstępów: do porównań nazw z banku."""
    decomposed = unicodedata.normalize("NFKD", value.replace("ł", "l").replace("Ł", "L"))
    plain = "".join(c for c in decomposed if not unicodedata.combining(c))
    return re.sub(r"[^0-9A-Z]", "", plain.upper())


# Lista wyżej jest rozdzielona spacjami, więc miasta wielowyrazowe są wyliczone wprost.
_MULTIWORD = {
    "Zielona Gora",
    "Ruda Slaska",
    "Gorzow Wielkopolski",
    "Dabrowa Gornicza",
    "Jastrzebie-Zdroj",
    "Nowy Sacz",
    "Jelenia Gora",
    "Piotrkow Trybunalski",
    "Ostrow Wielkopolski",
    "Ostrowiec Swietokrzyski",
    "Siemianowice Slaskie",
    "Tomaszow Mazowiecki",
    "Stalowa Wola",
    "Biala Podlaska",
    "Piekary Slaskie",
    "Tarnowskie Gory",
    "Wodzislaw Slaski",
    "Starogard Gdanski",
    "Nowy Targ",
    "Bialy Dunajec",
    "Bukowina Tatrzanska",
    "Czarny Dunajec",
    "Sucha Beskidzka",
    "Grodzisk Mazowiecki",
    "Pruszcz Gdanski",
    "Zdunska Wola",
    "Lwowek Slaski",
    "Nowa Sol",
    "Sroda Wielkopolska",
    "Naklo nad Notecia",
    "Ostrow Mazowiecka",
    "Sokolow Podlaski",
    "Minsk Mazowiecki",
    "Gora Kalwaria",
    "Tomaszow Lubelski",
    "Radzyn Podlaski",
    "Janow Lubelski",
    "Ustrzyki Dolne",
    "Bielsk Podlaski",
    "Wysokie Mazowieckie",
    "Lidzbark Warminski",
    "Nowe Miasto Lubawskie",
    "Gorowo Ilaweckie",
    "Nowy Dwor Gdanski",
    "Grodzisk Wielkopolski",
    "Nowy Tomysl",
    "Tarnowo Podgorne",
    "Suchy Las",
}
_MULTIWORD_PARTS = {part for city in _MULTIWORD for part in city.split()}
KNOWN_CITIES = frozenset(
    fold(city)
    for city in _MULTIWORD
    | {word for word in _KNOWN_CITIES.split() if word not in _MULTIWORD_PARTS}
)


@dataclass(frozen=True)
class CardLocation:
    city: str | None
    merchant: str
    country: str | None


def _strip_country(raw: str) -> tuple[str, str | None]:
    if len(raw) > 2 and raw[-2:].isupper() and raw[-2:] in _COUNTRIES:
        return raw[:-2].rstrip(), raw[-2:]
    return raw, None


def _looks_like_city(text: str) -> bool:
    """„krakow”, „Nowy Targ”, „Bielsko-Biala” — ale nie „ZAKOPANEMc”."""
    words = re.split(r"[\s-]+", text.strip())
    if not words or not all(words):
        return False
    if all(w.isalpha() and w.islower() for w in words):
        return True
    return all(w.isalpha() and w[0].isupper() and w[1:].islower() for w in words)


def _case_split(text: str) -> int | None:
    """Pierwsze przejście z małej litery na wielką lub cyfrę w obrębie pola miasta."""
    for i in range(1, min(len(text), CITY_FIELD_LENGTH + 1)):
        if text[i - 1].islower() and (text[i].isupper() or text[i].isdigit()):
            return i if _looks_like_city(text[:i]) else None
    return None


def _known_split(text: str, cities: frozenset[str] | set[str]) -> int | None:
    """Długość najdłuższego znanego miasta na początku napisu.

    Pełne 13 znaków pola miasta może być ucięte („KEDZIERZYN-KO”), więc wtedy
    wystarczy, że to początek znanej nazwy.
    """
    best: int | None = None
    for i in range(1, min(len(text) - 1, CITY_FIELD_LENGTH) + 1):
        folded = fold(text[:i])
        # Trzyliterowe miasta (Hel, Ełk) za często są początkiem nazwy („HELIOS”).
        if len(folded) < 4 or text[i - 1].isspace():
            continue
        if folded in cities or (
            i == CITY_FIELD_LENGTH and any(city.startswith(folded) for city in cities)
        ):
            best = i
    return best


def _repeats_after_city_field(text: str) -> bool:
    head = text[:CITY_FIELD_LENGTH].upper()
    return len(text) > CITY_FIELD_LENGTH and text[CITY_FIELD_LENGTH:].upper().startswith(head[:5])


def learned_cities(raws: Iterable[str]) -> set[str]:
    """Miasta rozpoznane po wielkości liter — pomagają rozciąć te same miasta wielkimi."""
    cities: set[str] = set()
    for raw in raws:
        text, _ = _strip_country(raw.strip())
        split = _case_split(text)
        if split:
            cities.add(fold(text[:split]))
    return cities


def split_card_location(raw: str, extra_cities: Iterable[str] = ()) -> CardLocation:
    text, country = _strip_country(raw.strip())
    split = _case_split(text)
    if split is None:
        split = _known_split(text, KNOWN_CITIES | set(extra_cities))
    if split is None and _repeats_after_city_field(text):
        # Sklepy internetowe wpisują adres zamiast miasta: „APPLE.COM/BIL” + „APPLE.COM/BILL”.
        return CardLocation(city=None, merchant=text[CITY_FIELD_LENGTH:], country=country)
    if split is None:
        return CardLocation(city=None, merchant=text, country=country)
    merchant = text[split:].strip()
    if not merchant:
        return CardLocation(city=None, merchant=text, country=country)
    return CardLocation(city=_title(text[:split].strip()), merchant=merchant, country=country)


def _title(city: str) -> str:
    return "-".join(
        " ".join(word.capitalize() for word in part.split()) for part in city.split("-")
    )


@dataclass(frozen=True)
class Description:
    payee: str | None
    memo: str | None


def describe(
    payee: str | None, remittance: Sequence[str], extra_cities: Iterable[str] = ()
) -> Description:
    """Sklep i komentarz z kontrahenta i pól tytułu; bez kodów typu operacji."""
    parts = [p.strip() for p in remittance if p and p.strip()]
    operation = parts[-1] if parts and parts[-1] in OPERATION_LABELS else None
    if operation is not None:
        parts = parts[:-1]
    if operation == CARD_PAYMENT and not payee and parts:
        location = split_card_location(parts[0], extra_cities)
        memo_parts = [location.city, *parts[1:]]
        return Description(
            payee=location.merchant, memo=SEPARATOR.join(p for p in memo_parts if p) or None
        )
    if not payee and parts:
        payee, parts = parts[0], parts[1:]
    label = OPERATION_LABELS.get(operation or "")
    seen = {fold(payee)} if payee else set()
    memo_parts: list[str] = []
    for part in [*parts, *([label] if label else [])]:
        if fold(part) not in seen:
            seen.add(fold(part))
            memo_parts.append(part)
    return Description(payee=payee, memo=SEPARATOR.join(memo_parts) or None)


def card_location_of(remittance: Sequence[str], payee: str | None) -> str | None:
    """Surowe pole miejsca płatności kartą (do uczenia się miast w paczce)."""
    parts = [p.strip() for p in remittance if p and p.strip()]
    if not payee and len(parts) >= 2 and parts[-1] == CARD_PAYMENT:
        return parts[0]
    return None
