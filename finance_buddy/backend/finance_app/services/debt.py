"""Wyliczenia dla kont typu `liability`: rozbicie raty i prognoza spłaty.

YNAB nie wystawia harmonogramu spłat przez API — pokazuje go tylko w swoim
interfejsie. Nie wystawia też podziału transakcji na kapitał i odsetki:
`debt_transaction_type` na transakcji jest zadeklarowane, ale w praktyce puste
(sprawdzone: `None` we wszystkich 3171 transakcjach konta testowego). Wszystko
poniżej jest więc LICZONE z salda, oprocentowania i raty — nic nie jest
pobierane.

Model jest świadomie prosty: rata stała, odsetki naliczane miesięcznie od
salda pozostałego, bez kapitalizacji dziennej i bez prowizji. Dla kredytu
o racie annuitetowej daje to wynik zgodny z harmonogramem banku co do
kilku groszy na ratę; nie jest to narzędzie do sporu z bankiem, tylko
odpowiedź na pytania „ile z raty poszło na odsetki" i „kiedy to się skończy".
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from enum import StrEnum

# Twardy limit iteracji prognozy. 600 miesięcy to 50 lat — dłuższego kredytu
# hipotecznego nie ma, a bez limitu każdy błąd w danych wejściowych zamienia
# pętlę w zawieszenie procesu.
MAX_FORECAST_MONTHS = 600

_BPS_PER_UNIT = 10_000
_MONTHS_PER_YEAR = 12


class ForecastStatus(StrEnum):
    """Dlaczego prognoza wyszła tak, jak wyszła.

    Rozróżnienie jest istotne dla interfejsu: „brak danych" to prośba
    o uzupełnienie warunków, a „nie spłaca się" to realna informacja
    o kredycie, którą trzeba pokazać, a nie ukryć jako brak wyniku.
    """

    OK = "ok"
    # Brak stopy albo brak raty w warunkach na dany miesiąc. Na żywym koncie
    # NULL-e są regułą, nie wyjątkiem (escrow bywa pustym słownikiem), więc to
    # nie jest przypadek brzegowy — to normalne wejście.
    MISSING_TERMS = "missing_terms"
    # Rata nie pokrywa nawet odsetek: saldo rośnie albo stoi w miejscu.
    NEVER_AMORTIZES = "never_amortizes"
    # Prognoza przekroczyła limit iteracji, choć saldo malało. Kredyt spłaci
    # się kiedyś, ale poza horyzontem, który ma sens do pokazania.
    EXCEEDS_HORIZON = "exceeds_horizon"


@dataclass(frozen=True)
class PaymentSplit:
    """Jedna rata rozbita na części."""

    month: str
    opening_balance_cents: int
    payment_cents: int
    interest_cents: int
    principal_cents: int
    closing_balance_cents: int


@dataclass(frozen=True)
class Forecast:
    """Wynik prognozy spłaty.

    Przy statusie innym niż `OK` pola liczbowe są `None` — celowo, żeby
    interfejs nie mógł przez przypadek pokazać zera jako „spłacone".
    """

    status: ForecastStatus
    months: int | None = None
    payoff_month: str | None = None
    total_interest_cents: int | None = None
    schedule: tuple[PaymentSplit, ...] = ()


def monthly_interest_cents(balance_cents: int, interest_rate_bps: int) -> int:
    """Odsetki za jeden miesiąc od salda `balance_cents`.

    `balance_cents` to DODATNIA wielkość długu. Saldo konta w bazie jest
    ujemne (suma transakcji), więc wołający musi je odwrócić — funkcja nie
    zgaduje znaku, bo pomyłka w tę stronę daje wynik wyglądający sensownie.
    """
    if balance_cents <= 0 or interest_rate_bps <= 0:
        return 0
    return round(balance_cents * interest_rate_bps / _BPS_PER_UNIT / _MONTHS_PER_YEAR)


def split_payment(
    balance_cents: int,
    interest_rate_bps: int,
    payment_cents: int,
    month: str = "",
) -> PaymentSplit:
    """Rozbija jedną ratę na odsetki i kapitał.

    Ostatnia rata jest przycinana do salda: nikt nie płaci więcej, niż jest
    winien, a bez tego prognoza kończyłaby się ujemnym saldem.
    """
    interest = monthly_interest_cents(balance_cents, interest_rate_bps)
    principal = min(payment_cents - interest, balance_cents)
    paid = interest + principal
    return PaymentSplit(
        month=month,
        opening_balance_cents=balance_cents,
        payment_cents=paid,
        interest_cents=interest,
        principal_cents=principal,
        closing_balance_cents=balance_cents - principal,
    )


def next_month(month: str) -> str:
    """`RRRR-MM` o jeden miesiąc dalej."""
    year, mon = (int(part) for part in month.split("-", 1))
    if mon >= _MONTHS_PER_YEAR:
        return f"{year + 1:04d}-01"
    return f"{year:04d}-{mon + 1:02d}"


def forecast_payoff(
    balance_cents: int,
    interest_rate_bps: int | None,
    payment_cents: int | None,
    start_month: str,
    max_months: int = MAX_FORECAST_MONTHS,
) -> Forecast:
    """Prognoza spłaty przy stałej stopie i stałej racie.

    `balance_cents` to DODATNIA wielkość długu (patrz `monthly_interest_cents`).

    Dwa wejścia nie dają wyniku liczbowego i muszą być obsłużone jawnie,
    bo inaczej kończą się pętlą bez końca albo bzdurą:
      * brak stopy albo brak raty — `MISSING_TERMS`, nie zgadujemy,
      * rata nie większa niż odsetki — `NEVER_AMORTIZES`, nie pętlimy.
    """
    if interest_rate_bps is None or payment_cents is None:
        return Forecast(status=ForecastStatus.MISSING_TERMS)
    if balance_cents <= 0:
        # Dług już spłacony: zero rat to poprawna odpowiedź, nie brak danych.
        return Forecast(
            status=ForecastStatus.OK,
            months=0,
            payoff_month=None,
            total_interest_cents=0,
        )
    if payment_cents <= 0:
        return Forecast(status=ForecastStatus.MISSING_TERMS)
    if payment_cents <= monthly_interest_cents(balance_cents, interest_rate_bps):
        return Forecast(status=ForecastStatus.NEVER_AMORTIZES)

    schedule: list[PaymentSplit] = []
    balance = balance_cents
    month = start_month
    total_interest = 0

    while balance > 0 and len(schedule) < max_months:
        # Saldo maleje z każdą ratą, więc odsetki też — skoro pierwsza rata
        # przeszła próg wyżej, kolejne przechodzą tym bardziej i kapitał nie
        # może zejść do zera. Przed pętlą bez końca broni sam `max_months`.
        step = split_payment(balance, interest_rate_bps, payment_cents, month)
        schedule.append(step)
        total_interest += step.interest_cents
        balance = step.closing_balance_cents
        if balance > 0:
            month = next_month(month)

    if balance > 0:
        return Forecast(status=ForecastStatus.EXCEEDS_HORIZON)

    return Forecast(
        status=ForecastStatus.OK,
        months=len(schedule),
        payoff_month=month,
        total_interest_cents=total_interest,
        schedule=tuple(schedule),
    )


@dataclass(frozen=True)
class DebtTerms:
    """Warunki na jeden miesiąc — kształt wiersza `account_debt_terms`."""

    month: str
    interest_rate_bps: int | None = None
    minimum_payment_cents: int | None = None
    escrow_cents: int | None = None


def current_terms(terms: Sequence[DebtTerms], as_of_month: str) -> DebtTerms | None:
    """Warunki obowiązujące w `as_of_month`.

    Szereg z YNAB-a jest rzadki: wpis pojawia się przy zmianie warunków, a nie
    co miesiąc. Obowiązuje więc ostatni wpis NIE PÓŹNIEJSZY niż pytany miesiąc.
    Gdy wszystkie wpisy są w przyszłości (świeżo założony kredyt), bierzemy
    najwcześniejszy — lepszy warunek z wyprzedzeniem niż pusty widok.
    """
    if not terms:
        return None
    ordered = sorted(terms, key=lambda row: row.month)
    past = [row for row in ordered if row.month <= as_of_month]
    return past[-1] if past else ordered[0]


def outstanding_from_balance(balance_cents: int) -> int:
    """Saldo konta (ujemne dla długu) na dodatnią wielkość zadłużenia.

    Saldo liczy się z sumy transakcji, więc kredyt ma je ujemne. Nadpłacony
    dług (saldo dodatnie) to zero do spłaty, nie ujemny dług.
    """
    return max(-balance_cents, 0)


def build_overview(
    terms: Sequence[DebtTerms],
    balance_cents: int,
    as_of_month: str,
    max_months: int = MAX_FORECAST_MONTHS,
) -> tuple[DebtTerms | None, Forecast]:
    """Warunki na dziś plus prognoza spłaty przy tych warunkach.

    Prognoza zakłada, że dzisiejsze warunki utrzymają się do końca — historia
    stóp służy do odtworzenia przeszłości, nie do przewidywania przyszłości.
    """
    active = current_terms(terms, as_of_month)
    if active is None:
        return None, Forecast(status=ForecastStatus.MISSING_TERMS)
    forecast = forecast_payoff(
        balance_cents=outstanding_from_balance(balance_cents),
        interest_rate_bps=active.interest_rate_bps,
        payment_cents=active.minimum_payment_cents,
        start_month=as_of_month,
        max_months=max_months,
    )
    return active, forecast
