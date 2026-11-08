"""Rozcinanie opisu płatności kartą z PKO BP (Enable Banking) na sklep i miasto."""

from __future__ import annotations

import pytest

from backend.finance_app.services import bank_descriptions as bd

# Prawdziwe napisy z pobrania PKO BP (listopad 2026).
SAMPLES = [
    "Nowy TargLIDL SZAFLARSKAPL",
    "NOWY TARGKAUFLANDPL",
    "NOWY TARGNOWY TARGDIS SIKORSKIEGPL",
    "Nowy TargMcDonaldsPL",
    "krakowInfakt Sp z ooPL",
    "RaszynIKEA MEBLE PRZEZ INTERNETPL",
    "WroclawPyszne.plPL",
    "ZLOTOWMEDIAEXPERT PLPL",
    "ZAKOPANEGMINA MIASTO ZAKOPANE URZPL",
    "BIALY DUNAJECZABKA Z4253 K.1PL",
    "BialystokkuchniavikingaPL",
    "Nowy TargA570PL",
    "APPLE.COM/BILAPPLE.COM/BILLIE",
]


@pytest.mark.parametrize(
    ("raw", "city", "merchant"),
    [
        ("Nowy TargLIDL SZAFLARSKAPL", "Nowy Targ", "LIDL SZAFLARSKA"),
        ("NOWY TARGKAUFLANDPL", "Nowy Targ", "KAUFLAND"),
        ("NOWY TARGNOWY TARGDIS SIKORSKIEGPL", "Nowy Targ", "NOWY TARGDIS SIKORSKIEG"),
        ("Nowy TargMcDonaldsPL", "Nowy Targ", "McDonalds"),
        ("krakowInfakt Sp z ooPL", "Krakow", "Infakt Sp z oo"),
        ("RaszynIKEA MEBLE PRZEZ INTERNETPL", "Raszyn", "IKEA MEBLE PRZEZ INTERNET"),
        ("WroclawPyszne.plPL", "Wroclaw", "Pyszne.pl"),
        ("ZLOTOWMEDIAEXPERT PLPL", "Zlotow", "MEDIAEXPERT PL"),
        ("ZAKOPANEGMINA MIASTO ZAKOPANE URZPL", "Zakopane", "GMINA MIASTO ZAKOPANE URZ"),
        ("BIALY DUNAJECZABKA Z4253 K.1PL", "Bialy Dunajec", "ZABKA Z4253 K.1"),
        ("BialystokkuchniavikingaPL", "Bialystok", "kuchniavikinga"),
        ("Nowy TargA570PL", "Nowy Targ", "A570"),
        ("KEDZIERZYN-KOBIEDRONKA 12PL", "Kedzierzyn-Ko", "BIEDRONKA 12"),
        ("APPLE.COM/BILAPPLE.COM/BILLIE", None, "APPLE.COM/BILL"),
    ],
)
def test_splits_city_and_merchant(raw: str, city: str | None, merchant: str) -> None:
    location = bd.split_card_location(raw, bd.learned_cities(SAMPLES))
    assert (location.city, location.merchant) == (city, merchant)


def test_unknown_city_keeps_whole_text_instead_of_guessing() -> None:
    assert bd.split_card_location("HELIOS SAHELIOS KINOPL").merchant == "HELIOS SAHELIOS KINO"
    assert bd.split_card_location("PCIMSKLEP GROSIKPL").merchant == "PCIMSKLEP GROSIK"


def test_uppercase_city_learned_from_mixed_case_in_same_batch() -> None:
    raws = ["SzaflaryBIEDRONKAPL", "SZAFLARYORLEN STACJA 12PL"]
    location = bd.split_card_location(raws[1], bd.learned_cities(raws))
    assert (location.city, location.merchant) == ("Szaflary", "ORLEN STACJA 12")


def test_describe_card_payment_puts_city_in_memo() -> None:
    described = bd.describe(None, ["NOWY TARGKAUFLANDPL", "CARD-PAYMENT"])
    assert (described.payee, described.memo) == ("KAUFLAND", "Nowy Targ")


def test_describe_drops_or_translates_operation_codes() -> None:
    assert bd.describe("MARIA ANTOLEC", ["MARIA", "TRANSFER"]) == bd.Description(
        "MARIA ANTOLEC", "MARIA"
    )
    assert bd.describe(None, ["WPŁATA", "CASH-IN"]) == bd.Description("WPŁATA", None)
    assert bd.describe(None, ["ZH51ZH522", "INSURANCE"]) == bd.Description(
        "ZH51ZH522", "Ubezpieczenie"
    )
    # Nieznany kod zostaje, jak był.
    assert bd.describe(None, ["BIEDRONKA 123", "Płatność kartą"]) == bd.Description(
        "BIEDRONKA 123", "Płatność kartą"
    )
