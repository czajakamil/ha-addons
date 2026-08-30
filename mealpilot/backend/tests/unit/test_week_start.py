"""parse_week_start: tydzień jest kluczowany literalnym stringiem, więc nie-poniedziałek
cicho nie pasuje do niczego. Walidacja musi krzyczeć i podpowiedzieć właściwą datę.
"""

import pytest

from app.services.common import parse_week_start
from app.services.errors import Invalid

pytestmark = pytest.mark.unit

MONDAY = "2026-07-06"


def test_accepts_a_monday():
    assert parse_week_start(MONDAY) == MONDAY


def test_trims_surrounding_whitespace():
    assert parse_week_start(f"  {MONDAY}  ") == MONDAY


@pytest.mark.parametrize(
    ("value", "monday"),
    [
        ("2026-07-07", "2026-07-06"),  # wtorek
        ("2026-07-12", "2026-07-06"),  # niedziela
        ("2026-07-08", "2026-07-06"),  # środa
    ],
)
def test_rejects_non_monday_and_names_the_right_one(value, monday):
    with pytest.raises(Invalid) as exc:
        parse_week_start(value)
    message = str(exc.value)
    assert monday in message, f"komunikat powinien podpowiadać {monday}: {message}"
    assert value in message


def test_non_monday_message_names_the_weekday():
    with pytest.raises(Invalid) as exc:
        parse_week_start("2026-07-07")
    assert "wtorek" in str(exc.value)


@pytest.mark.parametrize("value", ["", "   ", None])
def test_rejects_empty(value):
    with pytest.raises(Invalid) as exc:
        parse_week_start(value)
    assert "wymagany" in str(exc.value)


@pytest.mark.parametrize(
    "value",
    [
        "kiedyś",
        "06-07-2026",
        "2026/07/06",
        "2026-7-6",
        "20260706",
        "2026-07-06T00:00:00",
    ],
)
def test_rejects_wrong_format(value):
    with pytest.raises(Invalid) as exc:
        parse_week_start(value)
    assert "YYYY-MM-DD" in str(exc.value)


@pytest.mark.parametrize("value", ["2026-13-01", "2026-02-30"])
def test_rejects_well_shaped_but_impossible_dates(value):
    with pytest.raises(Invalid) as exc:
        parse_week_start(value)
    assert "poprawną datą" in str(exc.value)


def test_error_is_the_domain_invalid_type():
    with pytest.raises(Invalid):
        parse_week_start("2026-07-07")
