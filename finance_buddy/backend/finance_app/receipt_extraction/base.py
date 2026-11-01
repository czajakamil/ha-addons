"""Wspólny interfejs silników ekstrakcji paragonów.

Silnik (Tesseract, w przyszłości VLM/chmura) dostaje obraz strony paragonu
i zwraca ustrukturyzowany wynik `ReceiptExtraction`. Dzięki temu pipeline
importu nie zależy od konkretnej implementacji OCR.
"""

from __future__ import annotations

import abc
import datetime as dt
from dataclasses import dataclass, field

from PIL import Image


class ReceiptExtractionDependencyError(RuntimeError):
    """Brak zależności systemowych wymaganych przez silnik ekstrakcji."""


@dataclass
class ExtractedItem:
    """Pojedyncza pozycja odczytana z paragonu.

    `line_total_cents` to wartość **sprzed** rabatu, gdy paragon drukuje rabat
    osobną linią — ten sam niezmiennik, co w `ReceiptDraftItem`
    (`backend/finance_app/schemas.py`) i `receipt_checks.items_balance_cents`:
    Σ(wartość pozycji + rabat pozycji) + rabat na paragon = suma. Rabat nie
    jest osobną pozycją listy — to zepsułoby dalszy pipeline (szkic paragonu
    pokazałby go jako kupiony produkt „Rabat").
    """

    raw_name: str
    quantity: float | None = None
    unit: str | None = None
    unit_price_cents: int | None = None
    line_total_cents: int | None = None
    discount_cents: int | None = None
    discount_label: str | None = None


@dataclass
class ReceiptExtraction:
    """Ustrukturyzowany wynik ekstrakcji jednej strony paragonu."""

    store_name: str | None = None
    nip: str | None = None
    happened_at: dt.datetime | None = None
    total_cents: int | None = None
    currency: str = "PLN"
    items: list[ExtractedItem] = field(default_factory=list)
    raw_text: str = ""
    confidence: float = 0.0
    # Walidacja krzyżowa: suma pozycji vs total_cents (tolerancja 1%).
    needs_review: bool = True


class ReceiptExtractor(abc.ABC):
    """Bazowy interfejs silnika ekstrakcji paragonów."""

    @abc.abstractmethod
    def extract(self, image: Image.Image) -> ReceiptExtraction:
        """Przetwarza obraz strony paragonu na ustrukturyzowany wynik."""

    def ensure_ready(self) -> None:  # noqa: B027 -- intentional no-op default hook
        """Sprawdza zależności silnika przed przetwarzaniem (fail fast).

        Domyślnie brak wymagań; implementacje mogą rzucić
        `ReceiptExtractionDependencyError`.
        """
