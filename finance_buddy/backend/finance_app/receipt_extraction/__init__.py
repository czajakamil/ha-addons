"""Ekstrakcja strukturalna paragonów (Etap 1 pipeline'u OCR).

Silnik wybierany przez konfigurację `RECEIPT_EXTRACTOR`: "rapidocr"
(domyślny — zmierzony jako najlepszy dostępny na Linux/aarch64, patrz
`docs/design/ocr-quality-log.md` Runda 4) albo "tesseract" (wariant A,
zostaje jako zapasowy). Interfejs projektowany pod przyszłe backendy
(lokalny VLM / API chmurowe).
"""

from __future__ import annotations

from ..config import settings
from .base import (
    ExtractedItem,
    ReceiptExtraction,
    ReceiptExtractionDependencyError,
    ReceiptExtractor,
)
from .parsing import normalize_nip, parse_receipt_text
from .tesseract import TesseractExtractor

__all__ = [
    "ExtractedItem",
    "ReceiptExtraction",
    "ReceiptExtractionDependencyError",
    "ReceiptExtractor",
    "TesseractExtractor",
    "create_extractor",
    "normalize_nip",
    "parse_receipt_text",
]


def create_extractor(engine: str | None = None) -> ReceiptExtractor:
    """Fabryka silników ekstrakcji; domyślnie silnik z konfiguracji."""
    chosen = (engine or settings.receipt_extractor).strip().lower()
    if chosen == "tesseract":
        return TesseractExtractor()
    if chosen == "rapidocr":
        # Import lokalny: moduł ciągnie numpy/onnxruntime, których instalacja
        # z samym Tesseractem nie potrzebuje.
        from .rapidocr import RapidOcrExtractor

        return RapidOcrExtractor()
    raise ValueError(f"Nieznany silnik ekstrakcji paragonów: {chosen!r}")
