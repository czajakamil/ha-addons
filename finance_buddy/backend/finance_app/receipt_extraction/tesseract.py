"""Silnik ekstrakcji wariant A: Tesseract OCR + regexy linii.

Warstwa obrazowa (preprocessing + OCR) jest tu, a całe parsowanie tekstu
w `parsing.py` i składanie linii w `layout.py` — dzięki temu testy parsowania
i układu nie wymagają Tesseracta.

OCR idzie przez `image_to_data` (prostokąty słów), a nie `image_to_string`
(płaski tekst): paragon ma dwie szpalty i płaski odczyt gubi kolumnę z
kwotami — szczegóły w `layout.py`.
"""

from __future__ import annotations

from PIL import Image, ImageOps

from .base import ReceiptExtraction, ReceiptExtractionDependencyError, ReceiptExtractor
from .layout import Word, group_words_into_lines
from .parsing import merge_extractions, parse_receipt_text

try:  # pytesseract jest opcjonalny do czasu użycia importu paragonów
    import pytesseract
except Exception:  # pragma: no cover - dependency guard
    # Patrz `rapidocr.py` — strażnikiem jest `ensure_ready()`, wołane przed OCR.
    pytesseract = None

# Dwa przebiegi, bo tryby segmentacji Tesseracta wymieniają się zaletami
# (zmierzone na zbiorze ground truth, patrz tools/receipt_eval):
# tryb automatyczny lepiej czyta nagłówek — NIP 49% vs 35%, data 68% vs 53%,
# a "jednolity blok" lepiej kolumnę pozycji — recall 41% vs 31%.
HEADER_CONFIG = ""
ITEMS_CONFIG = "--psm 6"


class TesseractExtractor(ReceiptExtractor):
    """Ekstrakcja paragonu lokalnie: Tesseract (pol+eng) + regexy."""

    header_config: str = HEADER_CONFIG
    items_config: str = ITEMS_CONFIG

    def ensure_ready(self) -> None:
        if pytesseract is None:
            raise ReceiptExtractionDependencyError(
                "Brak biblioteki pytesseract – zainstaluj pakiet pytesseract "
                "i binarkę Tesseract OCR."
            )

    @staticmethod
    def _prepare_for_ocr(image: Image.Image) -> Image.Image:
        grayscale = ImageOps.grayscale(image)
        return ImageOps.autocontrast(grayscale)

    def _ocr_words(self, image: Image.Image, config: str) -> list[Word]:
        """Słowa z ich prostokątami — podstawa odtworzenia wierszy paragonu."""
        self.ensure_ready()
        processed = self._prepare_for_ocr(image)
        # `ensure_ready()` wyżej rzuca wyjątek, gdy importu nie było.
        data = pytesseract.image_to_data(  # ty: ignore[unresolved-attribute]
            processed,
            lang="pol+eng",
            config=config,
            output_type=pytesseract.Output.DICT,  # ty: ignore[unresolved-attribute]
        )
        words: list[Word] = []
        for index, raw_text in enumerate(data["text"]):
            text = (raw_text or "").strip()
            if not text:
                continue
            words.append(
                Word(
                    text=text,
                    left=int(data["left"][index]),
                    top=int(data["top"][index]),
                    width=int(data["width"][index]),
                    height=int(data["height"][index]),
                    conf=_to_confidence(data["conf"][index]),
                )
            )
        return words

    def _ocr_text(self, image: Image.Image, config: str = ITEMS_CONFIG) -> str:
        return "\n".join(group_words_into_lines(self._ocr_words(image, config)))

    def ocr_passes(self, image: Image.Image) -> dict[str, str]:
        """Tekst obu przebiegów OCR — wejście dla `extract_from_texts`."""
        return {
            "header": self._ocr_text(image, self.header_config),
            "items": self._ocr_text(image, self.items_config),
        }

    @staticmethod
    def extract_from_texts(header_text: str, items_text: str) -> ReceiptExtraction:
        """Parsowanie obu przebiegów, bez dotykania obrazu.

        Wydzielone z `extract`, żeby dało się powtórzyć samo parsowanie na
        zapisanym tekście OCR — narzędzie pomiarowe iteruje po regexach bez
        ponownego uruchamiania Tesseracta (minuty zamiast sekund różnicy).
        """
        return merge_extractions(parse_receipt_text(header_text), parse_receipt_text(items_text))

    def extract(self, image: Image.Image) -> ReceiptExtraction:
        passes = self.ocr_passes(image)
        return self.extract_from_texts(passes["header"], passes["items"])


def _to_confidence(raw: str | float) -> float:
    """Pewność słowa; Tesseract zwraca ją jako tekst, a dla pustych jako -1."""
    try:
        return float(raw)
    except (TypeError, ValueError):
        return 0.0
