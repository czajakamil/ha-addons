"""Silnik ekstrakcji wariant B: RapidOCR (PP-OCR przez ONNX Runtime).

Docelowy silnik dla wdrożenia jako add-on Home Assistanta na Raspberry Pi:
modele ~14 MB wgrane w pakiet, `onnxruntime` ma koła manylinux na aarch64
i nie potrzebuje binarki systemowej (Tesseract wymaga instalacji `tesseract`
z językiem `pol`). Jakość zmierzona na korpusie w `tools/receipt_eval` —
liczby i historia w `docs/design/ocr-quality-log.md` (Runda 4).

Warstwa parsowania jest w całości wspólna z wariantem Tesseracta: RapidOCR
zwraca słowa z prostokątami, które przechodzą przez to samo
`group_words_into_lines` i `parse_receipt_text`.
"""

from __future__ import annotations

from PIL import Image

from .base import ReceiptExtractionDependencyError
from .layout import Word, group_words_into_lines
from .tesseract import TesseractExtractor

try:  # rapidocr jest opcjonalny do czasu użycia importu paragonów
    import numpy as np
    from rapidocr_onnxruntime import RapidOCR
except Exception:  # pragma: no cover - dependency guard
    # Podmiana modułu/klasy na None jest dla type checkera niepoprawna z definicji
    # — i o to chodzi. Strażnikiem jest `ensure_ready()`, wołane przed każdym
    # użyciem silnika; wyciszamy punktowo, żeby reguły działały w reszcie pliku.
    np = None  # ty: ignore[invalid-assignment]
    RapidOCR = None


class RapidOcrExtractor(TesseractExtractor):
    """OCR przez PP-OCR (ONNX); parsowanie wspólne z wariantem Tesseracta.

    Dziedziczy po `TesseractExtractor` dla `extract`/`extract_from_texts` —
    warstwa obrazowa jest nadpisana w całości i Tesseract nie jest potrzebny.
    """

    # Modele ONNX ładują się ułamek sekundy i są bezstanowe między obrazami,
    # więc jedna instancja obsługuje wszystkie wywołania procesu.
    _engine = None

    def ensure_ready(self) -> None:
        if RapidOCR is None:
            raise ReceiptExtractionDependencyError(
                "Brak biblioteki rapidocr-onnxruntime – zainstaluj pakiet "
                "rapidocr-onnxruntime (modele OCR ma w środku)."
            )

    def _ocr(self):
        cls = type(self)
        if cls._engine is None:
            self.ensure_ready()
            # `ensure_ready()` wyżej rzuca wyjątek, gdy importu nie było.
            cls._engine = RapidOCR()  # ty: ignore[call-non-callable]
        return cls._engine

    def ocr_passes(self, image: Image.Image) -> dict[str, str]:
        """Jeden przebieg OCR — RapidOCR nie ma trybów segmentacji do scalania.

        Oba klucze dostają ten sam tekst, żeby zachować kontrakt
        `extract_from_texts` (scalenie dwóch identycznych przebiegów jest
        neutralne).
        """
        # Kolejność kanałów musi być RGB, nie BGR: RapidOCR dla ścieżki pliku
        # robi `np.array(Image.open(path))`, więc RGB dostają jego modele.
        # Odwrócenie kanałów "pod konwencję OpenCV" jest mierzalnie gorsze —
        # na korpusie 67 paragonów recall pozycji 93,5% → 92,6%, a paragonów
        # bez poprawek 52 → 49.
        pixels = np.array(image.convert("RGB"))
        result, _ = self._ocr()(pixels)
        words = []
        for box, text, score in result or []:
            xs = [point[0] for point in box]
            ys = [point[1] for point in box]
            words.append(
                Word(
                    text=str(text),
                    left=int(min(xs)),
                    top=int(min(ys)),
                    width=int(max(xs) - min(xs)),
                    height=int(max(ys) - min(ys)),
                    conf=float(score) * 100.0,
                )
            )
        text = "\n".join(group_words_into_lines(words))
        return {"header": text, "items": text}
