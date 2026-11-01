"""Wejście importu paragonów: rozbicie wgranych plików na strony i dopasowanie sklepu.

Ekstrakcja strukturalna (NIP, sklep, data, pozycje, suma) żyje w
`receipt_extraction/`, a składanie szkiców skrzynki w
`services/receipt_drafts.py`; ten moduł odpowiada wyłącznie za obsługę plików
(obraz/PDF → strony) i dopasowanie sklepu po NIP-ie.
"""

from __future__ import annotations

import io
import logging
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path

from PIL import Image, ImageOps
from sqlalchemy import select
from sqlalchemy.orm import Session

from .models import Store
from .receipt_extraction import (
    ReceiptExtractionDependencyError,
    normalize_nip,
)

try:  # pypdfium2 is only needed for PDF uploads
    import pypdfium2 as pdfium
except Exception:  # pragma: no cover - dependency guard
    pdfium = None

log = logging.getLogger(__name__)


class ReceiptImportError(RuntimeError):
    """Generic processing error for a single receipt page."""


# Zachowujemy nazwę w kontrakcie routera; kanoniczna klasa żyje w module ekstrakcji.
ReceiptImportDependencyError = ReceiptExtractionDependencyError


@dataclass
class ReceiptPage:
    """Jedna strona wgranego pliku — kandydat na jeden paragon.

    `source_name` to nazwa pliku bez numeru strony: numer stoi obok, bo skrzynka
    filtruje i sortuje po obu, a data skanu czyta się właśnie z nazwy pliku.
    `error` niesie stronę, której nie dało się wczytać — nie gubimy jej po cichu.
    """

    source_name: str
    page_number: int
    image: Image.Image | None
    error: str | None = None


def _looks_like_pdf(filename: str | None, content_type: str | None) -> bool:
    if content_type and "pdf" in content_type.lower():
        return True
    return (filename or "").lower().endswith(".pdf")


# Render PDF-ów szedł kiedyś przez pdf2image/poppler, czyli przez zewnętrzną
# binarkę `pdftoppm` — nie do spakowania w build desktopowy, więc PDF-y padały
# na maszynie bez Homebrew. pypdfium2 wchodzi zwykłym wheelem (własny pdfium
# w środku), więc PyInstaller pakuje go razem z resztą.
#
# 200 DPI to nie przypadkowa liczba: tyle domyślnie dawał poppler i na tym
# mierzona jest jakość ekstrakcji, więc render musi zostać w tej samej skali.
_PDF_RENDER_DPI = 200


def _load_pdf_pages(file_bytes: bytes) -> list[Image.Image]:
    if pdfium is None:
        raise ReceiptImportDependencyError(
            "Brak pypdfium2 – dodaj pypdfium2 do zależności, aby przetwarzać PDF."
        )
    try:
        document = pdfium.PdfDocument(file_bytes)
        try:
            scale = _PDF_RENDER_DPI / 72  # pdfium liczy w punktach (72 pkt = 1 cal)
            return [page.render(scale=scale).to_pil().convert("RGB") for page in document]
        finally:
            document.close()
    except Exception as exc:  # pragma: no cover - defensive
        raise ReceiptImportError(f"Nie udało się odczytać PDF: {exc}") from exc


def _load_image(file_bytes: bytes) -> Image.Image:
    try:
        image = Image.open(io.BytesIO(file_bytes))
        converted = image.convert("RGB")
        # `exif_transpose` zwraca None tylko w trybie in_place; tu zawsze jest obraz.
        rotated = ImageOps.exif_transpose(converted)
        return rotated if rotated is not None else converted
    except Exception as exc:  # pragma: no cover - defensive
        raise ReceiptImportError(f"Nie udało się odczytać obrazu: {exc}") from exc


# Zmierzone: skan 4745×6273 zabija proces (SIGSEGV w `cv2` — zoptymalizowana
# pod ARM funkcja `resize`, wołana wewnątrz silnika OCR), 5000 px na dłuższym
# boku też, 4000 px jest bezpieczne. To nie jest błąd naszego kodu, ale pada
# CAŁY proces, więc jeden wielki skan potrafi ubić aplikację razem z sesją.
#
# Limit stoi tam, gdzie schodzą się strony PDF-ów i wgrane zdjęcia, żeby objął
# też fotkę z telefonu (48 Mpx to dziś norma). Zwykły paragon skanowany w 200
# DPI ma ~2300 px wysokości, więc nie jest ruszany — mierzona jakość ekstrakcji
# zostaje bez zmian.
_MAX_OCR_EDGE_PX = 4000


def _cap_for_ocr(image: Image.Image) -> Image.Image:
    longest_edge = max(image.size)
    if longest_edge <= _MAX_OCR_EDGE_PX:
        return image
    scale = _MAX_OCR_EDGE_PX / longest_edge
    smaller = image.resize(
        (round(image.width * scale), round(image.height * scale)),
        Image.Resampling.LANCZOS,
    )
    log.info("Zmniejszono stronę %s -> %s przed OCR.", image.size, smaller.size)
    image.close()
    return smaller


def build_store_index(db: Session, user_id: str) -> dict[str, Store]:
    stores = db.scalars(select(Store).where(Store.user_id == user_id, Store.nip.is_not(None))).all()
    # NIP scalonego sklepu wskazuje sklep docelowy.
    by_id = {store.id: store for store in db.scalars(select(Store).where(Store.user_id == user_id))}
    return {
        nip: by_id.get(store.merged_into_id, store) if store.merged_into_id else store
        for store in stores
        if (nip := normalize_nip(store.nip))
    }


def match_store(store_index: dict[str, Store], nip: str | None) -> Store | None:
    if not nip:
        return None
    return store_index.get(nip)


def sanitize_source_name(raw_name: str | None) -> str:
    return Path(raw_name or "paragon").name


def iter_receipt_pages(
    files: Iterable[tuple[str | None, str | None, bytes]],
) -> Iterator[ReceiptPage]:
    """Rozbija wgrane pliki na strony: 1 strona PDF-a albo 1 zdjęcie = 1 paragon.

    Obrazy oddaje wołający — po zapisaniu strony trzeba wywołać `image.close()`,
    bo wsad potrafi mieć kilkadziesiąt stron i trzymanie ich wszystkich w
    pamięci naraz jest niepotrzebne.
    """
    for filename, content_type, payload in files:
        source_name = sanitize_source_name(filename)
        try:
            pages = (
                _load_pdf_pages(payload)
                if _looks_like_pdf(filename, content_type)
                else [_load_image(payload)]
            )
        except ReceiptImportDependencyError:
            raise
        except Exception as exc:
            log.warning("Nie udało się wczytać pliku %s: %s", source_name, exc)
            yield ReceiptPage(source_name=source_name, page_number=1, image=None, error=str(exc))
            continue

        for idx, page in enumerate(pages, start=1):
            yield ReceiptPage(source_name=source_name, page_number=idx, image=_cap_for_ocr(page))
