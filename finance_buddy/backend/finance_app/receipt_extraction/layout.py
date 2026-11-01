"""Odtwarzanie linii paragonu z prostokątów słów zwróconych przez OCR.

Paragon ma dwie szpalty: nazwy towarów po lewej i kwoty po prawej, rozdzielone
szeroką przerwą. Tesseract w trybie płaskiego tekstu bierze tę przerwę za
granicę kolumn i czyta dokument szpaltami — najpierw wszystkie nazwy, potem
sam słupek liczb, który w oderwaniu od nazw rozpada się na szum. Efekt: nazwy
czytelne, kwot brak.

Tu składamy linie z powrotem po współrzędnych: słowa leżące na tej samej
wysokości to jedna linia paragonu, niezależnie od tego, jak szeroka przerwa je
dzieli.

Moduł jest czysty — bez Tesseracta i bez PIL — więc grupowanie testujemy na
samych prostokątach.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass

# Poniżej tego progu Tesseract zwraca zwykle artefakty tła (kropki, krawędzie
# zagiętego paragonu), które psują sklejanie linii.
MIN_CONFIDENCE = 30.0

# Dopuszczalne odchylenie środka słowa od środka linii, w ułamku mediany
# wysokości słowa. Paragony bywają lekko przekrzywione, więc próg musi być
# luźniejszy niż idealna równość, ale ciaśniejszy niż odstęp między wierszami.
LINE_TOLERANCE = 0.5


@dataclass(frozen=True)
class Word:
    """Pojedyncze słowo z OCR wraz z jego prostokątem na obrazie."""

    text: str
    left: int
    top: int
    width: int
    height: int
    conf: float = 100.0

    @property
    def center_y(self) -> float:
        return self.top + self.height / 2


class _Row:
    """Budowana linia: słowa plus bieżący środek pionowy."""

    def __init__(self, word: Word) -> None:
        self.words = [word]
        self._sum_center = word.center_y

    @property
    def center(self) -> float:
        return self._sum_center / len(self.words)

    def add(self, word: Word) -> None:
        self.words.append(word)
        self._sum_center += word.center_y

    def text(self) -> str:
        return " ".join(word.text for word in sorted(self.words, key=lambda w: w.left))


def group_words_into_lines(
    words: Iterable[Word],
    *,
    min_confidence: float = MIN_CONFIDENCE,
    tolerance: float = LINE_TOLERANCE,
) -> list[str]:
    """Skleja słowa w linie paragonu po wspólnej wysokości na obrazie."""
    kept = [word for word in words if word.text.strip() and word.conf >= min_confidence]
    if not kept:
        return []

    limit = max(1.0, _median_height(kept) * tolerance)
    rows: list[_Row] = []
    # Sortowanie po środku pionowym sprawia, że wystarczy porównywać
    # z ostatnio budowaną linią — kolejne słowo nie może trafić wyżej.
    for word in sorted(kept, key=lambda w: (w.center_y, w.left)):
        if rows and abs(word.center_y - rows[-1].center) <= limit:
            rows[-1].add(word)
        else:
            rows.append(_Row(word))
    return [row.text() for row in rows]


def _median_height(words: Sequence[Word]) -> float:
    heights = sorted(word.height for word in words)
    middle = heights[len(heights) // 2]
    return float(middle) if middle > 0 else 1.0
