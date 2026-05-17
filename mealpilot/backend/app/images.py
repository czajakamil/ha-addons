import os
from pathlib import Path

from .db import DB_PATH

IMAGES_DIR = Path(os.environ.get("MEALPILOT_IMAGES_DIR", str(Path(DB_PATH).parent / "images")))

ALLOWED_CONTENT_TYPES = {
    "image/jpeg": "jpg",
    "image/png": "png",
    "image/webp": "webp",
}
MAX_IMAGE_BYTES = 10 * 1024 * 1024

# (magic_bytes, offset) → extension
_MAGIC: list[tuple[bytes, int, str]] = [
    (b"\xff\xd8\xff", 0, "jpg"),
    (b"\x89PNG\r\n\x1a\n", 0, "png"),
    # WebP: "RIFF" at 0, "WEBP" at 8
    (b"RIFF", 0, "_riff"),
    (b"WEBP", 8, "webp"),
]


def detect_image_ext(data: bytes) -> str | None:
    """Return the file extension inferred from magic bytes, or None if unknown."""
    if data[:3] == b"\xff\xd8\xff":
        return "jpg"
    if data[:8] == b"\x89PNG\r\n\x1a\n":
        return "png"
    if data[:4] == b"RIFF" and len(data) >= 12 and data[8:12] == b"WEBP":
        return "webp"
    return None
