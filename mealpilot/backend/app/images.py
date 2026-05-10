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
