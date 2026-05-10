"""Delete image files in IMAGES_DIR that no recipe references.

Run from a cron job, e.g.:
    0 4 * * * docker exec mealpilot python -m backend.scripts.cleanup_orphan_images

Use --dry-run to preview without deleting.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Allow running as a script: `python backend/scripts/cleanup_orphan_images.py`
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.db import SessionLocal  # noqa: E402
from app.images import IMAGES_DIR  # noqa: E402
from app.models import Recipe  # noqa: E402


def find_orphans() -> list[Path]:
    db = SessionLocal()
    try:
        referenced = {
            name for (name,) in db.query(Recipe.image_filename).all() if name
        }
    finally:
        db.close()
    if not IMAGES_DIR.is_dir():
        return []
    return [p for p in IMAGES_DIR.iterdir() if p.is_file() and p.name not in referenced]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="Print files but do not delete")
    args = parser.parse_args()

    orphans = find_orphans()
    if not orphans:
        print("No orphan images.")
        return 0

    for path in orphans:
        if args.dry_run:
            print(f"[dry-run] would delete: {path}")
        else:
            path.unlink()
            print(f"deleted: {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
