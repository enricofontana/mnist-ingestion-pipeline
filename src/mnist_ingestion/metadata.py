from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Tuple

from PIL import Image


def utc_now_iso() -> str:
    """Return an ISO-8601 timestamp in UTC."""
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    """Compute a stable SHA-256 hash for an object."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


def stable_image_id(batch_id: str, filename: str, row_number: int) -> str:
    """Create a deterministic ID for reruns of the same batch.

    The row number is included so duplicate filenames in a metadata file remain
    traceable as separate incoming records instead of silently overwriting each
    other.
    """
    raw = f"{batch_id}:{filename}:{row_number}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()[:24]


def read_image_dimensions(path: Path) -> Tuple[int, int]:
    with Image.open(path) as image:
        return image.size


def try_read_image_dimensions(path: Path) -> Optional[Tuple[int, int]]:
    try:
        return read_image_dimensions(path)
    except Exception:
        return None
