from __future__ import annotations

import shutil
from pathlib import Path

from mnist_ingestion.metadata import sha256_file


def copy_object_idempotent(source: Path, destination: Path) -> None:
    """Copy an object unless an identical destination object already exists.

    This keeps reruns safe: if the exact same object is already present, the
    copy is skipped. If a different object exists at the destination, the copy is
    rejected to avoid silently mutating already-ingested bytes.
    """
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        if sha256_file(source) == sha256_file(destination):
            return
        raise FileExistsError(
            f"Destination already exists with different content: {destination}"
        )
    shutil.copy2(source, destination)


def content_addressed_object_path(
    *,
    objects_dir: Path,
    content_hash: str,
    source_filename: str,
) -> Path:
    """Return the canonical object-store path for image bytes.

    The physical object identity is the SHA-256 hash, not the incoming filename.
    A short prefix directory avoids putting too many files in one folder.
    """
    suffix = Path(source_filename).suffix.lower() or ".bin"
    return objects_dir / content_hash[:2] / f"{content_hash}{suffix}"


def store_content_addressed_object(
    *,
    source: Path,
    objects_dir: Path,
    content_hash: str,
    source_filename: str,
) -> tuple[Path, bool]:
    """Store bytes once under a SHA-256 based path.

    Returns (object_path, created). If the same bytes are already present, the
    existing object path is reused and created=False.
    """
    object_path = content_addressed_object_path(
        objects_dir=objects_dir,
        content_hash=content_hash,
        source_filename=source_filename,
    )
    already_exists = object_path.exists()
    copy_object_idempotent(source, object_path)
    return object_path, not already_exists
