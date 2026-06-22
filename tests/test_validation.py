from __future__ import annotations

from pathlib import Path

import numpy as np
from PIL import Image

from mnist_ingestion.metadata import sha256_file
from mnist_ingestion.validation import validate_record, validation_status


def _write_png(path: Path, size: tuple[int, int] = (28, 28)) -> None:
    image = Image.fromarray(np.zeros(size, dtype=np.uint8), mode="L")
    image.save(path)


def test_validator_passes_readable_28x28_image_with_label_between_0_and_9(tmp_path: Path) -> None:
    image_path = tmp_path / "image.png"
    _write_png(image_path)
    events = validate_record(
        source_path=image_path,
        label="7",
        content_hash=sha256_file(image_path),
        seen_hashes=set(),
    )

    status, reason = validation_status(events)

    assert status == "PASSED"
    assert reason is None


def test_validator_fails_when_label_is_outside_expected_0_to_9_range(tmp_path: Path) -> None:
    image_path = tmp_path / "image.png"
    _write_png(image_path)
    events = validate_record(
        source_path=image_path,
        label="12",
        content_hash=sha256_file(image_path),
        seen_hashes=set(),
    )

    status, reason = validation_status(events)

    assert status == "FAILED"
    assert "label_in_range_0_9" in reason


def test_validator_fails_when_image_dimensions_are_not_28x28(tmp_path: Path) -> None:
    image_path = tmp_path / "image.png"
    _write_png(image_path, size=(20, 20))
    events = validate_record(
        source_path=image_path,
        label="4",
        content_hash=sha256_file(image_path),
        seen_hashes=set(),
    )

    status, reason = validation_status(events)

    assert status == "FAILED"
    assert "dimensions_28x28" in reason


def test_validator_fails_when_content_hash_is_duplicate_within_batch(tmp_path: Path) -> None:
    image_path = tmp_path / "image.png"
    _write_png(image_path)
    digest = sha256_file(image_path)
    events = validate_record(
        source_path=image_path,
        label="4",
        content_hash=digest,
        seen_hashes={digest},
    )

    status, reason = validation_status(events)

    assert status == "FAILED"
    assert "unique_content_hash_in_batch" in reason
