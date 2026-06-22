from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional

from PIL import Image


@dataclass(frozen=True)
class ValidationEvent:
    check_name: str
    status: str
    reason: Optional[str] = None


def _event(check_name: str, passed: bool, reason: Optional[str] = None) -> ValidationEvent:
    return ValidationEvent(check_name=check_name, status="PASS" if passed else "FAIL", reason=reason)


def validate_record(
    *,
    source_path: Path,
    label: Optional[str],
    content_hash: Optional[str],
    seen_hashes: set[str],
    expected_dimensions: tuple[int, int] = (28, 28),
    metadata_present: bool = True,
) -> list[ValidationEvent]:
    """Run quality checks for one image metadata record.

    Validation intentionally returns all events instead of raising at the first
    failure. This gives a complete audit trail and makes debugging batches much
    easier.
    """
    events: list[ValidationEvent] = []

    events.append(
        _event(
            "metadata_present",
            metadata_present,
            None if metadata_present else "Image file has no metadata row",
        )
    )

    label_present = label is not None and str(label).strip() != ""
    events.append(
        _event("label_present", label_present, None if label_present else "Missing label")
    )

    label_is_integer = False
    label_in_range = False
    if label_present:
        try:
            label_int = int(str(label))
            label_is_integer = True
            label_in_range = 0 <= label_int <= 9
        except ValueError:
            label_int = None

    events.append(
        _event(
            "label_is_integer",
            label_is_integer,
            None if label_is_integer else f"Label is not an integer: {label}",
        )
    )
    events.append(
        _event(
            "label_in_range_0_9",
            label_in_range,
            None if label_in_range else f"Label is outside expected range 0-9: {label}",
        )
    )

    file_exists = source_path.exists()
    events.append(
        _event(
            "file_exists",
            file_exists,
            None if file_exists else f"Image file does not exist: {source_path}",
        )
    )

    image_readable = False
    dimensions_ok = False
    if file_exists:
        try:
            with Image.open(source_path) as image:
                image.verify()
            image_readable = True
            with Image.open(source_path) as image:
                dimensions_ok = image.size == expected_dimensions
                dimensions_reason = (
                    None
                    if dimensions_ok
                    else f"Expected {expected_dimensions}, got {image.size}"
                )
        except Exception as exc:  # PIL exposes different exception types by format
            dimensions_reason = "Image could not be opened, dimensions unavailable"
            events.append(_event("image_readable", False, f"Unreadable image: {exc}"))
            events.append(_event("dimensions_28x28", False, dimensions_reason))
            if content_hash is not None:
                duplicate = content_hash in seen_hashes
                events.append(
                    _event(
                        "unique_content_hash_in_batch",
                        not duplicate,
                        None if not duplicate else "Duplicate content hash within batch",
                    )
                )
            else:
                events.append(
                    _event(
                        "unique_content_hash_in_batch",
                        False,
                        "Hash unavailable because file is missing or unreadable",
                    )
                )
            return events
    else:
        dimensions_reason = "Image file missing, dimensions unavailable"

    events.append(
        _event(
            "image_readable",
            image_readable,
            None if image_readable else "Image cannot be opened",
        )
    )
    events.append(_event("dimensions_28x28", dimensions_ok, dimensions_reason))

    if content_hash is not None:
        duplicate = content_hash in seen_hashes
        events.append(
            _event(
                "unique_content_hash_in_batch",
                not duplicate,
                None if not duplicate else "Duplicate content hash within batch",
            )
        )
    else:
        events.append(
            _event(
                "unique_content_hash_in_batch",
                False,
                "Hash unavailable because file is missing or unreadable",
            )
        )

    return events


def validation_status(events: Iterable[ValidationEvent]) -> tuple[str, Optional[str]]:
    failures = [event for event in events if event.status == "FAIL"]
    if not failures:
        return "PASSED", None
    return "FAILED", "; ".join(
        f"{event.check_name}: {event.reason or 'failed'}" for event in failures
    )
