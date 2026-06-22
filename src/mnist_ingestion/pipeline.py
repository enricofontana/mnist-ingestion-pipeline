from __future__ import annotations

import csv
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from mnist_ingestion.config import (
    EXPECTED_METADATA_COLUMNS,
    INGESTION_METHOD,
    PIPELINE_VERSION,
    PipelinePaths,
)
from mnist_ingestion.db import (
    complete_batch,
    complete_batch_run,
    connect,
    init_db,
    insert_batch_run_start,
    insert_image_record,
    insert_validation_events,
    upsert_batch,
    upsert_object,
)
from mnist_ingestion.metadata import (
    read_image_dimensions,
    sha256_file,
    stable_image_id,
    try_read_image_dimensions,
    utc_now_iso,
)
from mnist_ingestion.storage import store_content_addressed_object
from mnist_ingestion.validation import validate_record, validation_status


@dataclass(frozen=True)
class PipelineResult:
    batch_id: str
    total_records: int
    passed_records: int
    failed_records: int
    database_path: Path


@dataclass(frozen=True)
class IncomingRecord:
    row_number: int
    filename: str
    label: Optional[str]
    metadata_present: bool = True


def _read_metadata(metadata_path: Path) -> list[IncomingRecord]:
    records: list[IncomingRecord] = []
    with metadata_path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        fieldnames = reader.fieldnames or []
        if fieldnames != EXPECTED_METADATA_COLUMNS:
            raise ValueError(
                "metadata.csv schema mismatch: "
                f"expected columns {EXPECTED_METADATA_COLUMNS}, got {fieldnames}"
            )
        for index, row in enumerate(reader, start=1):
            records.append(
                IncomingRecord(
                    row_number=index,
                    filename=(row.get("filename") or "").strip(),
                    label=(row.get("label") or "").strip(),
                    metadata_present=True,
                )
            )
    return records


def _append_orphan_files(records: list[IncomingRecord], image_dir: Path) -> list[IncomingRecord]:
    """Add image files that arrived without a metadata row as failed records.

    This is the metadata-image cross-consistency check: extra files become
    explicit failed records, while metadata rows that reference missing files are
    caught by the per-record validator.
    """
    if not image_dir.exists() or not image_dir.is_dir():
        raise FileNotFoundError(f"Image directory not found: {image_dir}")
    known_filenames = {record.filename for record in records}
    next_row_number = len(records) + 1
    enriched = list(records)
    for image_path in sorted(image_dir.iterdir()):
        if image_path.is_file() and image_path.name not in known_filenames:
            enriched.append(
                IncomingRecord(
                    row_number=next_row_number,
                    filename=image_path.name,
                    label=None,
                    metadata_present=False,
                )
            )
            next_row_number += 1
    return enriched


def run_batch(
    *,
    project_root: Path | str,
    batch_id: str,
    expected_dimensions: tuple[int, int] = (28, 28),
) -> PipelineResult:
    paths = PipelinePaths.from_project_root(project_root)
    paths.ensure()

    batch_dir = paths.landing_dir / batch_id
    image_dir = batch_dir / "images"
    metadata_path = batch_dir / "metadata.csv"

    if not batch_dir.exists():
        raise FileNotFoundError(f"Batch directory not found: {batch_dir}")
    if not metadata_path.exists():
        raise FileNotFoundError(f"Metadata file not found: {metadata_path}")

    started_at = utc_now_iso()
    connection = connect(paths.database_path)
    init_db(connection)
    run_id = f"{batch_id}-{uuid.uuid4().hex[:12]}"
    insert_batch_run_start(
        connection,
        run_id=run_id,
        batch_id=batch_id,
        source_path=str(batch_dir),
        started_at=started_at,
        pipeline_version=PIPELINE_VERSION,
        ingestion_method=INGESTION_METHOD,
    )
    upsert_batch(
        connection,
        batch_id=batch_id,
        source_path=str(batch_dir),
        status="RUNNING",
        started_at=started_at,
    )

    records = _append_orphan_files(_read_metadata(metadata_path), image_dir)
    seen_hashes: set[str] = set()
    passed_records = 0
    failed_records = 0

    for record in records:
        timestamp = utc_now_iso()
        filename = record.filename
        image_id = stable_image_id(batch_id, filename, record.row_number)
        source_path = image_dir / filename if filename else image_dir / "<missing_filename>"
        content_hash: Optional[str] = None
        object_path: Optional[Path] = None
        width: Optional[int] = None
        height: Optional[int] = None

        if source_path.exists() and source_path.is_file():
            content_hash = sha256_file(source_path)
            object_path, _ = store_content_addressed_object(
                source=source_path,
                objects_dir=paths.objects_dir,
                content_hash=content_hash,
                source_filename=filename,
            )
            upsert_object(
                connection,
                sha256_hash=content_hash,
                object_path=str(object_path),
                original_filename=filename,
                first_seen_batch_id=batch_id,
                first_seen_image_id=image_id,
                created_at=timestamp,
            )
            dimensions = try_read_image_dimensions(source_path)
            if dimensions is not None:
                width, height = dimensions

        events = validate_record(
            source_path=source_path,
            label=record.label,
            content_hash=content_hash,
            seen_hashes=seen_hashes,
            expected_dimensions=expected_dimensions,
            metadata_present=record.metadata_present,
        )
        status, failure_reason = validation_status(events)

        # The stored bytes are content-addressed and written once per SHA-256.
        # raw/curated/quarantine paths below are catalogue pointers to the same
        # physical object, not additional byte copies. This separates physical
        # object identity from per-batch record identity.
        raw_object_str = str(object_path) if object_path and object_path.exists() else None
        curated_object_str = None
        quarantine_object_str = None

        if status == "PASSED":
            curated_object_str = raw_object_str
            passed_records += 1
            if content_hash is not None:
                seen_hashes.add(content_hash)
        else:
            quarantine_object_str = raw_object_str
            failed_records += 1
            # Add the hash after validation so the first occurrence can pass and
            # later duplicate objects fail within this batch.
            if content_hash is not None:
                seen_hashes.add(content_hash)

        label_int = None
        try:
            label_int = int(record.label) if record.label not in (None, "") else None
        except ValueError:
            label_int = None

        insert_image_record(
            connection,
            {
                "image_id": image_id,
                "batch_id": batch_id,
                "filename": filename,
                "label": label_int,
                "width": width,
                "height": height,
                "sha256_hash": content_hash,
                "source_path": str(source_path),
                "raw_object_path": raw_object_str,
                "curated_object_path": curated_object_str,
                "quarantine_object_path": quarantine_object_str,
                "ingestion_timestamp": timestamp,
                "validation_status": status,
                "failure_reason": failure_reason,
                "pipeline_version": PIPELINE_VERSION,
                "ingestion_method": INGESTION_METHOD,
            },
        )
        insert_validation_events(
            connection,
            image_id=image_id,
            batch_id=batch_id,
            events=events,
            created_at=timestamp,
        )
        connection.commit()

    total_records = len(records)
    completed_at = utc_now_iso()
    complete_batch(
        connection,
        batch_id=batch_id,
        completed_at=completed_at,
        total_records=total_records,
        passed_records=passed_records,
        failed_records=failed_records,
    )
    complete_batch_run(
        connection,
        run_id=run_id,
        status="COMPLETED_WITH_FAILURES" if failed_records else "COMPLETED",
        completed_at=completed_at,
        total_records=total_records,
        passed_records=passed_records,
        failed_records=failed_records,
    )
    connection.close()

    return PipelineResult(
        batch_id=batch_id,
        total_records=total_records,
        passed_records=passed_records,
        failed_records=failed_records,
        database_path=paths.database_path,
    )
