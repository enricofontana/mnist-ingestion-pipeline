#!/usr/bin/env python3
"""Single-file MNIST-style ingestion pipeline.

This script is intentionally small and reviewer-friendly. It accepts an incoming
batch directory containing `metadata.csv` and an `images/` folder, validates every
record, writes one metadata row per image, and stores image bytes in a local
object-store directory.

Object identity is content-addressed by SHA-256. Record identity remains the
per-batch `image_id`, so the same bytes can appear in multiple batches while
being stored only once.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import shutil
import sqlite3
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from PIL import Image, UnidentifiedImageError

EXPECTED_DIMENSIONS = (28, 28)
EXPECTED_METADATA_COLUMNS = ["filename", "label"]
PIPELINE_VERSION = "0.2.0"
INGESTION_METHOD = "local_batch_cli"

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS metadata (
    image_id TEXT PRIMARY KEY,
    batch_id TEXT NOT NULL,
    source_filename TEXT NOT NULL,
    label INTEGER,
    image_width INTEGER,
    image_height INTEGER,
    content_hash TEXT NOT NULL DEFAULT '',
    object_path TEXT,
    ingestion_timestamp TEXT,
    validation_status TEXT,
    failure_reasons TEXT,
    pipeline_version TEXT NOT NULL DEFAULT '0.2.0',
    ingestion_method TEXT NOT NULL DEFAULT 'local_batch_cli'
);

CREATE TABLE IF NOT EXISTS processed_batches (
    batch_id TEXT PRIMARY KEY,
    input_dir TEXT NOT NULL,
    output_dir TEXT NOT NULL,
    status TEXT NOT NULL,
    total_records INTEGER NOT NULL DEFAULT 0,
    passed_records INTEGER NOT NULL DEFAULT 0,
    failed_records INTEGER NOT NULL DEFAULT 0,
    started_at TEXT NOT NULL,
    completed_at TEXT
);

CREATE TABLE IF NOT EXISTS batch_runs (
    run_id TEXT PRIMARY KEY,
    batch_id TEXT NOT NULL,
    input_dir TEXT NOT NULL,
    output_dir TEXT NOT NULL,
    status TEXT NOT NULL,
    validate_only INTEGER NOT NULL DEFAULT 0,
    force INTEGER NOT NULL DEFAULT 0,
    total_records INTEGER NOT NULL DEFAULT 0,
    passed_records INTEGER NOT NULL DEFAULT 0,
    failed_records INTEGER NOT NULL DEFAULT 0,
    started_at TEXT NOT NULL,
    completed_at TEXT,
    error_message TEXT,
    pipeline_version TEXT NOT NULL,
    ingestion_method TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_metadata_batch_id ON metadata(batch_id);
CREATE INDEX IF NOT EXISTS idx_metadata_content_hash ON metadata(content_hash);
CREATE INDEX IF NOT EXISTS idx_metadata_validation_status ON metadata(validation_status);
CREATE INDEX IF NOT EXISTS idx_batch_runs_batch_id ON batch_runs(batch_id);
"""


@dataclass(frozen=True)
class CsvRecord:
    row_number: int
    filename: str
    label_raw: Optional[str]
    metadata_present: bool = True


@dataclass(frozen=True)
class ProcessedRecord:
    image_id: str
    batch_id: str
    source_filename: str
    label: Optional[int]
    image_width: Optional[int]
    image_height: Optional[int]
    content_hash: str
    object_path: Optional[str]
    ingestion_timestamp: str
    validation_status: str
    failure_reasons: list[str]
    pipeline_version: str
    ingestion_method: str


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )


def load_metadata_csv(input_dir: Path) -> list[CsvRecord]:
    metadata_path = input_dir / "metadata.csv"
    if not metadata_path.exists():
        raise FileNotFoundError(f"metadata.csv not found in {input_dir}")

    records: list[CsvRecord] = []
    seen_filenames: set[str] = set()

    with metadata_path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        fieldnames = reader.fieldnames or []
        if fieldnames != EXPECTED_METADATA_COLUMNS:
            raise ValueError(
                "metadata.csv schema mismatch: "
                f"expected columns {EXPECTED_METADATA_COLUMNS}, got {fieldnames}"
            )

        for row_number, row in enumerate(reader, start=1):
            filename = (row.get("filename") or "").strip()
            label = (row.get("label") or "").strip()
            records.append(CsvRecord(row_number=row_number, filename=filename, label_raw=label))
            seen_filenames.add(filename)

    # Metadata-image cross-consistency: include orphan files so they become
    # explicit failed catalogue records instead of silent leftovers. Missing
    # files referenced by metadata.csv are detected during per-record validation.
    images_dir = input_dir / "images"
    if not images_dir.exists() or not images_dir.is_dir():
        raise FileNotFoundError(f"images/ directory not found in {input_dir}")

    next_row_number = len(records) + 1
    orphan_count = 0
    for image_path in sorted(images_dir.iterdir()):
        if image_path.is_file() and image_path.name not in seen_filenames:
            records.append(
                CsvRecord(
                    row_number=next_row_number,
                    filename=image_path.name,
                    label_raw=None,
                    metadata_present=False,
                )
            )
            orphan_count += 1
            next_row_number += 1

    if orphan_count:
        logging.warning("Found %d image file(s) without metadata rows", orphan_count)

    return records


def stable_image_id(batch_id: str, filename: str, row_number: int) -> str:
    value = f"{batch_id}:{row_number}:{filename}".encode("utf-8")
    return hashlib.sha256(value).hexdigest()[:24]


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_image_dimensions(path: Path) -> tuple[int, int]:
    with Image.open(path) as image:
        return image.size


def object_path_for_hash(output_dir: Path, content_hash: str, source_filename: str) -> Path:
    suffix = Path(source_filename).suffix.lower() or ".bin"
    return output_dir / "objects" / content_hash[:2] / f"{content_hash}{suffix}"


def copy_object_once(source_path: Path, destination_path: Path) -> None:
    destination_path.parent.mkdir(parents=True, exist_ok=True)
    if destination_path.exists():
        # Same hash means same canonical object path. Do not duplicate bytes.
        return
    shutil.copy2(source_path, destination_path)


def parse_label(label_raw: Optional[str], failure_reasons: list[str]) -> Optional[int]:
    if label_raw in (None, ""):
        failure_reasons.append("label_missing")
        return None

    try:
        label = int(label_raw)
    except ValueError:
        failure_reasons.append(f"label_not_integer:{label_raw}")
        return None

    if not 0 <= label <= 9:
        failure_reasons.append(f"label_out_of_range:{label}")

    return label


def validate_and_process_record(
    *,
    record: CsvRecord,
    batch_id: str,
    input_dir: Path,
    output_dir: Path,
    seen_filenames: set[str],
    seen_hashes: set[str],
    validate_only: bool,
) -> ProcessedRecord:
    failure_reasons: list[str] = []
    timestamp = utc_now_iso()
    image_id = stable_image_id(batch_id, record.filename, record.row_number)

    if not record.metadata_present:
        failure_reasons.append("metadata_missing_for_image_file")

    if not record.filename:
        failure_reasons.append("filename_missing")

    if record.filename in seen_filenames:
        failure_reasons.append("duplicate_filename_in_batch")
    else:
        seen_filenames.add(record.filename)

    label = parse_label(record.label_raw, failure_reasons)

    source_path = input_dir / "images" / record.filename if record.filename else input_dir / "images"
    content_hash = ""
    width: Optional[int] = None
    height: Optional[int] = None
    object_rel_path: Optional[str] = None

    if not source_path.exists() or not source_path.is_file():
        failure_reasons.append("image_file_missing")
    else:
        content_hash = sha256_file(source_path)
        if content_hash in seen_hashes:
            failure_reasons.append("duplicate_content_hash_in_batch")
        else:
            seen_hashes.add(content_hash)

        try:
            width, height = read_image_dimensions(source_path)
            if (width, height) != EXPECTED_DIMENSIONS:
                failure_reasons.append(f"invalid_dimensions:{width}x{height}")
        except (UnidentifiedImageError, OSError, ValueError) as exc:
            failure_reasons.append(f"image_not_readable:{exc}")

    status = "FAIL" if failure_reasons else "PASS"

    if status == "PASS" and content_hash and not validate_only:
        absolute_object_path = object_path_for_hash(output_dir, content_hash, record.filename)
        copy_object_once(source_path, absolute_object_path)
        object_rel_path = str(absolute_object_path.relative_to(output_dir))

    return ProcessedRecord(
        image_id=image_id,
        batch_id=batch_id,
        source_filename=record.filename,
        label=label,
        image_width=width,
        image_height=height,
        content_hash=content_hash,
        object_path=object_rel_path,
        ingestion_timestamp=timestamp,
        validation_status=status,
        failure_reasons=failure_reasons,
        pipeline_version=PIPELINE_VERSION,
        ingestion_method=INGESTION_METHOD,
    )


def connect_database(output_dir: Path) -> sqlite3.Connection:
    output_dir.mkdir(parents=True, exist_ok=True)
    db_path = output_dir / "metadata.db"
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    connection.executescript(SCHEMA_SQL)
    # Lightweight migrations for reruns against an older local metadata.db.
    for column_name, column_type in {
        "pipeline_version": "TEXT NOT NULL DEFAULT '0.2.0'",
        "ingestion_method": "TEXT NOT NULL DEFAULT 'local_batch_cli'",
    }.items():
        existing_columns = {row[1] for row in connection.execute("PRAGMA table_info(metadata)")}
        if column_name not in existing_columns:
            connection.execute(f"ALTER TABLE metadata ADD COLUMN {column_name} {column_type}")
    connection.commit()
    return connection


def already_processed(connection: sqlite3.Connection, batch_id: str) -> bool:
    row = connection.execute(
        "SELECT status FROM processed_batches WHERE batch_id = ?",
        (batch_id,),
    ).fetchone()
    return bool(row and row["status"] in {"COMPLETED", "COMPLETED_WITH_FAILURES"})


def write_batch_run_start(
    connection: sqlite3.Connection,
    *,
    run_id: str,
    batch_id: str,
    input_dir: Path,
    output_dir: Path,
    started_at: str,
    validate_only: bool,
    force: bool,
) -> None:
    connection.execute(
        """
        INSERT INTO batch_runs(
            run_id, batch_id, input_dir, output_dir, status, validate_only, force,
            started_at, pipeline_version, ingestion_method
        )
        VALUES (?, ?, ?, ?, 'RUNNING', ?, ?, ?, ?, ?)
        """,
        (
            run_id,
            batch_id,
            str(input_dir),
            str(output_dir),
            int(validate_only),
            int(force),
            started_at,
            PIPELINE_VERSION,
            INGESTION_METHOD,
        ),
    )
    connection.commit()


def write_batch_run_complete(
    connection: sqlite3.Connection,
    *,
    run_id: str,
    status: str,
    total: int,
    passed: int,
    failed: int,
    completed_at: str,
    error_message: Optional[str] = None,
) -> None:
    connection.execute(
        """
        UPDATE batch_runs
        SET status = ?, total_records = ?, passed_records = ?, failed_records = ?,
            completed_at = ?, error_message = ?
        WHERE run_id = ?
        """,
        (status, total, passed, failed, completed_at, error_message, run_id),
    )
    connection.commit()


def write_processed_batch_start(
    connection: sqlite3.Connection,
    *,
    batch_id: str,
    input_dir: Path,
    output_dir: Path,
    started_at: str,
    force: bool,
) -> None:
    if force:
        connection.execute("DELETE FROM metadata WHERE batch_id = ?", (batch_id,))
        connection.execute("DELETE FROM processed_batches WHERE batch_id = ?", (batch_id,))

    connection.execute(
        """
        INSERT INTO processed_batches(
            batch_id, input_dir, output_dir, status, started_at
        )
        VALUES (?, ?, ?, 'RUNNING', ?)
        ON CONFLICT(batch_id) DO UPDATE SET
            input_dir = excluded.input_dir,
            output_dir = excluded.output_dir,
            status = 'RUNNING',
            started_at = excluded.started_at,
            completed_at = NULL,
            total_records = 0,
            passed_records = 0,
            failed_records = 0
        """,
        (batch_id, str(input_dir), str(output_dir), started_at),
    )
    connection.execute("DELETE FROM metadata WHERE batch_id = ?", (batch_id,))
    connection.commit()


def insert_metadata_row(connection: sqlite3.Connection, record: ProcessedRecord) -> None:
    connection.execute(
        """
        INSERT OR REPLACE INTO metadata(
            image_id,
            batch_id,
            source_filename,
            label,
            image_width,
            image_height,
            content_hash,
            object_path,
            ingestion_timestamp,
            validation_status,
            failure_reasons,
            pipeline_version,
            ingestion_method
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            record.image_id,
            record.batch_id,
            record.source_filename,
            record.label,
            record.image_width,
            record.image_height,
            record.content_hash,
            record.object_path,
            record.ingestion_timestamp,
            record.validation_status,
            json.dumps(record.failure_reasons),
            record.pipeline_version,
            record.ingestion_method,
        ),
    )


def write_processed_batch_complete(
    connection: sqlite3.Connection,
    *,
    batch_id: str,
    total: int,
    passed: int,
    failed: int,
    completed_at: str,
) -> None:
    status = "COMPLETED_WITH_FAILURES" if failed else "COMPLETED"
    connection.execute(
        """
        UPDATE processed_batches
        SET status = ?, total_records = ?, passed_records = ?, failed_records = ?, completed_at = ?
        WHERE batch_id = ?
        """,
        (status, total, passed, failed, completed_at, batch_id),
    )
    connection.commit()


def run_pipeline(input_dir: Path, output_dir: Path, validate_only: bool = False, force: bool = False) -> tuple[int, int, int]:
    input_dir = input_dir.resolve()
    output_dir = output_dir.resolve()
    batch_id = input_dir.name

    connection = connect_database(output_dir)
    started_at = utc_now_iso()
    run_id = f"{batch_id}-{uuid.uuid4().hex[:12]}"
    write_batch_run_start(
        connection,
        run_id=run_id,
        batch_id=batch_id,
        input_dir=input_dir,
        output_dir=output_dir,
        started_at=started_at,
        validate_only=validate_only,
        force=force,
    )

    if already_processed(connection, batch_id) and not force:
        logging.warning("Batch %s is already processed. Use --force to rebuild it.", batch_id)
        row = connection.execute(
            "SELECT total_records, passed_records, failed_records FROM processed_batches WHERE batch_id = ?",
            (batch_id,),
        ).fetchone()
        total, passed, failed = int(row["total_records"]), int(row["passed_records"]), int(row["failed_records"])
        write_batch_run_complete(
            connection,
            run_id=run_id,
            status="SKIPPED_ALREADY_PROCESSED",
            total=total,
            passed=passed,
            failed=failed,
            completed_at=utc_now_iso(),
        )
        connection.close()
        return total, passed, failed

    total = passed = failed = 0
    try:
        write_processed_batch_start(
            connection,
            batch_id=batch_id,
            input_dir=input_dir,
            output_dir=output_dir,
            started_at=started_at,
            force=force,
        )

        records = load_metadata_csv(input_dir)
        logging.info("Loaded %d metadata/image records for batch_id=%s", len(records), batch_id)

        seen_filenames: set[str] = set()
        seen_hashes: set[str] = set()

        for csv_record in records:
            processed = validate_and_process_record(
                record=csv_record,
                batch_id=batch_id,
                input_dir=input_dir,
                output_dir=output_dir,
                seen_filenames=seen_filenames,
                seen_hashes=seen_hashes,
                validate_only=validate_only,
            )
            insert_metadata_row(connection, processed)
            total += 1
            if processed.validation_status == "PASS":
                passed += 1
            else:
                failed += 1
                logging.warning(
                    "Record failed validation: filename=%s reasons=%s",
                    processed.source_filename,
                    processed.failure_reasons,
                )

        completed_at = utc_now_iso()
        write_processed_batch_complete(
            connection,
            batch_id=batch_id,
            total=total,
            passed=passed,
            failed=failed,
            completed_at=completed_at,
        )
        final_status = "COMPLETED_WITH_FAILURES" if failed else "COMPLETED"
        write_batch_run_complete(
            connection,
            run_id=run_id,
            status=final_status,
            total=total,
            passed=passed,
            failed=failed,
            completed_at=completed_at,
        )
        logging.info(
            "Batch completed: batch_id=%s total=%d passed=%d failed=%d validate_only=%s db=%s",
            batch_id,
            total,
            passed,
            failed,
            validate_only,
            output_dir / "metadata.db",
        )
        return total, passed, failed
    except Exception as exc:
        logging.error("Batch failed: batch_id=%s error=%s", batch_id, exc)
        write_batch_run_complete(
            connection,
            run_id=run_id,
            status="FAILED",
            total=total,
            passed=passed,
            failed=failed,
            completed_at=utc_now_iso(),
            error_message=str(exc),
        )
        raise
    finally:
        connection.close()

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="MNIST-style image ingestion pipeline")
    parser.add_argument("--input-dir", required=True, help="Incoming batch folder containing images/ and metadata.csv")
    parser.add_argument("--output-dir", required=True, help="Output object-store folder; metadata.db is written here")
    parser.add_argument("--validate-only", action="store_true", help="Validate and write metadata, but do not copy image objects")
    parser.add_argument("--force", action="store_true", help="Rebuild metadata for an already processed batch")
    return parser.parse_args()


def main() -> None:
    setup_logging()
    args = parse_args()
    run_pipeline(
        input_dir=Path(args.input_dir),
        output_dir=Path(args.output_dir),
        validate_only=args.validate_only,
        force=args.force,
    )


if __name__ == "__main__":
    main()
