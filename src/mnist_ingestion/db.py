from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Mapping, Sequence

from mnist_ingestion.validation import ValidationEvent

SCHEMA = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS batches (
    batch_id TEXT PRIMARY KEY,
    source_path TEXT NOT NULL,
    status TEXT NOT NULL,
    total_records INTEGER NOT NULL DEFAULT 0,
    passed_records INTEGER NOT NULL DEFAULT 0,
    failed_records INTEGER NOT NULL DEFAULT 0,
    started_at TEXT NOT NULL,
    completed_at TEXT
);

CREATE TABLE IF NOT EXISTS objects (
    sha256_hash TEXT PRIMARY KEY,
    object_path TEXT NOT NULL UNIQUE,
    original_filename TEXT NOT NULL,
    first_seen_batch_id TEXT NOT NULL,
    first_seen_image_id TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS images (
    image_id TEXT PRIMARY KEY,
    batch_id TEXT NOT NULL,
    filename TEXT NOT NULL,
    label INTEGER,
    width INTEGER,
    height INTEGER,
    sha256_hash TEXT,
    source_path TEXT NOT NULL,
    raw_object_path TEXT,
    curated_object_path TEXT,
    quarantine_object_path TEXT,
    ingestion_timestamp TEXT NOT NULL,
    validation_status TEXT NOT NULL,
    failure_reason TEXT,
    pipeline_version TEXT NOT NULL DEFAULT '0.2.0',
    ingestion_method TEXT NOT NULL DEFAULT 'modular_batch_cli',
    FOREIGN KEY(batch_id) REFERENCES batches(batch_id),
    FOREIGN KEY(sha256_hash) REFERENCES objects(sha256_hash)
);

CREATE TABLE IF NOT EXISTS batch_runs (
    run_id TEXT PRIMARY KEY,
    batch_id TEXT NOT NULL,
    source_path TEXT NOT NULL,
    status TEXT NOT NULL,
    total_records INTEGER NOT NULL DEFAULT 0,
    passed_records INTEGER NOT NULL DEFAULT 0,
    failed_records INTEGER NOT NULL DEFAULT 0,
    started_at TEXT NOT NULL,
    completed_at TEXT,
    error_message TEXT,
    pipeline_version TEXT NOT NULL,
    ingestion_method TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS validation_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    image_id TEXT NOT NULL,
    batch_id TEXT NOT NULL,
    check_name TEXT NOT NULL,
    status TEXT NOT NULL,
    reason TEXT,
    created_at TEXT NOT NULL,
    FOREIGN KEY(image_id) REFERENCES images(image_id),
    FOREIGN KEY(batch_id) REFERENCES batches(batch_id)
);

CREATE INDEX IF NOT EXISTS idx_images_batch_id ON images(batch_id);
CREATE INDEX IF NOT EXISTS idx_images_hash ON images(sha256_hash);
CREATE INDEX IF NOT EXISTS idx_batch_runs_batch_id ON batch_runs(batch_id);
CREATE INDEX IF NOT EXISTS idx_validation_events_image_id ON validation_events(image_id);
"""


def connect(database_path: Path) -> sqlite3.Connection:
    database_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(database_path)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    return connection


def init_db(connection: sqlite3.Connection) -> None:
    connection.executescript(SCHEMA)
    # Lightweight migrations for older local catalogues created before
    # provenance fields were introduced.
    existing_columns = {row[1] for row in connection.execute("PRAGMA table_info(images)")}
    for column_name, column_type in {
        "pipeline_version": "TEXT NOT NULL DEFAULT '0.2.0'",
        "ingestion_method": "TEXT NOT NULL DEFAULT 'modular_batch_cli'",
    }.items():
        if column_name not in existing_columns:
            connection.execute(f"ALTER TABLE images ADD COLUMN {column_name} {column_type}")
    connection.commit()


def insert_batch_run_start(
    connection: sqlite3.Connection,
    *,
    run_id: str,
    batch_id: str,
    source_path: str,
    started_at: str,
    pipeline_version: str,
    ingestion_method: str,
) -> None:
    connection.execute(
        """
        INSERT INTO batch_runs(
            run_id, batch_id, source_path, status, started_at, pipeline_version, ingestion_method
        )
        VALUES (?, ?, ?, 'RUNNING', ?, ?, ?)
        """,
        (run_id, batch_id, source_path, started_at, pipeline_version, ingestion_method),
    )
    connection.commit()


def complete_batch_run(
    connection: sqlite3.Connection,
    *,
    run_id: str,
    status: str,
    completed_at: str,
    total_records: int,
    passed_records: int,
    failed_records: int,
    error_message: str | None = None,
) -> None:
    connection.execute(
        """
        UPDATE batch_runs
        SET status = ?, completed_at = ?, total_records = ?, passed_records = ?,
            failed_records = ?, error_message = ?
        WHERE run_id = ?
        """,
        (
            status,
            completed_at,
            total_records,
            passed_records,
            failed_records,
            error_message,
            run_id,
        ),
    )
    connection.commit()


def upsert_batch(
    connection: sqlite3.Connection,
    *,
    batch_id: str,
    source_path: str,
    status: str,
    started_at: str,
) -> None:
    connection.execute(
        """
        INSERT INTO batches(batch_id, source_path, status, started_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(batch_id) DO UPDATE SET
            source_path = excluded.source_path,
            status = excluded.status,
            started_at = excluded.started_at,
            completed_at = NULL,
            total_records = 0,
            passed_records = 0,
            failed_records = 0
        """,
        (batch_id, source_path, status, started_at),
    )
    connection.execute("DELETE FROM validation_events WHERE batch_id = ?", (batch_id,))
    connection.execute("DELETE FROM images WHERE batch_id = ?", (batch_id,))
    connection.commit()


def upsert_object(
    connection: sqlite3.Connection,
    *,
    sha256_hash: str,
    object_path: str,
    original_filename: str,
    first_seen_batch_id: str,
    first_seen_image_id: str,
    created_at: str,
) -> None:
    """Register a physical image object if this hash has not been seen before.

    The object table is content-addressed: one row per unique SHA-256 hash. New
    batches can still create their own image catalogue records pointing to this
    same object row.
    """
    connection.execute(
        """
        INSERT INTO objects(
            sha256_hash,
            object_path,
            original_filename,
            first_seen_batch_id,
            first_seen_image_id,
            created_at
        )
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(sha256_hash) DO NOTHING
        """,
        (
            sha256_hash,
            object_path,
            original_filename,
            first_seen_batch_id,
            first_seen_image_id,
            created_at,
        ),
    )


def insert_image_record(connection: sqlite3.Connection, record: Mapping[str, object]) -> None:
    columns = [
        "image_id",
        "batch_id",
        "filename",
        "label",
        "width",
        "height",
        "sha256_hash",
        "source_path",
        "raw_object_path",
        "curated_object_path",
        "quarantine_object_path",
        "ingestion_timestamp",
        "validation_status",
        "failure_reason",
        "pipeline_version",
        "ingestion_method",
    ]
    values = [record.get(column) for column in columns]
    placeholders = ", ".join("?" for _ in columns)
    connection.execute(
        f"INSERT INTO images({', '.join(columns)}) VALUES ({placeholders})",
        values,
    )


def insert_validation_events(
    connection: sqlite3.Connection,
    *,
    image_id: str,
    batch_id: str,
    events: Sequence[ValidationEvent],
    created_at: str,
) -> None:
    connection.executemany(
        """
        INSERT INTO validation_events(image_id, batch_id, check_name, status, reason, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        [
            (image_id, batch_id, event.check_name, event.status, event.reason, created_at)
            for event in events
        ],
    )


def complete_batch(
    connection: sqlite3.Connection,
    *,
    batch_id: str,
    completed_at: str,
    total_records: int,
    passed_records: int,
    failed_records: int,
) -> None:
    status = "COMPLETED_WITH_FAILURES" if failed_records else "COMPLETED"
    connection.execute(
        """
        UPDATE batches
        SET status = ?, total_records = ?, passed_records = ?, failed_records = ?, completed_at = ?
        WHERE batch_id = ?
        """,
        (status, total_records, passed_records, failed_records, completed_at, batch_id),
    )
    connection.commit()
