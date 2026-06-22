#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path
from typing import Iterable, Set


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Print a compact ingestion catalogue summary")
    parser.add_argument("--db", default="data/catalog.db", help="Path to SQLite catalogue")
    parser.add_argument("--batch-id", default=None, help="Optional batch filter")
    parser.add_argument("--limit", type=int, default=20, help="Maximum rows to display")
    return parser.parse_args()


def table_names(connection: sqlite3.Connection) -> Set[str]:
    return {
        row[0]
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name"
        )
    }


def print_rows(title: str, rows: Iterable[sqlite3.Row]) -> None:
    print(title)
    any_rows = False
    for row in rows:
        any_rows = True
        print(dict(row))
    if not any_rows:
        print("(no rows)")


def print_root_cli_catalog(connection: sqlite3.Connection, batch_id: str | None, limit: int) -> None:
    print_rows(
        "Processed batches",
        connection.execute(
            """
            SELECT batch_id, input_dir, output_dir, status, total_records,
                   passed_records, failed_records, completed_at
            FROM processed_batches
            ORDER BY completed_at DESC
            LIMIT ?
            """,
            (limit,),
        ),
    )

    print()
    print_rows(
        "Batch runs",
        connection.execute(
            """
            SELECT run_id, batch_id, status, total_records, passed_records,
                   failed_records, started_at, completed_at, pipeline_version,
                   ingestion_method, error_message
            FROM batch_runs
            ORDER BY started_at DESC
            LIMIT ?
            """,
            (limit,),
        ),
    )

    print()
    params = []
    where = ""
    if batch_id:
        where = "WHERE batch_id = ?"
        params.append(batch_id)
    params.append(limit)
    print_rows(
        "Metadata records",
        connection.execute(
            f"""
            SELECT image_id, batch_id, source_filename, label, image_width,
                   image_height, substr(content_hash, 1, 12) AS hash_prefix,
                   object_path, validation_status, failure_reasons,
                   pipeline_version, ingestion_method
            FROM metadata
            {where}
            ORDER BY source_filename
            LIMIT ?
            """,
            params,
        ),
    )

    print()
    print_rows(
        "Validation counts",
        connection.execute(
            """
            SELECT batch_id, validation_status, COUNT(*) AS record_count
            FROM metadata
            GROUP BY batch_id, validation_status
            ORDER BY batch_id, validation_status
            """
        ),
    )


def print_modular_catalog(connection: sqlite3.Connection, batch_id: str | None, limit: int) -> None:
    print_rows(
        "Batches",
        connection.execute("SELECT * FROM batches ORDER BY started_at DESC LIMIT ?", (limit,)),
    )

    if "batch_runs" in table_names(connection):
        print()
        print_rows(
            "Batch runs",
            connection.execute("SELECT * FROM batch_runs ORDER BY started_at DESC LIMIT ?", (limit,)),
        )

    print()
    params = []
    where = ""
    if batch_id:
        where = "WHERE batch_id = ?"
        params.append(batch_id)
    params.append(limit)
    print_rows(
        "Images",
        connection.execute(
            f"""
            SELECT image_id, batch_id, filename, label, width, height,
                   substr(sha256_hash, 1, 12) AS sha256_prefix,
                   raw_object_path, curated_object_path, quarantine_object_path,
                   validation_status, failure_reason, pipeline_version, ingestion_method
            FROM images
            {where}
            ORDER BY filename
            LIMIT ?
            """,
            params,
        ),
    )

    if "objects" in table_names(connection):
        print()
        print_rows(
            "Objects",
            connection.execute(
                """
                SELECT substr(o.sha256_hash, 1, 12) AS sha256_prefix,
                       o.object_path, o.first_seen_batch_id,
                       COUNT(i.image_id) AS referencing_records
                FROM objects o
                LEFT JOIN images i ON i.sha256_hash = o.sha256_hash
                GROUP BY o.sha256_hash, o.object_path, o.first_seen_batch_id
                ORDER BY referencing_records DESC, sha256_prefix
                LIMIT ?
                """,
                (limit,),
            ),
        )


def main() -> None:
    args = parse_args()
    db_path = Path(args.db)
    if not db_path.exists():
        raise SystemExit(f"Database not found: {db_path}")

    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    tables = table_names(connection)

    if "metadata" in tables and "processed_batches" in tables:
        print_root_cli_catalog(connection, args.batch_id, args.limit)
    elif "images" in tables and "batches" in tables:
        print_modular_catalog(connection, args.batch_id, args.limit)
    else:
        raise SystemExit(
            f"Unrecognized catalogue schema in {db_path}. Found tables: {sorted(tables)}"
        )

    connection.close()


if __name__ == "__main__":
    main()
