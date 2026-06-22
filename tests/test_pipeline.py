from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from scripts.prepare_sample_batch import create_batch
from mnist_ingestion.pipeline import run_batch


def test_pipeline_records_pass_fail_counts_validation_events_and_provenance(tmp_path: Path) -> None:
    create_batch(
        project_root=tmp_path,
        batch_id="batch_test",
        sample_size=10,
        source="synthetic",
        seed=123,
        inject_bad_records=True,
    )

    result = run_batch(project_root=tmp_path, batch_id="batch_test")

    assert result.total_records > 10
    assert result.passed_records > 0
    assert result.failed_records > 0

    connection = sqlite3.connect(result.database_path)
    failed = connection.execute(
        "SELECT COUNT(*) FROM images WHERE validation_status = 'FAILED'"
    ).fetchone()[0]
    events = connection.execute("SELECT COUNT(*) FROM validation_events").fetchone()[0]
    batch_runs = connection.execute("SELECT COUNT(*) FROM batch_runs WHERE batch_id = 'batch_test'").fetchone()[0]
    provenance = connection.execute(
        """
        SELECT COUNT(*)
        FROM images
        WHERE batch_id = 'batch_test'
          AND pipeline_version IS NOT NULL
          AND ingestion_method = 'modular_batch_cli'
        """
    ).fetchone()[0]

    assert failed == result.failed_records
    assert events >= result.total_records
    assert batch_runs == 1
    assert provenance == result.total_records


def test_pipeline_reuses_content_addressed_objects_across_batches_without_losing_records(tmp_path: Path) -> None:
    create_batch(
        project_root=tmp_path,
        batch_id="batch_a",
        sample_size=8,
        source="synthetic",
        seed=42,
        inject_bad_records=False,
    )
    create_batch(
        project_root=tmp_path,
        batch_id="batch_b",
        sample_size=8,
        source="synthetic",
        seed=42,
        inject_bad_records=False,
    )

    run_batch(project_root=tmp_path, batch_id="batch_a")
    result = run_batch(project_root=tmp_path, batch_id="batch_b")

    connection = sqlite3.connect(result.database_path)
    image_records = connection.execute("SELECT COUNT(*) FROM images").fetchone()[0]
    stored_objects = connection.execute("SELECT COUNT(*) FROM objects").fetchone()[0]
    shared_hashes = connection.execute(
        """
        SELECT COUNT(*)
        FROM (
            SELECT sha256_hash
            FROM images
            WHERE sha256_hash IS NOT NULL
            GROUP BY sha256_hash
            HAVING COUNT(*) > 1
        )
        """
    ).fetchone()[0]

    assert image_records == 16
    assert stored_objects == 8
    assert shared_hashes == 8


def test_pipeline_rejects_metadata_csv_when_schema_has_unexpected_columns(tmp_path: Path) -> None:
    batch_dir = tmp_path / "data" / "landing" / "bad_schema"
    images_dir = batch_dir / "images"
    images_dir.mkdir(parents=True)
    (batch_dir / "metadata.csv").write_text("filename,label,unexpected\nimg_001.png,1,x\n", encoding="utf-8")

    with pytest.raises(ValueError, match="metadata.csv schema mismatch"):
        run_batch(project_root=tmp_path, batch_id="bad_schema")
