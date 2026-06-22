from pathlib import Path

from mnist_ingestion.metadata import sha256_file, stable_image_id


def test_sha256_hash_identifies_same_image_bytes_consistently(tmp_path: Path) -> None:
    first = tmp_path / "first.bin"
    second = tmp_path / "second.bin"
    first.write_bytes(b"same bytes")
    second.write_bytes(b"same bytes")

    assert sha256_file(first) == sha256_file(second)


def test_sha256_hash_changes_when_image_bytes_change(tmp_path: Path) -> None:
    first = tmp_path / "first.bin"
    second = tmp_path / "second.bin"
    first.write_bytes(b"same bytes")
    second.write_bytes(b"different bytes")

    assert sha256_file(first) != sha256_file(second)


def test_image_id_is_deterministic_for_same_batch_filename_and_label() -> None:
    assert stable_image_id("batch_001", "img_000001.png", 1) == stable_image_id(
        "batch_001", "img_000001.png", 1
    )
