#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

import argparse
import csv
import random
import shutil
from pathlib import Path

import numpy as np
from PIL import Image, ImageDraw

from mnist_ingestion.config import PipelinePaths


def _save_image(array: np.ndarray, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    image = Image.fromarray(array.astype(np.uint8), mode="L")
    image.save(output_path)


def _create_synthetic_digit(label: int, seed: int) -> np.ndarray:
    """Create a deterministic 28x28 grayscale digit-like image.

    This fallback keeps the repository runnable offline. The default source is
    real MNIST via OpenML when network access is available.
    """
    rng = random.Random(seed)
    image = Image.new("L", (28, 28), 0)
    draw = ImageDraw.Draw(image)
    offset_x = rng.randint(-2, 2)
    offset_y = rng.randint(-2, 2)
    draw.text((8 + offset_x, 5 + offset_y), str(label), fill=255)
    noise = np.array(image, dtype=np.uint8)
    random_noise = np.random.default_rng(seed).integers(0, 25, size=(28, 28), dtype=np.uint8)
    return np.maximum(noise, random_noise)


def _load_openml_mnist(sample_size: int, seed: int) -> tuple[np.ndarray, np.ndarray]:
    from sklearn.datasets import fetch_openml

    mnist = fetch_openml("mnist_784", version=1, as_frame=False, parser="auto")
    data = mnist.data.astype(np.uint8).reshape(-1, 28, 28)
    labels = mnist.target.astype(int)
    rng = np.random.default_rng(seed)
    indices = rng.choice(len(data), size=sample_size, replace=False)
    return data[indices], labels[indices]


def _load_synthetic(sample_size: int, seed: int) -> tuple[np.ndarray, np.ndarray]:
    labels = np.array([(seed + index) % 10 for index in range(sample_size)], dtype=int)
    data = np.array(
        [_create_synthetic_digit(int(label), seed + index) for index, label in enumerate(labels)]
    )
    return data, labels


def create_batch(
    *,
    project_root: Path,
    batch_id: str,
    sample_size: int,
    source: str,
    seed: int,
    inject_bad_records: bool,
) -> Path:
    paths = PipelinePaths.from_project_root(project_root)
    paths.ensure()
    batch_dir = paths.landing_dir / batch_id
    image_dir = batch_dir / "images"

    if batch_dir.exists():
        shutil.rmtree(batch_dir)
    image_dir.mkdir(parents=True, exist_ok=True)

    if source == "mnist":
        try:
            images, labels = _load_openml_mnist(sample_size, seed)
            print("Loaded real MNIST sample from OpenML")
        except Exception as exc:
            print(f"OpenML MNIST unavailable ({exc}); falling back to synthetic 28x28 sample")
            images, labels = _load_synthetic(sample_size, seed)
    elif source == "synthetic":
        images, labels = _load_synthetic(sample_size, seed)
    else:
        raise ValueError(f"Unsupported source: {source}")

    metadata_rows: list[dict[str, str]] = []
    for index, (array, label) in enumerate(zip(images, labels), start=1):
        filename = f"img_{index:06d}.png"
        _save_image(array, image_dir / filename)
        metadata_rows.append({"filename": filename, "label": str(int(label))})

    if inject_bad_records and sample_size >= 5:
        # 1) invalid label
        metadata_rows[0]["label"] = "12"

        # 2) duplicate content hash: copy first image bytes to another filename
        shutil.copy2(image_dir / metadata_rows[1]["filename"], image_dir / metadata_rows[2]["filename"])

        # 3) missing file referenced in metadata
        metadata_rows.append({"filename": "missing_image.png", "label": "4"})

        # 4) corrupted image file
        corrupt_name = "corrupted_image.png"
        (image_dir / corrupt_name).write_bytes(b"this is not a png")
        metadata_rows.append({"filename": corrupt_name, "label": "3"})

        # 5) orphan image with no metadata row
        orphan = np.zeros((28, 28), dtype=np.uint8)
        _save_image(orphan, image_dir / "orphan_without_metadata.png")

        # 6) wrong dimensions
        wrong_dimension_name = "wrong_dimensions.png"
        _save_image(np.zeros((20, 20), dtype=np.uint8), image_dir / wrong_dimension_name)
        metadata_rows.append({"filename": wrong_dimension_name, "label": "8"})

    with (batch_dir / "metadata.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["filename", "label"])
        writer.writeheader()
        writer.writerows(metadata_rows)

    return batch_dir


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Prepare a local MNIST-style landing batch")
    parser.add_argument("--project-root", default=".", help="Repository/project root")
    parser.add_argument("--batch-id", default="batch_001", help="Batch folder name")
    parser.add_argument("--sample-size", type=int, default=50, help="Number of good sample images")
    parser.add_argument(
        "--source",
        choices=["mnist", "synthetic"],
        default="mnist",
        help="Use real MNIST from OpenML when available, otherwise deterministic synthetic fallback",
    )
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    parser.add_argument(
        "--inject-bad-records",
        action="store_true",
        help="Inject invalid records to demonstrate failure handling",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    batch_dir = create_batch(
        project_root=Path(args.project_root),
        batch_id=args.batch_id,
        sample_size=args.sample_size,
        source=args.source,
        seed=args.seed,
        inject_bad_records=args.inject_bad_records,
    )
    print(f"Created landing batch at {batch_dir}")


if __name__ == "__main__":
    main()
