#!/usr/bin/env python3
"""Generate one or more MNIST-style landing batches for repeated pipeline runs.

This script is intentionally separate from ingestion. It creates folders shaped
like incoming lab drops:

    data/landing/<batch_id>/metadata.csv
    data/landing/<batch_id>/images/*.png

Use --source synthetic for deterministic offline data, or --source mnist to try
real MNIST from OpenML with an automatic synthetic fallback.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.prepare_sample_batch import create_batch


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate multiple MNIST-style landing batches for ingestion demos"
    )
    parser.add_argument("--project-root", default=".", help="Repository/project root")
    parser.add_argument(
        "--batch-prefix",
        default="batch_run",
        help="Prefix used to create batch IDs, e.g. batch_run_001",
    )
    parser.add_argument(
        "--num-batches",
        type=int,
        default=3,
        help="Number of landing batches to generate",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=30,
        help="Number of good sample images per batch before optional bad records",
    )
    parser.add_argument(
        "--source",
        choices=["mnist", "synthetic"],
        default="synthetic",
        help="Use real MNIST from OpenML when available, or deterministic synthetic images",
    )
    parser.add_argument("--seed", type=int, default=42, help="Base random seed")
    parser.add_argument(
        "--same-images-across-batches",
        action="store_true",
        help=(
            "Use the same seed for every batch. This intentionally creates duplicate "
            "image bytes across batches so content-addressed object reuse is visible."
        ),
    )
    parser.add_argument(
        "--inject-bad-records",
        action="store_true",
        help="Inject invalid records into each batch to demonstrate failure handling",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    project_root = Path(args.project_root)

    if args.num_batches < 1:
        raise ValueError("--num-batches must be at least 1")
    if args.sample_size < 1:
        raise ValueError("--sample-size must be at least 1")

    created: list[Path] = []
    for batch_number in range(1, args.num_batches + 1):
        batch_id = f"{args.batch_prefix}_{batch_number:03d}"
        seed = args.seed if args.same_images_across_batches else args.seed + batch_number - 1
        batch_dir = create_batch(
            project_root=project_root,
            batch_id=batch_id,
            sample_size=args.sample_size,
            source=args.source,
            seed=seed,
            inject_bad_records=args.inject_bad_records,
        )
        created.append(batch_dir)

    print("Created landing batches:")
    for batch_dir in created:
        print(f"  {batch_dir}")

    print("\nRun them with the reviewer CLI:")
    for batch_dir in created:
        print(f"  python pipeline.py --input-dir {batch_dir} --output-dir data/object_store")


if __name__ == "__main__":
    main()
