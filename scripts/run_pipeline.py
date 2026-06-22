#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

import argparse

from mnist_ingestion.pipeline import run_batch


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run MNIST batch ingestion pipeline")
    parser.add_argument("--project-root", default=".", help="Repository/project root")
    parser.add_argument("--batch-id", required=True, help="Batch folder name under data/landing")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = run_batch(project_root=Path(args.project_root), batch_id=args.batch_id)
    print(
        "Batch completed: "
        f"batch_id={result.batch_id}, "
        f"total={result.total_records}, "
        f"passed={result.passed_records}, "
        f"failed={result.failed_records}, "
        f"catalog={result.database_path}"
    )


if __name__ == "__main__":
    main()
