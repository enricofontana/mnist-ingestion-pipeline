#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

echo "==> Running ingestion demo"
python pipeline.py \
  --input-dir data/landing/batch_demo \
  --output-dir data/object_store \
  --force

echo
echo "==> Inspecting catalogue with Python helper"
python scripts/query_catalog.py \
  --db data/object_store/metadata.db \
  --batch-id batch_demo \
  --limit 10

echo
echo "==> Running unit tests with verbose names"
pytest -v

echo
echo "Demo completed successfully."
