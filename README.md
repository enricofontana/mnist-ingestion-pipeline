# MNIST Image Ingestion Pipeline

This repository contains a small, time-boxed data engineering solution for ingesting MNIST-style image batches into a clean, queryable and traceable dataset.

The exercise scenario is treated as a lab process dropping object-based image batches into a landing area. The pipeline validates each record, enforces a strict `metadata.csv` schema, checks metadata/image consistency, stores image bytes once in a content-addressed local object store, and writes a relational metadata catalogue in SQLite. Accepted and failed records are tracked separately in metadata without duplicating the same physical image bytes.

The implementation is intentionally plain Python rather than Kubeflow/Airflow. The goal is to demonstrate ingestion design, validation, lineage, idempotency and pragmatic trade-offs within the requested 3-4 hour scope.

---

## Repository structure

```text
.
├── pipeline.py                  # Simple reviewer CLI: ingest one batch from --input-dir to --output-dir
├── schema.sql                   # SQLite schema for metadata, objects, batch runs and idempotency
├── Makefile                     # Optional shortcuts for install, demo, tests, catalogue view and cleanup
├── src/mnist_ingestion/         # Modular package with reusable ingestion logic
│   ├── config.py                # Default local paths and project settings
│   ├── db.py                    # SQLite persistence for batches, images, objects and audit runs
│   ├── metadata.py              # Hashing, image dimensions, deterministic IDs and timestamps
│   ├── pipeline.py              # Modular end-to-end batch ingestion workflow
│   ├── storage.py               # Content-addressed object storage using SHA-256 paths
│   └── validation.py            # Image, label, schema and duplicate validation checks
├── scripts/                     # Helper scripts for generation, execution, inspection and demo
│   ├── prepare_sample_batch.py   # Creates the included-style sample batch
│   ├── generate_mnist_batches.py # Generates extra MNIST-like batches for reruns/dedup demos
│   ├── run_pipeline.py          # Modular CLI using --batch-id
│   ├── query_catalog.py         # Inspects SQLite catalogue using Python sqlite3
│   └── run_demo.sh              # Runs ingestion, catalogue inspection and verbose tests
├── tests/                       # Pytest suite covering validation, hashing, schema and pipeline behavior
├── data/                        # Demo landing data and ignored runtime outputs
├── pyproject.toml               # Package metadata, dependencies and pytest config
└── requirements.txt             # Minimal runtime dependencies for pip install -r
```

---

## Fast reviewer run

After creating a virtual environment, the shortest complete demo is:

```bash
make install       # Install the project and dev/test dependencies in the active virtual environment
make demo          # Run the demo ingestion on data/landing/batch_demo and write outputs to data/object_store
make show-catalog  # Print the SQLite catalogue contents using the Python helper script
make test          # Run the pytest suite with verbose test names
```

Or run the same flow with the helper script:

```bash
cd mnist-ingestion-pipeline                          #From the repository root
python3 -m venv .venv                                #Create a virtual environment
source .venv/bin/activate                            #Activate the virtual environment
python -m pip install --upgrade pip setuptools wheel #Upgrade pip setuptools wheel (optional)
pip install -e ".[dev]"                              #Install the project and test dependencies
bash scripts/run_demo.sh                             #Run the helper script
```

The simplest ingestion entrypoint is the root-level `pipeline.py`, which follows the exercise contract directly:

```bash
python pipeline.py \
  --input-dir data/landing/batch_demo \
  --output-dir data/object_store
```

This creates:

```text
data/object_store/metadata.db
data/object_store/objects/<sha256_prefix>/<sha256>.png
```

The root CLI also writes an execution audit row to `batch_runs` and records `pipeline_version` / `ingestion_method` on every metadata row.

To inspect the compact SQLite catalogue without requiring the system `sqlite3` CLI, use the Python helper script:

```bash
python scripts/query_catalog.py   --db data/object_store/metadata.db   --batch-id batch_demo
```

The database can also be inspected with the optional system `sqlite3` command-line tool if it is installed.

Dry-run validation without copying image objects:

```bash
python pipeline.py \
  --input-dir data/landing/batch_demo \
  --output-dir data/object_store_validate_only \
  --validate-only
```

The root-level script also supports `--force` to rebuild metadata for an already processed batch. Without `--force`, `processed_batches` makes reruns idempotent by skipping completed batches.

---

## How to run (step by step)

### 1. Create and activate a virtual environment

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip setuptools wheel
pip install -e ".[dev]"
```

The project supports Python 3.8+ so it can run on older Linux/WSL environments as well as newer Python versions.

The same installation command is also available through the Makefile:

```bash
make install
```

### 2. Prepare a sample batch

The script tries to load real MNIST from OpenML when `--source mnist` is used. If OpenML is unavailable, it falls back to a deterministic 28x28 grayscale sample so the pipeline remains runnable offline.

```bash
python scripts/prepare_sample_batch.py \
  --batch-id batch_001 \
  --sample-size 50 \
  --source mnist \
  --inject-bad-records
```

For a fully offline smoke test:

```bash
python scripts/prepare_sample_batch.py \
  --batch-id batch_001 \
  --sample-size 50 \
  --source synthetic \
  --inject-bad-records
```

This creates the incoming lab-style batch:

```text
data/landing/batch_001/
├── images/
│   ├── img_000001.png
│   ├── img_000002.png
│   └── ...
└── metadata.csv
```


### Generate multiple batches for repeated runs

To test repeated ingestion, idempotency and content-addressed object reuse, generate several incoming batches:

```bash
python scripts/generate_mnist_batches.py \
  --batch-prefix batch_run \
  --num-batches 3 \
  --sample-size 30 \
  --source synthetic \
  --inject-bad-records
```

To intentionally create the same image bytes across several batches and prove that object storage is deduplicated by SHA-256, add `--same-images-across-batches`:

```bash
python scripts/generate_mnist_batches.py \
  --batch-prefix batch_reuse \
  --num-batches 3 \
  --sample-size 30 \
  --source synthetic \
  --same-images-across-batches

for batch_dir in data/landing/batch_reuse_*; do
  python pipeline.py --input-dir "$batch_dir" --output-dir data/object_store
done
```

In that second example, each batch still creates its own metadata rows, but the physical object files are reused because their SHA-256 hashes are identical. This demonstrates the distinction between logical record identity and physical object identity.

### 3. Run ingestion

```bash
python scripts/run_pipeline.py --batch-id batch_001
```

Expected output is similar to:

```text
Batch completed: batch_id=batch_001, total=54, passed=48, failed=6, catalog=/.../data/catalog.db
```

### 4. Query the catalogue

```bash
python scripts/query_catalog.py --batch-id batch_001
```

Or inspect the SQLite database with the provided Python helper. This avoids requiring the system `sqlite3` binary:

```bash
python scripts/query_catalog.py --db data/catalog.db --batch-id batch_001
```

If the optional `sqlite3` CLI is installed, the catalogue can also be queried directly with SQL.

### 5. Run tests

```bash
pytest -v
```

Or:

```bash
make test
```

Expected output:

```text
10 passed
```

The tests are deliberately small but high-signal. They run in verbose mode so the console shows what is being checked, for example hashing stability, validator behavior, strict metadata schema checks, content-addressed object reuse and end-to-end mixed pass/fail pipeline behavior.

---

## Pipeline design

The pipeline uses a simple lakehouse-inspired layout on the local filesystem, with physical object identity separated from logical catalogue record identity:

```text
landing batch
   ↓
content-addressed object store, keyed by SHA-256
   ↓
record-level validation
   ↓
per-batch metadata records for passed and failed records
   ↓
SQLite metadata catalogue
```

Runtime object layout:

```text
data/object_store/
└── objects/<sha256_prefix>/<sha256>.<ext>   # Physical bytes stored once per unique hash
```

The raw data is not transformed. The pipeline writes image bytes to a canonical object path derived from the SHA-256 hash. If the same bytes arrive again in the same or a later batch, the existing object is reused while a new image metadata record is still written for the new batch occurrence.

The important distinction is:

```text
object identity = sha256_hash
record identity = image_id + batch_id + filename
```

This avoids duplicate object storage while preserving complete lineage for each batch arrival.

---

## Compact schema used by `pipeline.py`

The root-level script writes one row per incoming image record to a compact `metadata` table:

```sql
CREATE TABLE metadata (
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
    pipeline_version TEXT,
    ingestion_method TEXT
);
```

It also writes:

- `processed_batches`, which records whether a batch has already been completed. This gives the simple CLI true rerun behavior: rerunning the same input batch is skipped unless `--force` is supplied.
- `batch_runs`, which records every execution attempt, including skipped reruns, validation-only runs, start/end timestamps, row counts, status, `pipeline_version` and `ingestion_method`.

The stored object path is content-addressed by SHA-256, not random UUID. This is a deliberate improvement: duplicate bytes across batches reuse the same object while still creating separate metadata records for each batch occurrence.

---

## Metadata catalogue

SQLite is used as a lightweight relational store with five tables:

### `batches`

One row per ingestion batch:

- `batch_id`
- `source_path`
- `status`
- `total_records`
- `passed_records`
- `failed_records`
- `started_at`
- `completed_at`

### `batch_runs`

One row per pipeline execution attempt:

- `run_id`
- `batch_id`
- `source_path` / `input_dir`
- `status`
- `total_records`
- `passed_records`
- `failed_records`
- `started_at`
- `completed_at`
- `error_message`
- `pipeline_version`
- `ingestion_method`

This table is intentionally separate from `batches` / `processed_batches`: a batch is a logical input, while a run is an operational attempt to process it. That distinction matters for auditability and troubleshooting.

### `objects`

One row per unique physical image object, keyed by content hash:

- `sha256_hash`
- `object_path`
- `original_filename`
- `first_seen_batch_id`
- `first_seen_image_id`
- `created_at`

### `images`

One row per incoming image record, including failed records. Multiple image records can point to the same physical object through `sha256_hash`:

- `image_id`
- `batch_id`
- `filename`
- `label`
- `width`
- `height`
- `sha256_hash`
- `source_path`
- `raw_object_path`
- `curated_object_path`
- `quarantine_object_path`
- `ingestion_timestamp`
- `validation_status`
- `failure_reason`
- `pipeline_version`
- `ingestion_method`

`raw_object_path`, `curated_object_path` and `quarantine_object_path` are catalogue pointers. For a passed record, `curated_object_path` points to the same content-addressed object as `raw_object_path`. For a failed record with available bytes, `quarantine_object_path` points to that same stored object. This avoids unnecessary byte duplication.

### `validation_events`

One row per validation check per image:

- `image_id`
- `batch_id`
- `check_name`
- `status`
- `reason`
- `created_at`

This split keeps batch-level status, physical object identity, image-level metadata and validation audit details queryable independently.

---

## Validation strategy

A good record must satisfy all of the following checks:

| Check | Reason |
|---|---|
| `metadata.csv` has exactly `filename,label` columns | Fails fast on schema drift |
| Metadata row exists | Detects orphan image files |
| Every metadata row points to an existing file | Detects missing images referenced by metadata |
| Every image file has a metadata row | Detects orphan image files |
| Label is present | Metadata completeness |
| Label is an integer | Type correctness |
| Label is in range 0-9 | MNIST domain validity |
| File exists | Detects metadata pointing to missing objects |
| Image is readable | Detects corrupt or unsupported files |
| Dimensions are 28x28 | Ensures expected MNIST image shape |
| Content hash is unique within batch | Detects duplicate image bytes |

Failures are deliberate and visible. A record-level validation failure does not crash the batch. It is written to the `images` table with `validation_status = FAILED`, a `failure_reason`, and detailed check-level records in `validation_events`.

The sample preparation script can inject bad records to demonstrate this behavior:

- invalid label
- duplicated image bytes
- metadata pointing to a missing file
- corrupted image file
- orphan image without metadata
- wrong image dimensions

---

## Lineage and traceability

For any final catalogue record, the pipeline can answer:

| Question | Field/table |
|---|---|
| Where did this image come from? | `images.source_path` |
| Which batch did it arrive in? | `images.batch_id` |
| When was it ingested? | `images.ingestion_timestamp` |
| What bytes were ingested? | `images.sha256_hash` |
| Where are the physical bytes stored? | `objects.object_path` / `images.raw_object_path` |
| Which records reused the same object? | Join `images` to `objects` on `sha256_hash` |
| Did it pass or fail? | `images.validation_status` |
| Why did it fail? | `images.failure_reason` and `validation_events` |
| Which batch run produced it? | `batch_runs.run_id`, `batch_runs.started_at`, `batch_runs.completed_at` |
| Which pipeline wrote it? | `images.pipeline_version`, `images.ingestion_method` |

The content hash is the physical object identity. The deterministic `image_id` is the logical record identity. This means the same image bytes can appear in multiple batches without being stored multiple times, while each batch occurrence remains traceable. The `validation_events` table provides an audit trail rather than a single opaque pass/fail flag.

---

## Idempotency and reruns

The implementation is designed to be safe for deterministic reruns of the same batch:

- `batch_id` comes from the landing folder name.
- `image_id` is deterministic from `batch_id`, filename and metadata row number.
- Physical image objects are stored under a canonical SHA-256 path.
- If the same bytes arrive again, the existing object path is reused.
- A new metadata record is still written for each batch occurrence, preserving lineage.
- The root CLI records completed batches in `processed_batches` and skips them on rerun unless `--force` is supplied.
- The modular CLI rebuilds catalogue rows for the same `batch_id`, while the content-addressed object table is retained.
- `batch_runs` preserves a historical audit trail of execution attempts, including skipped and validation-only runs.

---

## Trade-offs

### Why plain Python?

The brief explicitly asks for a 3-4 hour time-boxed exercise. A plain Python CLI keeps the focus on the core data engineering concerns: reliable ingestion, validation, metadata modelling, lineage and failure handling.

Kubeflow, Airflow or Dagster would be reasonable production orchestration choices, but would add setup overhead that does not materially improve the core solution in this time box.

### Why local folders instead of GCS?

The local filesystem is used as an object-store emulator:

- `landing` maps to an incoming GCS bucket/prefix.
- `object_store/objects` maps to immutable, content-addressed object storage.
- Pass/fail state is represented in the metadata catalogue rather than by physically duplicating the same bytes into separate folders.

This keeps the design cloud-portable without requiring real cloud infrastructure or spending money. In GCS, the same pattern could be implemented with paths such as `gs://bucket/objects/<sha256_prefix>/<sha256>.png`.

### Why SQLite?

SQLite is enough for a compact local metadata catalogue and makes the solution easy to run. The relational design would map naturally to PostgreSQL, Cloud SQL or BigQuery in a larger system.

### Why keep failed records?

Failed records are part of the batch lineage. Dropping them would make the batch appear cleaner than it was and would hide operational issues from downstream users.

---

## What I implemented beyond the minimum

I deliberately implemented a small number of high-signal operational additions rather than adding many unfocused features:

1. **Batch run audit table**: `batch_runs` records every execution attempt, including skipped reruns and validation-only runs.
2. **Strict metadata schema validation**: `metadata.csv` must have exactly `filename,label`; unexpected or missing columns fail fast before object processing.
3. **Metadata-image cross-consistency checks**: missing files referenced by metadata and orphan files without metadata are both represented as failed records.
4. **Provenance fields**: each record stores `pipeline_version` and `ingestion_method`, making it possible to identify which pipeline implementation produced the catalogue entry.
5. **Visible quality checks**: `Makefile`, `scripts/run_demo.sh` and a dedicated test section make it easy for a reviewer to run the ingestion demo and the automated tests.

---

## What I would do with more time

Production improvements would include:

1. **Cloud storage**: replace local folders with GCS buckets and object versioning.
2. **Orchestration**: run the same logical stages in Kubeflow Pipelines or Cloud Composer/Airflow.
3. **Metadata store**: move from SQLite to Cloud SQL/Postgres for operational metadata, and optionally BigQuery for analytics.
4. **Data quality framework**: express validation rules with Great Expectations, Soda or custom data contracts.
5. **Observability**: structured JSON logs, metrics, alerting on failed batches, failed records and duplicate rates.
6. **Schema evolution**: explicit metadata schema versions and compatibility checks.
7. **Security**: service accounts, IAM, encryption, secret management and least-privilege access.
8. **CI/CD**: automated tests, linting, packaging and deployment pipeline.
9. **Performance**: parallel file validation and content-addressed object writes for larger image batches.
10. **Checkpointing**: resumable processing for very large batches where restarting from zero would be expensive.

---

## Potential commit history

A good commit sequence would be:

```text
Initial project structure and README outline
Add content-addressed object store layout and SQLite schema
Add sample batch preparation script
Add validation checks and failure tracking
Add end-to-end ingestion pipeline
Add catalogue query helper and tests
Add run audit, schema validation and provenance fields
Document trade-offs, lineage and future production design
```
