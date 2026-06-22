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
