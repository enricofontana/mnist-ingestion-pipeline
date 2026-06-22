from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

PIPELINE_VERSION = "0.2.0"
INGESTION_METHOD = "modular_batch_cli"
EXPECTED_METADATA_COLUMNS = ["filename", "label"]


@dataclass(frozen=True)
class PipelinePaths:
    """Filesystem layout used as a local object-store emulator.

    The names intentionally mirror cloud storage zones so that the same design
    can be mapped to GCS buckets/prefixes later.
    """

    project_root: Path
    landing_dir: Path
    object_store_dir: Path
    database_path: Path

    @classmethod
    def from_project_root(cls, project_root: Path | str) -> "PipelinePaths":
        root = Path(project_root).resolve()
        return cls(
            project_root=root,
            landing_dir=root / "data" / "landing",
            object_store_dir=root / "data" / "object_store",
            database_path=root / "data" / "catalog.db",
        )

    @property
    def objects_dir(self) -> Path:
        return self.object_store_dir / "objects"

    @property
    def raw_dir(self) -> Path:
        return self.object_store_dir / "raw"

    @property
    def curated_dir(self) -> Path:
        return self.object_store_dir / "curated"

    @property
    def quarantine_dir(self) -> Path:
        return self.object_store_dir / "quarantine"

    def ensure(self) -> None:
        for path in [
            self.landing_dir,
            self.objects_dir,
            self.raw_dir,
            self.curated_dir,
            self.quarantine_dir,
            self.database_path.parent,
        ]:
            path.mkdir(parents=True, exist_ok=True)
