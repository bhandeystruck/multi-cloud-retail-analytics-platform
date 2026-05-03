"""
Bronze ingestion pipeline.

This module uploads generated retail source files into object storage using
a production-style Bronze data lake layout.

Why this module exists:
- Raw data should be preserved before warehouse loading.
- Object storage gives us a durable replayable source of truth.
- Partitioned object keys make data easier to organize and reprocess.
- Run IDs allow every ingestion execution to be traced.

Local backend:
- MinIO, which simulates AWS S3.

Future backend:
- AWS S3
- GCP Cloud Storage

Example object key:
    bronze/sales/dt=2026-05-03/run_id=20260503T120000Z/sales.json
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv

from storage.exceptions import ObjectStorageError
from storage.minio_client import MinIOObjectStorageClient
from storage.object_storage_client import StoredObject

DEFAULT_DATASET_CONFIG_PATH = Path("config/datasets.yml")
DEFAULT_SOURCE_DIR = Path("data/generated")
DEFAULT_MANIFEST_DIR = Path("data/manifests")


class BronzeIngestionError(Exception):
    """
    Custom exception for Bronze ingestion failures.

    Why:
    We want ingestion-level failures to be easy to identify from scripts,
    Airflow logs, and future CI/CD checks.
    """


@dataclass(frozen=True)
class DatasetConfig:
    """
    Minimal dataset configuration needed for Bronze ingestion.

    Attributes:
        name: Dataset name, for example sales or products.
        source_format: Expected source format, currently json.
        primary_key: Primary key field from config. Not used heavily yet,
            but useful for future validation and warehouse loading.
        required_fields: Fields that should exist in each source record.
    """

    name: str
    source_format: str
    primary_key: str
    required_fields: list[str]


@dataclass(frozen=True)
class IngestedDataset:
    """
    Metadata for one dataset uploaded during an ingestion run.

    Attributes:
        dataset_name: Dataset that was uploaded.
        source_file: Local file path.
        bucket_name: Object storage bucket.
        object_name: Object key/path inside the bucket.
        record_count: Number of JSON records in the file.
        file_size_bytes: Local file size.
        content_sha256: SHA-256 hash of the file contents.
    """

    dataset_name: str
    source_file: str
    bucket_name: str
    object_name: str
    record_count: int
    file_size_bytes: int
    content_sha256: str


@dataclass(frozen=True)
class BronzeIngestionManifest:
    """
    Manifest describing a full Bronze ingestion run.

    Why manifests matter:
    - They make pipeline runs auditable.
    - They tell us exactly which files were uploaded.
    - They help future warehouse loading avoid duplicate processing.
    - They provide useful metadata for debugging.

    Attributes:
        run_id: Unique ingestion run identifier.
        ingestion_date: Logical date partition used in object keys.
        started_at: UTC timestamp when ingestion started.
        completed_at: UTC timestamp when ingestion completed.
        bucket_name: Target object storage bucket.
        source_dir: Local source directory.
        datasets: Uploaded dataset metadata.
    """

    run_id: str
    ingestion_date: str
    started_at: str
    completed_at: str
    bucket_name: str
    source_dir: str
    datasets: list[IngestedDataset]


def generate_run_id(now: datetime | None = None) -> str:
    """
    Generate a UTC run ID.

    Args:
        now: Optional datetime used mainly for testing.

    Returns:
        Run ID in compact UTC format.
    """

    current_time = now or datetime.now(UTC)
    return current_time.strftime("%Y%m%dT%H%M%SZ")


def calculate_sha256(file_path: Path) -> str:
    """
    Calculate SHA-256 hash for a file.

    Why:
    Hashes help us detect duplicate files and verify file integrity.

    Args:
        file_path: File to hash.

    Returns:
        Hexadecimal SHA-256 digest.

    Raises:
        BronzeIngestionError: If the file cannot be read.
    """

    try:
        digest = hashlib.sha256()

        with file_path.open("rb") as file:
            for chunk in iter(lambda: file.read(1024 * 1024), b""):
                digest.update(chunk)

        return digest.hexdigest()

    except OSError as exc:
        raise BronzeIngestionError(f"Failed to hash file {file_path}: {exc}") from exc


def load_dataset_configs(config_path: Path) -> list[DatasetConfig]:
    """
    Load dataset definitions from config/datasets.yml.

    Args:
        config_path: Path to dataset YAML config.

    Returns:
        List of DatasetConfig objects.

    Raises:
        BronzeIngestionError: If config is missing or invalid.
    """

    if not config_path.exists():
        raise BronzeIngestionError(f"Dataset config file not found: {config_path}")

    try:
        raw_config = yaml.safe_load(config_path.read_text(encoding="utf-8"))

    except yaml.YAMLError as exc:
        raise BronzeIngestionError(f"Invalid YAML in dataset config: {exc}") from exc

    except OSError as exc:
        raise BronzeIngestionError(f"Failed to read dataset config {config_path}: {exc}") from exc

    if not isinstance(raw_config, dict):
        raise BronzeIngestionError("Dataset config must be a YAML object")

    raw_datasets = raw_config.get("datasets")

    if not isinstance(raw_datasets, list) or not raw_datasets:
        raise BronzeIngestionError("Dataset config must contain a non-empty datasets list")

    dataset_configs: list[DatasetConfig] = []

    for raw_dataset in raw_datasets:
        if not isinstance(raw_dataset, dict):
            raise BronzeIngestionError("Each dataset config entry must be an object")

        try:
            name = str(raw_dataset["name"])
            source_format = str(raw_dataset["source_format"])
            primary_key = str(raw_dataset["primary_key"])
            required_fields = raw_dataset["required_fields"]

        except KeyError as exc:
            raise BronzeIngestionError(
                f"Dataset config entry is missing required field: {exc}",
            ) from exc

        if not isinstance(required_fields, list) or not required_fields:
            raise BronzeIngestionError(
                f"Dataset '{name}' must define a non-empty required_fields list",
            )

        dataset_configs.append(
            DatasetConfig(
                name=name,
                source_format=source_format,
                primary_key=primary_key,
                required_fields=[str(field) for field in required_fields],
            ),
        )

    return dataset_configs


def read_and_validate_json_records(
    file_path: Path,
    required_fields: list[str],
) -> list[dict[str, Any]]:
    """
    Read and validate a JSON source file.

    Validation here is intentionally basic because this is Bronze ingestion.
    Bronze should preserve raw data, not fully clean it. Deeper validation will
    happen in data quality and Silver transformations.

    Args:
        file_path: Local JSON file.
        required_fields: Required fields expected in each record.

    Returns:
        List of JSON records.

    Raises:
        BronzeIngestionError: If the file is missing, invalid, empty, or malformed.
    """

    if not file_path.exists():
        raise BronzeIngestionError(f"Source file not found: {file_path}")

    if not file_path.is_file():
        raise BronzeIngestionError(f"Source path is not a file: {file_path}")

    try:
        records = json.loads(file_path.read_text(encoding="utf-8"))

    except json.JSONDecodeError as exc:
        raise BronzeIngestionError(f"Invalid JSON file {file_path}: {exc}") from exc

    except OSError as exc:
        raise BronzeIngestionError(f"Failed to read source file {file_path}: {exc}") from exc

    if not isinstance(records, list):
        raise BronzeIngestionError(f"Expected JSON array in {file_path}")

    if not records:
        raise BronzeIngestionError(f"Source file contains no records: {file_path}")

    required_field_set = set(required_fields)

    for index, record in enumerate(records, start=1):
        if not isinstance(record, dict):
            raise BronzeIngestionError(
                f"Record {index} in {file_path} must be a JSON object",
            )

        missing_fields = required_field_set - set(record.keys())

        if missing_fields:
            raise BronzeIngestionError(
                f"Record {index} in {file_path} is missing required fields: "
                f"{sorted(missing_fields)}",
            )

    return records


def build_bronze_object_name(
    dataset_name: str,
    ingestion_date: date,
    run_id: str,
    source_format: str,
) -> str:
    """
    Build the object storage key for a Bronze dataset file.

    Args:
        dataset_name: Dataset name, for example sales.
        ingestion_date: Logical ingestion date.
        run_id: Pipeline run ID.
        source_format: File extension/format.

    Returns:
        Object storage key.
    """

    return (
        f"bronze/{dataset_name}/"
        f"dt={ingestion_date.isoformat()}/"
        f"run_id={run_id}/"
        f"{dataset_name}.{source_format}"
    )


def write_manifest_file(
    manifest: BronzeIngestionManifest,
    manifest_dir: Path,
) -> Path:
    """
    Write a local manifest file.

    Args:
        manifest: Manifest object.
        manifest_dir: Local manifest output directory.

    Returns:
        Path to the written manifest file.

    Raises:
        BronzeIngestionError: If manifest cannot be written.
    """

    manifest_path = manifest_dir / f"bronze_ingestion_manifest_{manifest.run_id}.json"

    try:
        manifest_dir.mkdir(parents=True, exist_ok=True)

        manifest_payload = asdict(manifest)

        manifest_path.write_text(
            json.dumps(manifest_payload, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

        return manifest_path

    except OSError as exc:
        raise BronzeIngestionError(f"Failed to write manifest file: {exc}") from exc


def build_manifest_object_name(
    ingestion_date: date,
    run_id: str,
) -> str:
    """
    Build object key for the uploaded manifest.

    Args:
        ingestion_date: Logical ingestion date.
        run_id: Pipeline run ID.

    Returns:
        Manifest object key.
    """

    return (
        f"manifests/bronze/"
        f"dt={ingestion_date.isoformat()}/"
        f"run_id={run_id}/"
        f"bronze_ingestion_manifest_{run_id}.json"
    )


def ingest_dataset_file(
    dataset_config: DatasetConfig,
    source_dir: Path,
    bucket_name: str,
    ingestion_date: date,
    run_id: str,
    storage_client: MinIOObjectStorageClient,
) -> IngestedDataset:
    """
    Validate and upload one dataset file to object storage.

    Args:
        dataset_config: Dataset configuration.
        source_dir: Local source directory.
        bucket_name: Target object storage bucket.
        ingestion_date: Logical ingestion date.
        run_id: Pipeline run ID.
        storage_client: Object storage client.

    Returns:
        Metadata about uploaded dataset.

    Raises:
        BronzeIngestionError: If validation or upload fails.
    """

    source_file = source_dir / f"{dataset_config.name}.{dataset_config.source_format}"

    records = read_and_validate_json_records(
        file_path=source_file,
        required_fields=dataset_config.required_fields,
    )

    object_name = build_bronze_object_name(
        dataset_name=dataset_config.name,
        ingestion_date=ingestion_date,
        run_id=run_id,
        source_format=dataset_config.source_format,
    )

    content_sha256 = calculate_sha256(source_file)

    try:
        uploaded_object: StoredObject = storage_client.upload_file(
            bucket_name=bucket_name,
            object_name=object_name,
            file_path=source_file,
            content_type="application/json",
        )

    except ObjectStorageError as exc:
        raise BronzeIngestionError(
            f"Failed to upload dataset '{dataset_config.name}' to Bronze storage: {exc}",
        ) from exc

    return IngestedDataset(
        dataset_name=dataset_config.name,
        source_file=str(source_file),
        bucket_name=uploaded_object.bucket_name,
        object_name=uploaded_object.object_name,
        record_count=len(records),
        file_size_bytes=source_file.stat().st_size,
        content_sha256=content_sha256,
    )


def ingest_all_datasets(
    config_path: Path,
    source_dir: Path,
    manifest_dir: Path,
    bucket_name: str,
    ingestion_date: date,
    run_id: str,
    storage_client: MinIOObjectStorageClient,
) -> BronzeIngestionManifest:
    """
    Ingest all configured datasets into Bronze object storage.

    Args:
        config_path: Dataset config path.
        source_dir: Local generated data directory.
        manifest_dir: Local manifest output directory.
        bucket_name: Target bucket.
        ingestion_date: Logical date partition.
        run_id: Pipeline run ID.
        storage_client: Object storage client.

    Returns:
        Full Bronze ingestion manifest.
    """

    started_at = datetime.now(UTC).isoformat()

    dataset_configs = load_dataset_configs(config_path)

    ingested_datasets: list[IngestedDataset] = []

    for dataset_config in dataset_configs:
        if dataset_config.source_format != "json":
            raise BronzeIngestionError(
                f"Unsupported source format for dataset '{dataset_config.name}': "
                f"{dataset_config.source_format}",
            )

        ingested_dataset = ingest_dataset_file(
            dataset_config=dataset_config,
            source_dir=source_dir,
            bucket_name=bucket_name,
            ingestion_date=ingestion_date,
            run_id=run_id,
            storage_client=storage_client,
        )

        ingested_datasets.append(ingested_dataset)

    completed_at = datetime.now(UTC).isoformat()

    manifest = BronzeIngestionManifest(
        run_id=run_id,
        ingestion_date=ingestion_date.isoformat(),
        started_at=started_at,
        completed_at=completed_at,
        bucket_name=bucket_name,
        source_dir=str(source_dir),
        datasets=ingested_datasets,
    )

    manifest_path = write_manifest_file(manifest, manifest_dir)

    manifest_object_name = build_manifest_object_name(
        ingestion_date=ingestion_date,
        run_id=run_id,
    )

    try:
        storage_client.upload_file(
            bucket_name=bucket_name,
            object_name=manifest_object_name,
            file_path=manifest_path,
            content_type="application/json",
        )

    except ObjectStorageError as exc:
        raise BronzeIngestionError(f"Failed to upload ingestion manifest: {exc}") from exc

    return manifest


def parse_ingestion_date(value: str | None) -> date:
    """
    Parse an ingestion date.

    Args:
        value: Date string in YYYY-MM-DD format, or None.

    Returns:
        Parsed date or today's UTC date.

    Raises:
        argparse.ArgumentTypeError: If the date is invalid.
    """

    if value is None:
        return datetime.now(UTC).date()

    try:
        return date.fromisoformat(value)

    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"Invalid date '{value}'. Expected format: YYYY-MM-DD",
        ) from exc


def parse_args() -> argparse.Namespace:
    """
    Parse command-line arguments.

    Returns:
        Parsed CLI arguments.
    """

    parser = argparse.ArgumentParser(
        description="Upload generated retail data files into Bronze object storage.",
    )

    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_DATASET_CONFIG_PATH,
        help="Path to dataset configuration YAML.",
    )

    parser.add_argument(
        "--source-dir",
        type=Path,
        default=DEFAULT_SOURCE_DIR,
        help="Directory containing generated source JSON files.",
    )

    parser.add_argument(
        "--manifest-dir",
        type=Path,
        default=DEFAULT_MANIFEST_DIR,
        help="Directory where ingestion manifest files will be written locally.",
    )

    parser.add_argument(
        "--bucket",
        type=str,
        default=None,
        help="Target object storage bucket. Defaults to MINIO_BUCKET from .env.",
    )

    parser.add_argument(
        "--ingestion-date",
        type=str,
        default=None,
        help="Logical ingestion date in YYYY-MM-DD format. Defaults to today's UTC date.",
    )

    parser.add_argument(
        "--run-id",
        type=str,
        default=None,
        help="Optional custom run ID. Defaults to generated UTC timestamp.",
    )

    return parser.parse_args()


def main() -> int:
    """
    CLI entry point.

    Returns:
        Process exit code.
    """

    load_dotenv()

    args = parse_args()

    bucket_name = args.bucket or os.getenv("MINIO_BUCKET", "retail-bronze")
    run_id = args.run_id or generate_run_id()
    ingestion_date = parse_ingestion_date(args.ingestion_date)

    try:
        storage_client = MinIOObjectStorageClient.from_env()

        manifest = ingest_all_datasets(
            config_path=args.config,
            source_dir=args.source_dir,
            manifest_dir=args.manifest_dir,
            bucket_name=bucket_name,
            ingestion_date=ingestion_date,
            run_id=run_id,
            storage_client=storage_client,
        )

    except (BronzeIngestionError, ValueError) as exc:
        print(f"Bronze ingestion failed: {exc}", file=sys.stderr)
        return 1

    print("Bronze ingestion completed successfully.")
    print(f"Run ID: {manifest.run_id}")
    print(f"Ingestion date: {manifest.ingestion_date}")
    print(f"Bucket: {manifest.bucket_name}")
    print("Uploaded datasets:")

    for dataset in manifest.datasets:
        print(
            f"- {dataset.dataset_name}: "
            f"{dataset.record_count} records -> "
            f"{dataset.object_name}",
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())