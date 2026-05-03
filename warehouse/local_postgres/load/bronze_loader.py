"""
Bronze warehouse loader.

This module loads Bronze JSON files from object storage into local PostgreSQL
Bronze raw tables.

Why this module exists:
- Object storage is our raw file landing zone.
- The warehouse needs queryable raw records for downstream transformations.
- The Bronze warehouse layer should preserve original source records.
- The ops schema should track every load for auditability and idempotency.

Flow:
    Bronze ingestion manifest
        ↓
    Download dataset files from MinIO
        ↓
    Insert raw JSON records into bronze.raw_* tables
        ↓
    Record load status in ops.loaded_files
        ↓
    Record run status in ops.pipeline_runs
"""

from __future__ import annotations

import json
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import psycopg2
from psycopg2 import sql
from psycopg2.extensions import connection as PsycopgConnection
from psycopg2.extras import Json

sys.path.insert(0, str(Path(__file__).parents[3]))
from scripts.init_local_warehouse import PostgresConfig
from storage.exceptions import ObjectStorageError
from storage.minio_client import MinIOObjectStorageClient

DEFAULT_MANIFEST_DIR = Path("data/manifests")


class BronzeWarehouseLoadError(Exception):
    """
    Raised when Bronze warehouse loading fails.
    """


# Dataset-to-table mapping is intentionally explicit.
#
# Why:
# We do not want user-controlled dataset names to be directly interpolated
# into SQL table names. A static map prevents SQL injection and avoids
# accidental writes into unexpected tables.
BRONZE_TABLE_BY_DATASET = {
    "sales": "raw_sales",
    "products": "raw_products",
    "customers": "raw_customers",
    "stores": "raw_stores",
    "inventory": "raw_inventory",
    "campaigns": "raw_campaigns",
    "returns": "raw_returns",
}


@dataclass(frozen=True)
class ManifestDataset:
    """
    One dataset entry from the Bronze ingestion manifest.
    """

    dataset_name: str
    source_file: str
    bucket_name: str
    object_name: str
    record_count: int
    file_size_bytes: int
    content_sha256: str


@dataclass(frozen=True)
class BronzeManifest:
    """
    Parsed Bronze ingestion manifest.

    This tells the warehouse loader exactly which object-storage files to load.
    """

    run_id: str
    ingestion_date: str
    started_at: str
    completed_at: str
    bucket_name: str
    source_dir: str
    datasets: list[ManifestDataset]


@dataclass(frozen=True)
class LoadedDatasetResult:
    """
    Result of loading one dataset into the Bronze warehouse.
    """

    dataset_name: str
    table_name: str
    object_name: str
    record_count: int
    inserted_count: int
    status: str


def stable_payload_hash(payload: dict[str, Any]) -> str:
    """
    Create a stable hash for one JSON payload.

    Why:
    Raw record hashes help with future deduplication, debugging, and integrity
    checks. Sorting keys makes the hash deterministic even if key order changes.

    Args:
        payload: Raw JSON record.

    Returns:
        SHA-256 hex digest.
    """

    import hashlib

    payload_text = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )

    return hashlib.sha256(payload_text.encode("utf-8")).hexdigest()


def find_latest_manifest(manifest_dir: Path = DEFAULT_MANIFEST_DIR) -> Path:
    """
    Find the latest local Bronze ingestion manifest.

    Args:
        manifest_dir: Directory containing local manifest files.

    Returns:
        Path to latest manifest file by modified time.

    Raises:
        BronzeWarehouseLoadError: If no manifest exists.
    """

    if not manifest_dir.exists():
        raise BronzeWarehouseLoadError(
            f"Manifest directory does not exist: {manifest_dir}. "
            "Run Bronze ingestion first.",
        )

    manifest_files = sorted(
        manifest_dir.glob("bronze_ingestion_manifest_*.json"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )

    if not manifest_files:
        raise BronzeWarehouseLoadError(
            f"No Bronze ingestion manifests found in {manifest_dir}. "
            "Run ingestion/ingest_to_object_storage.py first.",
        )

    return manifest_files[0]


def parse_manifest(manifest_path: Path) -> BronzeManifest:
    """
    Parse a Bronze ingestion manifest file.

    Args:
        manifest_path: Local manifest path.

    Returns:
        Parsed BronzeManifest.

    Raises:
        BronzeWarehouseLoadError: If the manifest is missing or invalid.
    """

    if not manifest_path.exists():
        raise BronzeWarehouseLoadError(f"Manifest file not found: {manifest_path}")

    try:
        raw_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    except json.JSONDecodeError as exc:
        raise BronzeWarehouseLoadError(f"Invalid manifest JSON: {manifest_path}") from exc

    except OSError as exc:
        raise BronzeWarehouseLoadError(f"Failed to read manifest: {manifest_path}") from exc

    try:
        raw_datasets = raw_manifest["datasets"]

        if not isinstance(raw_datasets, list) or not raw_datasets:
            raise BronzeWarehouseLoadError("Manifest must contain a non-empty datasets list")

        datasets = [
            ManifestDataset(
                dataset_name=str(dataset["dataset_name"]),
                source_file=str(dataset["source_file"]),
                bucket_name=str(dataset["bucket_name"]),
                object_name=str(dataset["object_name"]),
                record_count=int(dataset["record_count"]),
                file_size_bytes=int(dataset["file_size_bytes"]),
                content_sha256=str(dataset["content_sha256"]),
            )
            for dataset in raw_datasets
        ]

        return BronzeManifest(
            run_id=str(raw_manifest["run_id"]),
            ingestion_date=str(raw_manifest["ingestion_date"]),
            started_at=str(raw_manifest["started_at"]),
            completed_at=str(raw_manifest["completed_at"]),
            bucket_name=str(raw_manifest["bucket_name"]),
            source_dir=str(raw_manifest["source_dir"]),
            datasets=datasets,
        )

    except KeyError as exc:
        raise BronzeWarehouseLoadError(f"Manifest is missing required field: {exc}") from exc

    except (TypeError, ValueError) as exc:
        raise BronzeWarehouseLoadError(f"Manifest contains invalid values: {exc}") from exc


def read_json_records(file_path: Path) -> list[dict[str, Any]]:
    """
    Read JSON records from a downloaded Bronze source file.

    Args:
        file_path: Local JSON file path.

    Returns:
        List of JSON object records.

    Raises:
        BronzeWarehouseLoadError: If file is invalid.
    """

    try:
        records = json.loads(file_path.read_text(encoding="utf-8"))

    except json.JSONDecodeError as exc:
        raise BronzeWarehouseLoadError(f"Invalid JSON file: {file_path}") from exc

    except OSError as exc:
        raise BronzeWarehouseLoadError(f"Failed to read JSON file: {file_path}") from exc

    if not isinstance(records, list):
        raise BronzeWarehouseLoadError(f"Expected JSON array in file: {file_path}")

    parsed_records: list[dict[str, Any]] = []

    for index, record in enumerate(records, start=1):
        if not isinstance(record, dict):
            raise BronzeWarehouseLoadError(
                f"Record {index} in {file_path} must be a JSON object",
            )

        parsed_records.append(record)

    return parsed_records


def connect_to_warehouse() -> PsycopgConnection:
    """
    Connect to local PostgreSQL warehouse.

    Returns:
        psycopg2 connection.

    Raises:
        BronzeWarehouseLoadError: If connection fails.
    """

    config = PostgresConfig.from_env()

    try:
        return psycopg2.connect(
            host=config.host,
            port=config.port,
            dbname=config.database,
            user=config.user,
            password=config.password,
        )

    except psycopg2.Error as exc:
        raise BronzeWarehouseLoadError(
            "Failed to connect to local warehouse. "
            "Make sure Docker is running and warehouse initialization has completed. "
            f"Reason: {exc}",
        ) from exc


def start_pipeline_run(
    connection: PsycopgConnection,
    run_id: str,
    pipeline_name: str,
) -> None:
    """
    Insert or update the pipeline run as running.

    Args:
        connection: PostgreSQL connection.
        run_id: Pipeline run ID.
        pipeline_name: Name of this pipeline stage.
    """

    query = """
        INSERT INTO ops.pipeline_runs (
            run_id,
            pipeline_name,
            status,
            started_at,
            completed_at,
            error_message
        )
        VALUES (%s, %s, 'running', NOW(), NULL, NULL)
        ON CONFLICT (run_id)
        DO UPDATE SET
            pipeline_name = EXCLUDED.pipeline_name,
            status = 'running',
            started_at = NOW(),
            completed_at = NULL,
            error_message = NULL;
    """

    with connection.cursor() as cursor:
        cursor.execute(query, (run_id, pipeline_name))

    connection.commit()


def complete_pipeline_run(
    connection: PsycopgConnection,
    run_id: str,
    status: str,
    error_message: str | None = None,
) -> None:
    """
    Mark a pipeline run as completed.

    Args:
        connection: PostgreSQL connection.
        run_id: Pipeline run ID.
        status: Final status.
        error_message: Optional failure message.
    """

    query = """
        UPDATE ops.pipeline_runs
        SET
            status = %s,
            completed_at = NOW(),
            error_message = %s
        WHERE run_id = %s;
    """

    with connection.cursor() as cursor:
        cursor.execute(query, (status, error_message, run_id))

    connection.commit()


def is_object_already_loaded(
    connection: PsycopgConnection,
    bucket_name: str,
    object_name: str,
) -> bool:
    """
    Check whether a file has already been successfully loaded.

    Args:
        connection: PostgreSQL connection.
        bucket_name: Object storage bucket.
        object_name: Object storage key.

    Returns:
        True if object is already loaded, otherwise False.
    """

    query = """
        SELECT 1
        FROM ops.loaded_files
        WHERE bucket_name = %s
          AND object_name = %s
          AND load_status = 'loaded'
        LIMIT 1;
    """

    with connection.cursor() as cursor:
        cursor.execute(query, (bucket_name, object_name))
        result = cursor.fetchone()

    return result is not None


def upsert_loaded_file_status(
    connection: PsycopgConnection,
    run_id: str,
    dataset: ManifestDataset,
    status: str,
    error_message: str | None = None,
) -> None:
    """
    Insert or update file load status in ops.loaded_files.

    Args:
        connection: PostgreSQL connection.
        run_id: Pipeline run ID.
        dataset: Dataset manifest entry.
        status: pending, loaded, failed, or skipped.
        error_message: Optional error message.
    """

    loaded_at_expression = "NOW()" if status == "loaded" else "NULL"

    query = f"""
        INSERT INTO ops.loaded_files (
            run_id,
            dataset_name,
            bucket_name,
            object_name,
            source_file,
            record_count,
            file_size_bytes,
            content_sha256,
            load_status,
            loaded_at,
            error_message
        )
        VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, {loaded_at_expression}, %s
        )
        ON CONFLICT (bucket_name, object_name)
        DO UPDATE SET
            run_id = EXCLUDED.run_id,
            dataset_name = EXCLUDED.dataset_name,
            source_file = EXCLUDED.source_file,
            record_count = EXCLUDED.record_count,
            file_size_bytes = EXCLUDED.file_size_bytes,
            content_sha256 = EXCLUDED.content_sha256,
            load_status = EXCLUDED.load_status,
            loaded_at = EXCLUDED.loaded_at,
            error_message = EXCLUDED.error_message;
    """

    with connection.cursor() as cursor:
        cursor.execute(
            query,
            (
                run_id,
                dataset.dataset_name,
                dataset.bucket_name,
                dataset.object_name,
                dataset.source_file,
                dataset.record_count,
                dataset.file_size_bytes,
                dataset.content_sha256,
                status,
                error_message,
            ),
        )

    connection.commit()


def insert_bronze_records(
    connection: PsycopgConnection,
    dataset: ManifestDataset,
    records: list[dict[str, Any]],
    run_id: str,
) -> int:
    """
    Insert raw JSON records into the correct Bronze table.

    Args:
        connection: PostgreSQL connection.
        dataset: Dataset manifest entry.
        records: Raw JSON records.
        run_id: Pipeline run ID.

    Returns:
        Number of records newly inserted.

    Raises:
        BronzeWarehouseLoadError: If dataset is unsupported or insert fails.
    """

    table_name = BRONZE_TABLE_BY_DATASET.get(dataset.dataset_name)

    if table_name is None:
        raise BronzeWarehouseLoadError(
            f"Unsupported dataset for Bronze loading: {dataset.dataset_name}",
        )

    insert_query = sql.SQL(
        """
        INSERT INTO bronze.{table_name} (
            run_id,
            source_file,
            bucket_name,
            object_name,
            record_index,
            payload,
            payload_hash
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (object_name, record_index)
        DO NOTHING;
        """,
    ).format(table_name=sql.Identifier(table_name))

    inserted_count = 0

    try:
        with connection.cursor() as cursor:
            for record_index, payload in enumerate(records):
                cursor.execute(
                    insert_query,
                    (
                        run_id,
                        dataset.source_file,
                        dataset.bucket_name,
                        dataset.object_name,
                        record_index,
                        Json(payload),
                        stable_payload_hash(payload),
                    ),
                )

                # cursor.rowcount is 1 for inserted rows and 0 for skipped conflicts.
                inserted_count += max(cursor.rowcount, 0)

        connection.commit()

    except psycopg2.Error as exc:
        connection.rollback()

        raise BronzeWarehouseLoadError(
            f"Failed to insert records for dataset '{dataset.dataset_name}' "
            f"into bronze.{table_name}: {exc}",
        ) from exc

    return inserted_count


def load_one_dataset(
    connection: PsycopgConnection,
    storage_client: MinIOObjectStorageClient,
    dataset: ManifestDataset,
    run_id: str,
    temp_dir: Path,
    force_reload: bool = False,
) -> LoadedDatasetResult:
    """
    Load one dataset from object storage into the Bronze warehouse.

    Args:
        connection: PostgreSQL connection.
        storage_client: MinIO client.
        dataset: Dataset manifest entry.
        run_id: Pipeline run ID.
        temp_dir: Temporary directory for downloaded files.
        force_reload: If true, attempt to load even if file was loaded before.

    Returns:
        LoadedDatasetResult.
    """

    table_name = BRONZE_TABLE_BY_DATASET.get(dataset.dataset_name)

    if table_name is None:
        raise BronzeWarehouseLoadError(
            f"No Bronze table mapping for dataset: {dataset.dataset_name}",
        )

    if not force_reload and is_object_already_loaded(
        connection=connection,
        bucket_name=dataset.bucket_name,
        object_name=dataset.object_name,
    ):
        upsert_loaded_file_status(
            connection=connection,
            run_id=run_id,
            dataset=dataset,
            status="skipped",
            error_message="Object was already loaded successfully.",
        )

        return LoadedDatasetResult(
            dataset_name=dataset.dataset_name,
            table_name=table_name,
            object_name=dataset.object_name,
            record_count=dataset.record_count,
            inserted_count=0,
            status="skipped",
        )

    upsert_loaded_file_status(
        connection=connection,
        run_id=run_id,
        dataset=dataset,
        status="pending",
    )

    download_path = temp_dir / dataset.dataset_name / Path(dataset.object_name).name

    try:
        storage_client.download_file(
            bucket_name=dataset.bucket_name,
            object_name=dataset.object_name,
            destination_path=download_path,
        )

    except ObjectStorageError as exc:
        upsert_loaded_file_status(
            connection=connection,
            run_id=run_id,
            dataset=dataset,
            status="failed",
            error_message=str(exc),
        )

        raise BronzeWarehouseLoadError(
            f"Failed to download object {dataset.bucket_name}/{dataset.object_name}: {exc}",
        ) from exc

    records = read_json_records(download_path)

    if len(records) != dataset.record_count:
        raise BronzeWarehouseLoadError(
            f"Record count mismatch for {dataset.dataset_name}. "
            f"Manifest expected {dataset.record_count}, downloaded file has {len(records)}.",
        )

    inserted_count = insert_bronze_records(
        connection=connection,
        dataset=dataset,
        records=records,
        run_id=run_id,
    )

    upsert_loaded_file_status(
        connection=connection,
        run_id=run_id,
        dataset=dataset,
        status="loaded",
    )

    return LoadedDatasetResult(
        dataset_name=dataset.dataset_name,
        table_name=table_name,
        object_name=dataset.object_name,
        record_count=len(records),
        inserted_count=inserted_count,
        status="loaded",
    )


def load_bronze_manifest(
    manifest_path: Path,
    force_reload: bool = False,
) -> list[LoadedDatasetResult]:
    """
    Load all datasets from a Bronze ingestion manifest into the warehouse.

    Args:
        manifest_path: Local Bronze ingestion manifest.
        force_reload: Whether to attempt loading already-loaded files.

    Returns:
        List of dataset load results.

    Raises:
        BronzeWarehouseLoadError: If loading fails.
    """

    manifest = parse_manifest(manifest_path)
    storage_client = MinIOObjectStorageClient.from_env()
    connection = connect_to_warehouse()

    try:
        start_pipeline_run(
            connection=connection,
            run_id=manifest.run_id,
            pipeline_name="bronze_warehouse_load",
        )

        results: list[LoadedDatasetResult] = []

        with tempfile.TemporaryDirectory() as temp_directory:
            temp_dir = Path(temp_directory)

            for dataset in manifest.datasets:
                result = load_one_dataset(
                    connection=connection,
                    storage_client=storage_client,
                    dataset=dataset,
                    run_id=manifest.run_id,
                    temp_dir=temp_dir,
                    force_reload=force_reload,
                )

                results.append(result)

        complete_pipeline_run(
            connection=connection,
            run_id=manifest.run_id,
            status="success",
        )

        return results

    except BronzeWarehouseLoadError as exc:
        complete_pipeline_run(
            connection=connection,
            run_id=manifest.run_id,
            status="failed",
            error_message=str(exc),
        )

        raise

    finally:
        connection.close()