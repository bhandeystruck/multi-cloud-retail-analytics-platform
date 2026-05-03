"""
Unit tests for Bronze warehouse loader helpers.

These tests do not connect to PostgreSQL or MinIO.

Why:
- Database and object storage loading are integration concerns.
- Unit tests should validate deterministic logic such as manifest parsing,
  latest manifest discovery, payload hashing, and JSON record validation.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from warehouse.local_postgres.load.bronze_loader import (
    BronzeWarehouseLoadError,
    find_latest_manifest,
    parse_manifest,
    read_json_records,
    stable_payload_hash,
)


def test_stable_payload_hash_is_deterministic() -> None:
    """
    Verify equivalent JSON payloads produce the same hash despite key order.
    """

    first_payload = {"order_id": "ORD-1", "total_amount": 100, "customer_id": "CUST-1"}
    second_payload = {"customer_id": "CUST-1", "order_id": "ORD-1", "total_amount": 100}

    assert stable_payload_hash(first_payload) == stable_payload_hash(second_payload)


def test_read_json_records_accepts_json_array(tmp_path: Path) -> None:
    """
    Verify valid JSON arrays are parsed successfully.
    """

    file_path = tmp_path / "sales.json"
    file_path.write_text(
        json.dumps([{"order_id": "ORD-1"}]),
        encoding="utf-8",
    )

    records = read_json_records(file_path)

    assert records == [{"order_id": "ORD-1"}]


def test_read_json_records_rejects_non_array(tmp_path: Path) -> None:
    """
    Verify Bronze loader rejects non-array JSON files.
    """

    file_path = tmp_path / "sales.json"
    file_path.write_text(
        json.dumps({"order_id": "ORD-1"}),
        encoding="utf-8",
    )

    with pytest.raises(BronzeWarehouseLoadError, match="Expected JSON array"):
        read_json_records(file_path)


def test_read_json_records_rejects_non_object_records(tmp_path: Path) -> None:
    """
    Verify Bronze loader rejects arrays containing non-object records.
    """

    file_path = tmp_path / "sales.json"
    file_path.write_text(
        json.dumps(["bad-record"]),
        encoding="utf-8",
    )

    with pytest.raises(BronzeWarehouseLoadError, match="must be a JSON object"):
        read_json_records(file_path)


def test_parse_manifest_accepts_valid_manifest(tmp_path: Path) -> None:
    """
    Verify a valid Bronze ingestion manifest is parsed into typed objects.
    """

    manifest_path = tmp_path / "bronze_ingestion_manifest_test.json"

    manifest_path.write_text(
        json.dumps(
            {
                "run_id": "run_001",
                "ingestion_date": "2026-05-03",
                "started_at": "2026-05-03T12:00:00+00:00",
                "completed_at": "2026-05-03T12:01:00+00:00",
                "bucket_name": "retail-bronze",
                "source_dir": "data/generated",
                "datasets": [
                    {
                        "dataset_name": "sales",
                        "source_file": "data/generated/sales.json",
                        "bucket_name": "retail-bronze",
                        "object_name": "bronze/sales/dt=2026-05-03/run_id=run_001/sales.json",
                        "record_count": 10,
                        "file_size_bytes": 1000,
                        "content_sha256": "abc123",
                    },
                ],
            },
        ),
        encoding="utf-8",
    )

    manifest = parse_manifest(manifest_path)

    assert manifest.run_id == "run_001"
    assert len(manifest.datasets) == 1
    assert manifest.datasets[0].dataset_name == "sales"


def test_find_latest_manifest_returns_most_recent_file(tmp_path: Path) -> None:
    """
    Verify latest manifest discovery uses modified time.
    """

    older_manifest = tmp_path / "bronze_ingestion_manifest_old.json"
    newer_manifest = tmp_path / "bronze_ingestion_manifest_new.json"

    older_manifest.write_text("{}", encoding="utf-8")
    newer_manifest.write_text("{}", encoding="utf-8")

    # Force modification order in a simple deterministic way.
    older_manifest.touch()
    newer_manifest.touch()

    latest_manifest = find_latest_manifest(tmp_path)

    assert latest_manifest == newer_manifest


def test_find_latest_manifest_rejects_empty_directory(tmp_path: Path) -> None:
    """
    Verify a clear error is raised when no manifests exist.
    """

    with pytest.raises(BronzeWarehouseLoadError, match="No Bronze ingestion manifests"):
        find_latest_manifest(tmp_path)