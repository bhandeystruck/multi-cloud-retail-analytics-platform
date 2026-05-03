"""
Unit tests for Bronze ingestion helper functions.

These tests avoid real MinIO connections.

Why:
- Object storage integration is tested separately.
- Unit tests should validate deterministic logic such as object key building,
  run ID formatting, config parsing, and JSON validation.
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from ingestion.ingest_to_object_storage import (
    BronzeIngestionError,
    build_bronze_object_name,
    build_manifest_object_name,
    generate_run_id,
    read_and_validate_json_records,
)


def test_generate_run_id_uses_expected_format() -> None:
    """
    Verify run IDs are generated in compact UTC timestamp format.
    """

    run_id = generate_run_id(datetime(2026, 5, 3, 12, 30, 45, tzinfo=UTC))

    assert run_id == "20260503T123045Z"


def test_build_bronze_object_name() -> None:
    """
    Verify Bronze object keys follow the expected partitioned layout.
    """

    object_name = build_bronze_object_name(
        dataset_name="sales",
        ingestion_date=date(2026, 5, 3),
        run_id="run_001",
        source_format="json",
    )

    assert object_name == "bronze/sales/dt=2026-05-03/run_id=run_001/sales.json"


def test_build_manifest_object_name() -> None:
    """
    Verify manifest object keys follow the expected layout.
    """

    object_name = build_manifest_object_name(
        ingestion_date=date(2026, 5, 3),
        run_id="run_001",
    )

    assert (
        object_name
        == "manifests/bronze/dt=2026-05-03/run_id=run_001/"
        "bronze_ingestion_manifest_run_001.json"
    )


def test_read_and_validate_json_records_accepts_valid_records(tmp_path: Path) -> None:
    """
    Verify JSON validation accepts records with required fields.
    """

    file_path = tmp_path / "sales.json"

    file_path.write_text(
        json.dumps(
            [
                {
                    "order_id": "ORD-1",
                    "customer_id": "CUST-1",
                    "total_amount": 100.0,
                },
            ],
        ),
        encoding="utf-8",
    )

    records = read_and_validate_json_records(
        file_path=file_path,
        required_fields=["order_id", "customer_id", "total_amount"],
    )

    assert len(records) == 1
    assert records[0]["order_id"] == "ORD-1"


def test_read_and_validate_json_records_rejects_missing_fields(tmp_path: Path) -> None:
    """
    Verify JSON validation rejects records missing required fields.
    """

    file_path = tmp_path / "sales.json"

    file_path.write_text(
        json.dumps(
            [
                {
                    "order_id": "ORD-1",
                    "customer_id": "CUST-1",
                },
            ],
        ),
        encoding="utf-8",
    )

    with pytest.raises(BronzeIngestionError, match="missing required fields"):
        read_and_validate_json_records(
            file_path=file_path,
            required_fields=["order_id", "customer_id", "total_amount"],
        )


def test_read_and_validate_json_records_rejects_empty_file(tmp_path: Path) -> None:
    """
    Verify JSON validation rejects an empty JSON array.
    """

    file_path = tmp_path / "sales.json"
    file_path.write_text("[]", encoding="utf-8")

    with pytest.raises(BronzeIngestionError, match="contains no records"):
        read_and_validate_json_records(
            file_path=file_path,
            required_fields=["order_id"],
        )