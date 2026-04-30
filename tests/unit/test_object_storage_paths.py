"""
Unit tests for object storage-related structures.

These tests do not connect to MinIO.

Why:
- Unit tests should be fast and deterministic.
- MinIO connectivity belongs in integration tests or manual verification scripts.
"""

from __future__ import annotations

from storage.object_storage_client import StoredObject


def test_stored_object_holds_expected_metadata() -> None:
    """
    Verify StoredObject stores object metadata correctly.
    """

    stored_object = StoredObject(
        bucket_name="retail-bronze",
        object_name="bronze/sales/dt=2026-04-30/run_id=test/sales.json",
        size=1024,
        last_modified=None,
    )

    assert stored_object.bucket_name == "retail-bronze"
    assert stored_object.object_name.endswith("sales.json")
    assert stored_object.size == 1024


def test_stored_object_is_immutable() -> None:
    """
    Verify StoredObject cannot be modified after creation.

    Why:
    Storage metadata should be treated as a stable result from an operation.
    """

    stored_object = StoredObject(
        bucket_name="retail-bronze",
        object_name="bronze/products/products.json",
    )

    try:
        stored_object.bucket_name = "another-bucket"  # type: ignore[misc]
        was_modified = True
    except Exception:
        was_modified = False

    assert was_modified is False