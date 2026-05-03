"""
Unit tests for Gold transformation helper functions.

These tests do not connect to PostgreSQL.

Why:
- SQL execution is an integration concern.
- Unit tests validate deterministic file discovery behavior.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from scripts.run_gold_transformations import (
    GoldTransformationError,
    discover_transformation_files,
)


def test_discover_transformation_files_returns_sorted_sql_files(tmp_path: Path) -> None:
    """
    Verify Gold SQL files are discovered in sorted order.
    """

    second_file = tmp_path / "002_second.sql"
    first_file = tmp_path / "001_first.sql"
    ignored_file = tmp_path / "README.md"

    second_file.write_text("SELECT 2;", encoding="utf-8")
    first_file.write_text("SELECT 1;", encoding="utf-8")
    ignored_file.write_text("ignored", encoding="utf-8")

    discovered_files = discover_transformation_files(tmp_path)

    assert discovered_files == [first_file, second_file]


def test_discover_transformation_files_rejects_missing_directory() -> None:
    """
    Verify missing transformation directories fail clearly.
    """

    with pytest.raises(GoldTransformationError, match="directory not found"):
        discover_transformation_files(Path("missing-transform-dir"))


def test_discover_transformation_files_rejects_empty_directory(tmp_path: Path) -> None:
    """
    Verify empty transformation directories fail clearly.
    """

    with pytest.raises(GoldTransformationError, match="No Gold transformation"):
        discover_transformation_files(tmp_path)