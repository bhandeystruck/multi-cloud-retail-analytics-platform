"""
Unit tests for local warehouse initialization helpers.

These tests do not connect to PostgreSQL.

Why:
- Database connectivity belongs in integration verification scripts.
- Unit tests should validate deterministic filesystem and discovery behavior.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from scripts.init_local_warehouse import (
    LocalWarehouseInitializationError,
    discover_sql_files,
)


def test_discover_sql_files_returns_sorted_sql_files(tmp_path: Path) -> None:
    """
    Verify SQL files are discovered in sorted order.

    Sorted execution matters because DDL files often depend on earlier files.
    """

    second_file = tmp_path / "002_second.sql"
    first_file = tmp_path / "001_first.sql"
    ignored_file = tmp_path / "README.md"

    second_file.write_text("SELECT 2;", encoding="utf-8")
    first_file.write_text("SELECT 1;", encoding="utf-8")
    ignored_file.write_text("not sql", encoding="utf-8")

    discovered_files = discover_sql_files(tmp_path)

    assert discovered_files == [first_file, second_file]


def test_discover_sql_files_rejects_missing_directory() -> None:
    """
    Verify missing DDL directories fail clearly.
    """

    with pytest.raises(LocalWarehouseInitializationError, match="DDL directory not found"):
        discover_sql_files(Path("does-not-exist"))


def test_discover_sql_files_rejects_empty_directory(tmp_path: Path) -> None:
    """
    Verify empty DDL directories fail clearly.
    """

    with pytest.raises(LocalWarehouseInitializationError, match="No SQL files found"):
        discover_sql_files(tmp_path)