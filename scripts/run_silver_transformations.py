"""
Run Silver transformations.

Why this script exists:
- Turns raw JSONB Bronze records into typed Silver relational tables.
- Provides a repeatable local command.
- Can later be called from Airflow.
- Keeps transformation SQL separate from Python orchestration logic.

Run:

    python scripts/run_silver_transformations.py
"""

from __future__ import annotations

import sys
from pathlib import Path

# Add project root to sys.path to enable imports
sys.path.insert(0, str(Path(__file__).parent.parent))

import psycopg2
from psycopg2.extensions import connection as PsycopgConnection

from scripts.init_local_warehouse import PostgresConfig


DEFAULT_TRANSFORM_DIR = Path("transformations/silver")


class SilverTransformationError(Exception):
    """
    Raised when Silver transformations fail.
    """


def connect_to_warehouse() -> PsycopgConnection:
    """
    Connect to local PostgreSQL warehouse.

    Returns:
        PostgreSQL connection.

    Raises:
        SilverTransformationError: If connection fails.
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
        raise SilverTransformationError(
            "Failed to connect to local warehouse. "
            "Make sure PostgreSQL is running and warehouse DDL has been applied. "
            f"Reason: {exc}",
        ) from exc


def discover_transformation_files(transform_dir: Path = DEFAULT_TRANSFORM_DIR) -> list[Path]:
    """
    Discover Silver transformation SQL files.

    Args:
        transform_dir: Directory containing Silver SQL files.

    Returns:
        Sorted SQL file paths.

    Raises:
        SilverTransformationError: If files are missing.
    """

    if not transform_dir.exists():
        raise SilverTransformationError(
            f"Silver transformation directory not found: {transform_dir}",
        )

    sql_files = sorted(transform_dir.glob("*.sql"))

    if not sql_files:
        raise SilverTransformationError(
            f"No Silver transformation SQL files found in {transform_dir}",
        )

    return sql_files


def apply_transformation_file(
    connection: PsycopgConnection,
    sql_file: Path,
) -> None:
    """
    Apply one Silver transformation SQL file.

    Args:
        connection: PostgreSQL connection.
        sql_file: SQL transformation file.

    Raises:
        SilverTransformationError: If reading or SQL execution fails.
    """

    try:
        sql_text = sql_file.read_text(encoding="utf-8")

    except OSError as exc:
        raise SilverTransformationError(
            f"Failed to read transformation file {sql_file}: {exc}",
        ) from exc

    try:
        with connection.cursor() as cursor:
            cursor.execute(sql_text)

        connection.commit()

    except psycopg2.Error as exc:
        connection.rollback()

        raise SilverTransformationError(
            f"Failed to apply Silver transformation {sql_file}: {exc}",
        ) from exc


def run_silver_transformations(
    transform_dir: Path = DEFAULT_TRANSFORM_DIR,
) -> list[Path]:
    """
    Run all Silver SQL transformations.

    Args:
        transform_dir: Directory containing Silver transformation SQL files.

    Returns:
        List of applied SQL files.
    """

    sql_files = discover_transformation_files(transform_dir)
    connection = connect_to_warehouse()

    try:
        for sql_file in sql_files:
            print(f"Applying Silver transformation: {sql_file}")
            apply_transformation_file(connection, sql_file)

    finally:
        connection.close()

    return sql_files


def main() -> int:
    """
    CLI entry point.

    Returns:
        Process exit code.
    """

    try:
        applied_files = run_silver_transformations()

    except SilverTransformationError as exc:
        print(f"Silver transformation failed: {exc}", file=sys.stderr)
        return 1

    print("Silver transformations completed successfully.")
    print("Applied files:")

    for sql_file in applied_files:
        print(f"- {sql_file}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())