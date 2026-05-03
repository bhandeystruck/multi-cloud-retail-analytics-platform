"""
Run Gold transformations.

Why this script exists:
- Turns clean Silver tables into business-ready Gold analytics models.
- Provides a repeatable command for local development.
- Can later be called from Airflow.
- Keeps SQL transformations separate from Python orchestration.

Run:

    python scripts/run_gold_transformations.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import psycopg2
from psycopg2.extensions import connection as PsycopgConnection

sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.init_local_warehouse import PostgresConfig

DEFAULT_TRANSFORM_DIR = Path("transformations/gold")


class GoldTransformationError(Exception):
    """
    Raised when Gold transformations fail.
    """


def connect_to_warehouse() -> PsycopgConnection:
    """
    Connect to local PostgreSQL warehouse.

    Returns:
        PostgreSQL connection.

    Raises:
        GoldTransformationError: If connection fails.
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
        raise GoldTransformationError(
            "Failed to connect to local warehouse. "
            "Make sure PostgreSQL is running and warehouse DDL has been applied. "
            f"Reason: {exc}",
        ) from exc


def discover_transformation_files(transform_dir: Path = DEFAULT_TRANSFORM_DIR) -> list[Path]:
    """
    Discover Gold transformation SQL files.

    Args:
        transform_dir: Directory containing Gold SQL files.

    Returns:
        Sorted SQL file paths.

    Raises:
        GoldTransformationError: If files are missing.
    """

    if not transform_dir.exists():
        raise GoldTransformationError(
            f"Gold transformation directory not found: {transform_dir}",
        )

    sql_files = sorted(transform_dir.glob("*.sql"))

    if not sql_files:
        raise GoldTransformationError(
            f"No Gold transformation SQL files found in {transform_dir}",
        )

    return sql_files


def apply_transformation_file(
    connection: PsycopgConnection,
    sql_file: Path,
) -> None:
    """
    Apply one Gold transformation SQL file.

    Args:
        connection: PostgreSQL connection.
        sql_file: SQL transformation file.

    Raises:
        GoldTransformationError: If reading or SQL execution fails.
    """

    try:
        sql_text = sql_file.read_text(encoding="utf-8")

    except OSError as exc:
        raise GoldTransformationError(
            f"Failed to read transformation file {sql_file}: {exc}",
        ) from exc

    try:
        with connection.cursor() as cursor:
            cursor.execute(sql_text)

        connection.commit()

    except psycopg2.Error as exc:
        connection.rollback()

        raise GoldTransformationError(
            f"Failed to apply Gold transformation {sql_file}: {exc}",
        ) from exc


def run_gold_transformations(
    transform_dir: Path = DEFAULT_TRANSFORM_DIR,
) -> list[Path]:
    """
    Run all Gold SQL transformations.

    Args:
        transform_dir: Directory containing Gold transformation SQL files.

    Returns:
        List of applied SQL files.
    """

    sql_files = discover_transformation_files(transform_dir)
    connection = connect_to_warehouse()

    try:
        for sql_file in sql_files:
            print(f"Applying Gold transformation: {sql_file}")
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
        applied_files = run_gold_transformations()

    except GoldTransformationError as exc:
        print(f"Gold transformation failed: {exc}", file=sys.stderr)
        return 1

    print("Gold transformations completed successfully.")
    print("Applied files:")

    for sql_file in applied_files:
        print(f"- {sql_file}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())