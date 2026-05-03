"""
Verify local PostgreSQL warehouse objects.

Why this script exists:
- Confirms required schemas and tables exist.
- Provides a repeatable validation command for local development.
- Can be reused later in Jenkins CI/CD.

Run from the project root:

    python -m scripts.verify_local_warehouse
"""

from __future__ import annotations

import sys
from pathlib import Path

import psycopg2

from scripts.init_local_warehouse import PostgresConfig

# When this script is run directly, Python adds the script directory to sys.path.
# Insert the repository root first so `import scripts` resolves correctly.
ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))



REQUIRED_SCHEMAS = {
    "bronze",
    "silver",
    "gold",
    "ops",
}


REQUIRED_TABLES = {
    ("ops", "pipeline_runs"),
    ("ops", "loaded_files"),
    ("ops", "data_quality_results"),
    ("bronze", "raw_sales"),
    ("bronze", "raw_products"),
    ("bronze", "raw_customers"),
    ("bronze", "raw_stores"),
    ("bronze", "raw_inventory"),
    ("bronze", "raw_campaigns"),
    ("bronze", "raw_returns"),
}


class LocalWarehouseVerificationError(Exception):
    """
    Raised when local warehouse verification fails.
    """


def fetch_existing_schemas(config: PostgresConfig) -> set[str]:
    """
    Fetch schemas from PostgreSQL.

    Args:
        config: PostgreSQL connection config.

    Returns:
        Set of schema names.
    """

    query = """
        SELECT schema_name
        FROM information_schema.schemata;
    """

    with psycopg2.connect(
        host=config.host,
        port=config.port,
        dbname=config.database,
        user=config.user,
        password=config.password,
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()

    return {row[0] for row in rows}


def fetch_existing_tables(config: PostgresConfig) -> set[tuple[str, str]]:
    """
    Fetch tables from PostgreSQL.

    Args:
        config: PostgreSQL connection config.

    Returns:
        Set of (schema_name, table_name) pairs.
    """

    query = """
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_type = 'BASE TABLE';
    """

    with psycopg2.connect(
        host=config.host,
        port=config.port,
        dbname=config.database,
        user=config.user,
        password=config.password,
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()

    return {(row[0], row[1]) for row in rows}


def verify_local_warehouse() -> None:
    """
    Verify required local warehouse schemas and tables.

    Raises:
        LocalWarehouseVerificationError: If required objects are missing.
    """

    config = PostgresConfig.from_env()

    existing_schemas = fetch_existing_schemas(config)
    existing_tables = fetch_existing_tables(config)

    missing_schemas = REQUIRED_SCHEMAS - existing_schemas
    missing_tables = REQUIRED_TABLES - existing_tables

    if missing_schemas:
        raise LocalWarehouseVerificationError(
            f"Missing required schemas: {sorted(missing_schemas)}",
        )

    if missing_tables:
        raise LocalWarehouseVerificationError(
            f"Missing required tables: {sorted(missing_tables)}",
        )


def main() -> int:
    """
    CLI entry point.

    Returns:
        Exit code.
    """

    try:
        verify_local_warehouse()

    except (LocalWarehouseVerificationError, psycopg2.Error, ValueError) as exc:
        print(f"Local warehouse verification failed: {exc}", file=sys.stderr)
        return 1

    print("Local warehouse verification passed.")
    print("Required schemas and tables exist.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())