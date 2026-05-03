"""
Verify Silver table row counts.

Why this script exists:
- Confirms Bronze-to-Silver transformations produced data.
- Provides a reusable local validation command.
- Can later be used in CI/CD or Airflow validation tasks.

Run:

    python scripts/verify_silver_tables.py
"""

from __future__ import annotations

import sys
from pathlib import Path

# Add project root to sys.path to enable imports
sys.path.insert(0, str(Path(__file__).parent.parent))

import psycopg2

from scripts.init_local_warehouse import PostgresConfig


SILVER_TABLES = [
    "products",
    "customers",
    "stores",
    "campaigns",
    "sales",
    "inventory",
    "returns",
]


class SilverVerificationError(Exception):
    """
    Raised when Silver verification fails.
    """


def fetch_table_count(table_name: str) -> int:
    """
    Fetch row count for one Silver table.

    Args:
        table_name: Silver table name.

    Returns:
        Row count.

    Raises:
        SilverVerificationError: If query fails.
    """

    config = PostgresConfig.from_env()

    query = f"SELECT COUNT(*) FROM silver.{table_name};"

    try:
        with psycopg2.connect(
            host=config.host,
            port=config.port,
            dbname=config.database,
            user=config.user,
            password=config.password,
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchone()

        if result is None:
            raise SilverVerificationError(f"No result returned for silver.{table_name}")

        return int(result[0])

    except psycopg2.Error as exc:
        raise SilverVerificationError(
            f"Failed to fetch count for silver.{table_name}: {exc}",
        ) from exc


def verify_silver_tables() -> dict[str, int]:
    """
    Verify all Silver tables contain rows.

    Returns:
        Mapping of table name to row count.

    Raises:
        SilverVerificationError: If any expected table is empty.
    """

    counts: dict[str, int] = {}

    for table_name in SILVER_TABLES:
        count = fetch_table_count(table_name)
        counts[table_name] = count

        if count <= 0:
            raise SilverVerificationError(
                f"silver.{table_name} has no rows. "
                "Run Bronze load and Silver transformations first.",
            )

    return counts


def main() -> int:
    """
    CLI entry point.

    Returns:
        Exit code.
    """

    try:
        counts = verify_silver_tables()

    except SilverVerificationError as exc:
        print(f"Silver verification failed: {exc}", file=sys.stderr)
        return 1

    print("Silver verification passed.")
    print("Silver table row counts:")

    for table_name, count in counts.items():
        print(f"- silver.{table_name}: {count}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())