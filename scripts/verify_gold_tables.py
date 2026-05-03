"""
Verify Gold analytics table row counts.

Why this script exists:
- Confirms Silver-to-Gold transformations produced business-ready data.
- Provides a reusable validation command.
- Can later be used in Airflow or Jenkins.

Run:

    python scripts/verify_gold_tables.py
"""

from __future__ import annotations

import sys
from pathlib import Path

# Add project root to sys.path to enable imports
sys.path.insert(0, str(Path(__file__).parent.parent))

import psycopg2
from psycopg2 import sql

from scripts.init_local_warehouse import PostgresConfig

GOLD_TABLES = [
    "daily_revenue",
    "product_sales_performance",
    "customer_lifetime_value",
    "store_performance",
    "inventory_risk",
    "campaign_roi",
    "executive_kpis",
]


class GoldVerificationError(Exception):
    """
    Raised when Gold verification fails.
    """


def fetch_table_count(table_name: str) -> int:
    """
    Fetch row count for one Gold table.

    Args:
        table_name: Gold table name.

    Returns:
        Row count.

    Raises:
        GoldVerificationError: If query fails.
    """

    config = PostgresConfig.from_env()

    query = sql.SQL("SELECT COUNT(*) FROM gold.{table_name};").format(
        table_name=sql.Identifier(table_name),
    )

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
            raise GoldVerificationError(f"No result returned for gold.{table_name}")

        return int(result[0])

    except psycopg2.Error as exc:
        raise GoldVerificationError(
            f"Failed to fetch count for gold.{table_name}: {exc}",
        ) from exc


def verify_gold_tables() -> dict[str, int]:
    """
    Verify all Gold tables contain rows.

    Returns:
        Mapping of table name to row count.

    Raises:
        GoldVerificationError: If any expected table is empty.
    """

    counts: dict[str, int] = {}

    for table_name in GOLD_TABLES:
        count = fetch_table_count(table_name)
        counts[table_name] = count

        if count <= 0:
            raise GoldVerificationError(
                f"gold.{table_name} has no rows. "
                "Run Silver transformations and Gold transformations first.",
            )

    return counts


def main() -> int:
    """
    CLI entry point.

    Returns:
        Exit code.
    """

    try:
        counts = verify_gold_tables()

    except GoldVerificationError as exc:
        print(f"Gold verification failed: {exc}", file=sys.stderr)
        return 1

    print("Gold verification passed.")
    print("Gold table row counts:")

    for table_name, count in counts.items():
        print(f"- gold.{table_name}: {count}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())