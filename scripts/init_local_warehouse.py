"""
Initialize the local PostgreSQL warehouse.

Why this script exists:
- Docker init SQL files only run when the PostgreSQL volume is first created.
- During development, we need a repeatable way to apply warehouse DDL.
- This script applies SQL files from warehouse/local_postgres/ddl in sorted order.

Run from project root:

    python scripts/init_local_warehouse.py

Make sure Docker services are running first:

    docker compose up -d
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from pathlib import Path

import psycopg2
from dotenv import load_dotenv
from psycopg2.extensions import connection as PsycopgConnection

DEFAULT_DDL_DIR = Path("warehouse/local_postgres/ddl")


class LocalWarehouseInitializationError(Exception):
    """
    Raised when local warehouse initialization fails.
    """


@dataclass(frozen=True)
class PostgresConfig:
    """
    PostgreSQL connection configuration.

    Attributes:
        host: PostgreSQL host.
        port: PostgreSQL port.
        database: Target database name.
        user: Database user.
        password: Database password.
    """

    host: str
    port: int
    database: str
    user: str
    password: str

    @classmethod
    def from_env(cls) -> PostgresConfig:
        """
        Build PostgreSQL configuration from environment variables.

        Returns:
            PostgresConfig instance.

        Raises:
            ValueError: If POSTGRES_PORT is invalid.
        """

        load_dotenv()

        return cls(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            database=os.getenv("POSTGRES_DB", "retail_analytics"),
            user=os.getenv("POSTGRES_USER", "retail_user"),
            password=os.getenv("POSTGRES_PASSWORD", "retail_password"),
        )


def connect_to_postgres(config: PostgresConfig) -> PsycopgConnection:
    """
    Create a PostgreSQL connection.

    Args:
        config: PostgreSQL connection configuration.

    Returns:
        psycopg2 connection.

    Raises:
        LocalWarehouseInitializationError: If connection fails.
    """

    try:
        return psycopg2.connect(
            host=config.host,
            port=config.port,
            dbname=config.database,
            user=config.user,
            password=config.password,
        )

    except psycopg2.Error as exc:
        raise LocalWarehouseInitializationError(
            "Failed to connect to local PostgreSQL warehouse. "
            "Make sure Docker is running and the postgres service is healthy. "
            f"Reason: {exc}",
        ) from exc


def discover_sql_files(ddl_dir: Path) -> list[Path]:
    """
    Discover SQL files in a DDL directory.

    Args:
        ddl_dir: Directory containing SQL files.

    Returns:
        Sorted list of SQL file paths.

    Raises:
        LocalWarehouseInitializationError: If no SQL files are found.
    """

    if not ddl_dir.exists():
        raise LocalWarehouseInitializationError(f"DDL directory not found: {ddl_dir}")

    sql_files = sorted(ddl_dir.glob("*.sql"))

    if not sql_files:
        raise LocalWarehouseInitializationError(f"No SQL files found in {ddl_dir}")

    return sql_files


def apply_sql_file(connection: PsycopgConnection, sql_file: Path) -> None:
    """
    Apply one SQL file to the database.

    Args:
        connection: Active PostgreSQL connection.
        sql_file: SQL file to execute.

    Raises:
        LocalWarehouseInitializationError: If reading or execution fails.
    """

    try:
        sql = sql_file.read_text(encoding="utf-8")

    except OSError as exc:
        raise LocalWarehouseInitializationError(
            f"Failed to read SQL file {sql_file}: {exc}",
        ) from exc

    try:
        with connection.cursor() as cursor:
            cursor.execute(sql)

        connection.commit()

    except psycopg2.Error as exc:
        connection.rollback()

        raise LocalWarehouseInitializationError(
            f"Failed to apply SQL file {sql_file}: {exc}",
        ) from exc


def initialize_local_warehouse(ddl_dir: Path = DEFAULT_DDL_DIR) -> list[Path]:
    """
    Apply all local warehouse DDL files.

    Args:
        ddl_dir: Directory containing DDL files.

    Returns:
        List of applied SQL files.
    """

    config = PostgresConfig.from_env()
    sql_files = discover_sql_files(ddl_dir)

    connection = connect_to_postgres(config)

    try:
        for sql_file in sql_files:
            print(f"Applying DDL: {sql_file}")
            apply_sql_file(connection, sql_file)

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
        applied_files = initialize_local_warehouse()

    except (LocalWarehouseInitializationError, ValueError) as exc:
        print(f"Local warehouse initialization failed: {exc}", file=sys.stderr)
        return 1

    print("Local warehouse initialized successfully.")
    print("Applied files:")

    for sql_file in applied_files:
        print(f"- {sql_file}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())