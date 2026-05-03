"""
CLI script for loading Bronze object-storage files into local PostgreSQL.

Run latest manifest:

    python scripts/load_bronze_warehouse.py

Run a specific manifest:

    python scripts/load_bronze_warehouse.py --manifest-path data/manifests/example.json

Force reload attempt:

    python scripts/load_bronze_warehouse.py --force-reload
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Add project root to sys.path to enable imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from warehouse.local_postgres.load.bronze_loader import (
    BronzeWarehouseLoadError,
    find_latest_manifest,
    load_bronze_manifest,
)


def parse_args() -> argparse.Namespace:
    """
    Parse CLI arguments.
    """

    parser = argparse.ArgumentParser(
        description="Load Bronze object-storage files into local PostgreSQL warehouse.",
    )

    parser.add_argument(
        "--manifest-path",
        type=Path,
        default=None,
        help="Path to a local Bronze ingestion manifest. Defaults to latest manifest.",
    )

    parser.add_argument(
        "--force-reload",
        action="store_true",
        help=(
            "Attempt to load files even if ops.loaded_files says they were already loaded. "
            "Bronze raw tables still avoid duplicate rows using unique constraints."
        ),
    )

    return parser.parse_args()


def main() -> int:
    """
    CLI entry point.

    Returns:
        Process exit code.
    """

    args = parse_args()

    try:
        manifest_path = args.manifest_path or find_latest_manifest()

        print(f"Using manifest: {manifest_path}")

        results = load_bronze_manifest(
            manifest_path=manifest_path,
            force_reload=args.force_reload,
        )

    except BronzeWarehouseLoadError as exc:
        print(f"Bronze warehouse load failed: {exc}", file=sys.stderr)
        return 1

    print("Bronze warehouse load completed successfully.")
    print("Loaded datasets:")

    for result in results:
        print(
            f"- {result.dataset_name}: "
            f"status={result.status}, "
            f"records={result.record_count}, "
            f"inserted={result.inserted_count}, "
            f"table=bronze.{result.table_name}",
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())