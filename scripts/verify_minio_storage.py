"""
Verify local MinIO object storage integration.

Why this script exists:
- Confirms that the Python MinIO client can connect to the local MinIO container.
- Confirms that bucket creation works.
- Confirms that upload, list, existence check, and download all work.

Run from project root:

    python scripts/verify_minio_storage.py

Make sure Docker services are running first:

    docker compose up -d
"""

from __future__ import annotations

import os
import tempfile
from datetime import UTC, datetime
from pathlib import Path

from dotenv import load_dotenv

from storage.exceptions import ObjectStorageError
from storage.minio_client import MinIOObjectStorageClient


def main() -> int:
    """
    Run a full MinIO storage verification flow.

    Returns:
        Exit code.
    """

    load_dotenv()

    bucket_name = os.getenv("MINIO_BUCKET", "retail-bronze")
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")

    object_name = f"health-check/dt={timestamp[:8]}/storage_check_{timestamp}.txt"

    try:
        client = MinIOObjectStorageClient.from_env()

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            upload_file = temp_path / "storage_check.txt"
            download_file = temp_path / "downloaded_storage_check.txt"

            upload_file.write_text(
                "MinIO storage verification succeeded.\n",
                encoding="utf-8",
            )

            uploaded_object = client.upload_file(
                bucket_name=bucket_name,
                object_name=object_name,
                file_path=upload_file,
                content_type="text/plain",
            )

            print("Uploaded object:")
            print(f"- bucket: {uploaded_object.bucket_name}")
            print(f"- object: {uploaded_object.object_name}")
            print(f"- size: {uploaded_object.size}")

            exists = client.object_exists(bucket_name, object_name)

            if not exists:
                print("Object existence check failed.")
                return 1

            print("Object existence check passed.")

            objects = client.list_objects(
                bucket_name=bucket_name,
                prefix="health-check/",
            )

            print(f"Objects found under health-check/: {len(objects)}")

            client.download_file(
                bucket_name=bucket_name,
                object_name=object_name,
                destination_path=download_file,
            )

            downloaded_text = download_file.read_text(encoding="utf-8")

            if downloaded_text != "MinIO storage verification succeeded.\n":
                print("Downloaded content did not match uploaded content.")
                return 1

            print("Download verification passed.")
            print("MinIO storage verification completed successfully.")

            return 0

    except ObjectStorageError as exc:
        print(f"Storage verification failed: {exc}")
        return 1

    except OSError as exc:
        print(f"Local file operation failed: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())