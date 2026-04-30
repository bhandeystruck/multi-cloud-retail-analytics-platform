"""
MinIO object storage client.

Why this module exists:
- MinIO gives us an S3-compatible object store for local development.
- This allows us to build and test data lake behavior without using AWS.
- Later, the same object storage interface can be implemented for AWS S3 and GCS.

This client is intentionally defensive:
- It validates local file existence before upload.
- It creates buckets when needed.
- It wraps MinIO SDK errors in project-specific exceptions.
"""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv
from minio import Minio
from minio.error import S3Error

from storage.exceptions import (
    BucketNotFoundError,
    ObjectDownloadError,
    ObjectListError,
    ObjectNotFoundError,
    ObjectStorageError,
    ObjectUploadError,
)
from storage.object_storage_client import ObjectStorageClient, StoredObject


def parse_bool(value: str | bool | None, default: bool = False) -> bool:
    """
    Parse a boolean-like environment value.

    Args:
        value: String, bool, or None.
        default: Value to use when value is None.

    Returns:
        Boolean interpretation of the input.
    """

    if value is None:
        return default

    if isinstance(value, bool):
        return value

    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


class MinIOObjectStorageClient(ObjectStorageClient):
    """
    MinIO implementation of the ObjectStorageClient interface.

    This class hides MinIO-specific SDK details from the rest of the project.
    """

    def __init__(
        self,
        endpoint: str,
        access_key: str,
        secret_key: str,
        secure: bool = False,
    ) -> None:
        """
        Initialize the MinIO client.

        Args:
            endpoint: MinIO API endpoint, for example localhost:9000.
            access_key: MinIO access key.
            secret_key: MinIO secret key.
            secure: Whether to use HTTPS.
        """

        if not endpoint:
            raise ValueError("MinIO endpoint is required")

        if not access_key:
            raise ValueError("MinIO access key is required")

        if not secret_key:
            raise ValueError("MinIO secret key is required")

        self._client = Minio(
            endpoint=endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
        )

    @classmethod
    def from_env(cls) -> MinIOObjectStorageClient:
        """
        Create a MinIO client from environment variables.

        Environment variables:
            MINIO_ENDPOINT
            MINIO_ROOT_USER
            MINIO_ROOT_PASSWORD
            MINIO_SECURE

        Returns:
            Configured MinIOObjectStorageClient.
        """

        load_dotenv()

        endpoint = os.getenv("MINIO_ENDPOINT", "localhost:9000")
        access_key = os.getenv("MINIO_ROOT_USER", "minioadmin")
        secret_key = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
        secure = parse_bool(os.getenv("MINIO_SECURE"), default=False)

        return cls(
            endpoint=endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
        )

    def ensure_bucket_exists(self, bucket_name: str) -> None:
        """
        Ensure the bucket exists.

        If it does not exist, create it.

        Args:
            bucket_name: Bucket to validate or create.

        Raises:
            BucketNotFoundError: If bucket validation or creation fails.
        """

        try:
            if not self._client.bucket_exists(bucket_name):
                self._client.make_bucket(bucket_name)

        except S3Error as exc:
            raise BucketNotFoundError(
                f"Failed to ensure bucket exists: {bucket_name}. Reason: {exc}",
            ) from exc

    def upload_file(
        self,
        bucket_name: str,
        object_name: str,
        file_path: Path,
        content_type: str = "application/octet-stream",
    ) -> StoredObject:
        """
        Upload a local file to MinIO.

        Args:
            bucket_name: Target bucket.
            object_name: Object key/path inside the bucket.
            file_path: Local file path.
            content_type: MIME type for uploaded object.

        Returns:
            Metadata about the uploaded object.

        Raises:
            ObjectUploadError: If file does not exist or upload fails.
        """

        if not file_path.exists():
            raise ObjectUploadError(f"Cannot upload missing file: {file_path}")

        if not file_path.is_file():
            raise ObjectUploadError(f"Upload path is not a file: {file_path}")

        try:
            self.ensure_bucket_exists(bucket_name)

            self._client.fput_object(
                bucket_name=bucket_name,
                object_name=object_name,
                file_path=str(file_path),
                content_type=content_type,
            )

            stat = self._client.stat_object(bucket_name, object_name)

            return StoredObject(
                bucket_name=bucket_name,
                object_name=object_name,
                size=stat.size,
                last_modified=stat.last_modified,
            )

        except S3Error as exc:
            raise ObjectUploadError(
                f"Failed to upload {file_path} to {bucket_name}/{object_name}. "
                f"Reason: {exc}",
            ) from exc

    def download_file(
        self,
        bucket_name: str,
        object_name: str,
        destination_path: Path,
    ) -> Path:
        """
        Download an object from MinIO to a local file.

        Args:
            bucket_name: Source bucket.
            object_name: Object key/path inside the bucket.
            destination_path: Destination local path.

        Returns:
            Path to the downloaded file.

        Raises:
            ObjectDownloadError: If download fails.
        """

        try:
            destination_path.parent.mkdir(parents=True, exist_ok=True)

            self._client.fget_object(
                bucket_name=bucket_name,
                object_name=object_name,
                file_path=str(destination_path),
            )

            return destination_path

        except S3Error as exc:
            raise ObjectDownloadError(
                f"Failed to download {bucket_name}/{object_name} to "
                f"{destination_path}. Reason: {exc}",
            ) from exc

    def list_objects(
        self,
        bucket_name: str,
        prefix: str = "",
        recursive: bool = True,
    ) -> list[StoredObject]:
        """
        List objects in a MinIO bucket.

        Args:
            bucket_name: Bucket name.
            prefix: Optional object key prefix.
            recursive: Whether to list nested objects.

        Returns:
            List of StoredObject records.

        Raises:
            ObjectListError: If listing fails.
        """

        try:
            objects = self._client.list_objects(
                bucket_name=bucket_name,
                prefix=prefix,
                recursive=recursive,
            )

            stored_objects: list[StoredObject] = []

            for obj in objects:
                # MinIO's Python type hints allow object_name to be None.
                # In normal object listings, it should be a string, but we guard
                # against None so our platform does not create invalid metadata.
                if obj.object_name is None:
                    continue

                stored_objects.append(
                    StoredObject(
                        bucket_name=bucket_name,
                        object_name=obj.object_name,
                        size=obj.size,
                        last_modified=obj.last_modified,
                    ),
                )

            return stored_objects

        except S3Error as exc:
            raise ObjectListError(
                f"Failed to list objects in bucket {bucket_name} "
                f"with prefix '{prefix}'. Reason: {exc}",
            ) from exc

    def object_exists(self, bucket_name: str, object_name: str) -> bool:
        """
        Check if an object exists in MinIO.

        Args:
            bucket_name: Bucket name.
            object_name: Object key/path.

        Returns:
            True if the object exists, otherwise False.

        Raises:
            ObjectStorageError: If the check fails for an unexpected reason.
        """

        try:
            self._client.stat_object(bucket_name, object_name)
            return True

        except S3Error as exc:
            if exc.code in {"NoSuchKey", "NoSuchObject", "NoSuchBucket"}:
                return False

            raise ObjectStorageError(
                f"Failed to check object existence for "
                f"{bucket_name}/{object_name}. Reason: {exc}",
            ) from exc

    def require_object(self, bucket_name: str, object_name: str) -> StoredObject:
        """
        Return object metadata or raise if the object does not exist.

        This method is useful when downstream code expects a file to exist and
        should fail clearly if it does not.

        Args:
            bucket_name: Bucket name.
            object_name: Object key/path.

        Returns:
            Stored object metadata.

        Raises:
            ObjectNotFoundError: If the object does not exist.
        """

        try:
            stat = self._client.stat_object(bucket_name, object_name)

            return StoredObject(
                bucket_name=bucket_name,
                object_name=object_name,
                size=stat.size,
                last_modified=stat.last_modified,
            )

        except S3Error as exc:
            if exc.code in {"NoSuchKey", "NoSuchObject", "NoSuchBucket"}:
                raise ObjectNotFoundError(
                    f"Object not found: {bucket_name}/{object_name}",
                ) from exc

            raise ObjectStorageError(
                f"Failed to retrieve object metadata for "
                f"{bucket_name}/{object_name}. Reason: {exc}",
            ) from exc