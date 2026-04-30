"""
Abstract object storage client.

Why this module exists:
- Our platform should support multiple object storage providers.
- Locally, we use MinIO.
- In AWS, we will use S3.
- In GCP, we will use GCS.

The ingestion pipeline should not need to know which provider is being used.
It should only call this common interface.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class StoredObject:
    """
    Metadata about an object stored in object storage.

    Attributes:
        bucket_name: Name of the storage bucket.
        object_name: Full object path/key inside the bucket.
        size: Object size in bytes, when available.
        last_modified: Provider-specific timestamp, when available.
    """

    bucket_name: str
    object_name: str
    size: int | None = None
    last_modified: object | None = None


class ObjectStorageClient(ABC):
    """
    Abstract base class for object storage clients.

    Any provider implementation must follow this contract.

    Why:
    This gives the project a clean boundary between pipeline logic and cloud
    provider-specific SDKs.
    """

    @abstractmethod
    def ensure_bucket_exists(self, bucket_name: str) -> None:
        """
        Ensure that a bucket exists.

        If the bucket does not exist, the implementation may create it.
        """

    @abstractmethod
    def upload_file(
        self,
        bucket_name: str,
        object_name: str,
        file_path: Path,
        content_type: str = "application/octet-stream",
    ) -> StoredObject:
        """
        Upload a local file to object storage.
        """

    @abstractmethod
    def download_file(
        self,
        bucket_name: str,
        object_name: str,
        destination_path: Path,
    ) -> Path:
        """
        Download an object to a local file path.
        """

    @abstractmethod
    def list_objects(
        self,
        bucket_name: str,
        prefix: str = "",
        recursive: bool = True,
    ) -> list[StoredObject]:
        """
        List objects in a bucket.
        """

    @abstractmethod
    def object_exists(self, bucket_name: str, object_name: str) -> bool:
        """
        Check whether an object exists.
        """