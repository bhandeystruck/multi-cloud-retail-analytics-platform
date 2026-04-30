"""
Custom exceptions for the object storage layer.

Why this file exists:
- Keeps storage-related errors consistent.
- Prevents low-level provider exceptions from leaking into pipeline code.
- Makes future error handling easier in Airflow, ingestion scripts, and APIs.

Example:
Instead of every caller handling MinIO-specific exceptions, callers can catch
ObjectStorageError or one of its subclasses.
"""

from __future__ import annotations


class ObjectStorageError(Exception):
    """
    Base exception for object storage failures.

    Any storage client implementation should raise this exception or a subclass
    when an operation fails.
    """


class BucketNotFoundError(ObjectStorageError):
    """
    Raised when the target bucket does not exist and cannot be created.
    """


class ObjectUploadError(ObjectStorageError):
    """
    Raised when a local file cannot be uploaded to object storage.
    """


class ObjectDownloadError(ObjectStorageError):
    """
    Raised when an object cannot be downloaded from object storage.
    """


class ObjectListError(ObjectStorageError):
    """
    Raised when objects cannot be listed from object storage.
    """


class ObjectNotFoundError(ObjectStorageError):
    """
    Raised when an expected object does not exist in object storage.
    """