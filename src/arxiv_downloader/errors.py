"""Typed errors whose codes are safe to persist and display."""

from __future__ import annotations


class ArxivDownloaderError(Exception):
    code = "INTERNAL_ERROR"

    def __init__(self, message: str, *, code: str | None = None) -> None:
        super().__init__(message)
        if code is not None:
            self.code = code


class ConfigError(ArxivDownloaderError):
    code = "CONFIG_ERROR"


class InvalidArxivId(ArxivDownloaderError):
    code = "INVALID_ID"


class MetadataError(ArxivDownloaderError):
    code = "METADATA_NOT_FOUND"


class DownloadError(ArxivDownloaderError):
    """A recoverable or terminal error while retrieving a PDF."""


class StorageError(ArxivDownloaderError):
    code = "STORAGE_WRITE_FAILED"


class MountNotAvailable(StorageError):
    code = "MOUNT_NOT_AVAILABLE"
