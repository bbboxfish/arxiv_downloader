from __future__ import annotations

import hashlib
import os
import shutil
import uuid
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from uuid import UUID

from arxiv_downloader.config import StorageConfig
from arxiv_downloader.downloader.client import DownloadResult
from arxiv_downloader.errors import StorageError
from arxiv_downloader.ids import ArxivId
from arxiv_downloader.storage.mount import MountGuard


@dataclass(frozen=True, slots=True)
class PublishedArtifact:
    object_key: str
    size_bytes: int
    sha256: str


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def object_key_for(identifier: ArxivId, submitted_at: datetime) -> str:
    return f"objects/pdf/submitted/{submitted_at:%Y/%m/%d}/{identifier.filename}"


def file_matches(path: Path, size_bytes: int, sha256: str) -> bool:
    try:
        return path.is_file() and path.stat().st_size == size_bytes and sha256_file(path) == sha256
    except OSError:
        return False


class StoragePublisher:
    def __init__(self, config: StorageConfig, guard: MountGuard) -> None:
        self._config = config
        self._guard = guard

    def artifact_matches(self, object_key: str, size_bytes: int, sha256: str) -> bool:
        self._guard.ensure_available()
        target = self._safe_target(object_key)
        return file_matches(target, size_bytes, sha256)

    def discover_existing(
        self,
        identifier: ArxivId,
        submitted_at: datetime,
        max_size_bytes: int,
    ) -> PublishedArtifact | None:
        """Reconcile a valid final PDF that predates its database artifact row."""
        self._guard.ensure_available()
        object_key = object_key_for(identifier, submitted_at)
        target = self._safe_target(object_key)
        if not target.exists():
            return None
        try:
            size_bytes = target.stat().st_size
            with target.open("rb") as source:
                is_pdf = source.read(5) == b"%PDF-"
            if size_bytes > 5 and size_bytes <= max_size_bytes and is_pdf:
                return PublishedArtifact(object_key, size_bytes, sha256_file(target))
            self._quarantine(target, identifier.filename)
            return None
        except OSError as exc:
            raise StorageError(f"could not inspect existing PDF: {exc}") from exc

    def publish(
        self,
        task_id: UUID,
        identifier: ArxivId,
        submitted_at: datetime,
        downloaded: DownloadResult,
    ) -> PublishedArtifact:
        self._guard.ensure_available()
        if not file_matches(downloaded.path, downloaded.size_bytes, downloaded.sha256):
            raise StorageError("staging file checksum changed", code="CHECKSUM_FAILED")

        object_key = object_key_for(identifier, submitted_at)
        final_path = self._safe_target(object_key)
        incoming_directory = self._config.data_root / "incoming" / str(task_id)
        incoming_path = incoming_directory / f"{identifier.filename}.part"
        try:
            final_path.parent.mkdir(parents=True, exist_ok=True)
            incoming_directory.mkdir(parents=True, exist_ok=True)

            if final_path.exists():
                if file_matches(final_path, downloaded.size_bytes, downloaded.sha256):
                    return PublishedArtifact(object_key, downloaded.size_bytes, downloaded.sha256)
                self._quarantine(final_path, identifier.filename)

            shutil.copyfile(downloaded.path, incoming_path)
            if not file_matches(incoming_path, downloaded.size_bytes, downloaded.sha256):
                raise StorageError("incoming file checksum mismatch", code="CHECKSUM_FAILED")
            self._guard.ensure_available()
            os.replace(incoming_path, final_path)
            if not file_matches(final_path, downloaded.size_bytes, downloaded.sha256):
                raise StorageError("published file checksum mismatch", code="CHECKSUM_FAILED")
            return PublishedArtifact(object_key, downloaded.size_bytes, downloaded.sha256)
        except StorageError:
            raise
        except OSError as exc:
            raise StorageError(f"could not publish PDF: {exc}") from exc
        finally:
            try:
                incoming_path.unlink(missing_ok=True)
            except OSError:
                pass
            try:
                incoming_directory.rmdir()
            except OSError:
                pass

    def _safe_target(self, object_key: str) -> Path:
        target = (self._config.data_root / object_key).resolve(strict=False)
        root = self._config.data_root.resolve(strict=False)
        if not target.is_relative_to(root):
            raise StorageError("object key escapes data root")
        return target

    def _quarantine(self, path: Path, filename: str) -> Path:
        quarantine = self._config.data_root / "quarantine"
        quarantine.mkdir(parents=True, exist_ok=True)
        quarantine_path = quarantine / f"{filename}.{uuid.uuid4().hex}.corrupt"
        os.replace(path, quarantine_path)
        return quarantine_path
