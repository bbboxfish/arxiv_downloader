import hashlib
import subprocess
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import pytest

from arxiv_downloader.config import StorageConfig
from arxiv_downloader.downloader.client import DownloadResult
from arxiv_downloader.errors import MountNotAvailable
from arxiv_downloader.ids import ArxivId
from arxiv_downloader.storage.mount import MountGuard
from arxiv_downloader.storage.publisher import StoragePublisher, object_key_for, sha256_file


def successful_runner(command):
    return subprocess.CompletedProcess(command, 0, "mounted", "")


def storage_config(tmp_path: Path) -> StorageConfig:
    mount = tmp_path / "mnt"
    data = mount / "arxiv"
    data.mkdir(parents=True)
    sentinel = data / ".mount_sentinel"
    sentinel.write_text("expected-volume")
    return StorageConfig(
        mount_root=mount,
        data_root=data,
        staging_root=tmp_path / "staging",
        sentinel_file=sentinel,
    )


def test_mount_guard_requires_sentinel(tmp_path):
    config = storage_config(tmp_path)
    config.sentinel_file.unlink()
    guard = MountGuard(config, successful_runner)
    with pytest.raises(MountNotAvailable, match="missing"):
        guard.ensure_available()


def test_mount_guard_requires_real_mount_check(tmp_path):
    config = storage_config(tmp_path)

    def failed_runner(command):
        return subprocess.CompletedProcess(command, 1, "", "not mounted")

    with pytest.raises(MountNotAvailable, match="not a mount point"):
        MountGuard(config, failed_runner).ensure_available()


def test_local_mode_keeps_sentinel_but_skips_linux_commands(tmp_path):
    config = replace(storage_config(tmp_path), mode="local")

    def command_must_not_run(command):
        raise AssertionError(f"unexpected command: {command}")

    MountGuard(config, command_must_not_run).ensure_available()
    config.sentinel_file.unlink()
    with pytest.raises(MountNotAvailable, match="missing"):
        MountGuard(config, command_must_not_run).ensure_available()


def test_object_key_uses_submission_day_and_safe_legacy_name():
    key = object_key_for(ArxivId("hep-th/9901001", 2), datetime(1999, 1, 15, tzinfo=timezone.utc))
    assert key == "objects/pdf/submitted/1999/01/15/hep-th__9901001v2.pdf"


def test_publish_copies_verifies_and_reuses_file(tmp_path):
    config = storage_config(tmp_path)
    source = tmp_path / "paper.part"
    payload = b"%PDF-1.7\nexample"
    source.write_bytes(payload)
    result = DownloadResult(
        path=source,
        size_bytes=len(payload),
        sha256=hashlib.sha256(payload).hexdigest(),
    )
    publisher = StoragePublisher(config, MountGuard(config, successful_runner))
    submitted = datetime(2024, 1, 15, tzinfo=timezone.utc)
    published = publisher.publish(uuid4(), ArxivId("2401.01234", 2), submitted, result)
    final_path = config.data_root / published.object_key
    assert final_path.read_bytes() == payload
    assert sha256_file(final_path) == published.sha256

    second = publisher.publish(uuid4(), ArxivId("2401.01234", 2), submitted, result)
    assert second == published
    assert list((config.data_root / "objects").rglob("*.pdf")) == [final_path]


def test_discovers_existing_pdf_without_database_record(tmp_path):
    config = storage_config(tmp_path)
    publisher = StoragePublisher(config, MountGuard(config, successful_runner))
    identifier = ArxivId("2401.01234", 1)
    submitted = datetime(2024, 1, 15, tzinfo=timezone.utc)
    final_path = config.data_root / object_key_for(identifier, submitted)
    final_path.parent.mkdir(parents=True)
    final_path.write_bytes(b"%PDF-1.7\npreexisting")

    discovered = publisher.discover_existing(identifier, submitted, 1024)
    assert discovered is not None
    assert discovered.size_bytes == final_path.stat().st_size
    assert discovered.sha256 == sha256_file(final_path)
