from pathlib import Path

from arxiv_downloader.config import StorageConfig
from arxiv_downloader.storage.capacity import StorageCapacityGuard


def test_capacity_guard_accepts_disabled_thresholds(tmp_path: Path):
    config = StorageConfig(
        mount_root=tmp_path,
        data_root=tmp_path / "data",
        staging_root=tmp_path / "staging",
        sentinel_file=tmp_path / "data/.mount_sentinel",
    )
    guard = StorageCapacityGuard(config)

    assert guard.has_capacity()


def test_capacity_guard_rejects_impossible_byte_threshold(tmp_path: Path):
    config = StorageConfig(
        mount_root=tmp_path,
        data_root=tmp_path,
        staging_root=tmp_path,
        sentinel_file=tmp_path / ".mount_sentinel",
        staging_min_free_bytes=10**18,
    )

    assert not StorageCapacityGuard(config).has_capacity()
