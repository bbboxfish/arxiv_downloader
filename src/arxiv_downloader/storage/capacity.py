from __future__ import annotations

import shutil
from dataclasses import dataclass
from pathlib import Path

from arxiv_downloader.config import StorageConfig


@dataclass(frozen=True, slots=True)
class Capacity:
    path: Path
    free_bytes: int
    total_bytes: int
    free_percent: float


class StorageCapacityGuard:
    """Gate new work when either configured filesystem reaches its low-water mark."""

    def __init__(self, config: StorageConfig) -> None:
        self._config = config

    def snapshot(self, path: Path) -> Capacity:
        usage = shutil.disk_usage(path if path.exists() else path.parent)
        free_percent = (usage.free * 100 / usage.total) if usage.total else 0
        return Capacity(path, usage.free, usage.total, free_percent)

    def has_capacity(self) -> bool:
        staging = self.snapshot(self._config.staging_root)
        data = self.snapshot(self._config.data_root)
        return self._above_threshold(
            staging,
            self._config.staging_min_free_bytes,
            self._config.staging_min_free_percent,
        ) and self._above_threshold(
            data,
            self._config.data_min_free_bytes,
            self._config.data_min_free_percent,
        )

    @staticmethod
    def _above_threshold(capacity: Capacity, minimum_bytes: int, minimum_percent: float) -> bool:
        return capacity.free_bytes >= minimum_bytes and capacity.free_percent >= minimum_percent
