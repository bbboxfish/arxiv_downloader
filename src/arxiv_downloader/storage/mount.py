from __future__ import annotations

import stat
import subprocess
from collections.abc import Callable, Sequence

from arxiv_downloader.config import StorageConfig
from arxiv_downloader.errors import MountNotAvailable

CommandRunner = Callable[[Sequence[str]], subprocess.CompletedProcess[str]]


def _run_check(command: Sequence[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, capture_output=True, text=True, check=False)


class MountGuard:
    """Refuse all final-storage writes unless the configured mount is present."""

    def __init__(self, config: StorageConfig, runner: CommandRunner = _run_check) -> None:
        self.config = config
        self._runner = runner

    def ensure_available(self) -> None:
        if self.config.mode == "mounted":
            try:
                mountpoint = self._runner(["mountpoint", "-q", str(self.config.mount_root)])
                if mountpoint.returncode != 0:
                    raise MountNotAvailable(f"{self.config.mount_root} is not a mount point")
                findmnt = self._runner(
                    [
                        "findmnt",
                        "--mountpoint",
                        str(self.config.mount_root),
                        "--noheadings",
                    ]
                )
                if findmnt.returncode != 0:
                    raise MountNotAvailable(f"findmnt cannot resolve {self.config.mount_root}")
            except FileNotFoundError as exc:
                raise MountNotAvailable(
                    "mountpoint/findmnt are required for mounted storage mode"
                ) from exc
        try:
            sentinel_stat = self.config.sentinel_file.stat()
            if not stat.S_ISREG(sentinel_stat.st_mode):
                raise MountNotAvailable("mount sentinel is not a regular file")
            with self.config.sentinel_file.open("rb") as sentinel:
                sentinel.read(1)
        except FileNotFoundError as exc:
            raise MountNotAvailable(
                f"mount sentinel is missing: {self.config.sentinel_file}"
            ) from exc
        except OSError as exc:
            raise MountNotAvailable(f"mount sentinel is not readable: {exc}") from exc
