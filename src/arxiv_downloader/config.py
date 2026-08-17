"""TOML plus environment configuration for the daemon."""

from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from arxiv_downloader.errors import ConfigError

DEFAULT_CONFIG_PATH = Path("/etc/arxiv-downloader/config.toml")


@dataclass(frozen=True)
class ServerConfig:
    host: str = "127.0.0.1"
    port: int = 8765


@dataclass(frozen=True)
class DatabaseConfig:
    dsn_env: str
    url: str

    def __repr__(self) -> str:
        return f"DatabaseConfig(dsn_env={self.dsn_env!r}, url='<redacted>')"


@dataclass(frozen=True)
class DownloadConfig:
    concurrency: int = 2
    request_timeout_seconds: float = 120
    max_attempts: int = 2
    max_file_size_mb: int = 200
    min_request_interval_seconds: float = 3
    user_agent: str = "arxiv-downloader-a0/0.1 contact@example.com"

    @property
    def max_file_size_bytes(self) -> int:
        return self.max_file_size_mb * 1024 * 1024


@dataclass(frozen=True)
class StorageConfig:
    mount_root: Path = Path("/mnt")
    data_root: Path = Path("/mnt/arxiv")
    staging_root: Path = Path("/var/cache/arxiv-downloader")
    sentinel_file: Path = Path("/mnt/arxiv/.mount_sentinel")
    mode: str = "mounted"


@dataclass(frozen=True)
class Settings:
    server: ServerConfig
    database: DatabaseConfig
    download: DownloadConfig
    storage: StorageConfig

    def safe_summary(self) -> dict[str, Any]:
        return {
            "server": {"host": self.server.host, "port": self.server.port},
            "database": {"dsn_env": self.database.dsn_env, "url": "<redacted>"},
            "download": {
                "concurrency": self.download.concurrency,
                "request_timeout_seconds": self.download.request_timeout_seconds,
                "max_attempts": self.download.max_attempts,
                "max_file_size_mb": self.download.max_file_size_mb,
                "min_request_interval_seconds": self.download.min_request_interval_seconds,
                "user_agent": self.download.user_agent,
            },
            "storage": {
                "mode": self.storage.mode,
                "mount_root": str(self.storage.mount_root),
                "data_root": str(self.storage.data_root),
                "staging_root": str(self.storage.staging_root),
                "sentinel_file": str(self.storage.sentinel_file),
            },
        }


def _table(document: dict[str, Any], name: str) -> dict[str, Any]:
    value = document.get(name, {})
    if not isinstance(value, dict):
        raise ConfigError(f"[{name}] must be a TOML table")
    return value


def _storage_path(
    storage_data: dict[str, Any], key: str, default: str, config_directory: Path
) -> Path:
    path = Path(storage_data.get(key, default)).expanduser()
    if not path.is_absolute():
        path = config_directory / path
    return path.resolve(strict=False)


def load_settings(path: Path | str | None = None) -> Settings:
    configured_path = path or os.environ.get("ARXIV_DOWNLOADER_CONFIG") or DEFAULT_CONFIG_PATH
    config_path = Path(configured_path).expanduser().resolve(strict=False)
    try:
        with config_path.open("rb") as config_file:
            document = tomllib.load(config_file)
    except FileNotFoundError as exc:
        raise ConfigError(f"configuration file not found: {config_path}") from exc
    except tomllib.TOMLDecodeError as exc:
        raise ConfigError(f"invalid TOML in {config_path}: {exc}") from exc

    server_data = _table(document, "server")
    database_data = _table(document, "database")
    download_data = _table(document, "download")
    storage_data = _table(document, "storage")

    dsn_env = str(database_data.get("dsn_env", "ARXIV_DATABASE_URL"))
    database_url = os.environ.get(dsn_env)
    if not database_url:
        raise ConfigError(f"required database environment variable is not set: {dsn_env}")

    settings = Settings(
        server=ServerConfig(
            host=str(server_data.get("host", "127.0.0.1")),
            port=int(server_data.get("port", 8765)),
        ),
        database=DatabaseConfig(dsn_env=dsn_env, url=database_url),
        download=DownloadConfig(
            concurrency=int(download_data.get("concurrency", 2)),
            request_timeout_seconds=float(download_data.get("request_timeout_seconds", 120)),
            max_attempts=int(download_data.get("max_attempts", 2)),
            max_file_size_mb=int(download_data.get("max_file_size_mb", 200)),
            min_request_interval_seconds=float(
                download_data.get("min_request_interval_seconds", 3)
            ),
            user_agent=str(
                download_data.get("user_agent", "arxiv-downloader-a0/0.1 contact@example.com")
            ),
        ),
        storage=StorageConfig(
            mount_root=_storage_path(storage_data, "mount_root", "/mnt", config_path.parent),
            data_root=_storage_path(storage_data, "data_root", "/mnt/arxiv", config_path.parent),
            staging_root=_storage_path(
                storage_data,
                "staging_root",
                "/var/cache/arxiv-downloader",
                config_path.parent,
            ),
            sentinel_file=_storage_path(
                storage_data,
                "sentinel_file",
                "/mnt/arxiv/.mount_sentinel",
                config_path.parent,
            ),
            mode=str(storage_data.get("mode", "mounted")).lower(),
        ),
    )
    _validate_settings(settings)
    return settings


def _validate_settings(settings: Settings) -> None:
    if settings.server.host != "127.0.0.1":
        raise ConfigError("server.host must be 127.0.0.1 for A0")
    if not 1 <= settings.server.port <= 65535:
        raise ConfigError("server.port must be between 1 and 65535")
    if not 1 <= settings.download.concurrency <= 2:
        raise ConfigError("download.concurrency must be 1 or 2 for A0")
    if settings.download.max_attempts < 1:
        raise ConfigError("download.max_attempts must be at least 1")
    if settings.download.max_file_size_mb < 1:
        raise ConfigError("download.max_file_size_mb must be positive")
    if settings.download.request_timeout_seconds <= 0:
        raise ConfigError("download.request_timeout_seconds must be positive")
    if settings.download.min_request_interval_seconds < 0:
        raise ConfigError("download.min_request_interval_seconds cannot be negative")
    if settings.storage.mode not in {"mounted", "local"}:
        raise ConfigError("storage.mode must be 'mounted' or 'local'")

    paths = (
        settings.storage.mount_root,
        settings.storage.data_root,
        settings.storage.staging_root,
        settings.storage.sentinel_file,
    )
    if any(not path.is_absolute() for path in paths):
        raise ConfigError("all storage paths must be absolute")
    if not settings.storage.data_root.is_relative_to(settings.storage.mount_root):
        raise ConfigError("storage.data_root must be inside storage.mount_root")
    if not settings.storage.sentinel_file.is_relative_to(settings.storage.data_root):
        raise ConfigError("storage.sentinel_file must be inside storage.data_root")
    if settings.storage.mode == "local" and settings.storage.mount_root in {
        Path("/"),
        Path("/mnt"),
    }:
        raise ConfigError("local storage mode cannot use / or /mnt as storage.mount_root")
