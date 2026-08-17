import pytest

from arxiv_downloader.config import load_settings
from arxiv_downloader.errors import ConfigError

CONFIG = """
[server]
host = "127.0.0.1"
port = 8765
[database]
dsn_env = "TEST_DATABASE_URL"
[download]
concurrency = 2
max_attempts = 2
[storage]
mount_root = "/mnt"
data_root = "/mnt/arxiv"
staging_root = "/var/cache/arxiv-downloader"
sentinel_file = "/mnt/arxiv/.mount_sentinel"
"""


def test_load_settings_reads_database_from_environment(tmp_path, monkeypatch):
    path = tmp_path / "config.toml"
    path.write_text(CONFIG)
    monkeypatch.setenv("TEST_DATABASE_URL", "postgresql+asyncpg://user:secret@db/arxiv")
    settings = load_settings(path)
    assert settings.download.concurrency == 2
    assert settings.storage.mode == "mounted"
    assert settings.database.url.endswith("@db/arxiv")
    assert settings.safe_summary()["database"]["url"] == "<redacted>"
    assert "secret" not in repr(settings.database)


def test_non_loopback_bind_is_rejected(tmp_path, monkeypatch):
    path = tmp_path / "config.toml"
    path.write_text(CONFIG.replace('host = "127.0.0.1"', 'host = "0.0.0.0"'))
    monkeypatch.setenv("TEST_DATABASE_URL", "postgresql://db/arxiv")
    with pytest.raises(ConfigError, match="127.0.0.1"):
        load_settings(path)


def test_missing_database_environment_is_rejected(tmp_path, monkeypatch):
    path = tmp_path / "config.toml"
    path.write_text(CONFIG)
    monkeypatch.delenv("TEST_DATABASE_URL", raising=False)
    with pytest.raises(ConfigError, match="TEST_DATABASE_URL"):
        load_settings(path)


def test_local_mode_resolves_paths_relative_to_config(tmp_path, monkeypatch):
    config_directory = tmp_path / "config"
    config_directory.mkdir()
    path = config_directory / "config.dev.toml"
    path.write_text(
        CONFIG.replace(
            'mount_root = "/mnt"\n'
            'data_root = "/mnt/arxiv"\n'
            'staging_root = "/var/cache/arxiv-downloader"\n'
            'sentinel_file = "/mnt/arxiv/.mount_sentinel"',
            'mode = "local"\n'
            'mount_root = "../runtime/storage"\n'
            'data_root = "../runtime/storage/arxiv"\n'
            'staging_root = "../runtime/cache"\n'
            'sentinel_file = "../runtime/storage/arxiv/.mount_sentinel"',
        )
    )
    monkeypatch.setenv("TEST_DATABASE_URL", "postgresql://db/arxiv")

    settings = load_settings(path)

    assert settings.storage.mode == "local"
    assert settings.storage.data_root == tmp_path / "runtime/storage/arxiv"
    assert settings.storage.staging_root == tmp_path / "runtime/cache"


def test_local_mode_cannot_disable_protection_for_mnt(tmp_path, monkeypatch):
    path = tmp_path / "config.toml"
    path.write_text(CONFIG.replace("[storage]", '[storage]\nmode = "local"'))
    monkeypatch.setenv("TEST_DATABASE_URL", "postgresql://db/arxiv")

    with pytest.raises(ConfigError, match="cannot use / or /mnt"):
        load_settings(path)
