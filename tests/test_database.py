import os
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest
from sqlalchemy import text
from typer.testing import CliRunner

from arxiv_downloader.cli.app import app
from arxiv_downloader.database.session import async_database_url, create_engine_and_sessionmaker


def test_async_database_url_adds_supported_drivers():
    assert async_database_url("sqlite:///arxiv.db") == "sqlite+aiosqlite:///arxiv.db"
    assert async_database_url("postgresql://db/arxiv") == "postgresql+asyncpg://db/arxiv"
    assert async_database_url("postgres://db/arxiv") == "postgresql+asyncpg://db/arxiv"


@pytest.mark.asyncio
async def test_sqlite_engine_enables_safety_pragmas(tmp_path):
    database_path = tmp_path / "runtime" / "arxiv.db"
    engine, _ = create_engine_and_sessionmaker(f"sqlite:///{database_path}")
    async with engine.connect() as connection:
        foreign_keys = await connection.scalar(text("PRAGMA foreign_keys"))
        journal_mode = await connection.scalar(text("PRAGMA journal_mode"))
        busy_timeout = await connection.scalar(text("PRAGMA busy_timeout"))
    await engine.dispose()

    assert database_path.is_file()
    assert foreign_keys == 1
    assert journal_mode == "wal"
    assert busy_timeout == 30_000


def test_alembic_upgrade_creates_sqlite_schema(tmp_path):
    database_path = tmp_path / "migration.db"
    environment = os.environ.copy()
    environment["ARXIV_DATABASE_URL"] = f"sqlite+aiosqlite:///{database_path}"
    project_root = Path(__file__).resolve().parents[1]

    result = subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        cwd=project_root,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    with sqlite3.connect(database_path) as connection:
        tables = {
            row[0]
            for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
        }
        foreign_keys = connection.execute("PRAGMA foreign_key_list(download_tasks)").fetchall()
    assert {"batches", "batch_inputs", "papers", "download_tasks", "artifacts"} <= tables
    assert len(foreign_keys) == 2


def test_database_export_backs_up_sqlite(tmp_path, monkeypatch):
    database_path = tmp_path / "arxiv.db"
    backup_path = tmp_path / "exports" / "arxiv.sqlite3"
    config_path = tmp_path / "config.toml"
    config_path.write_text(
        f"""
[database]
url = "sqlite+aiosqlite:///{database_path}"
[storage]
mode = "local"
mount_root = "{tmp_path}/storage"
data_root = "{tmp_path}/storage/arxiv"
staging_root = "{tmp_path}/cache"
sentinel_file = "{tmp_path}/storage/arxiv/.mount_sentinel"
"""
    )
    with sqlite3.connect(database_path) as connection:
        connection.execute("CREATE TABLE sample (value TEXT NOT NULL)")
        connection.execute("INSERT INTO sample VALUES ('persisted')")
    monkeypatch.setenv("ARXIV_DOWNLOADER_CONFIG", str(config_path))
    monkeypatch.delenv("ARXIV_DATABASE_URL", raising=False)

    result = CliRunner().invoke(app, ["database", "export", "--output", str(backup_path)])

    assert result.exit_code == 0, result.output
    with sqlite3.connect(backup_path) as connection:
        assert connection.execute("SELECT value FROM sample").fetchone() == ("persisted",)
