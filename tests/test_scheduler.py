from datetime import datetime, timezone
from pathlib import Path
from time import monotonic
from uuid import uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from arxiv_downloader.daemon.scheduler import Scheduler
from arxiv_downloader.database.models import Base, Batch, BatchInput, DownloadTask, Paper
from arxiv_downloader.metadata.client import MetadataRecord
from arxiv_downloader.models.states import BatchInputState, BatchState, TaskState


class UnusedDownloader:
    pass


class UnusedPublisher:
    pass


class FakeMetadata:
    async def fetch(self, identifier):
        return MetadataRecord(
            arxiv_id=identifier.arxiv_id,
            version=identifier.version or 1,
            title="Imported",
            submitted_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
            pdf_url=f"https://arxiv.org/pdf/{identifier.full_id}.pdf",
            raw={"id": identifier.full_id},
        )


async def scheduler_fixture(tmp_path: Path):
    engine = create_async_engine("sqlite+aiosqlite://")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    scheduler = Scheduler(
        sessions,
        UnusedDownloader(),
        UnusedPublisher(),
        concurrency=1,
        max_attempts=3,
        max_file_size_bytes=1024,
        staging_root=tmp_path / "staging",
        retry_base_delay_seconds=10,
        retry_max_delay_seconds=60,
    )
    return engine, sessions, scheduler


async def insert_running_task(sessions, task_id):
    async with sessions.begin() as session:
        batch = Batch(
            batch_id=uuid4(),
            name="recovery",
            state=BatchState.RUNNING,
            total_count=1,
        )
        paper = Paper(
            paper_id=uuid4(),
            arxiv_id="2401.01234",
            version=1,
            title="Recovery",
            submitted_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
            pdf_url="https://arxiv.org/pdf/2401.01234v1.pdf",
            metadata_json={},
        )
        task = DownloadTask(
            task_id=task_id,
            batch=batch,
            paper=paper,
            state=TaskState.RUNNING,
            attempts=1,
        )
        session.add_all([batch, paper, task])


@pytest.mark.asyncio
async def test_recover_preserves_part_and_requeues_running_task(tmp_path):
    engine, sessions, scheduler = await scheduler_fixture(tmp_path)
    task_id = uuid4()
    await insert_running_task(sessions, task_id)
    part = tmp_path / "staging" / str(task_id) / "paper.part"
    part.parent.mkdir(parents=True)
    part.write_bytes(b"partial")
    part.with_name("paper.part.meta.json").write_text("{}")

    recovered = await scheduler.recover()

    assert recovered == 1
    assert part.exists()
    async with sessions() as session:
        task = await session.get(DownloadTask, task_id)
        assert task is not None
        assert task.state == TaskState.PENDING
        assert task.next_attempt_at is None
    await engine.dispose()


@pytest.mark.asyncio
async def test_failure_sets_exponential_retry_time(tmp_path):
    engine, sessions, scheduler = await scheduler_fixture(tmp_path)
    task_id = uuid4()
    await insert_running_task(sessions, task_id)

    await scheduler._record_failure(task_id, "HTTP_5XX", "temporary", 1, monotonic())

    async with sessions() as session:
        task = await session.scalar(select(DownloadTask).where(DownloadTask.task_id == task_id))
        assert task is not None
        assert task.state == TaskState.PENDING
        assert task.next_attempt_at is not None
        retry_at = task.next_attempt_at.replace(tzinfo=timezone.utc)
        delay = (retry_at - datetime.now(timezone.utc)).total_seconds()
        assert 9 <= delay <= 11
    await engine.dispose()


@pytest.mark.asyncio
async def test_metadata_input_is_imported_in_background_before_download_task(tmp_path):
    engine, sessions, scheduler = await scheduler_fixture(tmp_path)
    async with sessions.begin() as session:
        batch = Batch(name="async", state=BatchState.RUNNING, total_count=1)
        session.add(batch)
        await session.flush()
        session.add(
            BatchInput(
                batch_id=batch.batch_id,
                arxiv_id="2401.01234",
                version=1,
                state=BatchInputState.RUNNING,
                attempts=1,
            )
        )
        batch_id = batch.batch_id
    scheduler._metadata = FakeMetadata()
    input_id = await scheduler._claim_input()
    assert input_id is None

    async with sessions() as session:
        input_row = await session.scalar(
            select(BatchInput).where(BatchInput.batch_id == batch_id)
        )
        assert input_row is not None
        input_row.state = BatchInputState.RUNNING
        await session.commit()
        input_id = input_row.input_id

    await scheduler._process_input(input_id, monotonic())

    async with sessions() as session:
        input_row = await session.get(BatchInput, input_id)
        task = await session.scalar(
            select(DownloadTask).where(DownloadTask.batch_id == batch_id)
        )
        assert input_row is not None
        assert input_row.state == BatchInputState.SUCCEEDED
        assert task is not None
        assert task.state == TaskState.PENDING
    await engine.dispose()
