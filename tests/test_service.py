from datetime import datetime, timezone

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from arxiv_downloader.daemon.schemas import BatchCreateRequest
from arxiv_downloader.daemon.service import BatchService
from arxiv_downloader.database.models import Base
from arxiv_downloader.metadata.client import MetadataRecord


class FakeMetadata:
    def __init__(self):
        self.batch_sizes = []

    async def fetch(self, identifier):
        version = identifier.version or 1
        return MetadataRecord(
            arxiv_id=identifier.arxiv_id,
            version=version,
            title=f"Paper {identifier.arxiv_id}",
            submitted_at=datetime(2024, 1, 15, tzinfo=timezone.utc),
            pdf_url=f"https://arxiv.org/pdf/{identifier.arxiv_id}v{version}.pdf",
            raw={"test": True},
        )

    async def fetch_many(self, identifiers):
        self.batch_sizes.append(len(identifiers))
        return [await self.fetch(identifier) for identifier in identifiers]


class UnusedPublisher:
    def artifact_matches(self, *args):
        raise AssertionError("no artifacts should be verified in this test")


def test_batch_request_accepts_more_than_fifty_ids():
    request = BatchCreateRequest(
        name="large", arxiv_ids=[f"2001.{index:05d}v1" for index in range(51)]
    )

    assert len(request.arxiv_ids) == 51


@pytest.mark.asyncio
async def test_create_batch_and_report_pending_progress():
    engine = create_async_engine("sqlite+aiosqlite://")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    service = BatchService(FakeMetadata(), UnusedPublisher())
    async with sessions() as session:
        created = await service.create_batch(
            session,
            BatchCreateRequest(
                name="demo",
                arxiv_ids=["2401.01234v1", "2401.01234v1", "bad", "1706.03762v7"],
            ),
        )
    assert created.papers_accepted == 0
    assert created.queued_count == 2
    assert created.metadata_pending == 2
    assert created.duplicates_skipped == 1
    assert created.invalid_ids == ["bad"]

    async with sessions() as session:
        progress = await service.get_progress(session, created.batch_id)
    assert progress.total == 2
    assert progress.pending == 0
    assert progress.metadata_pending == 2
    assert progress.progress_percent == 0
    await engine.dispose()


@pytest.mark.asyncio
async def test_large_batch_fetches_metadata_in_chunks():
    engine = create_async_engine("sqlite+aiosqlite://")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    metadata = FakeMetadata()
    service = BatchService(metadata, UnusedPublisher())
    identifiers = [f"2001.{index:05d}v1" for index in range(101)]

    sessions = async_sessionmaker(engine, expire_on_commit=False)
    async with sessions() as session:
        created = await service.create_batch(
            session, BatchCreateRequest(name="large", arxiv_ids=identifiers)
        )

    assert created.papers_accepted == 0
    assert created.queued_count == 101
    assert created.metadata_pending == 101
    await engine.dispose()
