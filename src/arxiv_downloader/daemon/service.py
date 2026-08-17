from __future__ import annotations

from collections import Counter
from uuid import UUID

from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.ext.asyncio import AsyncSession

from arxiv_downloader.daemon.schemas import (
    ActionResponse,
    BatchCreateRequest,
    BatchCreateResponse,
    BatchProgressResponse,
    ImportErrorItem,
    TaskErrorItem,
    VerifyIssue,
    VerifyResponse,
)
from arxiv_downloader.database.models import Artifact, Batch, DownloadTask, Paper, utc_now
from arxiv_downloader.errors import ArxivDownloaderError, MetadataError
from arxiv_downloader.ids import normalize_unique
from arxiv_downloader.metadata.client import ArxivMetadataClient, MetadataRecord
from arxiv_downloader.models.states import ArtifactStatus, BatchState, TaskState
from arxiv_downloader.storage.publisher import StoragePublisher


class BatchNotFound(LookupError):
    pass


class EmptyBatch(ValueError):
    pass


class BatchService:
    def __init__(self, metadata_client: ArxivMetadataClient, publisher: StoragePublisher) -> None:
        self._metadata = metadata_client
        self._publisher = publisher

    async def create_batch(
        self, session: AsyncSession, request: BatchCreateRequest
    ) -> BatchCreateResponse:
        normalized, duplicates, invalid = normalize_unique(request.arxiv_ids)
        metadata_records: list[MetadataRecord] = []
        metadata_errors: list[ImportErrorItem] = []
        resolved_seen: set[tuple[str, int]] = set()
        for identifier in normalized:
            try:
                record = await self._metadata.fetch(identifier)
            except MetadataError as exc:
                metadata_errors.append(
                    ImportErrorItem(
                        arxiv_id=identifier.full_id,
                        code=exc.code,
                        message=str(exc),
                    )
                )
                continue
            resolved_key = (record.arxiv_id, record.version)
            if resolved_key in resolved_seen:
                duplicates += 1
                continue
            resolved_seen.add(resolved_key)
            metadata_records.append(record)

        if not metadata_records:
            raise EmptyBatch("no valid paper metadata was available; batch was not created")

        async with session.begin():
            batch = Batch(
                name=request.name.strip(),
                state=BatchState.RUNNING,
                total_count=len(metadata_records),
            )
            session.add(batch)
            await session.flush()
            for record in metadata_records:
                if session.bind is not None and session.bind.dialect.name == "postgresql":
                    paper_id = await session.scalar(
                        postgresql_insert(Paper)
                        .values(
                            arxiv_id=record.arxiv_id,
                            version=record.version,
                            title=record.title,
                            submitted_at=record.submitted_at,
                            pdf_url=record.pdf_url,
                            metadata_json=record.raw,
                        )
                        .on_conflict_do_update(
                            constraint="uq_papers_arxiv_version",
                            set_={
                                "title": record.title,
                                "submitted_at": record.submitted_at,
                                "pdf_url": record.pdf_url,
                                "metadata_json": record.raw,
                            },
                        )
                        .returning(Paper.paper_id)
                    )
                else:
                    paper = await session.scalar(
                        select(Paper).where(
                            Paper.arxiv_id == record.arxiv_id,
                            Paper.version == record.version,
                        )
                    )
                    if paper is None:
                        paper = Paper(
                            arxiv_id=record.arxiv_id,
                            version=record.version,
                            title=record.title,
                            submitted_at=record.submitted_at,
                            pdf_url=record.pdf_url,
                            metadata_json=record.raw,
                        )
                        session.add(paper)
                        await session.flush()
                    else:
                        paper.title = record.title
                        paper.submitted_at = record.submitted_at
                        paper.pdf_url = record.pdf_url
                        paper.metadata_json = record.raw
                    paper_id = paper.paper_id
                session.add(
                    DownloadTask(
                        batch_id=batch.batch_id,
                        paper_id=paper_id,
                        state=TaskState.PENDING,
                    )
                )

        return BatchCreateResponse(
            batch_id=batch.batch_id,
            papers_accepted=len(metadata_records),
            duplicates_skipped=duplicates,
            invalid_ids=invalid,
            metadata_errors=metadata_errors,
        )

    async def get_progress(self, session: AsyncSession, batch_id: UUID) -> BatchProgressResponse:
        batch = await session.get(Batch, batch_id)
        if batch is None:
            raise BatchNotFound(str(batch_id))
        rows = (
            await session.execute(
                select(DownloadTask.state, func.count(DownloadTask.task_id))
                .where(DownloadTask.batch_id == batch_id)
                .group_by(DownloadTask.state)
            )
        ).all()
        counts: Counter[TaskState] = Counter({state: count for state, count in rows})
        terminal = (
            counts[TaskState.SUCCEEDED] + counts[TaskState.FAILED] + counts[TaskState.CANCELLED]
        )
        error_rows = (
            await session.execute(
                select(
                    Paper.arxiv_id,
                    Paper.version,
                    DownloadTask.last_error_code,
                    DownloadTask.last_error_message,
                )
                .join(DownloadTask, DownloadTask.paper_id == Paper.paper_id)
                .where(
                    DownloadTask.batch_id == batch_id,
                    DownloadTask.last_error_code.is_not(None),
                )
                .order_by(Paper.arxiv_id)
            )
        ).all()
        return BatchProgressResponse(
            batch_id=batch.batch_id,
            name=batch.name,
            state=batch.state,
            total=batch.total_count,
            pending=counts[TaskState.PENDING],
            running=counts[TaskState.RUNNING],
            succeeded=counts[TaskState.SUCCEEDED],
            failed=counts[TaskState.FAILED],
            cancelled=counts[TaskState.CANCELLED],
            progress_percent=int(terminal * 100 / batch.total_count) if batch.total_count else 100,
            created_at=batch.created_at,
            completed_at=batch.completed_at,
            errors=[
                TaskErrorItem(
                    arxiv_id=f"{arxiv_id}v{version}",
                    code=code,
                    message=message or "",
                )
                for arxiv_id, version, code, message in error_rows
            ],
        )

    async def cancel_batch(self, session: AsyncSession, batch_id: UUID) -> ActionResponse:
        async with session.begin():
            batch = await session.scalar(
                select(Batch).where(Batch.batch_id == batch_id).with_for_update()
            )
            if batch is None:
                raise BatchNotFound(str(batch_id))
            if batch.state == BatchState.COMPLETED:
                return ActionResponse(batch_id=batch_id, affected_tasks=0, state=batch.state)
            now = utc_now()
            result = await session.execute(
                update(DownloadTask)
                .where(
                    DownloadTask.batch_id == batch_id,
                    DownloadTask.state.in_([TaskState.PENDING, TaskState.RUNNING]),
                )
                .values(state=TaskState.CANCELLED, finished_at=now, updated_at=now)
            )
            batch.state = BatchState.CANCELLED
            batch.completed_at = now
        return ActionResponse(
            batch_id=batch_id, affected_tasks=result.rowcount or 0, state=BatchState.CANCELLED
        )

    async def retry_failed(self, session: AsyncSession, batch_id: UUID) -> ActionResponse:
        async with session.begin():
            batch = await session.scalar(
                select(Batch).where(Batch.batch_id == batch_id).with_for_update()
            )
            if batch is None:
                raise BatchNotFound(str(batch_id))
            now = utc_now()
            result = await session.execute(
                update(DownloadTask)
                .where(
                    DownloadTask.batch_id == batch_id,
                    DownloadTask.state == TaskState.FAILED,
                )
                .values(
                    state=TaskState.PENDING,
                    attempts=0,
                    last_error_code=None,
                    last_error_message=None,
                    started_at=None,
                    finished_at=None,
                    updated_at=now,
                )
            )
            if result.rowcount:
                batch.state = BatchState.RUNNING
                batch.completed_at = None
        return ActionResponse(
            batch_id=batch_id,
            affected_tasks=result.rowcount or 0,
            state=batch.state,
        )

    async def verify_batch(self, session: AsyncSession, batch_id: UUID) -> VerifyResponse:
        batch = await session.get(Batch, batch_id)
        if batch is None:
            raise BatchNotFound(str(batch_id))
        rows = (
            await session.execute(
                select(Paper, DownloadTask, Artifact)
                .join(DownloadTask, DownloadTask.paper_id == Paper.paper_id)
                .outerjoin(
                    Artifact,
                    (Artifact.paper_id == Paper.paper_id) & (Artifact.kind == "pdf"),
                )
                .where(DownloadTask.batch_id == batch_id)
                .order_by(Paper.arxiv_id)
            )
        ).all()
        checked = 0
        valid = 0
        issues: list[VerifyIssue] = []
        for paper, task, artifact in rows:
            full_id = f"{paper.arxiv_id}v{paper.version}"
            if artifact is None or artifact.status != ArtifactStatus.COMPLETE:
                issues.append(
                    VerifyIssue(
                        arxiv_id=full_id,
                        code="ARTIFACT_MISSING",
                        message=f"task state is {task.state.value}; no complete artifact is recorded",
                    )
                )
                continue
            checked += 1
            try:
                matches = self._publisher.artifact_matches(
                    artifact.object_key, artifact.size_bytes, artifact.sha256
                )
            except ArxivDownloaderError as exc:
                matches = False
                issue_code = exc.code
                issue_message = str(exc)
            else:
                issue_code = "CHECKSUM_FAILED"
                issue_message = "file is missing or does not match size/SHA-256"
            if matches:
                valid += 1
            else:
                artifact.status = ArtifactStatus.CORRUPT
                issues.append(
                    VerifyIssue(
                        arxiv_id=full_id,
                        code=issue_code,
                        message=issue_message,
                    )
                )
        await session.commit()
        return VerifyResponse(
            batch_id=batch_id,
            expected=batch.total_count,
            checked=checked,
            valid=valid,
            missing_or_corrupt=batch.total_count - valid,
            issues=issues,
        )
