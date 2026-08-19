from __future__ import annotations

from collections import Counter
from uuid import UUID

from sqlalchemy import func, insert, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from arxiv_downloader.daemon.schemas import (
    ActionResponse,
    BatchCreateRequest,
    BatchCreateResponse,
    BatchProgressResponse,
    TaskErrorItem,
    VerifyIssue,
    VerifyResponse,
)
from arxiv_downloader.database.models import (
    Artifact,
    Batch,
    BatchInput,
    DownloadTask,
    Paper,
    utc_now,
)
from arxiv_downloader.errors import ArxivDownloaderError
from arxiv_downloader.ids import normalize_unique
from arxiv_downloader.metadata.client import ArxivMetadataClient
from arxiv_downloader.models.states import ArtifactStatus, BatchInputState, BatchState, TaskState
from arxiv_downloader.storage.publisher import StoragePublisher

_EMPTY_ERROR_MESSAGE = "no diagnostic message recorded; inspect arxivd --debug logs"


def _elapsed_ms(started_at, finished_at) -> int:
    if started_at.tzinfo is None and finished_at.tzinfo is not None:
        started_at = started_at.replace(tzinfo=finished_at.tzinfo)
    elif finished_at.tzinfo is None and started_at.tzinfo is not None:
        finished_at = finished_at.replace(tzinfo=started_at.tzinfo)
    return max(0, int((finished_at - started_at).total_seconds() * 1000))


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
        if not normalized:
            raise EmptyBatch("no valid arXiv IDs were submitted; batch was not created")
        async with session.begin():
            batch = Batch(
                name=request.name.strip(),
                state=BatchState.RUNNING,
                total_count=len(normalized),
            )
            session.add(batch)
            await session.flush()
            await session.execute(
                insert(BatchInput),
                [
                    {
                        "batch_id": batch.batch_id,
                        "arxiv_id": identifier.arxiv_id,
                        "version": identifier.version,
                        "state": BatchInputState.PENDING,
                        "attempts": 0,
                        "updated_at": utc_now(),
                    }
                    for identifier in normalized
                ],
            )

        return BatchCreateResponse(
            batch_id=batch.batch_id,
            papers_accepted=0,
            queued_count=len(normalized),
            metadata_pending=len(normalized),
            duplicates_skipped=duplicates,
            invalid_ids=invalid,
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
        input_rows = (
            await session.execute(
                select(BatchInput.state, func.count(BatchInput.input_id))
                .where(BatchInput.batch_id == batch_id)
                .group_by(BatchInput.state)
            )
        ).all()
        input_counts: Counter[BatchInputState] = Counter(
            {state: count for state, count in input_rows}
        )
        metadata_rate_limit_wait_ms = (
            await session.scalar(
                select(func.sum(BatchInput.rate_limit_wait_ms)).where(
                    BatchInput.batch_id == batch_id
                )
            )
            or 0
        )
        download_rate_limit_wait_ms = (
            await session.scalar(
                select(func.sum(DownloadTask.rate_limit_wait_ms)).where(
                    DownloadTask.batch_id == batch_id
                )
            )
            or 0
        )
        metadata_running_rows = (
            await session.execute(
                select(BatchInput.arxiv_id, BatchInput.version)
                .where(
                    BatchInput.batch_id == batch_id,
                    BatchInput.state == BatchInputState.RUNNING,
                )
                .order_by(BatchInput.updated_at, BatchInput.input_id)
            )
        ).all()
        metadata_running_ids = [
            f"{arxiv_id}v{version}" if version else arxiv_id
            for arxiv_id, version in metadata_running_rows
        ]
        metadata_failed_rows = (
            await session.execute(
                select(BatchInput.arxiv_id, BatchInput.version)
                .where(
                    BatchInput.batch_id == batch_id,
                    BatchInput.state == BatchInputState.FAILED,
                )
                .order_by(BatchInput.arxiv_id, BatchInput.version)
            )
        ).all()
        metadata_failed_ids = [
            f"{arxiv_id}v{version}" if version else arxiv_id
            for arxiv_id, version in metadata_failed_rows
        ]
        downloading_rows = (
            await session.execute(
                select(Paper.arxiv_id, Paper.version)
                .join(DownloadTask, DownloadTask.paper_id == Paper.paper_id)
                .where(
                    DownloadTask.batch_id == batch_id,
                    DownloadTask.state == TaskState.RUNNING,
                )
                .order_by(DownloadTask.updated_at, DownloadTask.task_id)
            )
        ).all()
        downloading_ids = [f"{arxiv_id}v{version}" for arxiv_id, version in downloading_rows]
        terminal = (
            input_counts[BatchInputState.FAILED]
            + input_counts[BatchInputState.CANCELLED]
            + counts[TaskState.SUCCEEDED]
            + counts[TaskState.FAILED]
            + counts[TaskState.CANCELLED]
        )
        error_rows = (
            await session.execute(
                select(
                    Paper.arxiv_id,
                    Paper.version,
                    DownloadTask.last_error_code,
                    DownloadTask.last_error_message,
                    DownloadTask.started_at,
                    DownloadTask.finished_at,
                )
                .join(DownloadTask, DownloadTask.paper_id == Paper.paper_id)
                .where(
                    DownloadTask.batch_id == batch_id,
                    DownloadTask.last_error_code.is_not(None),
                )
                .order_by(Paper.arxiv_id)
            )
        ).all()
        input_error_rows = (
            await session.execute(
                select(
                    BatchInput.arxiv_id,
                    BatchInput.version,
                    BatchInput.last_error_code,
                    BatchInput.last_error_message,
                    BatchInput.started_at,
                    BatchInput.finished_at,
                )
                .where(
                    BatchInput.batch_id == batch_id,
                    BatchInput.last_error_code.is_not(None),
                )
                .order_by(BatchInput.arxiv_id)
            )
        ).all()
        return BatchProgressResponse(
            batch_id=batch.batch_id,
            name=batch.name,
            state=batch.state,
            total=batch.total_count,
            metadata_pending=input_counts[BatchInputState.PENDING],
            metadata_running=input_counts[BatchInputState.RUNNING],
            metadata_succeeded=input_counts[BatchInputState.SUCCEEDED],
            metadata_failed=input_counts[BatchInputState.FAILED],
            metadata_running_ids=metadata_running_ids,
            metadata_failed_ids=metadata_failed_ids,
            pending=counts[TaskState.PENDING],
            running=counts[TaskState.RUNNING],
            downloading_ids=downloading_ids,
            succeeded=counts[TaskState.SUCCEEDED],
            failed=counts[TaskState.FAILED],
            cancelled=counts[TaskState.CANCELLED],
            progress_percent=int(terminal * 100 / batch.total_count) if batch.total_count else 100,
            created_at=batch.created_at,
            started_at=batch.started_at,
            completed_at=batch.completed_at,
            duration_ms=(
                _elapsed_ms(batch.started_at, batch.completed_at)
                if batch.started_at is not None and batch.completed_at is not None
                else None
            ),
            queue_duration_ms=(
                _elapsed_ms(batch.created_at, batch.started_at)
                if batch.started_at is not None
                else None
            ),
            metadata_worker_count=batch.metadata_worker_count,
            download_worker_count=batch.download_worker_count,
            metadata_rate_limit_wait_ms=metadata_rate_limit_wait_ms,
            download_rate_limit_wait_ms=download_rate_limit_wait_ms,
            rate_limit_wait_ms=(
                metadata_rate_limit_wait_ms + download_rate_limit_wait_ms
            ),
            errors=[
                TaskErrorItem(
                    arxiv_id=f"{arxiv_id}{f'v{version}' if version else ''}",
                    code=code,
                    message=message or _EMPTY_ERROR_MESSAGE,
                    duration_ms=(
                        _elapsed_ms(started_at, finished_at)
                        if started_at is not None and finished_at is not None
                        else None
                    ),
                )
                for arxiv_id, version, code, message, started_at, finished_at in [
                    *input_error_rows,
                    *error_rows,
                ]
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
            input_result = await session.execute(
                update(BatchInput)
                .where(
                    BatchInput.batch_id == batch_id,
                    BatchInput.state.in_([BatchInputState.PENDING, BatchInputState.RUNNING]),
                )
                .values(state=BatchInputState.CANCELLED, finished_at=now, updated_at=now)
            )
            batch.state = BatchState.CANCELLED
            batch.completed_at = now
        return ActionResponse(
            batch_id=batch_id,
            affected_tasks=(result.rowcount or 0) + (input_result.rowcount or 0),
            state=BatchState.CANCELLED,
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
                    next_attempt_at=None,
                    last_error_code=None,
                    last_error_message=None,
                    started_at=None,
                    finished_at=None,
                    updated_at=now,
                )
            )
            input_result = await session.execute(
                update(BatchInput)
                .where(
                    BatchInput.batch_id == batch_id,
                    BatchInput.state == BatchInputState.FAILED,
                )
                .values(
                    state=BatchInputState.PENDING,
                    attempts=0,
                    last_error_code=None,
                    last_error_message=None,
                    started_at=None,
                    finished_at=None,
                    updated_at=now,
                )
            )
            if result.rowcount or input_result.rowcount:
                batch.state = BatchState.RUNNING
                batch.completed_at = None
        return ActionResponse(
            batch_id=batch_id,
            affected_tasks=(result.rowcount or 0) + (input_result.rowcount or 0),
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
