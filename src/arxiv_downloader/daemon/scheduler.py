from __future__ import annotations

import asyncio
import logging
import time
from collections import defaultdict
from datetime import timedelta
from pathlib import Path
from uuid import UUID

from sqlalchemy import func, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from arxiv_downloader.database.models import (
    Artifact,
    Batch,
    BatchInput,
    DownloadTask,
    Paper,
    utc_now,
)
from arxiv_downloader.downloader.client import PDFDownloader
from arxiv_downloader.errors import ArxivDownloaderError, MetadataError
from arxiv_downloader.ids import ArxivId
from arxiv_downloader.logging import log_event
from arxiv_downloader.metadata.client import ArxivMetadataClient
from arxiv_downloader.models.states import ArtifactStatus, BatchInputState, BatchState, TaskState
from arxiv_downloader.storage.capacity import StorageCapacityGuard
from arxiv_downloader.storage.publisher import PublishedArtifact, StoragePublisher

logger = logging.getLogger("arxivd.scheduler")


class Scheduler:
    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        downloader: PDFDownloader,
        publisher: StoragePublisher,
        metadata: ArxivMetadataClient | None = None,
        *,
        concurrency: int,
        max_attempts: int,
        max_file_size_bytes: int,
        staging_root: Path,
        capacity: StorageCapacityGuard | None = None,
        resume_downloads: bool = True,
        retry_base_delay_seconds: float = 5,
        retry_max_delay_seconds: float = 300,
        poll_interval_seconds: float = 0.5,
    ) -> None:
        self._sessions = sessions
        self._downloader = downloader
        self._publisher = publisher
        self._metadata = metadata
        self._concurrency = concurrency
        self._max_attempts = max_attempts
        self._max_file_size_bytes = max_file_size_bytes
        self._staging_root = staging_root
        self._capacity = capacity
        self._resume_downloads = resume_downloads
        self._retry_base_delay_seconds = retry_base_delay_seconds
        self._retry_max_delay_seconds = retry_max_delay_seconds
        self._poll_interval = poll_interval_seconds
        self._stop = asyncio.Event()
        self._workers: list[asyncio.Task[None]] = []
        self._import_worker: asyncio.Task[None] | None = None
        self._claim_lock = asyncio.Lock()
        self._paper_locks: defaultdict[UUID, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._last_capacity_warning = 0.0

    async def recover(self) -> int:
        self._staging_root.mkdir(parents=True, exist_ok=True)
        if not self._resume_downloads:
            for part_path in self._staging_root.rglob("*.part"):
                try:
                    part_path.unlink()
                    part_path.with_name(f"{part_path.name}.meta.json").unlink(missing_ok=True)
                except OSError:
                    log_event(logger, "staging_cleanup_failed", path=str(part_path))
        async with self._sessions.begin() as session:
            now = utc_now()
            result = await session.execute(
                update(DownloadTask)
                .where(DownloadTask.state == TaskState.RUNNING)
                .values(
                    state=TaskState.PENDING,
                    started_at=None,
                    next_attempt_at=None,
                    updated_at=now,
                    last_error_code="DAEMON_RESTARTED",
                    last_error_message="task was requeued during daemon startup recovery",
                )
            )
            input_result = await session.execute(
                update(BatchInput)
                .where(BatchInput.state == BatchInputState.RUNNING)
                .values(
                    state=BatchInputState.PENDING,
                    started_at=None,
                    updated_at=now,
                    last_error_code="DAEMON_RESTARTED",
                    last_error_message="metadata input was requeued during daemon startup recovery",
                )
            )
        recovered = (result.rowcount or 0) + (input_result.rowcount or 0)
        log_event(logger, "startup_recovery", recovered_tasks=recovered)
        return recovered

    def start(self) -> None:
        if self._workers:
            return
        self._stop.clear()
        self._workers = [
            asyncio.create_task(self._worker(worker_id), name=f"arxiv-worker-{worker_id}")
            for worker_id in range(1, self._concurrency + 1)
        ]
        self._import_worker = asyncio.create_task(
            self._metadata_worker(), name="arxiv-metadata-worker"
        )

    async def stop(self) -> None:
        self._stop.set()
        for worker in self._workers:
            worker.cancel()
        if self._import_worker is not None:
            self._import_worker.cancel()
        if self._workers:
            await asyncio.gather(*self._workers, return_exceptions=True)
        self._workers.clear()
        if self._import_worker is not None:
            await asyncio.gather(self._import_worker, return_exceptions=True)
            self._import_worker = None

    async def _metadata_worker(self) -> None:
        if self._metadata is None:
            return
        while not self._stop.is_set():
            input_id = await self._claim_input()
            if input_id is None:
                await self._finalize_batches()
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=self._poll_interval)
                except TimeoutError:
                    pass
                continue
            started = time.monotonic()
            try:
                await self._process_input(input_id, started)
            except asyncio.CancelledError:
                raise
            except MetadataError as exc:
                await self._record_input_failure(
                    input_id,
                    exc.code,
                    str(exc),
                    started,
                    exc.rate_limit_wait_ms,
                )
            except Exception as exc:
                logger.exception("unexpected metadata import error")
                await self._record_input_failure(input_id, "INTERNAL_ERROR", str(exc), started)
            finally:
                await self._finalize_batches()

    async def _claim_input(self) -> UUID | None:
        async with self._claim_lock:
            async with self._sessions.begin() as session:
                item = await session.scalar(
                    select(BatchInput)
                    .join(Batch, Batch.batch_id == BatchInput.batch_id)
                    .where(
                        BatchInput.state == BatchInputState.PENDING,
                        Batch.state == BatchState.RUNNING,
                    )
                    .order_by(BatchInput.updated_at, BatchInput.input_id)
                    .limit(1)
                    .with_for_update(skip_locked=True)
                )
                if item is None:
                    return None
                now = utc_now()
                batch = await session.get(Batch, item.batch_id, with_for_update=True)
                if batch is not None:
                    if batch.started_at is None:
                        batch.started_at = now
                    if batch.metadata_worker_count is None:
                        batch.metadata_worker_count = 1
                    if batch.download_worker_count is None:
                        batch.download_worker_count = self._concurrency
                item.state = BatchInputState.RUNNING
                item.attempts += 1
                item.started_at = now
                item.finished_at = None
                item.updated_at = now
                item.last_error_code = None
                item.last_error_message = None
                return item.input_id

    async def _process_input(self, input_id: UUID, started: float) -> None:
        async with self._sessions() as session:
            item = await session.get(BatchInput, input_id)
            if item is None or item.state != BatchInputState.RUNNING:
                return
            identifier = ArxivId(item.arxiv_id, item.version)
            batch_id = item.batch_id
        record = await self._metadata.fetch(identifier)
        async with self._sessions.begin() as session:
            item = await session.get(BatchInput, input_id, with_for_update=True)
            if item is None or item.state != BatchInputState.RUNNING:
                return
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
            existing = await session.scalar(
                select(DownloadTask).where(
                    DownloadTask.batch_id == batch_id,
                    DownloadTask.paper_id == paper.paper_id,
                )
            )
            if existing is None:
                session.add(
                    DownloadTask(
                        batch_id=batch_id,
                        paper_id=paper.paper_id,
                        state=TaskState.PENDING,
                    )
                )
            item.state = BatchInputState.SUCCEEDED
            item.rate_limit_wait_ms += record.rate_limit_wait_ms
            item.finished_at = utc_now()
            item.updated_at = utc_now()
        log_event(
            logger,
            "metadata_imported",
            level=logging.DEBUG,
            batch_id=batch_id,
            input_id=input_id,
            arxiv_id=record.normalized.full_id,
            duration_ms=int((time.monotonic() - started) * 1000),
        )

    async def _record_input_failure(
        self,
        input_id: UUID,
        code: str,
        message: str,
        started: float,
        rate_limit_wait_ms: int = 0,
    ) -> None:
        async with self._sessions.begin() as session:
            item = await session.get(BatchInput, input_id, with_for_update=True)
            if item is None or item.state != BatchInputState.RUNNING:
                return
            item.last_error_code = code
            item.last_error_message = message[:2000]
            item.rate_limit_wait_ms += rate_limit_wait_ms
            item.updated_at = utc_now()
            retryable = code not in {"HTTP_404", "METADATA_NOT_FOUND"}
            if item.attempts < self._max_attempts and retryable:
                item.state = BatchInputState.PENDING
                item.started_at = None
                item.finished_at = None
                next_state = BatchInputState.PENDING
            else:
                item.state = BatchInputState.FAILED
                item.finished_at = utc_now()
                next_state = BatchInputState.FAILED
            log_event(
                logger,
                "metadata_import_failed",
                batch_id=item.batch_id,
                input_id=input_id,
                arxiv_id=f"{item.arxiv_id}v{item.version}" if item.version else item.arxiv_id,
                state=next_state,
                attempt=item.attempts,
                duration_ms=int((time.monotonic() - started) * 1000),
                error_code=code,
            )

    async def _worker(self, worker_id: int) -> None:
        while not self._stop.is_set():
            task_id = await self._claim_task()
            if task_id is None:
                await self._finalize_batches()
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=self._poll_interval)
                except TimeoutError:
                    pass
                continue
            started = time.monotonic()
            try:
                await self._process_task(task_id, worker_id, started)
            except asyncio.CancelledError:
                raise
            except ArxivDownloaderError as exc:
                await self._record_failure(
                    task_id,
                    exc.code,
                    str(exc),
                    worker_id,
                    started,
                    exc.rate_limit_wait_ms,
                )
            except Exception as exc:  # keep a daemon worker alive after an unexpected task error
                logger.exception("unexpected worker error")
                await self._record_failure(task_id, "INTERNAL_ERROR", str(exc), worker_id, started)
            finally:
                await self._finalize_batches()

    async def _claim_task(self) -> UUID | None:
        if self._capacity is not None:
            try:
                if not self._capacity.has_capacity():
                    now = time.monotonic()
                    if now - self._last_capacity_warning >= 60:
                        log_event(logger, "storage_low_watermark")
                        self._last_capacity_warning = now
                    return None
            except OSError as exc:
                now = time.monotonic()
                if now - self._last_capacity_warning >= 60:
                    log_event(logger, "storage_capacity_check_failed", error=str(exc))
                    self._last_capacity_warning = now
                return None
        # A0 has one daemon process. Serializing the short claim transaction makes
        # SQLite's lack of SELECT FOR UPDATE safe while retaining PostgreSQL support.
        async with self._claim_lock:
            async with self._sessions.begin() as session:
                task = await session.scalar(
                    select(DownloadTask)
                    .join(Batch, Batch.batch_id == DownloadTask.batch_id)
                    .where(
                        DownloadTask.state == TaskState.PENDING,
                        Batch.state == BatchState.RUNNING,
                        or_(
                            DownloadTask.next_attempt_at.is_(None),
                            DownloadTask.next_attempt_at <= utc_now(),
                        ),
                    )
                    .order_by(DownloadTask.updated_at, DownloadTask.task_id)
                    .limit(1)
                    .with_for_update(skip_locked=True)
                )
                if task is None:
                    return None
                now = utc_now()
                batch = await session.get(Batch, task.batch_id, with_for_update=True)
                if batch is not None:
                    if batch.started_at is None:
                        batch.started_at = now
                    if batch.metadata_worker_count is None:
                        batch.metadata_worker_count = 1
                    if batch.download_worker_count is None:
                        batch.download_worker_count = self._concurrency
                task.state = TaskState.RUNNING
                task.attempts += 1
                task.started_at = now
                task.finished_at = None
                task.updated_at = now
                task.next_attempt_at = None
                task.last_error_code = None
                task.last_error_message = None
                return task.task_id

    async def _process_task(self, task_id: UUID, worker_id: int, started: float) -> None:
        async with self._sessions() as session:
            row = (
                await session.execute(
                    select(DownloadTask, Paper)
                    .join(Paper, Paper.paper_id == DownloadTask.paper_id)
                    .where(DownloadTask.task_id == task_id)
                )
            ).one()
            initial_task, initial_paper = row
            paper_id = initial_paper.paper_id
            batch_id = initial_task.batch_id

        async with self._paper_locks[paper_id]:
            async with self._sessions() as session:
                row = (
                    await session.execute(
                        select(DownloadTask, Paper)
                        .join(Paper, Paper.paper_id == DownloadTask.paper_id)
                        .where(DownloadTask.task_id == task_id)
                    )
                ).one()
                task, paper = row
                if task.state != TaskState.RUNNING:
                    return
                artifact = await session.scalar(
                    select(Artifact).where(
                        Artifact.paper_id == paper.paper_id, Artifact.kind == "pdf"
                    )
                )
                if artifact is not None and artifact.status == ArtifactStatus.COMPLETE:
                    matches = await asyncio.to_thread(
                        self._publisher.artifact_matches,
                        artifact.object_key,
                        artifact.size_bytes,
                        artifact.sha256,
                    )
                    if matches:
                        await self._mark_succeeded(task_id)
                        log_event(
                            logger,
                            "task_reused_artifact",
                            level=logging.DEBUG,
                            batch_id=batch_id,
                            task_id=task_id,
                            arxiv_id=f"{paper.arxiv_id}v{paper.version}",
                            state=TaskState.SUCCEEDED,
                            attempt=task.attempts,
                            worker_id=worker_id,
                            duration_ms=int((time.monotonic() - started) * 1000),
                            size_bytes=artifact.size_bytes,
                        )
                        return
                    artifact.status = ArtifactStatus.CORRUPT
                    await session.commit()

                identifier = ArxivId(paper.arxiv_id, paper.version)
                attempt = task.attempts
                pdf_url = paper.pdf_url
                submitted_at = paper.submitted_at

                if artifact is None:
                    discovered = await asyncio.to_thread(
                        self._publisher.discover_existing,
                        identifier,
                        submitted_at,
                        self._max_file_size_bytes,
                    )
                    if discovered is not None:
                        await self._record_artifact_and_success(task_id, paper_id, discovered)
                        log_event(
                            logger,
                            "task_reconciled_file",
                            level=logging.DEBUG,
                            batch_id=batch_id,
                            task_id=task_id,
                            arxiv_id=identifier.full_id,
                            state=TaskState.SUCCEEDED,
                            attempt=attempt,
                            worker_id=worker_id,
                            duration_ms=int((time.monotonic() - started) * 1000),
                            size_bytes=discovered.size_bytes,
                        )
                        return

            downloaded = None
            try:
                downloaded = await self._downloader.download(task_id, paper_id, pdf_url)
                if not await self._is_running(task_id):
                    return
                published = await asyncio.to_thread(
                    self._publisher.publish,
                    task_id,
                    identifier,
                    submitted_at,
                    downloaded,
                )
                await self._record_artifact_and_success(
                    task_id,
                    paper_id,
                    published,
                    downloaded.rate_limit_wait_ms,
                )
                log_event(
                    logger,
                    "task_finished",
                    level=logging.DEBUG,
                    batch_id=batch_id,
                    task_id=task_id,
                    arxiv_id=identifier.full_id,
                    state=TaskState.SUCCEEDED,
                    attempt=attempt,
                    worker_id=worker_id,
                    duration_ms=int((time.monotonic() - started) * 1000),
                    size_bytes=published.size_bytes,
                )
                _log_download_summary(published)
            finally:
                if downloaded is not None:
                    try:
                        downloaded.path.unlink(missing_ok=True)
                    except OSError:
                        pass
                    try:
                        downloaded.path.parent.rmdir()
                    except OSError:
                        pass
    async def _is_running(self, task_id: UUID) -> bool:
        async with self._sessions() as session:
            state = await session.scalar(
                select(DownloadTask.state).where(DownloadTask.task_id == task_id)
            )
            return state == TaskState.RUNNING

    async def _mark_succeeded(self, task_id: UUID) -> None:
        async with self._sessions.begin() as session:
            task = await session.get(DownloadTask, task_id, with_for_update=True)
            if task is not None and task.state == TaskState.RUNNING:
                task.state = TaskState.SUCCEEDED
                task.finished_at = utc_now()
                task.updated_at = utc_now()

    async def _record_artifact_and_success(
        self,
        task_id: UUID,
        paper_id: UUID,
        published: PublishedArtifact,
        rate_limit_wait_ms: int = 0,
    ) -> None:
        async with self._sessions.begin() as session:
            artifact = await session.scalar(
                select(Artifact)
                .where(Artifact.paper_id == paper_id, Artifact.kind == "pdf")
                .with_for_update()
            )
            if artifact is None:
                artifact = Artifact(paper_id=paper_id, kind="pdf")
                session.add(artifact)
            artifact.object_key = published.object_key
            artifact.size_bytes = published.size_bytes
            artifact.sha256 = published.sha256
            artifact.status = ArtifactStatus.COMPLETE
            artifact.created_at = utc_now()

            task = await session.get(DownloadTask, task_id, with_for_update=True)
            if task is not None and task.state == TaskState.RUNNING:
                task.state = TaskState.SUCCEEDED
                task.rate_limit_wait_ms += rate_limit_wait_ms
                task.finished_at = utc_now()
                task.updated_at = utc_now()

    async def _record_failure(
        self,
        task_id: UUID,
        code: str,
        message: str,
        worker_id: int,
        started: float,
        rate_limit_wait_ms: int = 0,
    ) -> None:
        async with self._sessions.begin() as session:
            row = (
                await session.execute(
                    select(DownloadTask, Paper)
                    .join(Paper, Paper.paper_id == DownloadTask.paper_id)
                    .where(DownloadTask.task_id == task_id)
                    .with_for_update()
                )
            ).one_or_none()
            if row is None:
                return
            task, paper = row
            if task.state != TaskState.RUNNING:
                return
            task.last_error_code = code
            task.last_error_message = message[:2000]
            task.rate_limit_wait_ms += rate_limit_wait_ms
            task.updated_at = utc_now()
            retryable = code not in {"HTTP_404", "FILE_TOO_LARGE", "NOT_A_PDF"}
            if task.attempts < self._max_attempts and retryable:
                delay = min(
                    self._retry_base_delay_seconds * (2 ** max(task.attempts - 1, 0)),
                    self._retry_max_delay_seconds,
                )
                task.state = TaskState.PENDING
                task.started_at = None
                task.finished_at = None
                task.next_attempt_at = utc_now() + timedelta(seconds=delay)
                next_state = TaskState.PENDING
            else:
                task.state = TaskState.FAILED
                task.finished_at = utc_now()
                task.next_attempt_at = None
                next_state = TaskState.FAILED
            log_event(
                logger,
                "task_failed",
                batch_id=task.batch_id,
                task_id=task.task_id,
                arxiv_id=f"{paper.arxiv_id}v{paper.version}",
                state=next_state,
                attempt=task.attempts,
                worker_id=worker_id,
                duration_ms=int((time.monotonic() - started) * 1000),
                error_code=code,
            )

    async def _finalize_batches(self) -> None:
        async with self._sessions.begin() as session:
            batch_ids = list(
                await session.scalars(
                    select(Batch.batch_id).where(Batch.state == BatchState.RUNNING)
                )
            )
            for batch_id in batch_ids:
                active_count = await session.scalar(
                    select(func.count(DownloadTask.task_id)).where(
                        DownloadTask.batch_id == batch_id,
                        DownloadTask.state.in_([TaskState.PENDING, TaskState.RUNNING]),
                    )
                )
                input_active_count = await session.scalar(
                    select(func.count(BatchInput.input_id)).where(
                        BatchInput.batch_id == batch_id,
                        BatchInput.state.in_(
                            [BatchInputState.PENDING, BatchInputState.RUNNING]
                        ),
                    )
                )
                if active_count == 0 and input_active_count == 0:
                    batch = await session.get(Batch, batch_id, with_for_update=True)
                    if batch is not None and batch.state == BatchState.RUNNING:
                        completed_at = utc_now()
                        task_counts = {
                            state: await session.scalar(
                                select(func.count(DownloadTask.task_id)).where(
                                    DownloadTask.batch_id == batch_id,
                                    DownloadTask.state == state,
                                )
                            )
                            for state in TaskState
                        }
                        metadata_failed = await session.scalar(
                            select(func.count(BatchInput.input_id)).where(
                                BatchInput.batch_id == batch_id,
                                BatchInput.state == BatchInputState.FAILED,
                            )
                        )
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
                        batch.state = BatchState.COMPLETED
                        batch.completed_at = completed_at
                        log_event(
                            logger,
                            "batch_finished",
                            batch_id=batch_id,
                            state=BatchState.COMPLETED,
                            queue_duration_ms=_elapsed_ms(
                                batch.created_at, batch.started_at or completed_at
                            ),
                            duration_ms=_elapsed_ms(
                                batch.started_at or completed_at, completed_at
                            ),
                            total=batch.total_count,
                            succeeded=task_counts.get(TaskState.SUCCEEDED, 0) or 0,
                            failed=task_counts.get(TaskState.FAILED, 0) or 0,
                            cancelled=task_counts.get(TaskState.CANCELLED, 0) or 0,
                            metadata_failed=metadata_failed or 0,
                            metadata_failed_ids=metadata_failed_ids,
                            metadata_worker_count=batch.metadata_worker_count,
                            download_worker_count=batch.download_worker_count,
                            metadata_rate_limit_wait_ms=metadata_rate_limit_wait_ms,
                            download_rate_limit_wait_ms=download_rate_limit_wait_ms,
                            rate_limit_wait_ms=(
                                metadata_rate_limit_wait_ms
                                + download_rate_limit_wait_ms
                            ),
                        )


def _log_download_summary(published: PublishedArtifact) -> None:
    filename = Path(published.object_key).name
    log_event(
        logger,
        "file_saved",
        filename=filename,
        size_bytes=published.size_bytes,
        size=_human_size(published.size_bytes),
        object_key=published.object_key,
    )


def _human_size(size_bytes: int) -> str:
    size = float(size_bytes)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if size < 1024 or unit == "TiB":
            if unit == "B":
                return f"{int(size)} {unit}"
            return f"{size:.1f} {unit}"
        size /= 1024
    raise AssertionError("unreachable")


def _elapsed_ms(started_at, finished_at) -> int:
    # SQLite may return timezone-naive datetimes even though the model uses
    # DateTime(timezone=True); normalize both sides for subtraction.
    if started_at.tzinfo is None and finished_at.tzinfo is not None:
        started_at = started_at.replace(tzinfo=finished_at.tzinfo)
    elif finished_at.tzinfo is None and started_at.tzinfo is not None:
        finished_at = finished_at.replace(tzinfo=started_at.tzinfo)
    return max(0, int((finished_at - started_at).total_seconds() * 1000))
