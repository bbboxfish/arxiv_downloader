from __future__ import annotations

import asyncio
import logging
import time
from collections import defaultdict
from pathlib import Path
from uuid import UUID

from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from arxiv_downloader.database.models import Artifact, Batch, DownloadTask, Paper, utc_now
from arxiv_downloader.downloader.client import PDFDownloader
from arxiv_downloader.errors import ArxivDownloaderError
from arxiv_downloader.ids import ArxivId
from arxiv_downloader.logging import log_event
from arxiv_downloader.models.states import ArtifactStatus, BatchState, TaskState
from arxiv_downloader.storage.publisher import PublishedArtifact, StoragePublisher

logger = logging.getLogger("arxivd.scheduler")


class Scheduler:
    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        downloader: PDFDownloader,
        publisher: StoragePublisher,
        *,
        concurrency: int,
        max_attempts: int,
        max_file_size_bytes: int,
        staging_root: Path,
        poll_interval_seconds: float = 0.5,
    ) -> None:
        self._sessions = sessions
        self._downloader = downloader
        self._publisher = publisher
        self._concurrency = concurrency
        self._max_attempts = max_attempts
        self._max_file_size_bytes = max_file_size_bytes
        self._staging_root = staging_root
        self._poll_interval = poll_interval_seconds
        self._stop = asyncio.Event()
        self._workers: list[asyncio.Task[None]] = []
        self._claim_lock = asyncio.Lock()
        self._paper_locks: defaultdict[UUID, asyncio.Lock] = defaultdict(asyncio.Lock)

    async def recover(self) -> int:
        self._staging_root.mkdir(parents=True, exist_ok=True)
        for part_path in self._staging_root.rglob("*.part"):
            try:
                part_path.unlink()
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
                    updated_at=now,
                    last_error_code="DAEMON_RESTARTED",
                    last_error_message="task was requeued during daemon startup recovery",
                )
            )
        recovered = result.rowcount or 0
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

    async def stop(self) -> None:
        self._stop.set()
        for worker in self._workers:
            worker.cancel()
        if self._workers:
            await asyncio.gather(*self._workers, return_exceptions=True)
        self._workers.clear()

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
                await self._record_failure(task_id, exc.code, str(exc), worker_id, started)
            except Exception as exc:  # keep a daemon worker alive after an unexpected task error
                logger.exception("unexpected worker error")
                await self._record_failure(task_id, "INTERNAL_ERROR", str(exc), worker_id, started)
            finally:
                await self._finalize_batches()

    async def _claim_task(self) -> UUID | None:
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
                    )
                    .order_by(DownloadTask.updated_at, DownloadTask.task_id)
                    .limit(1)
                    .with_for_update(skip_locked=True)
                )
                if task is None:
                    return None
                task.state = TaskState.RUNNING
                task.attempts += 1
                task.started_at = utc_now()
                task.finished_at = None
                task.updated_at = utc_now()
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
                await self._record_artifact_and_success(task_id, paper_id, published)
                log_event(
                    logger,
                    "task_finished",
                    batch_id=batch_id,
                    task_id=task_id,
                    arxiv_id=identifier.full_id,
                    state=TaskState.SUCCEEDED,
                    attempt=attempt,
                    worker_id=worker_id,
                    duration_ms=int((time.monotonic() - started) * 1000),
                    size_bytes=published.size_bytes,
                )
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
        self, task_id: UUID, paper_id: UUID, published: PublishedArtifact
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
                task.finished_at = utc_now()
                task.updated_at = utc_now()

    async def _record_failure(
        self,
        task_id: UUID,
        code: str,
        message: str,
        worker_id: int,
        started: float,
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
            task.updated_at = utc_now()
            if task.attempts < self._max_attempts:
                task.state = TaskState.PENDING
                task.started_at = None
                next_state = TaskState.PENDING
            else:
                task.state = TaskState.FAILED
                task.finished_at = utc_now()
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
                if active_count == 0:
                    batch = await session.get(Batch, batch_id, with_for_update=True)
                    if batch is not None and batch.state == BatchState.RUNNING:
                        batch.state = BatchState.COMPLETED
                        batch.completed_at = utc_now()
                        log_event(
                            logger,
                            "batch_finished",
                            batch_id=batch_id,
                            state=BatchState.COMPLETED,
                        )
