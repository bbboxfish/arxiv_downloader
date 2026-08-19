from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from uuid import UUID

from fastapi import Depends, FastAPI, HTTPException, Request
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from arxiv_downloader.config import Settings, load_settings
from arxiv_downloader.daemon.scheduler import Scheduler
from arxiv_downloader.daemon.schemas import (
    ActionResponse,
    BatchCreateRequest,
    BatchCreateResponse,
    BatchProgressResponse,
    HealthResponse,
    VerifyResponse,
)
from arxiv_downloader.daemon.service import BatchNotFound, BatchService, EmptyBatch
from arxiv_downloader.database.session import create_engine_and_sessionmaker
from arxiv_downloader.downloader.client import PDFDownloader
from arxiv_downloader.downloader.rate_limit import AsyncRateLimiter
from arxiv_downloader.errors import MountNotAvailable
from arxiv_downloader.logging import configure_logging, log_event
from arxiv_downloader.metadata.client import ArxivMetadataClient
from arxiv_downloader.storage.capacity import StorageCapacityGuard
from arxiv_downloader.storage.mount import MountGuard
from arxiv_downloader.storage.publisher import StoragePublisher

logger = logging.getLogger("arxivd")


def create_app(settings: Settings | None = None) -> FastAPI:
    resolved_settings = settings or load_settings()
    configure_logging()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        guard = MountGuard(resolved_settings.storage)
        guard.ensure_available()
        engine, sessions = create_engine_and_sessionmaker(resolved_settings.database.url)
        limiter = AsyncRateLimiter(resolved_settings.download.min_request_interval_seconds)
        metadata = ArxivMetadataClient(resolved_settings.download, limiter)
        downloader = PDFDownloader(
            resolved_settings.download,
            resolved_settings.storage.staging_root,
            limiter,
        )
        publisher = StoragePublisher(resolved_settings.storage, guard)
        capacity = StorageCapacityGuard(resolved_settings.storage)
        scheduler = Scheduler(
            sessions,
            downloader,
            publisher,
            metadata,
            concurrency=resolved_settings.download.concurrency,
            max_attempts=resolved_settings.download.max_attempts,
            max_file_size_bytes=resolved_settings.download.max_file_size_bytes,
            staging_root=resolved_settings.storage.staging_root,
            capacity=capacity,
            resume_downloads=resolved_settings.download.resume_downloads,
            retry_base_delay_seconds=resolved_settings.download.retry_base_delay_seconds,
            retry_max_delay_seconds=resolved_settings.download.retry_max_delay_seconds,
        )
        app.state.settings = resolved_settings
        app.state.engine = engine
        app.state.sessions = sessions
        app.state.guard = guard
        app.state.capacity = capacity
        app.state.service = BatchService(metadata, publisher)
        app.state.scheduler = scheduler
        try:
            async with engine.connect() as connection:
                await connection.execute(text("SELECT 1"))
            await scheduler.recover()
            scheduler.start()
            log_event(logger, "daemon_started", config=resolved_settings.safe_summary())
            yield
        finally:
            await scheduler.stop()
            await downloader.close()
            await metadata.close()
            await engine.dispose()
            log_event(logger, "daemon_stopped")

    app = FastAPI(
        title="arxivd internal control API",
        version="0.1.0",
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
        lifespan=lifespan,
    )

    async def get_session(request: Request) -> AsyncIterator[AsyncSession]:
        async with request.app.state.sessions() as session:
            yield session

    def service(request: Request) -> BatchService:
        return request.app.state.service

    @app.get("/health", response_model=HealthResponse)
    async def health(request: Request, session: AsyncSession = Depends(get_session)):
        database_status = "ok"
        storage_status = "ok"
        try:
            await session.execute(text("SELECT 1"))
        except Exception:
            database_status = "unavailable"
        try:
            await asyncio.to_thread(request.app.state.guard.ensure_available)
            if not await asyncio.to_thread(request.app.state.capacity.has_capacity):
                storage_status = "low_capacity"
        except MountNotAvailable:
            storage_status = "unavailable"
        except OSError:
            storage_status = "unavailable"
        status = "ok" if database_status == storage_status == "ok" else "degraded"
        return HealthResponse(status=status, database=database_status, storage=storage_status)

    @app.post("/v1/batches", response_model=BatchCreateResponse, status_code=201)
    async def create_batch(
        payload: BatchCreateRequest,
        batch_service: BatchService = Depends(service),
        session: AsyncSession = Depends(get_session),
    ):
        try:
            return await batch_service.create_batch(session, payload)
        except EmptyBatch as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    @app.get("/v1/batches/{batch_id}", response_model=BatchProgressResponse)
    async def show_batch(
        batch_id: UUID,
        batch_service: BatchService = Depends(service),
        session: AsyncSession = Depends(get_session),
    ):
        try:
            return await batch_service.get_progress(session, batch_id)
        except BatchNotFound as exc:
            raise HTTPException(status_code=404, detail="batch not found") from exc

    @app.post("/v1/batches/{batch_id}/cancel", response_model=ActionResponse)
    async def cancel_batch(
        batch_id: UUID,
        batch_service: BatchService = Depends(service),
        session: AsyncSession = Depends(get_session),
    ):
        try:
            return await batch_service.cancel_batch(session, batch_id)
        except BatchNotFound as exc:
            raise HTTPException(status_code=404, detail="batch not found") from exc

    @app.post("/v1/batches/{batch_id}/retry-failed", response_model=ActionResponse)
    async def retry_failed(
        batch_id: UUID,
        batch_service: BatchService = Depends(service),
        session: AsyncSession = Depends(get_session),
    ):
        try:
            return await batch_service.retry_failed(session, batch_id)
        except BatchNotFound as exc:
            raise HTTPException(status_code=404, detail="batch not found") from exc

    @app.post("/v1/batches/{batch_id}/verify", response_model=VerifyResponse)
    async def verify_batch(
        batch_id: UUID,
        batch_service: BatchService = Depends(service),
        session: AsyncSession = Depends(get_session),
    ):
        try:
            return await batch_service.verify_batch(session, batch_id)
        except BatchNotFound as exc:
            raise HTTPException(status_code=404, detail="batch not found") from exc

    return app
