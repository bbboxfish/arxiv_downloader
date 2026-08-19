from __future__ import annotations

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, Field

from arxiv_downloader.models.states import BatchState


class HealthResponse(BaseModel):
    status: str
    database: str
    storage: str


class BatchCreateRequest(BaseModel):
    name: str = Field(min_length=1, max_length=200)
    arxiv_ids: list[str] = Field(min_length=1)


class ImportErrorItem(BaseModel):
    arxiv_id: str
    code: str
    message: str


class BatchCreateResponse(BaseModel):
    batch_id: UUID
    papers_accepted: int = 0
    queued_count: int = 0
    metadata_pending: int = 0
    duplicates_skipped: int = 0
    invalid_ids: list[str] = Field(default_factory=list)
    metadata_errors: list[ImportErrorItem] = Field(default_factory=list)


class TaskErrorItem(BaseModel):
    arxiv_id: str
    code: str
    message: str


class BatchProgressResponse(BaseModel):
    batch_id: UUID
    name: str
    state: BatchState
    total: int
    metadata_pending: int
    metadata_running: int
    metadata_succeeded: int
    metadata_failed: int
    pending: int
    running: int
    succeeded: int
    failed: int
    cancelled: int
    progress_percent: int
    created_at: datetime
    completed_at: datetime | None
    errors: list[TaskErrorItem]


class ActionResponse(BaseModel):
    batch_id: UUID
    affected_tasks: int
    state: BatchState


class VerifyIssue(BaseModel):
    arxiv_id: str
    code: str
    message: str


class VerifyResponse(BaseModel):
    batch_id: UUID
    expected: int
    checked: int
    valid: int
    missing_or_corrupt: int
    issues: list[VerifyIssue]
