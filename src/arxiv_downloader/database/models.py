"""SQLAlchemy models for the four A0 tables."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import (
    JSON,
    BigInteger,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    Uuid,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

from arxiv_downloader.models.states import ArtifactStatus, BatchState, TaskState


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def enum_values(enum_class: type) -> list[str]:
    return [member.value for member in enum_class]


class Base(DeclarativeBase):
    pass


class Batch(Base):
    __tablename__ = "batches"

    batch_id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    state: Mapped[BatchState] = mapped_column(
        Enum(BatchState, name="batch_state", values_callable=enum_values),
        nullable=False,
        default=BatchState.RUNNING,
    )
    total_count: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now
    )
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    tasks: Mapped[list[DownloadTask]] = relationship(back_populates="batch")


class Paper(Base):
    __tablename__ = "papers"
    __table_args__ = (UniqueConstraint("arxiv_id", "version", name="uq_papers_arxiv_version"),)

    paper_id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    arxiv_id: Mapped[str] = mapped_column(String(80), nullable=False)
    version: Mapped[int] = mapped_column(Integer, nullable=False)
    title: Mapped[str] = mapped_column(Text, nullable=False)
    submitted_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    pdf_url: Mapped[str] = mapped_column(Text, nullable=False)
    metadata_json: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)

    tasks: Mapped[list[DownloadTask]] = relationship(back_populates="paper")
    artifacts: Mapped[list[Artifact]] = relationship(back_populates="paper")


class DownloadTask(Base):
    __tablename__ = "download_tasks"
    __table_args__ = (
        UniqueConstraint("batch_id", "paper_id", name="uq_download_tasks_batch_paper"),
        Index("ix_download_tasks_state_updated", "state", "updated_at"),
    )

    task_id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    batch_id: Mapped[uuid.UUID] = mapped_column(
        Uuid, ForeignKey("batches.batch_id", ondelete="CASCADE"), nullable=False, index=True
    )
    paper_id: Mapped[uuid.UUID] = mapped_column(
        Uuid, ForeignKey("papers.paper_id", ondelete="RESTRICT"), nullable=False, index=True
    )
    state: Mapped[TaskState] = mapped_column(
        Enum(TaskState, name="task_state", values_callable=enum_values),
        nullable=False,
        default=TaskState.PENDING,
    )
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_error_code: Mapped[str | None] = mapped_column(String(64))
    last_error_message: Mapped[str | None] = mapped_column(Text)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    next_attempt_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now, onupdate=utc_now
    )

    batch: Mapped[Batch] = relationship(back_populates="tasks")
    paper: Mapped[Paper] = relationship(back_populates="tasks")


class Artifact(Base):
    __tablename__ = "artifacts"
    __table_args__ = (UniqueConstraint("paper_id", "kind", name="uq_artifacts_paper_kind"),)

    artifact_id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    paper_id: Mapped[uuid.UUID] = mapped_column(
        Uuid, ForeignKey("papers.paper_id", ondelete="RESTRICT"), nullable=False, index=True
    )
    kind: Mapped[str] = mapped_column(String(16), nullable=False, default="pdf")
    object_key: Mapped[str] = mapped_column(Text, nullable=False)
    size_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)
    sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[ArtifactStatus] = mapped_column(
        Enum(ArtifactStatus, name="artifact_status", values_callable=enum_values),
        nullable=False,
        default=ArtifactStatus.COMPLETE,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now
    )

    paper: Mapped[Paper] = relationship(back_populates="artifacts")
