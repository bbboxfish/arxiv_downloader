"""Create the four A0 core tables."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0001_a0_schema"
down_revision = None
branch_labels = None
depends_on = None

BATCH_STATES = ("RUNNING", "COMPLETED", "CANCELLED")
TASK_STATES = ("PENDING", "RUNNING", "SUCCEEDED", "FAILED", "CANCELLED")
ARTIFACT_STATUSES = ("COMPLETE", "CORRUPT")


def _enum(name: str, values: tuple[str, ...], *, postgresql_native: bool):
    if postgresql_native:
        return postgresql.ENUM(*values, name=name, create_type=False)
    return sa.Enum(*values, name=name, native_enum=False, create_constraint=True)


def upgrade() -> None:
    bind = op.get_bind()
    postgresql_native = bind.dialect.name == "postgresql"
    batch_state = _enum("batch_state", BATCH_STATES, postgresql_native=postgresql_native)
    task_state = _enum("task_state", TASK_STATES, postgresql_native=postgresql_native)
    artifact_status = _enum(
        "artifact_status", ARTIFACT_STATUSES, postgresql_native=postgresql_native
    )
    if postgresql_native:
        batch_state.create(bind, checkfirst=True)
        task_state.create(bind, checkfirst=True)
        artifact_status.create(bind, checkfirst=True)

    op.create_table(
        "batches",
        sa.Column("batch_id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.String(length=200), nullable=False),
        sa.Column("state", batch_state, nullable=False),
        sa.Column("total_count", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("batch_id"),
    )
    op.create_table(
        "papers",
        sa.Column("paper_id", sa.Uuid(), nullable=False),
        sa.Column("arxiv_id", sa.String(length=80), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("title", sa.Text(), nullable=False),
        sa.Column("submitted_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("pdf_url", sa.Text(), nullable=False),
        sa.Column("metadata_json", sa.JSON(), nullable=False),
        sa.PrimaryKeyConstraint("paper_id"),
        sa.UniqueConstraint("arxiv_id", "version", name="uq_papers_arxiv_version"),
    )
    op.create_table(
        "artifacts",
        sa.Column("artifact_id", sa.Uuid(), nullable=False),
        sa.Column("paper_id", sa.Uuid(), nullable=False),
        sa.Column("kind", sa.String(length=16), nullable=False),
        sa.Column("object_key", sa.Text(), nullable=False),
        sa.Column("size_bytes", sa.BigInteger(), nullable=False),
        sa.Column("sha256", sa.String(length=64), nullable=False),
        sa.Column("status", artifact_status, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["paper_id"], ["papers.paper_id"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("artifact_id"),
        sa.UniqueConstraint("paper_id", "kind", name="uq_artifacts_paper_kind"),
    )
    op.create_index("ix_artifacts_paper_id", "artifacts", ["paper_id"])
    op.create_table(
        "download_tasks",
        sa.Column("task_id", sa.Uuid(), nullable=False),
        sa.Column("batch_id", sa.Uuid(), nullable=False),
        sa.Column("paper_id", sa.Uuid(), nullable=False),
        sa.Column("state", task_state, nullable=False),
        sa.Column("attempts", sa.Integer(), nullable=False),
        sa.Column("last_error_code", sa.String(length=64), nullable=True),
        sa.Column("last_error_message", sa.Text(), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["batch_id"], ["batches.batch_id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["paper_id"], ["papers.paper_id"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("task_id"),
        sa.UniqueConstraint("batch_id", "paper_id", name="uq_download_tasks_batch_paper"),
    )
    op.create_index("ix_download_tasks_batch_id", "download_tasks", ["batch_id"])
    op.create_index("ix_download_tasks_paper_id", "download_tasks", ["paper_id"])
    op.create_index("ix_download_tasks_state_updated", "download_tasks", ["state", "updated_at"])


def downgrade() -> None:
    bind = op.get_bind()
    op.drop_table("download_tasks")
    op.drop_table("artifacts")
    op.drop_table("papers")
    op.drop_table("batches")
    if bind.dialect.name == "postgresql":
        _enum("artifact_status", ARTIFACT_STATUSES, postgresql_native=True).drop(
            bind, checkfirst=True
        )
        _enum("task_state", TASK_STATES, postgresql_native=True).drop(bind, checkfirst=True)
        _enum("batch_state", BATCH_STATES, postgresql_native=True).drop(bind, checkfirst=True)
