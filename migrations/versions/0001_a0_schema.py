"""Create the four A0 core tables."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0001_a0_schema"
down_revision = None
branch_labels = None
depends_on = None

batch_state = postgresql.ENUM(
    "RUNNING", "COMPLETED", "CANCELLED", name="batch_state", create_type=False
)
task_state = postgresql.ENUM(
    "PENDING",
    "RUNNING",
    "SUCCEEDED",
    "FAILED",
    "CANCELLED",
    name="task_state",
    create_type=False,
)
artifact_status = postgresql.ENUM("COMPLETE", "CORRUPT", name="artifact_status", create_type=False)


def upgrade() -> None:
    batch_state.create(op.get_bind(), checkfirst=True)
    task_state.create(op.get_bind(), checkfirst=True)
    artifact_status.create(op.get_bind(), checkfirst=True)

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
    op.drop_table("download_tasks")
    op.drop_table("artifacts")
    op.drop_table("papers")
    op.drop_table("batches")
    artifact_status.drop(op.get_bind(), checkfirst=True)
    task_state.drop(op.get_bind(), checkfirst=True)
    batch_state.drop(op.get_bind(), checkfirst=True)
