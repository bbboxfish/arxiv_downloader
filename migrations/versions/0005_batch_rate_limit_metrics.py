"""Persist per-batch rate-limit wait metrics and worker counts."""

import sqlalchemy as sa
from alembic import op

revision = "0005_batch_rate_limit_metrics"
down_revision = "0004_batch_execution_timing"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("batches", sa.Column("metadata_worker_count", sa.Integer(), nullable=True))
    op.add_column("batches", sa.Column("download_worker_count", sa.Integer(), nullable=True))
    op.add_column(
        "batch_inputs",
        sa.Column("rate_limit_wait_ms", sa.Integer(), server_default="0", nullable=False),
    )
    op.add_column(
        "download_tasks",
        sa.Column("rate_limit_wait_ms", sa.Integer(), server_default="0", nullable=False),
    )


def downgrade() -> None:
    op.drop_column("download_tasks", "rate_limit_wait_ms")
    op.drop_column("batch_inputs", "rate_limit_wait_ms")
    op.drop_column("batches", "download_worker_count")
    op.drop_column("batches", "metadata_worker_count")
