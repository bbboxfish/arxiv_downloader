"""Add retry scheduling metadata to download tasks."""

import sqlalchemy as sa
from alembic import op

revision = "0002_task_recovery_fields"
down_revision = "0001_a0_schema"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("download_tasks", sa.Column("next_attempt_at", sa.DateTime(timezone=True)))
    op.create_index(
        "ix_download_tasks_next_attempt",
        "download_tasks",
        ["state", "next_attempt_at", "updated_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_download_tasks_next_attempt", table_name="download_tasks")
    op.drop_column("download_tasks", "next_attempt_at")
