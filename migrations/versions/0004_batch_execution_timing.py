"""Track batch queue and execution start times."""

import sqlalchemy as sa
from alembic import op

revision = "0004_batch_execution_timing"
down_revision = "0003_async_batch_inputs"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("batches", sa.Column("started_at", sa.DateTime(timezone=True), nullable=True))


def downgrade() -> None:
    op.drop_column("batches", "started_at")
