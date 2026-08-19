"""Persist batch inputs for asynchronous metadata ingestion."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0003_async_batch_inputs"
down_revision = "0002_task_recovery_fields"
branch_labels = None
depends_on = None

INPUT_STATES = ("PENDING", "RUNNING", "SUCCEEDED", "FAILED", "CANCELLED")


def upgrade() -> None:
    bind = op.get_bind()
    postgresql_native = bind.dialect.name == "postgresql"
    if postgresql_native:
        input_state = postgresql.ENUM(
            *INPUT_STATES, name="batch_input_state", create_type=False
        )
        input_state.create(bind, checkfirst=True)
    else:
        input_state = sa.Enum(
            *INPUT_STATES,
            name="batch_input_state",
            native_enum=False,
            create_constraint=True,
        )
    op.create_table(
        "batch_inputs",
        sa.Column("input_id", sa.Uuid(), nullable=False),
        sa.Column("batch_id", sa.Uuid(), nullable=False),
        sa.Column("arxiv_id", sa.String(length=80), nullable=False),
        sa.Column("version", sa.Integer(), nullable=True),
        sa.Column("state", input_state, nullable=False),
        sa.Column("attempts", sa.Integer(), nullable=False),
        sa.Column("last_error_code", sa.String(length=64), nullable=True),
        sa.Column("last_error_message", sa.Text(), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["batch_id"], ["batches.batch_id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("input_id"),
        sa.UniqueConstraint("batch_id", "arxiv_id", "version", name="uq_batch_inputs_identifier"),
    )
    op.create_index("ix_batch_inputs_batch_id", "batch_inputs", ["batch_id"])
    op.create_index(
        "ix_batch_inputs_state_updated", "batch_inputs", ["state", "updated_at"]
    )


def downgrade() -> None:
    bind = op.get_bind()
    op.drop_table("batch_inputs")
    if bind.dialect.name == "postgresql":
        postgresql.ENUM(*INPUT_STATES, name="batch_input_state", create_type=False).drop(
            bind, checkfirst=True
        )
