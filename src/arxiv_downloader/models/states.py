from __future__ import annotations

from enum import StrEnum


class BatchState(StrEnum):
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    CANCELLED = "CANCELLED"


class TaskState(StrEnum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class BatchInputState(StrEnum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class ArtifactStatus(StrEnum):
    COMPLETE = "COMPLETE"
    CORRUPT = "CORRUPT"


ALLOWED_TASK_TRANSITIONS: dict[TaskState, frozenset[TaskState]] = {
    TaskState.PENDING: frozenset({TaskState.RUNNING, TaskState.CANCELLED}),
    TaskState.RUNNING: frozenset(
        {TaskState.PENDING, TaskState.SUCCEEDED, TaskState.FAILED, TaskState.CANCELLED}
    ),
    TaskState.FAILED: frozenset({TaskState.PENDING, TaskState.CANCELLED}),
    TaskState.SUCCEEDED: frozenset(),
    TaskState.CANCELLED: frozenset(),
}


def can_transition(current: TaskState, target: TaskState) -> bool:
    return target in ALLOWED_TASK_TRANSITIONS[current]
