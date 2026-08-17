from arxiv_downloader.models.states import TaskState, can_transition


def test_a0_state_transitions():
    assert can_transition(TaskState.PENDING, TaskState.RUNNING)
    assert can_transition(TaskState.RUNNING, TaskState.SUCCEEDED)
    assert can_transition(TaskState.RUNNING, TaskState.PENDING)
    assert can_transition(TaskState.FAILED, TaskState.PENDING)
    assert not can_transition(TaskState.SUCCEEDED, TaskState.PENDING)
