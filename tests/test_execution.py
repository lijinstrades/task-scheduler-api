import multiprocessing
import time
from datetime import timedelta

from job_api.execution import execute
from job_api.models import JobSubmission
from job_api.repository import utcnow


def attempt(repository, handler="print", payload=None):
    job = repository.submit(
        JobSubmission(
            handler=handler,
            payload=payload or {"message": "hello"},
            timeout_seconds=10,
        )
    )
    return repository.claim(job["_id"], 1, "worker")


def test_real_child_process_prints_and_returns_result(repository):
    doc = attempt(repository)
    outcome = execute(doc, True, lambda: None)
    assert outcome.error is None
    assert outcome.result == {"printed": True, "message": "hello"}


def test_child_transient_and_permanent_failure(repository):
    transient = execute(attempt(repository, "test-transient"), True, lambda: None)
    assert transient.error.code == "TRANSIENT_FAILURE"
    assert not transient.permanent
    permanent = execute(attempt(repository, "test-permanent"), True, lambda: None)
    assert permanent.error.code == "PERMANENT_FAILURE"
    assert permanent.permanent


def test_hard_timeout_terminates_child(repository):
    doc = attempt(repository, "test-slow", {"message": "hello", "sleep_seconds": 10})
    doc["attempt_deadline"] = utcnow() + timedelta(seconds=1)
    before = {p.pid for p in multiprocessing.active_children()}
    started = time.monotonic()
    outcome = execute(doc, True, lambda: None)
    assert outcome.error.code == "TIMEOUT"
    assert time.monotonic() - started < 4
    assert {p.pid for p in multiprocessing.active_children()} == before


def test_heartbeat_failure_cleans_up_handler(repository):
    doc = attempt(repository, "test-slow", {"message": "hello", "sleep_seconds": 10})

    def fail():
        raise RuntimeError("heartbeat failed")

    import pytest

    before = {p.pid for p in multiprocessing.active_children()}
    with pytest.raises(RuntimeError, match="heartbeat failed"):
        execute(doc, True, fail)
    assert {p.pid for p in multiprocessing.active_children()} == before
