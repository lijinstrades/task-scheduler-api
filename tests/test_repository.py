from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta

import pytest

from job_api.models import JobError, JobSubmission
from job_api.repository import (
    IdempotencyConflictError,
    QueueDrainingError,
    retry_delay,
    utcnow,
)

ERROR = JobError(code="TRANSIENT", message="try again")


def make_due(repository, job_id):
    past = utcnow() - timedelta(seconds=1)
    repository.jobs.update_one(
        {"_id": job_id},
        {"$set": {"next_attempt_at": past, "pending_dispatch.publish_after": past}},
    )


def test_submission_is_one_document_with_dispatch(repository, submission):
    job = repository.submit(submission)
    assert job["_id"] == repository.get(job["_id"])["_id"]
    assert job["state"] == "queued"
    assert job["attempt_count"] == 0
    assert job["pending_dispatch"]["generation"] == job["generation"]
    assert repository.status(job).result is None


def test_idempotency_returns_same_job_and_detects_conflict(repository, submission):
    first = repository.submit(submission, "same-key")
    second = repository.submit(submission, "same-key")
    assert first["_id"] == second["_id"]
    assert repository.jobs.count_documents({}) == 1
    with pytest.raises(IdempotencyConflictError):
        repository.submit(JobSubmission(payload={"message": "different"}), "same-key")


def test_idempotency_normalizes_explicit_defaults(repository, submission):
    first = repository.submit(submission, "normalized")
    explicit = JobSubmission(
        payload={"message": "hello"},
        handler="print",
        priority=5,
        max_retries=3,
        timeout_seconds=10,
    )
    assert repository.submit(explicit, "normalized")["_id"] == first["_id"]


def test_concurrent_idempotency_inserts_once(repository, submission):
    with ThreadPoolExecutor(max_workers=8) as executor:
        ids = list(executor.map(lambda _: repository.submit(submission, "key")["_id"], range(16)))
    assert len(set(ids)) == 1
    assert repository.jobs.count_documents({}) == 1


def test_concurrent_claim_has_one_owner(repository, submission):
    job = repository.submit(submission)
    with ThreadPoolExecutor(max_workers=8) as executor:
        claims = list(
            executor.map(lambda n: repository.claim(job["_id"], 1, f"worker-{n}"), range(16))
        )
    assert sum(claim is not None for claim in claims) == 1
    assert repository.get(job["_id"])["attempt_count"] == 1


def test_success_is_terminal_and_preserves_result(repository, submission):
    job = repository.submit(submission)
    attempt = repository.claim(job["_id"], 1, "worker")
    assert repository.succeed(attempt, {"printed": True})
    saved = repository.get(job["_id"])
    assert saved["state"] == "succeeded"
    assert saved["result"] == {"printed": True}
    assert saved["finished_at"] is not None
    assert saved["attempts"][0]["outcome"] == "succeeded"
    assert repository.claim(job["_id"], 1, "other") is None
    assert not repository.succeed(attempt, {"wrong": True})


@pytest.mark.parametrize("max_retries", [0, 1, 3])
def test_exact_attempt_budget_and_dead_letters(repository, max_retries):
    job = repository.submit(JobSubmission(payload={"message": "hello"}, max_retries=max_retries))
    for number in range(1, max_retries + 2):
        current = repository.get(job["_id"])
        attempt = repository.claim(job["_id"], current["generation"], "worker")
        assert attempt["attempt_count"] == number
        assert repository.fail(attempt, ERROR)
        if number <= max_retries:
            saved = repository.get(job["_id"])
            assert saved["state"] == "failed"
            assert repository.claim(job["_id"], saved["generation"], "worker") is None
            make_due(repository, job["_id"])
    saved = repository.get(job["_id"])
    assert saved["state"] == "dead-lettered"
    assert saved["attempt_count"] == max_retries + 1
    assert saved["dead_letter"]["error"] == ERROR.model_dump()
    assert saved["pending_dispatch"]["destination"] == "dead"
    assert len(saved["attempts"]) == max_retries + 1


def test_permanent_failure_does_not_retry(repository, submission):
    job = repository.submit(submission)
    attempt = repository.claim(job["_id"], 1, "worker")
    repository.fail(attempt, ERROR, permanent=True)
    assert repository.get(job["_id"])["state"] == "dead-lettered"
    assert repository.get(job["_id"])["attempt_count"] == 1


def test_expiry_recovery_rejects_old_owner(repository, submission):
    job = repository.submit(submission)
    attempt = repository.claim(job["_id"], 1, "crashed-worker")
    repository.jobs.update_one(
        {"_id": job["_id"]}, {"$set": {"lease_expires_at": utcnow() - timedelta(seconds=1)}}
    )
    assert repository.recover_expired() == 1
    assert repository.recover_expired() == 0
    assert not repository.succeed(attempt, {"stale": True})
    recovered = repository.get(job["_id"])
    assert recovered["last_error"]["code"] == "LEASE_EXPIRED"
    make_due(repository, job["_id"])
    newer = repository.claim(job["_id"], recovered["generation"], "new-worker")
    assert repository.succeed(newer, {"printed": True})
    assert repository.get(job["_id"])["last_error"]["code"] == "LEASE_EXPIRED"


def test_cancel_claim_race_has_only_one_winner(repository, submission):
    for _ in range(20):
        job = repository.submit(submission)
        with ThreadPoolExecutor(max_workers=2) as executor:
            claim = executor.submit(repository.claim, job["_id"], 1, "worker")
            cancel = executor.submit(repository.cancel, job["_id"])
            assert (claim.result() is not None) != (cancel.result() is not None)
        assert repository.get(job["_id"])["state"] in {"running", "cancelled"}


def test_dispatch_clear_cannot_remove_new_retry(repository, submission):
    job = repository.submit(submission)
    original_dispatch = repository.claim_dispatch()
    attempt = repository.claim(job["_id"], 1, "worker")
    repository.fail(attempt, ERROR)
    assert not repository.dispatch_sent(original_dispatch)
    saved = repository.get(job["_id"])
    assert saved["pending_dispatch"]["generation"] == 2


def test_dispatch_lease_recovers_after_publisher_crash(repository, submission):
    job = repository.submit(submission)
    first = repository.claim_dispatch()
    assert repository.claim_dispatch() is None
    repository.jobs.update_one(
        {"_id": job["_id"]},
        {"$set": {"pending_dispatch.lock_until": utcnow() - timedelta(seconds=1)}},
    )
    second = repository.claim_dispatch()
    assert second["_id"] == first["_id"]
    assert second["pending_dispatch"]["lock_token"] != first["pending_dispatch"]["lock_token"]
    assert not repository.dispatch_sent(first)
    assert repository.dispatch_sent(second)


def test_drain_is_persisted_and_waits_for_workers(repository, submission):
    job = repository.submit(submission)
    repository.worker_seen("worker", 1, False)
    requested = repository.set_mode("draining")
    assert not requested.drain_complete
    assert repository.claim(job["_id"], 1, "worker") is None
    assert repository.claim_dispatch() is None
    with pytest.raises(QueueDrainingError):
        repository.submit(submission)
    repository.worker_seen("worker", requested.generation, True)
    assert repository.control_status().drain_complete
    assert repository.set_mode("draining").generation == requested.generation
    resumed = repository.set_mode("active")
    assert not resumed.drain_complete
    assert resumed.generation > requested.generation
    assert repository.claim(job["_id"], 1, "worker") is not None


def test_drain_waits_for_running_job_and_still_dispatches_dead_letters(repository, submission):
    job = repository.submit(submission)
    attempt = repository.claim(job["_id"], 1, "worker")
    repository.set_mode("draining")
    assert not repository.control_status().drain_complete
    repository.fail(attempt, ERROR, permanent=True)
    assert repository.claim_dispatch()["pending_dispatch"]["destination"] == "dead"


def test_depth_distinguishes_pending_delayed_dead(repository, submission):
    first = repository.submit(submission)
    repository.submit(submission)
    attempt = repository.claim(first["_id"], 1, "worker")
    repository.fail(attempt, ERROR)
    counts = repository.depth()
    assert counts["database_pending_jobs"] == 2
    assert counts["database_delayed_jobs"] == 1
    assert counts["database_dead_letter_jobs"] == 0


def test_backoff_bounds_and_cap(settings):
    for attempt in range(1, 12):
        cap = min(settings.backoff_cap_seconds, settings.backoff_base_seconds * 2 ** (attempt - 1))
        assert all(0 <= retry_delay(attempt, settings) <= cap for _ in range(30))
