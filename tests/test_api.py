from uuid import uuid4

import httpx
import pytest
from pymongo.errors import ServerSelectionTimeoutError


def test_submit_immediately_and_read_queued_job(client):
    response = client.post("/jobs", json={"payload": {"message": "hello"}})
    assert response.status_code == 202
    job_id = response.json()["job_id"]
    assert response.headers["Location"] == f"/jobs/{job_id}"
    status = client.get(f"/jobs/{job_id}").json()
    assert status["state"] == "queued"
    assert status["attempt_count"] == 0
    assert "last_error" in status and status["last_error"] is None
    assert "result" in status and status["result"] is None


@pytest.mark.parametrize(
    "changes",
    [
        {"priority": -1},
        {"priority": 10},
        {"priority": True},
        {"max_retries": -1},
        {"max_retries": 11},
        {"max_retries": "3"},
        {"timeout_seconds": 0},
        {"timeout_seconds": True},
        {"timeout_seconds": "10"},
        {"timeout_seconds": 3601},
        {"handler": "not-registered"},
        {"payload": {}},
        {"payload": {"message": 123}},
        {"payload": {"message": "x" * 16_385}},
        {"payload": {"message": "hi", "extra": "not allowed"}},
        {"payload": {"message": "x" * (256 * 1024)}},
    ],
)
def test_invalid_submission(client, changes):
    response = client.post("/jobs", json={"payload": {"message": "hello"}, **changes})
    assert response.status_code == 422
    assert response.json()["detail"]["code"] in {"INVALID_REQUEST", "INVALID_JOB"}


def test_unknown_and_invalid_id(client):
    assert client.get(f"/jobs/{uuid4()}").status_code == 404
    assert client.get("/jobs/not-a-uuid").status_code == 422


def test_api_idempotency_and_conflict(client):
    first = client.post(
        "/jobs", json={"payload": {"message": "hello"}}, headers={"Idempotency-Key": "key"}
    )
    replay = client.post(
        "/jobs", json={"payload": {"message": "hello"}}, headers={"Idempotency-Key": "key"}
    )
    assert first.json()["job_id"] == replay.json()["job_id"]
    conflict = client.post(
        "/jobs", json={"payload": {"message": "different"}}, headers={"Idempotency-Key": "key"}
    )
    assert conflict.status_code == 409


def test_pending_cancel_is_idempotent(client, repository):
    job_id = client.post("/jobs", json={"payload": {"message": "hello"}}).json()["job_id"]
    assert client.post(f"/jobs/{job_id}/cancel").json()["state"] == "cancelled"
    assert client.post(f"/jobs/{job_id}/cancel").status_code == 200
    assert repository.claim(job_id, 1, "worker") is None
    assert "pending_dispatch" not in repository.get(job_id)
    assert client.post(f"/jobs/{uuid4()}/cancel").status_code == 404


def test_running_cancel_conflicts(client, repository):
    job_id = client.post("/jobs", json={"payload": {"message": "hello"}}).json()["job_id"]
    repository.claim(job_id, 1, "worker")
    assert client.post(f"/jobs/{job_id}/cancel").status_code == 409


def test_drain_resume_control_and_depth(client):
    assert client.post("/queue/drain").status_code == 202
    assert client.get("/queue/control").json()["mode"] == "draining"
    denied = client.post("/jobs", json={"payload": {"message": "hello"}})
    assert denied.status_code == 503
    assert denied.json()["detail"]["code"] == "QUEUE_DRAINING"
    assert client.post("/queue/resume").json()["mode"] == "active"
    assert client.post("/jobs", json={"payload": {"message": "hello"}}).status_code == 202
    depth = client.get("/queue/depth").json()
    assert depth["broker_ready_messages"] == 2
    assert depth["broker_unacknowledged_messages"] == 1
    assert depth["database_pending_jobs"] == 1
    assert depth["database_delayed_jobs"] == 0


def test_broker_stats_failure_is_not_reported_as_zero(client, management):
    management.counts.side_effect = httpx.ConnectError("offline")
    response = client.get("/queue/depth")
    assert response.status_code == 503
    assert response.json()["detail"]["code"] == "BROKER_UNAVAILABLE"


def test_database_failure_surfaces_as_unavailable(client, repository, monkeypatch):
    def unavailable(*args):
        raise ServerSelectionTimeoutError("offline")

    monkeypatch.setattr(repository, "get", unavailable)
    response = client.get(f"/jobs/{uuid4()}")
    assert response.status_code == 503
    assert response.json()["detail"]["code"] == "DATABASE_UNAVAILABLE"
