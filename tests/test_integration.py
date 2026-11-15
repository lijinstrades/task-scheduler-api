import json
import logging
import multiprocessing
import time
from datetime import timedelta
from unittest.mock import Mock
from uuid import uuid4

import pika
import pytest
from fastapi.testclient import TestClient

from job_api.api import create_app
from job_api.broker import RabbitBroker
from job_api.config import Settings
from job_api.models import JobSubmission
from job_api.pool import run_pool
from job_api.publisher import Publisher
from job_api.repository import QueueDrainingError, Repository, connect, utcnow
from job_api.worker import run_coordinator, run_worker

pytestmark = pytest.mark.integration
logger = logging.getLogger(__name__)


def wait_for(predicate, timeout=25):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(0.05)
    raise AssertionError("Condition did not become true before the test deadline")


def terminal(repository, job_id, timeout=25):
    def check():
        doc = repository.get(job_id)
        return doc if doc["state"] in {"succeeded", "dead-lettered", "cancelled"} else None

    return wait_for(check, timeout)


@pytest.fixture
def services():
    suffix = uuid4().hex
    settings = Settings(
        mongo_database=f"job_api_test_{suffix}",
        work_queue=f"test.work.{suffix}",
        dead_queue=f"test.dead.{suffix}",
        dead_exchange=f"test.dead.{suffix}",
        enable_test_handlers=True,
        worker_count=3,
        poll_interval_seconds=0.1,
        backoff_base_seconds=0.1,
        backoff_cap_seconds=0.2,
        recovery_grace_seconds=0.5,
        shutdown_grace_seconds=2,
    )
    client = connect(settings)
    repository = Repository(client[settings.mongo_database], settings)
    broker = None
    try:
        repository.initialize()
        broker = RabbitBroker(settings)
        yield settings, repository, broker
    finally:
        if broker is not None and broker.connection.is_open:
            broker.channel.queue_delete(settings.work_queue)
            broker.channel.queue_delete(settings.dead_queue)
            broker.channel.exchange_delete(settings.dead_exchange)
            broker.close()
        client.drop_database(settings.mongo_database)
        client.close()


@pytest.fixture
def launch_pool(services):
    settings, repository, _ = services
    processes = []
    context = multiprocessing.get_context("spawn")

    def launch(count=3):
        selected = settings.model_copy(update={"worker_count": count})
        stop = context.Event()
        process = context.Process(target=run_pool, args=(selected, stop))
        process.start()
        processes.append((process, stop))
        wait_for(lambda: repository.control_status().live_workers == count)
        return process, stop

    yield launch
    for process, stop in processes:
        stop.set()
        process.join(timeout=15)
        if process.is_alive():
            logger.error("Integration pool failed to stop gracefully pid=%s", process.pid)
            process.terminate()
            process.join(timeout=3)
            pytest.fail("Pool did not stop within its shutdown grace")
        assert process.exitcode == 0
        process.close()


def test_submission_and_result_across_processes(services, launch_pool):
    settings, repository, _ = services
    launch_pool()
    with TestClient(create_app(settings, repository)) as api:
        response = api.post("/jobs", json={"payload": {"message": "integration hello"}})
        assert response.status_code == 202
        job_id = response.json()["job_id"]
        assert terminal(repository, job_id)["state"] == "succeeded"
        status = api.get(f"/jobs/{job_id}").json()
        assert status["attempt_count"] == 1
        assert status["result"] == {"printed": True, "message": "integration hello"}


def test_transient_retry_eventually_succeeds(services, launch_pool):
    _, repository, _ = services
    launch_pool()
    job = repository.submit(
        JobSubmission(
            handler="test-transient",
            payload={"message": "retry", "failures": 2},
            max_retries=2,
        )
    )
    saved = terminal(repository, job["_id"])
    assert saved["state"] == "succeeded"
    assert saved["attempt_count"] == 3
    assert saved["last_error"]["code"] == "TRANSIENT_FAILURE"
    assert len(saved["attempts"]) == 3


@pytest.mark.parametrize(
    ("handler", "retries", "attempts"),
    [("test-transient", 0, 1), ("test-transient", 1, 2), ("test-permanent", 3, 1)],
)
def test_terminal_failures_reach_dead_letter_queue(
    services, launch_pool, handler, retries, attempts
):
    _, repository, broker = services
    launch_pool()
    job = repository.submit(
        JobSubmission(
            handler=handler,
            payload={"message": "fail", "failures": 10},
            max_retries=retries,
        )
    )
    saved = terminal(repository, job["_id"])
    assert saved["state"] == "dead-lettered"
    assert saved["attempt_count"] == attempts
    assert "dead_letter" in saved

    def receive():
        method, _, body = broker.channel.basic_get(broker.settings.dead_queue, auto_ack=False)
        if method is None:
            return None
        broker.channel.basic_ack(method.delivery_tag)
        return json.loads(body)

    notification = wait_for(receive)
    assert notification["job_id"] == job["_id"]
    assert notification["attempt_count"] == attempts


def test_timeout_is_saved_and_dead_lettered(services, launch_pool):
    _, repository, _ = services
    launch_pool(1)
    job = repository.submit(
        JobSubmission(
            handler="test-slow",
            payload={"message": "timeout", "sleep_seconds": 10},
            timeout_seconds=2,
            max_retries=0,
        )
    )
    started = time.monotonic()
    saved = terminal(repository, job["_id"])
    assert saved["state"] == "dead-lettered"
    assert saved["last_error"]["code"] == "TIMEOUT"
    assert time.monotonic() - started < 10


def test_replayed_publication_executes_once_when_completion_is_saved(services, launch_pool):
    _, repository, broker = services
    job = repository.submit(JobSubmission(payload={"message": "duplicate"}))
    dispatch = repository.claim_dispatch()
    broker.publish(dispatch)
    broker.publish(dispatch)
    repository.dispatch_sent(dispatch)
    launch_pool()
    saved = terminal(repository, job["_id"])
    assert saved["state"] == "succeeded"
    assert saved["attempt_count"] == 1


def test_cancelled_broker_buffered_job_is_skipped(services, launch_pool):
    _, repository, broker = services
    job = repository.submit(JobSubmission(payload={"message": "cancel"}))
    assert Publisher(repository, broker).tick()
    assert repository.cancel(job["_id"])["state"] == "cancelled"
    launch_pool(1)
    time.sleep(1)
    saved = repository.get(job["_id"])
    assert saved["state"] == "cancelled"
    assert saved["attempt_count"] == 0


def test_drain_finishes_active_work_but_keeps_backlog(services, launch_pool):
    _, repository, _ = services
    launch_pool(1)
    first = repository.submit(
        JobSubmission(
            handler="test-slow",
            payload={"message": "active", "sleep_seconds": 2},
        )
    )
    wait_for(lambda: repository.get(first["_id"])["state"] == "running")
    pending = repository.submit(JobSubmission(payload={"message": "pending"}))
    assert not repository.set_mode("draining").drain_complete
    with pytest.raises(QueueDrainingError):
        repository.submit(JobSubmission(payload={"message": "denied"}))
    wait_for(lambda: repository.control_status().drain_complete)
    assert repository.get(first["_id"])["state"] == "succeeded"
    assert repository.get(pending["_id"])["state"] == "queued"
    repository.set_mode("active")
    assert terminal(repository, pending["_id"])["state"] == "succeeded"


def test_real_pool_obeys_concurrency_limit(services, launch_pool):
    _, repository, _ = services
    jobs = [
        repository.submit(
            JobSubmission(
                handler="test-slow",
                payload={"message": str(number), "sleep_seconds": 1.5},
            )
        )["_id"]
        for number in range(7)
    ]
    launch_pool(3)
    maximum = 0
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        running = repository.jobs.count_documents({"state": "running"})
        maximum = max(maximum, running)
        assert running <= 3
        completed = repository.jobs.count_documents({"_id": {"$in": jobs}, "state": "succeeded"})
        if completed == len(jobs):
            break
        time.sleep(0.025)
    else:
        pytest.fail("Pool did not complete all jobs")
    assert maximum == 3
    assert all(repository.get(job_id)["attempt_count"] == 1 for job_id in jobs)


def test_priority_orders_buffered_messages(services):
    _, repository, broker = services
    low = repository.submit(JobSubmission(payload={"message": "low"}, priority=0))
    high = repository.submit(JobSubmission(payload={"message": "high"}, priority=9))
    assert Publisher(repository, broker).tick()
    assert Publisher(repository, broker).tick()
    first, _, body = broker.channel.basic_get(broker.settings.work_queue, auto_ack=False)
    assert first is not None
    assert json.loads(body)["job_id"] == high["_id"]
    broker.channel.basic_ack(first.delivery_tag)
    second, _, body = broker.channel.basic_get(broker.settings.work_queue, auto_ack=False)
    assert second is not None
    assert json.loads(body)["job_id"] == low["_id"]
    broker.channel.basic_ack(second.delivery_tag)


def test_publish_failure_recovers_with_real_store_and_broker(services):
    _, repository, broker = services
    job = repository.submit(JobSubmission(payload={"message": "recover publication"}))
    failed_broker = Mock(spec=RabbitBroker)
    failed_broker.publish.side_effect = pika.exceptions.AMQPConnectionError("injected outage")
    with pytest.raises(pika.exceptions.AMQPConnectionError):
        Publisher(repository, failed_broker).tick()
    repository.jobs.update_one(
        {"_id": job["_id"]},
        {"$set": {"pending_dispatch.publish_after": utcnow() - timedelta(seconds=1)}},
    )
    assert Publisher(repository, broker).tick()
    assert "pending_dispatch" not in repository.get(job["_id"])
    method, _, body = broker.channel.basic_get(broker.settings.work_queue, auto_ack=False)
    assert method is not None
    assert json.loads(body)["job_id"] == job["_id"]
    broker.channel.basic_ack(method.delivery_tag)


def test_expired_attempt_and_stale_completion_with_real_store(services):
    _, repository, _ = services
    job = repository.submit(JobSubmission(payload={"message": "lease"}))
    old = repository.claim(job["_id"], 1, "old-worker")
    repository.jobs.update_one(
        {"_id": job["_id"]},
        {"$set": {"lease_expires_at": utcnow() - timedelta(seconds=1)}},
    )
    assert repository.recover_expired() == 1
    assert not repository.succeed(old, {"stale": True})
    saved = repository.get(job["_id"])
    assert saved["last_error"]["code"] == "LEASE_EXPIRED"
    assert saved["generation"] == 2


def test_worker_crash_is_recovered_by_another_worker(services):
    settings, repository, _ = services
    context = multiprocessing.get_context("spawn")
    stop = context.Event()
    coordinator = context.Process(target=run_coordinator, args=(settings, stop))
    first = context.Process(target=run_worker, args=(settings, stop, "crash-test-worker"))
    second = None
    coordinator.start()
    first.start()
    try:
        job = repository.submit(
            JobSubmission(
                handler="test-slow",
                payload={"message": "crash recovery", "sleep_seconds": 2},
                timeout_seconds=10,
                max_retries=1,
            )
        )
        wait_for(lambda: repository.get(job["_id"])["state"] == "running")
        first.terminate()
        first.join(timeout=5)
        assert not first.is_alive()
        repository.jobs.update_one(
            {"_id": job["_id"]},
            {"$set": {"lease_expires_at": utcnow() - timedelta(seconds=1)}},
        )
        second = context.Process(target=run_worker, args=(settings, stop, "replacement-worker"))
        second.start()
        saved = terminal(repository, job["_id"])
        assert saved["state"] == "succeeded"
        assert saved["attempt_count"] == 2
        assert saved["last_error"]["code"] == "LEASE_EXPIRED"
    finally:
        stop.set()
        for process in [coordinator, first, *([second] if second is not None else [])]:
            process.join(timeout=10)
            if process.is_alive():
                logger.error("Stopping unresponsive integration process pid=%s", process.pid)
                process.terminate()
                process.join(timeout=3)
            process.close()


def test_depth_reports_broker_and_database_categories(services):
    settings, repository, broker = services
    job = repository.submit(JobSubmission(payload={"message": "depth"}))
    assert Publisher(repository, broker).tick()
    with TestClient(create_app(settings, repository)) as api:

        def observed():
            response = api.get("/queue/depth")
            if response.status_code != 200:
                return None
            data = response.json()
            return data if data["broker_ready_messages"] >= 1 else None

        data = wait_for(observed)
        assert data["database_pending_jobs"] == 1
        assert data["database_delayed_jobs"] == 0
        assert repository.get(job["_id"])["state"] == "queued"
