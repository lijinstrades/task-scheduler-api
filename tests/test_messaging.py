import json
import multiprocessing
from unittest.mock import Mock

import httpx
import pika
import pytest

from job_api.broker import BrokerManagement, BrokerStatsError, RabbitBroker
from job_api.execution import ExecutionResult
from job_api.models import JobError
from job_api.publisher import Publisher
from job_api.worker import Worker


def test_publish_failure_retains_and_delays_dispatch(repository, submission):
    job = repository.submit(submission)
    broker = Mock(spec=RabbitBroker)
    broker.publish.side_effect = pika.exceptions.AMQPConnectionError("offline")
    with pytest.raises(pika.exceptions.AMQPConnectionError):
        Publisher(repository, broker).tick()
    saved = repository.get(job["_id"])
    assert "pending_dispatch" in saved
    assert saved["pending_dispatch"]["failures"] == 1
    assert "lock_token" not in saved["pending_dispatch"]
    assert saved["state"] == "queued"


def test_publisher_clears_only_confirmed_dispatch(repository, submission):
    job = repository.submit(submission)
    broker = Mock(spec=RabbitBroker)
    publisher = Publisher(repository, broker)
    assert publisher.tick()
    assert not publisher.tick()
    broker.publish.assert_called_once()
    assert "pending_dispatch" not in repository.get(job["_id"])


def test_worker_persists_before_ack_and_skips_duplicates(repository, submission, monkeypatch):
    job = repository.submit(submission)
    broker = Mock(spec=RabbitBroker)
    channel = Mock()
    method = pika.spec.Basic.Deliver(delivery_tag=7)
    executions = []

    def run(doc, *args):
        executions.append(doc["_id"])
        return ExecutionResult(result={"printed": True})

    monkeypatch.setattr("job_api.worker.execute", run)
    channel.basic_ack.side_effect = lambda **kwargs: (
        repository.get(job["_id"])["state"] == "succeeded"
        or pytest.fail("Acknowledged before saving success")
    )
    worker = Worker(repository, broker, "worker", multiprocessing.get_context("spawn").Event())
    body = json.dumps({"job_id": job["_id"], "generation": 1}).encode()
    worker.on_message(channel, method, None, body)
    worker.on_message(channel, method, None, body)
    assert executions == [job["_id"]]
    assert channel.basic_ack.call_count == 2
    assert repository.get(job["_id"])["attempt_count"] == 1


def test_worker_rejects_malformed_message(repository):
    channel = Mock()
    worker = Worker(
        repository, Mock(spec=RabbitBroker), "worker", multiprocessing.get_context("spawn").Event()
    )
    worker.on_message(channel, pika.spec.Basic.Deliver(delivery_tag=1), None, b"not-json")
    channel.basic_reject.assert_called_once_with(delivery_tag=1, requeue=False)


def test_worker_skips_cancelled_message(repository, submission):
    job = repository.submit(submission)
    repository.cancel(job["_id"])
    channel = Mock()
    worker = Worker(
        repository, Mock(spec=RabbitBroker), "worker", multiprocessing.get_context("spawn").Event()
    )
    worker.on_message(
        channel,
        pika.spec.Basic.Deliver(delivery_tag=2),
        None,
        json.dumps({"job_id": job["_id"], "generation": 1}).encode(),
    )
    channel.basic_ack.assert_called_once_with(delivery_tag=2)
    assert repository.get(job["_id"])["attempt_count"] == 0


def test_worker_saves_retry_before_ack(repository, submission, monkeypatch):
    job = repository.submit(submission)
    monkeypatch.setattr(
        "job_api.worker.execute",
        lambda *args: ExecutionResult(error=JobError(code="TRANSIENT", message="retry")),
    )
    channel = Mock()
    channel.basic_ack.side_effect = lambda **kwargs: (
        repository.get(job["_id"])["state"] == "failed"
        or pytest.fail("Acknowledged before saving retry")
    )
    worker = Worker(
        repository, Mock(spec=RabbitBroker), "worker", multiprocessing.get_context("spawn").Event()
    )
    worker.on_message(
        channel,
        pika.spec.Basic.Deliver(delivery_tag=3),
        None,
        json.dumps({"job_id": job["_id"], "generation": 1}).encode(),
    )
    channel.basic_ack.assert_called_once_with(delivery_tag=3)
    assert repository.get(job["_id"])["pending_dispatch"]["generation"] == 2


def test_worker_requeues_database_failure(repository, submission, monkeypatch):
    from pymongo.errors import AutoReconnect

    job = repository.submit(submission)
    monkeypatch.setattr(repository, "claim", Mock(side_effect=AutoReconnect("offline")))
    channel = Mock()
    worker = Worker(
        repository, Mock(spec=RabbitBroker), "worker", multiprocessing.get_context("spawn").Event()
    )
    with pytest.raises(AutoReconnect):
        worker.on_message(
            channel,
            pika.spec.Basic.Deliver(delivery_tag=4),
            None,
            json.dumps({"job_id": job["_id"], "generation": 1}).encode(),
        )
    channel.basic_nack.assert_called_once_with(delivery_tag=4, requeue=True)
    channel.basic_ack.assert_not_called()


def test_worker_subscription_pauses_and_resumes(repository):
    broker = Mock(spec=RabbitBroker)
    broker.channel = Mock()
    broker.channel.basic_consume.return_value = "consumer"
    broker.connection = Mock()
    stop = multiprocessing.get_context("spawn").Event()
    steps = []

    def events(**kwargs):
        steps.append(repository.control()["mode"])
        if len(steps) == 1:
            repository.set_mode("draining")
        elif len(steps) == 2:
            assert repository.control_status().drain_complete
            repository.set_mode("active")
        else:
            stop.set()

    broker.connection.process_data_events.side_effect = events
    Worker(repository, broker, "worker", stop).run()
    assert steps == ["active", "draining", "active"]
    assert broker.channel.basic_consume.call_count == 2
    broker.channel.basic_cancel.assert_called_once_with("consumer")
    broker.channel.basic_qos.assert_called_once_with(prefetch_count=1)
    assert repository.workers.count_documents({}) == 0


def test_publish_uses_persistence_priority_and_mandatory_routing(settings, repository, submission):
    repository.submit(submission)
    doc = repository.claim_dispatch()
    broker = RabbitBroker.__new__(RabbitBroker)
    broker.settings = settings
    broker.channel = Mock()
    broker.publish(doc)
    published = broker.channel.basic_publish.call_args.kwargs
    assert published["mandatory"] is True
    assert published["properties"].delivery_mode == 2
    assert published["properties"].priority == submission.priority
    assert published["routing_key"] == settings.work_queue
    assert json.loads(published["body"]) == {"job_id": doc["_id"], "generation": 1}


def test_broker_declares_priority_and_dead_letter_topology(settings):
    channel = Mock()
    RabbitBroker.declare(channel, settings)
    args = channel.queue_declare.call_args.kwargs["arguments"]
    assert args["x-max-priority"] == 9
    assert args["x-dead-letter-exchange"] == settings.dead_exchange
    channel.queue_bind.assert_called_once_with(
        queue=settings.dead_queue, exchange=settings.dead_exchange, routing_key="dead"
    )


def test_management_rejects_missing_statistics(settings, monkeypatch):
    mock_client = Mock()
    mock_client.get.return_value.json.return_value = {}
    monkeypatch.setattr(
        "job_api.broker.httpx.Client",
        lambda **kwargs: mock_client,
    )
    mock_client.__enter__ = Mock(return_value=mock_client)
    mock_client.__exit__ = Mock(return_value=False)
    with pytest.raises(BrokerStatsError):
        BrokerManagement(settings).counts()


def test_management_encodes_vhost_and_reads_counts(settings, monkeypatch):
    work = Mock()
    work.json.return_value = {"messages_ready": 4, "messages_unacknowledged": 2}
    dead = Mock()
    dead.json.return_value = {"messages": 3}
    transport = httpx.MockTransport(
        lambda request: httpx.Response(
            200, json=work.json() if "jobs.work" in request.url.path else dead.json()
        )
    )
    real_client = httpx.Client
    monkeypatch.setattr(
        "job_api.broker.httpx.Client",
        lambda **kwargs: real_client(**kwargs, transport=transport),
    )
    assert BrokerManagement(settings).counts() == {
        "broker_ready_messages": 4,
        "broker_unacknowledged_messages": 2,
        "broker_dead_letter_messages": 3,
    }
