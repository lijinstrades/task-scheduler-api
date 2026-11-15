import logging
import signal
import time
from multiprocessing.synchronize import Event

import pika
from pika.adapters.blocking_connection import BlockingChannel
from pydantic import ValidationError
from pymongo.errors import PyMongoError

from job_api.broker import RabbitBroker
from job_api.config import Settings
from job_api.execution import execute
from job_api.models import QueueMessage
from job_api.repository import PENDING, Repository, connect

logger = logging.getLogger(__name__)


class Worker:
    def __init__(
        self,
        repository: Repository,
        broker: RabbitBroker,
        worker_id: str,
        stop: Event,
    ):
        self.repository = repository
        self.broker = broker
        self.worker_id = worker_id
        self.stop = stop
        self.observed_generation = 0
        self.last_heartbeat = 0.0

    def heartbeat(self) -> None:
        self.broker.pump()
        if time.monotonic() - self.last_heartbeat >= 1:
            self.repository.worker_seen(self.worker_id, self.observed_generation, paused=False)
            self.last_heartbeat = time.monotonic()

    def on_message(
        self, channel: BlockingChannel, method: pika.spec.Basic.Deliver, properties, body: bytes
    ) -> None:
        tag = method.delivery_tag
        try:
            message = QueueMessage.model_validate_json(body)
        except ValidationError:
            logger.exception("Rejecting malformed work message")
            channel.basic_reject(delivery_tag=tag, requeue=False)
            return
        job_id = str(message.job_id)
        try:
            if self.stop.is_set() or self.repository.control()["mode"] != "active":
                channel.basic_nack(delivery_tag=tag, requeue=True)
                return
            doc = self.repository.claim(job_id, message.generation, self.worker_id)
            if doc is None:
                current = self.repository.get(job_id)
                if current is None:
                    logger.error("Rejecting message for missing job=%s", job_id)
                    channel.basic_reject(delivery_tag=tag, requeue=False)
                elif current["generation"] == message.generation and current["state"] in PENDING:
                    logger.info("Deferring currently ineligible message job=%s", job_id)
                    channel.basic_nack(delivery_tag=tag, requeue=True)
                    time.sleep(0.05)
                else:
                    logger.info("Skipping duplicate or obsolete message job=%s", job_id)
                    channel.basic_ack(delivery_tag=tag)
                return
            settings = self.repository.settings
            outcome = execute(
                doc,
                settings.enable_test_handlers,
                self.heartbeat,
                self.stop,
                settings.shutdown_grace_seconds,
            )
            if outcome.error is None:
                self.repository.succeed(doc, outcome.result)
            else:
                self.repository.fail(doc, outcome.error, permanent=outcome.permanent)
            channel.basic_ack(delivery_tag=tag)
        except PyMongoError:
            logger.exception("Database failure during delivery job=%s; requeueing", job_id)
            channel.basic_nack(delivery_tag=tag, requeue=True)
            raise

    def run(self) -> None:
        channel = self.broker.channel
        channel.basic_qos(prefetch_count=1)
        consumer_tag = None
        try:
            while not self.stop.is_set():
                control = self.repository.control()
                self.observed_generation = control["generation"]
                paused = control["mode"] != "active"
                if paused and consumer_tag is not None:
                    channel.basic_cancel(consumer_tag)
                    consumer_tag = None
                if not paused and consumer_tag is None:
                    consumer_tag = channel.basic_consume(
                        queue=self.repository.settings.work_queue,
                        on_message_callback=self.on_message,
                        auto_ack=False,
                    )
                self.repository.worker_seen(self.worker_id, self.observed_generation, paused=paused)
                self.broker.connection.process_data_events(time_limit=0.2)
                if paused:
                    self.stop.wait(0.2)
        finally:
            self.repository.worker_gone(self.worker_id)


def run_worker(settings: Settings, stop: Event, worker_id: str) -> None:
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(processName)s %(levelname)s %(message)s"
    )
    client = connect(settings)
    repository = Repository(client[settings.mongo_database], settings)
    broker = None
    try:
        repository.initialize()
        while not stop.is_set():
            try:
                broker = RabbitBroker(settings)
                Worker(repository, broker, worker_id, stop).run()
            except (pika.exceptions.AMQPError, PyMongoError, OSError):
                logger.exception("Worker connection failed; reconnecting worker=%s", worker_id)
                stop.wait(settings.poll_interval_seconds)
            finally:
                if broker is not None:
                    broker.close()
                    broker = None
    finally:
        client.close()


def run_coordinator(settings: Settings, stop: Event) -> None:
    from job_api.publisher import Publisher

    signal.signal(signal.SIGINT, signal.SIG_IGN)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(processName)s %(levelname)s %(message)s"
    )
    client = connect(settings)
    repository = Repository(client[settings.mongo_database], settings)
    broker = None
    try:
        repository.initialize()
        while not stop.is_set():
            try:
                repository.recover_expired()
                if broker is None:
                    broker = RabbitBroker(settings)
                publisher = Publisher(repository, broker)
                for _ in range(100):
                    if stop.is_set() or not publisher.tick():
                        break
                broker.pump()
            except (pika.exceptions.AMQPError, PyMongoError, OSError):
                logger.exception("Coordinator operation failed; pending work remains durable")
                if broker is not None:
                    broker.close()
                    broker = None
            stop.wait(settings.poll_interval_seconds)
    finally:
        if broker is not None:
            broker.close()
        client.close()
