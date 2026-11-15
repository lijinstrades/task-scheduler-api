import json
from urllib.parse import quote

import httpx
import pika
from pika.adapters.blocking_connection import BlockingChannel

from job_api.config import Settings
from job_api.repository import Document


class BrokerStatsError(RuntimeError):
    pass


class RabbitBroker:
    def __init__(self, settings: Settings):
        self.settings = settings
        params = pika.URLParameters(settings.rabbitmq_url)
        params.heartbeat = 30
        params.blocked_connection_timeout = 5
        params.socket_timeout = 3
        params.stack_timeout = 10
        self.connection = pika.BlockingConnection(params)
        self.channel = self.connection.channel()
        try:
            self.declare(self.channel, settings)
            self.channel.confirm_delivery()
        except pika.exceptions.AMQPError:
            self.close()
            raise

    @staticmethod
    def declare(channel: BlockingChannel, settings: Settings) -> None:
        channel.exchange_declare(
            exchange=settings.dead_exchange, exchange_type="direct", durable=True
        )
        channel.queue_declare(queue=settings.dead_queue, durable=True)
        channel.queue_bind(
            queue=settings.dead_queue, exchange=settings.dead_exchange, routing_key="dead"
        )
        channel.queue_declare(
            queue=settings.work_queue,
            durable=True,
            arguments={
                "x-queue-type": "classic",
                "x-max-priority": 9,
                "x-dead-letter-exchange": settings.dead_exchange,
                "x-dead-letter-routing-key": "dead",
            },
        )

    def publish(self, doc: Document) -> None:
        dispatch = doc["pending_dispatch"]
        dead = dispatch["destination"] == "dead"
        body = {"job_id": doc["_id"], "generation": dispatch["generation"]}
        if dead:
            body["error"] = doc["last_error"]
            body["attempt_count"] = doc["attempt_count"]
        self.channel.basic_publish(
            exchange=self.settings.dead_exchange if dead else "",
            routing_key="dead" if dead else self.settings.work_queue,
            body=json.dumps(body).encode(),
            properties=pika.BasicProperties(
                content_type="application/json",
                delivery_mode=2,
                message_id=f"{doc['_id']}:{dispatch['generation']}",
                priority=doc["priority"],
            ),
            mandatory=True,
        )

    def pump(self) -> None:
        self.connection.process_data_events(time_limit=0)

    def close(self) -> None:
        if self.connection.is_open:
            self.connection.close()


class BrokerManagement:
    def __init__(self, settings: Settings):
        self.settings = settings

    def counts(self) -> dict[str, int]:
        settings = self.settings
        with httpx.Client(
            base_url=settings.rabbitmq_management_url.rstrip("/") + "/api/",
            auth=(settings.rabbitmq_management_user, settings.rabbitmq_management_password),
            timeout=5,
        ) as client:
            work = client.get(
                f"queues/{quote(settings.rabbitmq_vhost, safe='')}/"
                f"{quote(settings.work_queue, safe='')}"
            )
            dead = client.get(
                f"queues/{quote(settings.rabbitmq_vhost, safe='')}/"
                f"{quote(settings.dead_queue, safe='')}"
            )
            work.raise_for_status()
            dead.raise_for_status()
            work_data, dead_data = work.json(), dead.json()
        if not isinstance(work_data, dict) or not isinstance(dead_data, dict):
            raise BrokerStatsError("RabbitMQ management returned an invalid statistics object")
        required = [
            (work_data, "messages_ready"),
            (work_data, "messages_unacknowledged"),
            (dead_data, "messages"),
        ]
        if any(type(data.get(key)) is not int or data[key] < 0 for data, key in required):
            raise BrokerStatsError("RabbitMQ management statistics are not yet available")
        return {
            "broker_ready_messages": work_data["messages_ready"],
            "broker_unacknowledged_messages": work_data["messages_unacknowledged"],
            "broker_dead_letter_messages": dead_data["messages"],
        }
