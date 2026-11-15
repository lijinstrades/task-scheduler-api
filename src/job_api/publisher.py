import logging

import pika

from job_api.broker import RabbitBroker
from job_api.repository import Repository

logger = logging.getLogger(__name__)


class Publisher:
    def __init__(self, repository: Repository, broker: RabbitBroker):
        self.repository = repository
        self.broker = broker

    def tick(self) -> bool:
        doc = self.repository.claim_dispatch()
        if doc is None:
            return False
        try:
            self.broker.publish(doc)
        except (pika.exceptions.AMQPError, OSError):
            logger.exception("Publish failed; retaining pending dispatch job=%s", doc["_id"])
            self.repository.dispatch_failed(doc)
            raise
        if not self.repository.dispatch_sent(doc):
            logger.info(
                "Dispatch changed during publication job=%s; left newer state intact", doc["_id"]
            )
        return True
