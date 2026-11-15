import hashlib
import json
import logging
import random
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import NAMESPACE_URL, uuid4, uuid5

from pydantic import JsonValue
from pymongo import ASCENDING, MongoClient, ReturnDocument
from pymongo.database import Database
from pymongo.errors import DuplicateKeyError

from job_api.config import Settings
from job_api.models import JobError, JobStatus, JobSubmission, QueueControl

logger = logging.getLogger(__name__)
Document = dict[str, Any]
PENDING = ["queued", "failed"]


class QueueDrainingError(RuntimeError):
    pass


class IdempotencyConflictError(RuntimeError):
    pass


def utcnow() -> datetime:
    return datetime.now(UTC)


def retry_delay(attempt: int, settings: Settings) -> float:
    return random.uniform(
        0, min(settings.backoff_cap_seconds, settings.backoff_base_seconds * 2 ** (attempt - 1))
    )


def connect(settings: Settings) -> MongoClient[Document]:
    return MongoClient(
        settings.mongo_uri,
        tz_aware=True,
        serverSelectionTimeoutMS=3000,
        timeoutMS=5000,
        retryWrites=True,
        w="majority",
        journal=True,
    )


class Repository:
    def __init__(self, database: Database[Document], settings: Settings):
        self.db = database
        self.settings = settings
        self.jobs = database["jobs"]
        self.controls = database["queue_control"]
        self.workers = database["workers"]

    def initialize(self) -> None:
        self.db.command("ping")
        self.jobs.create_index([("pending_dispatch.publish_after", ASCENDING)])
        self.jobs.create_index([("state", ASCENDING), ("lease_expires_at", ASCENDING)])
        self.jobs.create_index([("state", ASCENDING), ("next_attempt_at", ASCENDING)])
        self.workers.create_index("last_seen")
        self.controls.update_one(
            {"_id": "default"},
            {"$setOnInsert": {"mode": "active", "generation": 1, "updated_at": utcnow()}},
            upsert=True,
        )

    def control(self) -> Document:
        doc = self.controls.find_one({"_id": "default"})
        if doc is None:
            raise RuntimeError("Queue control is missing; initialize the database first")
        return doc

    def submit(self, request: JobSubmission, idempotency_key: str | None = None) -> Document:
        canonical = json.dumps(
            request.model_dump(mode="json"), sort_keys=True, separators=(",", ":")
        )
        digest = hashlib.sha256(canonical.encode()).hexdigest()
        job_id = str(
            uuid5(NAMESPACE_URL, f"job-api:{self.settings.mongo_database}:{idempotency_key}")
            if idempotency_key
            else uuid4()
        )
        if idempotency_key:
            existing = self.get(job_id)
            if existing:
                if existing["request_digest"] != digest:
                    raise IdempotencyConflictError("Idempotency key was used for another request")
                return existing
        if self.control()["mode"] != "active":
            raise QueueDrainingError("Queue is draining; resume before submitting new jobs")
        now = utcnow()
        doc = {
            "_id": job_id,
            **request.model_dump(mode="python"),
            "max_attempts": request.max_retries + 1,
            "request_digest": digest,
            "state": "queued",
            "attempt_count": 0,
            "generation": 1,
            "owner_token": None,
            "last_error": None,
            "result": None,
            "next_attempt_at": now,
            "created_at": now,
            "updated_at": now,
            "finished_at": None,
            "attempts": [],
            "pending_dispatch": {
                "generation": 1,
                "destination": "work",
                "publish_after": now,
            },
        }
        try:
            self.jobs.insert_one(doc)
        except DuplicateKeyError:
            existing = self.get(job_id)
            if existing is None:
                raise
            if existing["request_digest"] != digest:
                raise IdempotencyConflictError(
                    "Idempotency key was used for another request"
                ) from None
            return existing
        return doc

    def get(self, job_id: str) -> Document | None:
        return self.jobs.find_one({"_id": job_id})

    def status(self, doc: Document) -> JobStatus:
        return JobStatus(
            job_id=doc["_id"],
            **{name: doc[name] for name in JobStatus.model_fields if name != "job_id"},
        )

    def claim(self, job_id: str, generation: int, worker_id: str) -> Document | None:
        if self.control()["mode"] != "active":
            return None
        existing = self.get(job_id)
        if existing is None:
            return None
        now = utcnow()
        deadline = now + timedelta(seconds=existing["timeout_seconds"])
        return self.jobs.find_one_and_update(
            {
                "_id": job_id,
                "generation": generation,
                "state": {"$in": PENDING},
                "next_attempt_at": {"$lte": now},
                "attempt_count": {"$lt": existing["max_attempts"]},
            },
            {
                "$set": {
                    "state": "running",
                    "owner_token": str(uuid4()),
                    "worker_id": worker_id,
                    "attempt_started_at": now,
                    "attempt_deadline": deadline,
                    "lease_expires_at": deadline
                    + timedelta(seconds=self.settings.recovery_grace_seconds),
                    "next_attempt_at": None,
                    "updated_at": now,
                },
                "$inc": {"attempt_count": 1},
            },
            return_document=ReturnDocument.AFTER,
        )

    def _ownership(self, doc: Document, *, expired: bool = False) -> Document:
        return {
            "_id": doc["_id"],
            "state": "running",
            "generation": doc["generation"],
            "owner_token": doc["owner_token"],
            "lease_expires_at": {"$lte" if expired else "$gt": utcnow()},
        }

    def succeed(self, doc: Document, result: JsonValue) -> bool:
        now = utcnow()
        record = self._attempt_record(doc, now, "succeeded", None)
        update = {
            "$set": {
                "state": "succeeded",
                "result": result,
                "finished_at": now,
                "updated_at": now,
                "owner_token": None,
            },
            "$push": {"attempts": record},
            "$unset": {"pending_dispatch": "", "lease_expires_at": "", "attempt_deadline": ""},
        }
        return self._finalize(self._ownership(doc), update, doc)

    def fail(
        self, doc: Document, error: JobError, *, permanent: bool = False, expired: bool = False
    ) -> bool:
        now = utcnow()
        dead = permanent or doc["attempt_count"] >= doc["max_attempts"]
        generation = doc["generation"] + 1
        eligible = (
            now
            if dead
            else now + timedelta(seconds=retry_delay(doc["attempt_count"], self.settings))
        )
        values = {
            "state": "dead-lettered" if dead else "failed",
            "last_error": error.model_dump(),
            "owner_token": None,
            "generation": generation,
            "updated_at": now,
            "next_attempt_at": None if dead else eligible,
            "finished_at": now if dead else None,
            "pending_dispatch": {
                "generation": generation,
                "destination": "dead" if dead else "work",
                "publish_after": eligible,
            },
        }
        if dead:
            values["dead_letter"] = {
                "error": error.model_dump(),
                "attempt_count": doc["attempt_count"],
                "created_at": now,
            }
        return self._finalize(
            self._ownership(doc, expired=expired),
            {
                "$set": values,
                "$push": {"attempts": self._attempt_record(doc, now, "failed", error)},
                "$unset": {"lease_expires_at": "", "attempt_deadline": ""},
            },
            doc,
        )

    @staticmethod
    def _attempt_record(
        doc: Document, now: datetime, outcome: str, error: JobError | None
    ) -> Document:
        return {
            "number": doc["attempt_count"],
            "owner_token": doc["owner_token"],
            "started_at": doc["attempt_started_at"],
            "finished_at": now,
            "outcome": outcome,
            "error": error.model_dump() if error else None,
        }

    def _finalize(self, query: Document, update: Document, doc: Document) -> bool:
        changed = self.jobs.update_one(query, update).modified_count == 1
        if not changed:
            logger.warning(
                "Rejected stale attempt completion job=%s attempt=%s",
                doc["_id"],
                doc["attempt_count"],
            )
        return changed

    def recover_expired(self, limit: int = 100) -> int:
        expired = self.jobs.find(
            {"state": "running", "lease_expires_at": {"$lte": utcnow()}}
        ).limit(limit)
        count = 0
        for doc in expired:
            logger.warning("Recovering expired attempt job=%s", doc["_id"])
            count += self.fail(
                doc,
                JobError(code="LEASE_EXPIRED", message="Worker attempt expired before completion"),
                expired=True,
            )
        return count

    def cancel(self, job_id: str) -> Document | None:
        now = utcnow()
        return self.jobs.find_one_and_update(
            {"_id": job_id, "state": {"$in": PENDING}},
            {
                "$set": {"state": "cancelled", "updated_at": now, "finished_at": now},
                "$unset": {"pending_dispatch": ""},
            },
            return_document=ReturnDocument.AFTER,
        )

    def claim_dispatch(self) -> Document | None:
        now = utcnow()
        query = {
            "pending_dispatch.publish_after": {"$lte": now},
            "$or": [
                {"pending_dispatch.lock_until": {"$exists": False}},
                {"pending_dispatch.lock_until": {"$lte": now}},
            ],
        }
        if self.control()["mode"] != "active":
            query["pending_dispatch.destination"] = "dead"
        return self.jobs.find_one_and_update(
            query,
            {
                "$set": {
                    "pending_dispatch.lock_token": str(uuid4()),
                    "pending_dispatch.lock_until": now
                    + timedelta(seconds=self.settings.dispatch_lease_seconds),
                }
            },
            sort=[("pending_dispatch.publish_after", ASCENDING)],
            return_document=ReturnDocument.AFTER,
        )

    @staticmethod
    def _dispatch_query(doc: Document) -> Document:
        return {
            "_id": doc["_id"],
            "pending_dispatch.generation": doc["pending_dispatch"]["generation"],
            "pending_dispatch.lock_token": doc["pending_dispatch"]["lock_token"],
        }

    def dispatch_sent(self, doc: Document) -> bool:
        return (
            self.jobs.update_one(
                self._dispatch_query(doc), {"$unset": {"pending_dispatch": ""}}
            ).modified_count
            == 1
        )

    def dispatch_failed(self, doc: Document) -> None:
        failures = doc["pending_dispatch"].get("failures", 0) + 1
        self.jobs.update_one(
            self._dispatch_query(doc),
            {
                "$set": {
                    "pending_dispatch.publish_after": utcnow()
                    + timedelta(seconds=min(30, 2 ** min(failures, 5))),
                    "pending_dispatch.failures": failures,
                },
                "$unset": {
                    "pending_dispatch.lock_token": "",
                    "pending_dispatch.lock_until": "",
                },
            },
        )

    def set_mode(self, mode: str) -> QueueControl:
        self.controls.update_one(
            {"_id": "default", "mode": {"$ne": mode}},
            {"$set": {"mode": mode, "updated_at": utcnow()}, "$inc": {"generation": 1}},
        )
        return self.control_status()

    def worker_seen(self, worker_id: str, generation: int, paused: bool) -> None:
        self.workers.update_one(
            {"_id": worker_id},
            {"$set": {"generation": generation, "paused": paused, "last_seen": utcnow()}},
            upsert=True,
        )

    def worker_gone(self, worker_id: str) -> None:
        self.workers.delete_one({"_id": worker_id})

    def control_status(self) -> QueueControl:
        control = self.control()
        live = list(
            self.workers.find(
                {
                    "last_seen": {
                        "$gte": utcnow() - timedelta(seconds=self.settings.worker_stale_seconds)
                    }
                }
            )
        )
        paused = sum(
            worker["paused"] and worker["generation"] == control["generation"] for worker in live
        )
        running = self.jobs.count_documents({"state": "running"})
        return QueueControl(
            mode=control["mode"],
            generation=control["generation"],
            running_jobs=running,
            live_workers=len(live),
            paused_workers=paused,
            drain_complete=control["mode"] == "draining" and running == 0 and paused == len(live),
        )

    def depth(self) -> Document:
        now = utcnow()
        return {
            "database_pending_jobs": self.jobs.count_documents({"state": {"$in": PENDING}}),
            "database_delayed_jobs": self.jobs.count_documents(
                {"state": {"$in": PENDING}, "next_attempt_at": {"$gt": now}}
            ),
            "database_dead_letter_jobs": self.jobs.count_documents({"state": "dead-lettered"}),
        }
