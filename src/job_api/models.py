import json
from datetime import datetime
from enum import StrEnum
from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, JsonValue, model_validator


class JobState(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    FAILED = "failed"
    SUCCEEDED = "succeeded"
    DEAD_LETTERED = "dead-lettered"
    CANCELLED = "cancelled"


class JobSubmission(BaseModel):
    model_config = ConfigDict(extra="forbid")

    handler: str = Field(default="print", min_length=1, max_length=64)
    payload: dict[str, JsonValue]
    priority: Annotated[int, Field(strict=True, ge=0, le=9)] = 5
    max_retries: Annotated[int, Field(strict=True, ge=0, le=10)] = 3
    timeout_seconds: Annotated[float, Field(strict=True, gt=0, le=3600, allow_inf_nan=False)] = 10.0

    @model_validator(mode="after")
    def bound_payload(self) -> "JobSubmission":
        if len(json.dumps(self.payload).encode("utf-8")) > 256 * 1024:
            raise ValueError("payload exceeds 256 KiB")
        return self


class JobError(BaseModel):
    code: str
    message: str


class Attempt(BaseModel):
    number: int
    owner_token: str
    started_at: datetime
    finished_at: datetime
    outcome: Literal["succeeded", "failed"]
    error: JobError | None = None


class JobStatus(BaseModel):
    job_id: str
    state: JobState
    attempt_count: int
    max_attempts: int
    priority: int
    timeout_seconds: float
    last_error: JobError | None
    result: JsonValue
    next_attempt_at: datetime | None
    created_at: datetime
    updated_at: datetime
    finished_at: datetime | None
    attempts: list[Attempt]


class QueueMessage(BaseModel):
    model_config = ConfigDict(extra="forbid")

    job_id: UUID
    generation: Annotated[int, Field(strict=True, ge=1)]


class QueueControl(BaseModel):
    mode: Literal["active", "draining"]
    generation: int
    running_jobs: int
    live_workers: int
    paused_workers: int
    drain_complete: bool


class QueueDepth(BaseModel):
    observed_at: datetime
    broker_ready_messages: int
    broker_unacknowledged_messages: int
    broker_dead_letter_messages: int
    database_pending_jobs: int
    database_delayed_jobs: int
    database_dead_letter_jobs: int
    control: QueueControl
