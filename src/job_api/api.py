import logging
from contextlib import asynccontextmanager
from typing import Annotated
from uuid import UUID

import httpx
from fastapi import FastAPI, Header, HTTPException, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import ValidationError
from pymongo.errors import PyMongoError

from job_api.broker import BrokerManagement, BrokerStatsError
from job_api.config import Settings
from job_api.handlers import PermanentJobError, validate_payload
from job_api.models import JobStatus, JobSubmission, QueueControl, QueueDepth
from job_api.repository import (
    IdempotencyConflictError,
    QueueDrainingError,
    Repository,
    connect,
    utcnow,
)

logger = logging.getLogger(__name__)


def create_app(
    settings: Settings | None = None,
    repository: Repository | None = None,
    management: BrokerManagement | None = None,
) -> FastAPI:
    settings = settings or Settings()

    @asynccontextmanager
    async def lifespan(application: FastAPI):
        client = None
        if repository is None:
            client = connect(settings)
            application.state.repository = Repository(client[settings.mongo_database], settings)
        else:
            application.state.repository = repository
        try:
            application.state.repository.initialize()
            yield
        finally:
            if client is not None:
                client.close()

    application = FastAPI(title="Task Scheduler API", version="0.1.0", lifespan=lifespan)
    application.state.management = management or BrokerManagement(settings)

    @application.exception_handler(PyMongoError)
    async def database_error(request: Request, exc: PyMongoError):
        logger.error("Database request failed path=%s", request.url.path, exc_info=exc)
        return JSONResponse(
            status_code=503,
            content={
                "detail": {"code": "DATABASE_UNAVAILABLE", "message": "Job storage unavailable"}
            },
        )

    @application.exception_handler(RequestValidationError)
    async def validation_error(request: Request, exc: RequestValidationError):
        return JSONResponse(
            status_code=422,
            content={"detail": {"code": "INVALID_REQUEST", "message": "Invalid request fields"}},
        )

    def repo(request: Request) -> Repository:
        return request.app.state.repository

    @application.get("/health")
    def health(request: Request):
        repo(request).db.command("ping")
        return {"status": "ok"}

    @application.post("/jobs", status_code=202)
    def submit(
        body: JobSubmission,
        request: Request,
        response: Response,
        idempotency_key: Annotated[str | None, Header(max_length=128, min_length=1)] = None,
    ):
        try:
            validate_payload(body.handler, body.payload, settings.enable_test_handlers)
        except (PermanentJobError, ValidationError) as exc:
            raise HTTPException(
                422,
                {"code": "INVALID_JOB", "message": "Unknown handler or invalid handler payload"},
            ) from exc
        try:
            doc = repo(request).submit(body, idempotency_key)
        except QueueDrainingError as exc:
            raise HTTPException(503, {"code": "QUEUE_DRAINING", "message": str(exc)}) from exc
        except IdempotencyConflictError as exc:
            raise HTTPException(409, {"code": "IDEMPOTENCY_CONFLICT", "message": str(exc)}) from exc
        location = f"/jobs/{doc['_id']}"
        response.headers["Location"] = location
        return {"job_id": doc["_id"], "state": doc["state"], "status_url": location}

    @application.get("/jobs/{job_id}", response_model=JobStatus)
    def status(job_id: UUID, request: Request):
        doc = repo(request).get(str(job_id))
        if doc is None:
            raise HTTPException(404, {"code": "JOB_NOT_FOUND", "message": "Job not found"})
        return repo(request).status(doc)

    @application.post("/jobs/{job_id}/cancel", response_model=JobStatus)
    def cancel(job_id: UUID, request: Request):
        repository = repo(request)
        doc = repository.cancel(str(job_id))
        if doc is None:
            doc = repository.get(str(job_id))
            if doc is None:
                raise HTTPException(404, {"code": "JOB_NOT_FOUND", "message": "Job not found"})
            if doc["state"] != "cancelled":
                raise HTTPException(
                    409,
                    {"code": "JOB_NOT_PENDING", "message": "Only pending jobs can be cancelled"},
                )
        return repository.status(doc)

    @application.get("/queue/control", response_model=QueueControl)
    def control(request: Request):
        return repo(request).control_status()

    @application.post("/queue/drain", status_code=202, response_model=QueueControl)
    def drain(request: Request):
        return repo(request).set_mode("draining")

    @application.post("/queue/resume", response_model=QueueControl)
    def resume(request: Request):
        return repo(request).set_mode("active")

    @application.get("/queue/depth", response_model=QueueDepth)
    def depth(request: Request):
        try:
            counts = request.app.state.management.counts()
        except (httpx.HTTPError, BrokerStatsError, ValueError) as exc:
            logger.error("Queue statistics unavailable", exc_info=exc)
            raise HTTPException(
                503, {"code": "BROKER_UNAVAILABLE", "message": "Queue statistics unavailable"}
            ) from exc
        repository = repo(request)
        return QueueDepth(
            observed_at=utcnow(),
            **counts,
            **repository.depth(),
            control=repository.control_status(),
        )

    return application


app = create_app()
