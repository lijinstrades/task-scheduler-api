import json
import logging
import multiprocessing
import os
import signal
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from multiprocessing.connection import Connection
from multiprocessing.synchronize import Event

from pydantic import JsonValue, TypeAdapter, ValidationError

from job_api.handlers import PermanentJobError, RetryableJobError, resolve_handler
from job_api.models import JobError
from job_api.repository import Document, utcnow

logger = logging.getLogger(__name__)


@dataclass
class ExecutionResult:
    result: JsonValue = None
    error: JobError | None = None
    permanent: bool = False


def _watch_parent() -> None:
    parent = multiprocessing.parent_process()
    if parent is None:
        raise RuntimeError("Handler must run as a child of a worker")
    while parent.is_alive():
        time.sleep(0.2)
    os._exit(1)


def _child(
    name: str, payload: dict[str, JsonValue], attempt: int, enable_tests: bool, sender: Connection
) -> None:
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    threading.Thread(target=_watch_parent, daemon=True).start()
    try:
        result = resolve_handler(name, enable_tests)(payload, attempt)
        result = TypeAdapter(JsonValue).validate_python(result)
        if len(json.dumps(result).encode()) > 256 * 1024:
            raise PermanentJobError("Handler result exceeds 256 KiB")
        sender.send(ExecutionResult(result=result))
    except PermanentJobError as exc:
        logger.error("Permanent handler failure: %s", exc)
        sender.send(
            ExecutionResult(
                error=JobError(code="PERMANENT_FAILURE", message=str(exc)), permanent=True
            )
        )
    except RetryableJobError as exc:
        logger.warning("Retryable handler failure: %s", exc)
        sender.send(ExecutionResult(error=JobError(code="TRANSIENT_FAILURE", message=str(exc))))
    except ValidationError:
        logger.exception("Invalid handler payload or result")
        sender.send(
            ExecutionResult(
                error=JobError(
                    code="INVALID_HANDLER_DATA", message="Handler data failed validation"
                ),
                permanent=True,
            )
        )
    finally:
        sender.close()


def execute(
    doc: Document,
    enable_tests: bool,
    heartbeat: Callable[[], None],
    stop: Event | None = None,
    shutdown_grace: float = 10,
) -> ExecutionResult:
    context = multiprocessing.get_context("spawn")
    receiver, sender = context.Pipe(duplex=False)
    process = context.Process(
        target=_child,
        args=(doc["handler"], doc["payload"], doc["attempt_count"], enable_tests, sender),
        daemon=True,
    )
    remaining = max(0, (doc["attempt_deadline"] - utcnow()).total_seconds())
    deadline = time.monotonic() + remaining
    shutdown_deadline = None
    process.start()
    sender.close()
    try:
        while True:
            now = time.monotonic()
            if now >= deadline:
                logger.warning("Attempt timed out job=%s", doc["_id"])
                return ExecutionResult(
                    error=JobError(
                        code="TIMEOUT", message="Attempt exceeded its configured timeout"
                    )
                )
            if stop is not None and stop.is_set():
                if shutdown_deadline is None:
                    shutdown_deadline = now + shutdown_grace
                if now >= shutdown_deadline:
                    logger.warning("Stopping unfinished attempt during shutdown job=%s", doc["_id"])
                    return ExecutionResult(
                        error=JobError(code="SHUTDOWN", message="Worker shutdown grace expired")
                    )
            heartbeat()
            if receiver.poll(min(0.02, max(0, deadline - time.monotonic()))):
                if time.monotonic() >= deadline:
                    logger.warning("Handler returned after its deadline job=%s", doc["_id"])
                    return ExecutionResult(
                        error=JobError(code="TIMEOUT", message="Attempt exceeded its timeout")
                    )
                try:
                    return receiver.recv()
                except EOFError:
                    logger.error("Handler exited without an outcome job=%s", doc["_id"])
                    return ExecutionResult(
                        error=JobError(code="HANDLER_CRASHED", message="Handler process exited")
                    )
            if not process.is_alive():
                logger.error("Handler process crashed job=%s exit=%s", doc["_id"], process.exitcode)
                return ExecutionResult(
                    error=JobError(code="HANDLER_CRASHED", message="Handler process exited")
                )
    finally:
        receiver.close()
        process.join(timeout=0.1)
        if process.is_alive():
            process.terminate()
            process.join(timeout=2)
        if process.is_alive():
            process.kill()
            process.join(timeout=2)
        if process.is_alive():
            raise RuntimeError(f"Could not stop handler process for job {doc['_id']}")
        process.close()
