import time
from typing import Protocol

from pydantic import BaseModel, ConfigDict, Field, JsonValue


class RetryableJobError(RuntimeError):
    pass


class PermanentJobError(RuntimeError):
    pass


class Handler(Protocol):
    def __call__(self, payload: dict[str, JsonValue], attempt: int) -> JsonValue: ...


class PrintPayload(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    message: str = Field(min_length=1, max_length=16_384)


class TestPayload(PrintPayload):
    failures: int = Field(default=1, ge=0, le=10)
    sleep_seconds: float = Field(default=0, ge=0, le=30)


def print_message(payload: dict[str, JsonValue], attempt: int) -> JsonValue:
    data = PrintPayload.model_validate(payload)
    print(data.message, flush=True)
    return {"printed": True, "message": data.message}


def transient_handler(payload: dict[str, JsonValue], attempt: int) -> JsonValue:
    data = TestPayload.model_validate(payload)
    time.sleep(data.sleep_seconds)
    if attempt <= data.failures:
        raise RetryableJobError("Simulated temporary failure")
    print(data.message, flush=True)
    return {"printed": True, "message": data.message}


def permanent_handler(payload: dict[str, JsonValue], attempt: int) -> JsonValue:
    raise PermanentJobError("Simulated permanent failure")


def slow_handler(payload: dict[str, JsonValue], attempt: int) -> JsonValue:
    data = TestPayload.model_validate(payload)
    time.sleep(data.sleep_seconds)
    return {"printed": True, "message": data.message}


HANDLERS: dict[str, Handler] = {"print": print_message}
TEST_HANDLERS: dict[str, Handler] = {
    "test-transient": transient_handler,
    "test-permanent": permanent_handler,
    "test-slow": slow_handler,
}


def resolve_handler(name: str, enable_tests: bool) -> Handler:
    handler = HANDLERS.get(name)
    if handler is None and enable_tests:
        handler = TEST_HANDLERS.get(name)
    if handler is None:
        raise PermanentJobError(f"Unknown or disabled handler: {name}")
    return handler


def validate_payload(name: str, payload: dict[str, JsonValue], enable_tests: bool) -> None:
    resolve_handler(name, enable_tests)
    schema = PrintPayload if name == "print" else TestPayload
    schema.model_validate(payload)
