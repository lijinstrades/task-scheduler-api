from unittest.mock import Mock

import mongomock
import pytest
from fastapi.testclient import TestClient

from job_api.api import create_app
from job_api.broker import BrokerManagement
from job_api.config import Settings
from job_api.models import JobSubmission
from job_api.repository import Repository


def pytest_addoption(parser):
    parser.addoption(
        "--integration",
        action="store_true",
        help="Also run real MongoDB/RabbitMQ integration tests",
    )


def pytest_collection_modifyitems(config, items):
    if not config.getoption("--integration"):
        skip = pytest.mark.skip(reason="Run with --integration and local MongoDB/RabbitMQ services")
        for item in items:
            if "integration" in item.keywords:
                item.add_marker(skip)


@pytest.fixture
def settings():
    return Settings(
        _env_file=None,
        enable_test_handlers=True,
        backoff_base_seconds=0.1,
        backoff_cap_seconds=0.2,
        recovery_grace_seconds=0.5,
    )


@pytest.fixture
def repository(settings):
    client = mongomock.MongoClient(tz_aware=True)
    repository = Repository(client["unit_jobs"], settings)
    repository.initialize()
    yield repository
    client.close()


@pytest.fixture
def submission():
    return JobSubmission(payload={"message": "hello"})


@pytest.fixture
def management():
    management = Mock(spec=BrokerManagement)
    management.counts.return_value = {
        "broker_ready_messages": 2,
        "broker_unacknowledged_messages": 1,
        "broker_dead_letter_messages": 0,
    }
    return management


@pytest.fixture
def client(settings, repository, management):
    with TestClient(create_app(settings, repository, management)) as client:
        yield client
