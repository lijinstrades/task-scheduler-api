from unittest.mock import Mock

import pytest
from pydantic import ValidationError

from job_api.config import Settings
from job_api.pool import run_pool


def test_settings_reject_invalid_concurrency_and_backoff():
    with pytest.raises(ValidationError):
        Settings(_env_file=None, worker_count=0)
    with pytest.raises(ValidationError):
        Settings(_env_file=None, backoff_base_seconds=3, backoff_cap_seconds=1)


def test_pool_starts_configured_workers_and_restarts_failed_child(settings, monkeypatch):
    stop = Mock()
    stop.wait.side_effect = [False, True]
    processes = []

    def create_process(**kwargs):
        process = Mock()
        process.name = kwargs["name"]
        process.exitcode = 1 if len(processes) == 1 else 0
        process.alive = len(processes) != 1
        process.is_alive.side_effect = lambda: process.alive
        process.join.side_effect = lambda **kwargs: setattr(process, "alive", False)
        processes.append(process)
        return process

    context = Mock()
    context.Process.side_effect = create_process
    monkeypatch.setattr("job_api.pool.multiprocessing.get_context", lambda _: context)
    run_pool(settings, stop)
    assert len(processes) == settings.worker_count + 2
    assert sum(p.name.startswith("worker-") for p in processes) == settings.worker_count + 1
    for process in processes:
        process.start.assert_called_once()
        process.close.assert_called_once()
    stop.set.assert_called_once()
