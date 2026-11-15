import argparse
import logging
import multiprocessing
import signal
import time
from multiprocessing.synchronize import Event
from uuid import uuid4

from job_api.config import Settings
from job_api.worker import run_coordinator, run_worker

logger = logging.getLogger(__name__)


def run_pool(settings: Settings, stop: Event | None = None) -> None:
    context = multiprocessing.get_context("spawn")
    stop = stop if stop is not None else context.Event()
    workers: dict[str, multiprocessing.Process] = {}

    def start(name: str) -> multiprocessing.Process:
        coordinator = name == "coordinator"
        process = context.Process(
            target=run_coordinator if coordinator else run_worker,
            args=(settings, stop) if coordinator else (settings, stop, name),
            name=name,
        )
        process.start()
        return process

    names = ["coordinator", *(f"worker-{uuid4()}" for _ in range(settings.worker_count))]
    try:
        for name in names:
            workers[name] = start(name)
        while not stop.wait(0.5):
            for name, process in list(workers.items()):
                if not process.is_alive():
                    logger.error(
                        "Restarting exited pool process=%s exit=%s", name, process.exitcode
                    )
                    process.join()
                    process.close()
                    workers[name] = start(name)
    except KeyboardInterrupt:
        logger.info("Stopping worker pool")
        stop.set()
    finally:
        stop.set()
        deadline = time.monotonic() + settings.shutdown_grace_seconds + 8
        for process in workers.values():
            process.join(timeout=max(0, deadline - time.monotonic()))
        for process in workers.values():
            if process.is_alive():
                logger.error("Terminating unresponsive pool process=%s", process.name)
                process.terminate()
                process.join(timeout=3)
            if process.is_alive():
                process.kill()
                process.join(timeout=3)
            if process.is_alive():
                raise RuntimeError(f"Could not stop pool process {process.name}")
            process.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the RabbitMQ worker pool and coordinator")
    parser.add_argument("--workers", type=int, help="Override WORKER_COUNT")
    args = parser.parse_args()
    settings = Settings(**({"worker_count": args.workers} if args.workers is not None else {}))
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    context = multiprocessing.get_context("spawn")
    stop = context.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    run_pool(settings, stop)


if __name__ == "__main__":
    main()
