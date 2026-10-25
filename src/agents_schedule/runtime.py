"""Process entry points: the api server and the single-instance worker (design D9, D13)."""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Iterator

import uvicorn
from agents_common.db import open_pool
from agents_common.orchestrator import OrchestratorClient
from agents_common.runtime import advisory_singleton, run_service
from psycopg_pool import AsyncConnectionPool
from starlette.types import ASGIApp

from agents_schedule.api.app import create_app
from agents_schedule.dispatcher import run_dispatcher
from agents_schedule.scheduler import run_scheduler
from agents_schedule.settings import Settings
from agents_schedule.tracker import run_tracker

log = logging.getLogger(__name__)

LOCK_NAME = "agents-schedule-worker"
LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s %(message)s"


def configure_logging(settings: Settings) -> None:
    logging.basicConfig(level=settings.log_level.upper(), format=LOG_FORMAT, force=True)


def _client(settings: Settings) -> OrchestratorClient:
    return OrchestratorClient(
        settings.orchestrator_url, settings.orchestrator_token.get_secret_value()
    )


class _Server(uvicorn.Server):
    """uvicorn without its own signal handling: run_service owns SIGTERM and SIGINT."""

    @contextlib.contextmanager
    def capture_signals(self) -> Iterator[None]:
        yield


def _api_config(settings: Settings, app: ASGIApp) -> uvicorn.Config:
    return uvicorn.Config(
        app,
        host=settings.http_host,
        port=settings.http_port,
        log_level=settings.log_level.lower(),
        timeout_graceful_shutdown=int(settings.shutdown_grace_seconds),
        proxy_headers=False,  # the allowlist must see the socket peer, never X-Forwarded-For
    )


async def _serve(settings: Settings, stop: asyncio.Event) -> None:
    pool = await open_pool(settings.database_url.get_secret_value(), min_size=1, max_size=10)
    client = _client(settings)
    try:
        app = create_app(settings, pool, client)
        server = _Server(_api_config(settings, app))
        serving = asyncio.create_task(server.serve())
        stopping = asyncio.create_task(stop.wait())
        await asyncio.wait({serving, stopping}, return_when=asyncio.FIRST_COMPLETED)
        server.should_exit = True
        stopping.cancel()
        await serving
    finally:
        await client.aclose()
        await pool.close()


def api_main(settings: Settings) -> int:
    """Serve the http api until SIGTERM or SIGINT; the exit code of the process."""
    configure_logging(settings)
    return asyncio.run(
        run_service(
            lambda stop: _serve(settings, stop), grace_seconds=settings.shutdown_grace_seconds
        )
    )


async def _hold_and_run(
    settings: Settings,
    pool: AsyncConnectionPool,
    client: OrchestratorClient,
    stop: asyncio.Event,
    holding: asyncio.Event,
) -> None:
    log.info("worker waiting for the lock %s", LOCK_NAME)
    async with advisory_singleton(pool, LOCK_NAME):
        holding.set()
        log.info("worker holds the lock %s: scheduler, dispatcher and tracker start", LOCK_NAME)
        async with asyncio.TaskGroup() as group:
            group.create_task(run_scheduler(pool, settings, stop), name="scheduler")
            group.create_task(run_dispatcher(pool, client, settings, stop), name="dispatcher")
            group.create_task(run_tracker(pool, client, settings, stop), name="tracker")


async def _work(settings: Settings, stop: asyncio.Event) -> None:
    pool = await open_pool(settings.database_url.get_secret_value(), min_size=2, max_size=10)
    client = _client(settings)
    holding = asyncio.Event()
    try:
        holder = asyncio.create_task(_hold_and_run(settings, pool, client, stop, holding))
        stopping = asyncio.create_task(stop.wait())
        await asyncio.wait({holder, stopping}, return_when=asyncio.FIRST_COMPLETED)
        stopping.cancel()
        if not holder.done() and not holding.is_set():
            holder.cancel()  # still queued for the lock: nothing to stop
        with contextlib.suppress(asyncio.CancelledError):
            await holder
    finally:
        await client.aclose()
        await pool.close()


def worker_main(settings: Settings) -> int:
    """Run the scheduler, dispatcher and tracker under the singleton lock; the exit code."""
    configure_logging(settings)
    return asyncio.run(
        run_service(
            lambda stop: _work(settings, stop), grace_seconds=settings.shutdown_grace_seconds
        )
    )
