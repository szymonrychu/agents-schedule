"""The run outcome tracker: every in-progress run ends handled or failed with a reason (D10, D15).

`track_once` polls the message of each in-progress run once, on agents-common `runs.track`
(first turn of a created session) or `runs.track_delivery` (a prompt delivered to a reused
session). Every change is the guarded `in-progress -> handled|failed` transition of the
dispatcher's `RunsStore`, so a run already moved by another process is left alone. A delivered
run whose message is gone is reset to `reuse = fallback` with session and message cleared, and
the dispatcher resumes it with a new session. A transient orchestrator error leaves the run
unchanged and is logged once per run per minute. The service never deletes sessions: cleanup
is the preset's autoclose.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import uuid
from datetime import UTC, datetime, timedelta

from agents_common import runs as common_runs
from agents_common.orchestrator import (
    Message,
    OrchestratorClient,
    OrchestratorError,
    OrchestratorUnavailable,
)
from psycopg_pool import AsyncConnectionPool

from agents_schedule.db import runs
from agents_schedule.dispatcher import RunsStore
from agents_schedule.settings import Settings

log = logging.getLogger(__name__)

LOG_INTERVAL = timedelta(minutes=1)
_last_logged: dict[uuid.UUID, datetime] = {}
"""When the transient error of each run was last logged."""


class _TrackStore(RunsStore):
    """The runs store that finishes a handled run at its turn's `completed_at`."""

    def __init__(self, pool: AsyncConnectionPool) -> None:
        super().__init__(pool)
        self.message: Message | None = None

    async def finish(
        self, run_id: common_runs.RunId, status: common_runs.RunStatus, reason: str | None
    ) -> bool:
        turn = self.message.turn if self.message is not None else None
        if status == "handled" and turn is not None and turn.completed_at is not None:
            async with self._pool.connection() as conn:
                return await runs.transition(
                    conn,
                    uuid.UUID(str(run_id)),
                    "in-progress",
                    status,
                    reason=reason,
                    finished_at=turn.completed_at,
                )
        return await super().finish(run_id, status, reason)

    async def reset_to_fallback(self, run_id: uuid.UUID) -> bool:
        """Forget the delivery in one guarded update so the dispatcher creates a session."""
        return await self._set(run_id, reuse="fallback", session_id=None, message_id=None)


class _RecordingClient(OrchestratorClient):
    """The client of `inner`, remembering the last message it read for the store."""

    def __init__(self, inner: OrchestratorClient, store: _TrackStore) -> None:
        self._inner = inner
        self._store = store

    async def get_message(self, message_id: int) -> Message:
        message = await self._inner.get_message(message_id)
        self._store.message = message
        return message


async def track_once(
    pool: AsyncConnectionPool, client: OrchestratorClient, settings: Settings, now: datetime
) -> int:
    """Poll every in-progress run with a message once; returns how many runs it moved."""
    async with pool.connection() as conn:
        active = [r for r in await runs.list_in_progress(conn) if r.message_id is not None]
    for gone in set(_last_logged) - {r.id for r in active}:
        del _last_logged[gone]
    moved = 0
    for run in active:
        store = _TrackStore(pool)
        try:
            if await _track(_RecordingClient(client, store), store, settings, run, now):
                moved += 1
        except (OrchestratorError, OrchestratorUnavailable) as exc:
            _log_transient(run, exc, now)
    return moved


async def _track(
    client: OrchestratorClient,
    store: _TrackStore,
    settings: Settings,
    run: runs.Run,
    now: datetime,
) -> bool:
    assert run.message_id is not None and run.started_at is not None
    ref = common_runs.RunRef(
        id=str(run.id),
        slug=run.session_slug or "",
        preset_id="",
        session_id=str(run.session_id) if run.session_id is not None else None,
        message_id=run.message_id,
        started_at=run.started_at,
    )
    timeout = settings.run_timeout_seconds
    if run.reuse != "delivered":
        status = await common_runs.track(client, store, ref, now, timeout)
        if status is not None:
            log.info("run %s: %s", run.id, status)
        return status is not None
    outcome = await common_runs.track_delivery(client, store, ref, timeout_seconds=timeout, now=now)
    if outcome == "gone":
        log.info("run %s: delivered message gone, falling back to a new session", run.id)
        return await store.reset_to_fallback(run.id)
    if outcome == "handled":
        log.info("run %s: handled", run.id)
        return True
    return (now - run.started_at).total_seconds() > timeout


def _log_transient(run: runs.Run, exc: Exception, now: datetime) -> None:
    last = _last_logged.get(run.id)
    if last is None or now - last >= LOG_INTERVAL:
        _last_logged[run.id] = now
        log.warning("run %s: tracking failed, will retry: %s", run.id, exc)


async def run_tracker(
    pool: AsyncConnectionPool,
    client: OrchestratorClient,
    settings: Settings,
    stop: asyncio.Event,
) -> None:
    """`track_once` every `SCHED_POLL_SECONDS` until `stop` is set."""
    while not stop.is_set():
        await track_once(pool, client, settings, datetime.now(UTC))
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(stop.wait(), timeout=settings.poll_seconds)
