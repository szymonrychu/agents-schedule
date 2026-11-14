"""The run outcome tracker: every in-progress run ends handled or failed with a reason (D10, D15).

`track_once` polls the message of each in-progress run once, on agents-common `runs.track`
(first turn of a created session) or `runs.track_delivery` (a prompt delivered to a reused
session). Every change is the guarded `in-progress -> handled|failed` transition of the
dispatcher's `RunsStore`, so a run already moved by another process is left alone. A handled
run stores its turn's final text (`result_text`, capped by agents-common) in that same update,
so it survives the session being purged and feeds a chained run's `previous_result`. A delivered
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


class TransientLog:
    """Logs a run's transient orchestrator error at most once per `LOG_INTERVAL`."""

    def __init__(self, logger: logging.Logger, what: str) -> None:
        self._log = logger
        self._what = what
        self._last: dict[uuid.UUID, datetime] = {}

    def keep_only(self, run_ids: set[uuid.UUID]) -> None:
        """Forget the runs no longer polled."""
        for gone in set(self._last) - run_ids:
            del self._last[gone]

    def warn(self, run_id: uuid.UUID, exc: Exception, now: datetime) -> None:
        last = self._last.get(run_id)
        if last is None or now - last >= LOG_INTERVAL:
            self._last[run_id] = now
            self._log.warning("run %s: %s failed, will retry: %s", run_id, self._what, exc)


_transient = TransientLog(log, "tracking")


class _TrackStore(RunsStore):
    """The runs store that finishes a handled run at its turn's `completed_at` and stores the
    turn's final text (`result_text`) in the same guarded update."""

    def __init__(self, pool: AsyncConnectionPool, client: common_runs.RecordingClient) -> None:
        super().__init__(pool)
        self._client = client

    async def finish(
        self, run_id: common_runs.RunId, status: common_runs.RunStatus, reason: str | None
    ) -> bool:
        if status != "handled":
            return await super().finish(run_id, status, reason)
        message = self._client.message
        turn = message.turn if message is not None else None
        finished_at = turn.completed_at if turn is not None and turn.completed_at else None
        async with self._pool.connection() as conn:
            return await runs.transition(
                conn,
                uuid.UUID(str(run_id)),
                "in-progress",
                status,
                reason=reason,
                finished_at=finished_at or datetime.now(UTC),
                result_text=common_runs.result_text(message),
            )

    async def reset_to_fallback(self, run_id: uuid.UUID) -> bool:
        """Forget the delivery in one guarded update so the dispatcher creates a session."""
        return await self._set(run_id, reuse="fallback", session_id=None, message_id=None)


async def track_once(
    pool: AsyncConnectionPool, client: OrchestratorClient, settings: Settings, now: datetime
) -> int:
    """Poll every in-progress run with a message once; returns how many runs it moved."""
    async with pool.connection() as conn:
        active = [r for r in await runs.list_in_progress(conn) if r.message_id is not None]
    _transient.keep_only({r.id for r in active})
    moved = 0
    for run in active:
        recording = common_runs.RecordingClient(client)
        store = _TrackStore(pool, recording)
        try:
            if await _track(recording, store, settings, run, now):
                moved += 1
        except (OrchestratorError, OrchestratorUnavailable) as exc:
            _transient.warn(run.id, exc, now)
        except Exception:
            log.exception("run %s: unexpected tracking error, will retry", run.id)
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
