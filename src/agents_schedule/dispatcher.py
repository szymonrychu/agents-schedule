"""The run dispatcher: one session and one prompt per run, under the caps (D7, D8, D9, D15).

`dispatch_once` claims the oldest eligible `new` run (`new -> in-progress`) and sends it; every
in-progress run without a message is resumed by `resume_pending`. Sending is agents-common:

- reuse (D15): when the run has not decided yet (`reuse` null, no session), the preset opts in
  (`Preset.reuse_running_agent`) and the schedule's previous run has a session that
  `runs.running_session` reports running, the run records `reuse = delivered` with that session
  in one guarded update and posts there with `runs.deliver`. `deliver` returning None (session
  gone or being deleted), or a previous session that is not running, records `reuse = fallback`
  (and clears the session) and the run is created as below;
- create: `runs.dispatch` creates the session from the preset with the run's slug (409 resolves
  the existing session by slug), records it, adopts a message carrying the marker or posts one,
  and records the message.

Each step is skipped when its id is already recorded, so a restart at any point creates no
second session and posts no second prompt. Errors: a missing preset fails `preset_not_found`;
a refusal fails `orchestrator_refused: <code>`; a transient error keeps the run in progress with
`attempts + 1`, resumed after a backoff, and fails `orchestrator_unavailable` at
`SCHED_DISPATCH_MAX_ATTEMPTS`.
"""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import logging
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from agents_common import runs as common_runs
from agents_common.db import listen
from agents_common.orchestrator import (
    NotFound,
    OrchestratorClient,
    OrchestratorError,
    OrchestratorUnavailable,
)
from psycopg_pool import AsyncConnectionPool

from agents_schedule.db import runs, schedules
from agents_schedule.prompt import render_prompt, session_slug
from agents_schedule.settings import Settings

log = logging.getLogger(__name__)

RUN_KIND = "schedule"
CHANNEL = "runs_new"
"""NOTIFY channel that wakes the dispatcher when a `new` run is inserted."""
MAX_BACKOFF_SECONDS = 300.0


def _uuid(run_id: common_runs.RunId) -> uuid.UUID:
    """The runs-table id of an agents-common RunRef id (`str(run.id)`)."""
    return uuid.UUID(str(run_id))


class RunsStore:
    """agents-common `RunStore` over the runs table; every write is guarded on in-progress."""

    def __init__(self, pool: AsyncConnectionPool) -> None:
        self._pool = pool

    async def _set(self, run_id: uuid.UUID, **fields: Any) -> bool:
        async with self._pool.connection() as conn:
            return await runs.set_fields(conn, run_id, "in-progress", **fields)

    async def _move(self, run_id: uuid.UUID, status: str, **fields: Any) -> bool:
        async with self._pool.connection() as conn:
            return await runs.transition(
                conn,
                run_id,
                "in-progress",
                status,
                finished_at=datetime.now(UTC),
                **fields,
            )

    async def set_session(self, run_id: common_runs.RunId, session_id: str) -> None:
        await self._set(_uuid(run_id), session_id=uuid.UUID(session_id))

    async def set_message(self, run_id: common_runs.RunId, message_id: str) -> None:
        await self._set(_uuid(run_id), message_id=message_id)

    async def finish(
        self, run_id: common_runs.RunId, status: common_runs.RunStatus, reason: str | None
    ) -> bool:
        return await self._move(_uuid(run_id), status, reason=reason)

    async def set_slug(self, run_id: uuid.UUID, slug: str) -> bool:
        return await self._set(run_id, session_slug=slug)

    async def set_reuse(self, run_id: uuid.UUID, reuse: str, session_id: str | None) -> bool:
        """Record the reuse decision and the session it targets in one guarded update."""
        sid = uuid.UUID(session_id) if session_id is not None else None
        return await self._set(run_id, reuse=reuse, session_id=sid)

    async def ignore(self, run_id: uuid.UUID, reason: str) -> bool:
        return await self._move(run_id, "ignored", reason=reason)

    async def record_attempt(self, run_id: uuid.UUID, attempts: int, max_attempts: int) -> bool:
        """Count a transient failure; fail the run `orchestrator_unavailable` at the cap."""
        if attempts >= max_attempts:
            return await self._move(
                run_id, "failed", reason="orchestrator_unavailable", attempts=attempts
            )
        return await self._set(run_id, attempts=attempts)


class _PresetGone(Exception):
    """`get_preset` answered 404."""


def backoff(settings: Settings, attempts: int) -> timedelta:
    """Wait before resuming a run after its `attempts`-th transient failure."""
    if attempts <= 0:
        return timedelta(0)
    seconds = settings.poll_seconds * 2 ** (attempts - 1)
    return timedelta(seconds=min(seconds, MAX_BACKOFF_SECONDS))


async def dispatch_once(
    pool: AsyncConnectionPool, client: OrchestratorClient, settings: Settings
) -> bool:
    """Claim one `new` run under the caps and send it; False when nothing was claimable."""
    async with pool.connection() as conn:
        run = await runs.claim_next(
            conn, settings.max_in_progress, settings.max_per_schedule, datetime.now(UTC)
        )
    if run is None:
        return False
    await _send_isolated(pool, client, settings, run)
    return True


async def resume_pending(
    pool: AsyncConnectionPool,
    client: OrchestratorClient,
    settings: Settings,
    now: datetime | None = None,
) -> int:
    """Continue every in-progress run without a message whose backoff has passed.

    Returns how many runs were resumed.
    """
    now = now or datetime.now(UTC)
    async with pool.connection() as conn:
        pending = [r for r in await runs.list_in_progress(conn) if r.message_id is None]
    resumed = 0
    for run in pending:
        if now < run.updated_at + backoff(settings, run.attempts):
            continue
        if await _send_isolated(pool, client, settings, run):
            resumed += 1
    return resumed


async def run_dispatcher(
    pool: AsyncConnectionPool,
    client: OrchestratorClient,
    settings: Settings,
    stop: asyncio.Event,
) -> None:
    """Resume, then dispatch until nothing is claimable, then sleep `SCHED_POLL_SECONDS` or
    until NOTIFY `runs_new`; until `stop` is set."""
    wake = asyncio.Event()
    listener = asyncio.create_task(_wake_on_notify(settings.database_url.get_secret_value(), wake))
    try:
        while not stop.is_set():
            wake.clear()
            await resume_pending(pool, client, settings)
            while not stop.is_set() and await dispatch_once(pool, client, settings):
                pass
            await _sleep(settings.poll_seconds, wake, stop)
    finally:
        listener.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await listener


async def _send_isolated(
    pool: AsyncConnectionPool, client: OrchestratorClient, settings: Settings, run: runs.Run
) -> bool:
    """`_send` that logs an unexpected error and returns False, so one run never stops the loop.

    The run keeps its recorded ids, so a later pass resumes it without a second session or post.
    """
    try:
        await _send(pool, client, settings, run)
    except Exception:
        log.exception("run %s: unexpected error, will retry", run.id)
        return False
    return True


async def _send(
    pool: AsyncConnectionPool, client: OrchestratorClient, settings: Settings, run: runs.Run
) -> None:
    store = RunsStore(pool)
    async with pool.connection() as conn:
        schedule = await schedules.get(conn, run.schedule_id)
    if schedule is None:  # deleted: its runs went with it
        return
    undecided = run.reuse is None and run.session_id is None
    if undecided and run.trigger == "scheduled" and not schedule.enabled:
        await store.ignore(run.id, "schedule_disabled")
        log.info("run %s: schedule %s is disabled, ignored", run.id, schedule.name)
        return
    if run.session_slug is None:
        slug = session_slug(schedule.name, run.id)
        await store.set_slug(run.id, slug)
        run = dataclasses.replace(run, session_slug=slug)
    assert run.session_slug is not None and run.started_at is not None

    binding = common_runs.PresetBinding(str(schedule.preset_id), schedule.prompt)
    assert binding.prompt_template is not None
    fired_at = run.fire_time.astimezone(ZoneInfo(schedule.timezone))
    text = render_prompt(binding.prompt_template, schedule.name, fired_at)
    marker = common_runs.marker(RUN_KIND, str(run.id))
    ref = common_runs.RunRef(
        id=str(run.id),
        slug=run.session_slug,
        preset_id=binding.preset_id,
        session_id=str(run.session_id) if run.session_id is not None else None,
        message_id=run.message_id,
        started_at=run.started_at,
    )
    try:
        reuse = run.reuse
        if undecided:
            reuse, target = await _decide_reuse(pool, client, run, binding)
            if reuse is not None:
                await store.set_reuse(run.id, reuse, target)
                ref = dataclasses.replace(ref, session_id=target)
        if reuse == "delivered":
            assert ref.session_id is not None
            sent = await common_runs.deliver(client, store, ref, ref.session_id, text, marker)
            if sent is not None:
                log.info("run %s: delivered to session %s", run.id, ref.session_id)
                return
            log.info("run %s: session %s refused the prompt, falling back", run.id, ref.session_id)
            await store.set_reuse(run.id, "fallback", None)
            ref = dataclasses.replace(ref, session_id=None)
        sent = await common_runs.dispatch(client, store, ref, text, marker)
        log.info("run %s: dispatched to session %s", run.id, sent.session_id)
    except _PresetGone:
        await store.finish(ref.id, "failed", "preset_not_found")
        log.warning("run %s: preset %s not found", run.id, binding.preset_id)
    except NotFound as exc:  # runs.dispatch already failed the run with its reason
        log.warning("run %s: failed: %s", run.id, exc)
    except OrchestratorUnavailable as exc:
        attempts = run.attempts + 1
        await store.record_attempt(run.id, attempts, settings.dispatch_max_attempts)
        log.warning("run %s: orchestrator unavailable (attempt %d): %s", run.id, attempts, exc)
    except OrchestratorError as exc:
        await store.finish(ref.id, "failed", f"orchestrator_refused: {exc.code}")
        log.warning("run %s: orchestrator refused: %s", run.id, exc)


async def _decide_reuse(
    pool: AsyncConnectionPool,
    client: OrchestratorClient,
    run: runs.Run,
    binding: common_runs.PresetBinding,
) -> tuple[str | None, str | None]:
    """`(reuse, session_id)` for a run that has not decided yet (design D15).

    `("delivered", id)` when the preset opts in and the schedule's previous run's session runs;
    `("fallback", None)` when the preset opts in but that session is not running; `(None, None)`
    when the preset does not opt in or no previous run has a session.
    """
    try:
        preset = await client.get_preset(binding.preset_id)
    except NotFound as exc:
        raise _PresetGone from exc
    if not preset.reuse_running_agent:
        return None, None
    async with pool.connection() as conn:
        previous = await runs.latest_with_session(conn, run.schedule_id, run.id)
    if previous is None or previous.session_id is None:
        return None, None
    session = await common_runs.running_session(client, str(previous.session_id))
    if session is None:
        log.info("run %s: session %s is not running", run.id, previous.session_id)
        return "fallback", None
    return "delivered", str(session.id)


async def _wake_on_notify(url: str, wake: asyncio.Event) -> None:
    async for _ in listen(url, CHANNEL):
        wake.set()


async def _sleep(seconds: float, *events: asyncio.Event) -> None:
    """Sleep `seconds` or until any of `events` is set."""
    waits = [asyncio.create_task(e.wait()) for e in events]
    try:
        await asyncio.wait(waits, timeout=seconds, return_when=asyncio.FIRST_COMPLETED)
    finally:
        for w in waits:
            w.cancel()
