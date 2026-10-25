"""The scheduler: enabled schedules fire on time, once per fire time (design D5, D6, D9; A3).

`tick` runs one pass. Per enabled schedule, in one transaction under a per-schedule advisory
lock: the stamp is the latest scheduled fire time, else `armed_at` (whichever is later); when a
fire time is due in `(stamp, now]` the latest one is inserted, `ignored` with reason
`previous_running` when the schedule already has `SCHED_MAX_PER_SCHEDULE` runs new or
in-progress, else `new` with a NOTIFY on `dispatcher.CHANNEL`. Either insert advances the stamp.
Earlier due fire times are logged and get no row (one catch-up run). The insert is keyed on
`(schedule_id, fire_time)`, so a re-run of the same tick, from any process, adds nothing.

`run_scheduler` loops `tick`, sleeping until the earliest next fire time or `SCHED_TICK_SECONDS`,
whichever is sooner, and waking early on NOTIFY `schedules_changed`.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from zoneinfo import ZoneInfo

import psycopg
from agents_common.db import listen, notify
from psycopg_pool import AsyncConnectionPool

from agents_schedule import cron, dispatcher
from agents_schedule.db import runs, schedules
from agents_schedule.settings import Settings

log = logging.getLogger(__name__)

CHANNEL = "schedules_changed"
"""NOTIFY channel the api sends on every schedule write; it wakes the scheduler."""


@dataclass(frozen=True)
class TickResult:
    created: int
    ignored: int
    skipped: int


@dataclass(frozen=True)
class _Fired:
    status: str
    skipped: int


async def tick(pool: AsyncConnectionPool, settings: Settings, now: datetime) -> TickResult:
    """Fire every enabled schedule due at `now`; a failing schedule is logged and skipped."""
    async with pool.connection() as conn:
        enabled = await schedules.list_enabled(conn)
    created = ignored = skipped = 0
    for sched in enabled:
        try:
            async with pool.connection() as conn:
                fired = await _fire(conn, sched.id, settings.max_per_schedule, now)
        except Exception:
            log.exception("schedule %s (%s) failed to fire, skipped", sched.name, sched.id)
            continue
        if fired is None:
            continue
        if fired.status == "new":
            created += 1
        else:
            ignored += 1
        skipped += fired.skipped
    return TickResult(created=created, ignored=ignored, skipped=skipped)


async def _fire(
    conn: psycopg.AsyncConnection, schedule_id: uuid.UUID, max_per_schedule: int, now: datetime
) -> _Fired | None:
    """Insert the schedule's due run, if any; None when nothing was due or it was recorded."""
    async with conn.transaction():
        await runs.fire_lock(conn, schedule_id)
        sched = await schedules.get(conn, schedule_id)
        if sched is None or not sched.enabled:  # changed since the tick listed it
            return None
        last = await runs.last_scheduled_fire(conn, sched.id)
        stamp = max(last, sched.armed_at) if last is not None else sched.armed_at
        due = cron.due(sched.cron, ZoneInfo(sched.timezone), stamp, now)
        if due.fire_time is None:
            return None
        if await runs.in_flight(conn, sched.id) >= max_per_schedule:
            run = await runs.insert_scheduled(
                conn, sched.id, due.fire_time, "ignored", "previous_running"
            )
        else:
            run = await runs.insert_scheduled(conn, sched.id, due.fire_time, "new")
            if run is not None:
                await notify(conn, dispatcher.CHANNEL)
        if run is None:
            return None
    if due.skipped > 0:
        log.info(
            "missed %d fire times of %s before %s",
            due.skipped,
            sched.name,
            due.fire_time.isoformat(),
        )
    if run.status == "ignored":
        log.info("schedule %s fired at %s while running, ignored", sched.name, run.fire_time)
    return _Fired(status=run.status, skipped=due.skipped)


async def next_fire(pool: AsyncConnectionPool, now: datetime) -> datetime | None:
    """The earliest fire time after `now` over enabled schedules; None when there is none."""
    async with pool.connection() as conn:
        enabled = await schedules.list_enabled(conn)
    earliest: datetime | None = None
    for sched in enabled:
        try:
            (fire,) = cron.next_fires(sched.cron, ZoneInfo(sched.timezone), now, 1)
        except Exception:
            continue  # tick logs it
        if earliest is None or fire < earliest:
            earliest = fire
    return earliest


async def run_scheduler(pool: AsyncConnectionPool, settings: Settings, stop: asyncio.Event) -> None:
    """Tick, then sleep until the earliest next fire, `SCHED_TICK_SECONDS` or NOTIFY
    `schedules_changed`, whichever comes first; until `stop` is set."""
    wake = asyncio.Event()
    listener = asyncio.create_task(_wake_on_notify(settings.database_url.get_secret_value(), wake))
    try:
        while not stop.is_set():
            wake.clear()
            now = datetime.now(UTC)
            await tick(pool, settings, now)
            seconds = settings.tick_seconds
            fire = await next_fire(pool, now)
            if fire is not None:
                until = (fire - datetime.now(UTC)).total_seconds()
                seconds = max(0.0, min(seconds, until))
            await _sleep(seconds, wake, stop)
    finally:
        listener.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await listener


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
