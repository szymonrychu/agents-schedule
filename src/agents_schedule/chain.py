"""The chain watcher: a handled run whose session finished starts its next schedules.

A run inserted with links (`then_schedule_ids`, snapshotted at insert) has `chain_status`
`pending`. `chain_once` decides each pending run once per pass:

- source `ignored` or `failed` (timeout included): `skipped`, nothing chains;
- source `new` or `in-progress`: wait, even when its session already asked to close (a session
  may self-close inside its first turn; the chain fires once the run is handled);
- source `handled`: a delivered run (prompt posted into a reused session the run does not own)
  is finished at handled; any other run is finished when agents-common `session_finished` says
  the session asked to close, is no longer desired `active` or is gone (404), whichever first.
  Then one transaction flips `pending -> fired` with a guarded update first (a pass that loses
  the race rolls back) and inserts one chained run per target (`fire_chain`); the unique index
  `runs_chain_once` is the second guard, so a retry after a partial failure never doubles a run;
- pending for longer than `SCHED_CHAIN_MAX_WAIT_SECONDS` (checked before polling): `expired`.

An orchestrator error keeps the run pending and is logged once per run per minute.
`previous_result` is the `result_text` the tracker captured at handled, never a session read.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import psycopg
from agents_common import runs as common_runs
from agents_common.db import notify
from agents_common.orchestrator import (
    OrchestratorClient,
    OrchestratorError,
    OrchestratorUnavailable,
)
from psycopg_pool import AsyncConnectionPool

from agents_schedule import dispatcher
from agents_schedule.db import runs, schedules
from agents_schedule.settings import Settings
from agents_schedule.tracker import TransientLog

log = logging.getLogger(__name__)

_transient = TransientLog(log, "chain check")


@dataclass(frozen=True)
class ChainSource:
    """What a chained run records about the run (or event) it follows."""

    chain_source: str
    chain_ref: str
    chained_from: uuid.UUID | None
    depth: int
    previous_name: str
    previous_status: str
    previous_session_slug: str | None
    previous_result: str | None


async def fire_chain(
    conn: psycopg.AsyncConnection,
    *,
    targets: list[uuid.UUID],
    source: ChainSource,
    max_per_schedule: int,
    now: datetime,
) -> list[runs.Run]:
    """Insert one chained run per target, like run now; call inside a transaction.

    Targets go in id order under each one's fire lock. A deleted target is skipped; past
    `MAX_CHAIN_DEPTH` nothing is inserted; a target at its in-flight cap gets an `ignored`
    `previous_running` run (visible in history, and it uses up this source's slot). A target
    this source already chained into is left alone. Returns the inserted runs.
    """
    depth = source.depth + 1
    if depth > runs.MAX_CHAIN_DEPTH:
        log.info(
            "chain %s %s: depth %d exceeds %d, not chained",
            source.chain_source,
            source.chain_ref,
            depth,
            runs.MAX_CHAIN_DEPTH,
        )
        return []
    fields = {
        "chain_source": source.chain_source,
        "chain_ref": source.chain_ref,
        "chained_from": source.chained_from,
        "chain_depth": depth,
        "previous_name": source.previous_name,
        "previous_status": source.previous_status,
        "previous_session_slug": source.previous_session_slug,
        "previous_result": source.previous_result,
    }
    inserted: list[runs.Run] = []
    for target in sorted(set(targets)):
        await runs.fire_lock(conn, target)
        if await schedules.get(conn, target) is None:
            log.info("chain %s: target schedule %s is gone, skipped", source.chain_ref, target)
            continue
        if await runs.in_flight(conn, target) >= max_per_schedule:
            run = await runs.insert_chained(
                conn, target, now, "ignored", "previous_running", **fields
            )
        else:
            run = await runs.insert_chained(conn, target, now, "new", None, **fields)
            if run is not None:
                await notify(conn, dispatcher.CHANNEL)
        if run is not None:
            inserted.append(run)
    return inserted


async def chain_once(
    pool: AsyncConnectionPool, client: OrchestratorClient, settings: Settings, now: datetime
) -> int:
    """Decide every pending chain once; returns how many runs left `pending`."""
    async with pool.connection() as conn:
        pending = await runs.list_chain_pending(conn)
    _transient.keep_only({r.id for r in pending})
    moved = 0
    for run in pending:
        try:
            if await _advance(pool, client, settings, run, now):
                moved += 1
        except (OrchestratorError, OrchestratorUnavailable) as exc:
            _transient.warn(run.id, exc, now)
        except Exception:
            log.exception("run %s: unexpected chain error, will retry", run.id)
    return moved


async def _set(pool: AsyncConnectionPool, run: runs.Run, status: str) -> bool:
    async with pool.connection() as conn:
        return await runs.set_chain_status(conn, run.id, status)


async def _advance(
    pool: AsyncConnectionPool,
    client: OrchestratorClient,
    settings: Settings,
    run: runs.Run,
    now: datetime,
) -> bool:
    if run.status in ("ignored", "failed"):
        log.info("run %s: %s, chain skipped", run.id, run.status)
        return await _set(pool, run, "skipped")
    if now - run.created_at > timedelta(seconds=settings.chain_max_wait_seconds):
        moved = await _set(pool, run, "expired")
        if moved:
            log.info("run %s: chain expired after %ss", run.id, settings.chain_max_wait_seconds)
        return moved
    if run.status != "handled":
        return False
    owns_session = run.reuse != "delivered" and run.session_id is not None
    if owns_session and not await common_runs.session_finished(client, str(run.session_id)):
        return False
    return await _fire(pool, settings, run, now)


async def _fire(
    pool: AsyncConnectionPool, settings: Settings, run: runs.Run, now: datetime
) -> bool:
    async with pool.connection() as conn, conn.transaction():
        if not await runs.set_chain_status(conn, run.id, "fired"):
            return False  # another pass won; nothing written here
        schedule = await schedules.get(conn, run.schedule_id)
        source = ChainSource(
            chain_source="schedule",
            chain_ref=str(run.id),
            chained_from=run.id,
            depth=run.chain_depth,
            previous_name=schedule.name if schedule is not None else "",
            previous_status="handled",
            previous_session_slug=run.session_slug,
            previous_result=run.result_text,
        )
        fired = await fire_chain(
            conn,
            targets=list(run.then_schedule_ids or []),
            source=source,
            max_per_schedule=settings.max_per_schedule,
            now=now,
        )
    log.info("run %s: chain fired %d run(s)", run.id, len(fired))
    return True


async def run_chain(
    pool: AsyncConnectionPool,
    client: OrchestratorClient,
    settings: Settings,
    stop: asyncio.Event,
) -> None:
    """`chain_once` every `SCHED_POLL_SECONDS` until `stop` is set."""
    while not stop.is_set():
        await chain_once(pool, client, settings, datetime.now(UTC))
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(stop.wait(), timeout=settings.poll_seconds)
