import asyncio
import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta

import psycopg
import pytest

from agents_schedule.db import runs

T0 = datetime(2026, 1, 1, tzinfo=UTC)
NOW = datetime(2026, 6, 1, tzinfo=UTC)


def at(minutes: int) -> datetime:
    return T0 + timedelta(minutes=minutes)


@pytest.fixture
async def aconn(db_url: str) -> AsyncIterator[psycopg.AsyncConnection]:
    async with await psycopg.AsyncConnection.connect(db_url, autocommit=True) as connection:
        yield connection


async def add_schedule(conn, name="nightly") -> uuid.UUID:
    sid = uuid.uuid4()
    await conn.execute(
        "INSERT INTO schedules (id, name, cron, timezone, preset_id, prompt, armed_at) "
        "VALUES (%s, %s, '0 3 * * *', 'UTC', %s, 'go', now())",
        (sid, name, uuid.uuid4()),
    )
    return sid


async def test_insert_scheduled_once(aconn):
    sid = await add_schedule(aconn)
    first = await runs.insert_scheduled(aconn, sid, at(0), "new")
    assert first is not None
    assert (first.status, first.trigger, first.fire_time) == ("new", "scheduled", at(0))
    assert await runs.insert_scheduled(aconn, sid, at(0), "ignored", "x") is None
    cur = await aconn.execute("SELECT count(*), min(status) FROM runs")
    assert await cur.fetchone() == (1, "new")


async def test_manual_run_at_fire_time_does_not_suppress_scheduled(aconn):
    sid = await add_schedule(aconn)
    manual = await runs.insert_manual(aconn, sid, at(0), "new")
    again = await runs.insert_manual(aconn, sid, at(0), "new")
    assert manual.id != again.id
    scheduled = await runs.insert_scheduled(aconn, sid, at(0), "new")
    assert scheduled is not None
    assert await runs.insert_scheduled(aconn, sid, at(0), "new") is None
    cur = await aconn.execute("SELECT count(*) FROM runs")
    assert await cur.fetchone() == (3,)


async def test_insert_scheduled_ignored_keeps_reason(aconn):
    sid = await add_schedule(aconn)
    run = await runs.insert_scheduled(aconn, sid, at(0), "ignored", "missed")
    assert (run.status, run.reason) == ("ignored", "missed")


async def test_ignored_insert_sets_finished_at(aconn):
    sid = await add_schedule(aconn)
    run = await runs.insert_scheduled(aconn, sid, at(0), "ignored", "previous_running")
    assert run.finished_at is not None
    manual = await runs.insert_manual(aconn, sid, NOW, "ignored", "x")
    assert manual.finished_at is not None


async def test_new_insert_has_no_finished_at(aconn):
    sid = await add_schedule(aconn)
    run = await runs.insert_scheduled(aconn, sid, at(0), "new")
    assert run.finished_at is None
    assert (await runs.insert_manual(aconn, sid, NOW, "new")).finished_at is None


async def test_insert_manual(aconn):
    sid = await add_schedule(aconn)
    run = await runs.insert_manual(aconn, sid, NOW, "new")
    assert (run.trigger, run.fire_time, run.status) == ("manual", NOW, "new")
    assert (await runs.get(aconn, run.id)) == run
    assert await runs.get(aconn, uuid.uuid4()) is None


async def test_transition_is_status_guarded(aconn):
    sid = await add_schedule(aconn)
    run = await runs.insert_scheduled(aconn, sid, at(0), "new")
    assert await runs.transition(aconn, run.id, "new", "in-progress", started_at=NOW)
    assert not await runs.transition(aconn, run.id, "new", "failed", reason="stale")
    got = await runs.get(aconn, run.id)
    assert (got.status, got.reason, got.started_at) == ("in-progress", None, NOW)
    assert got.updated_at >= run.updated_at


async def test_set_fields_is_guarded_and_keeps_status(aconn):
    sid = await add_schedule(aconn)
    run = await runs.insert_scheduled(aconn, sid, at(0), "new")
    session_id = uuid.uuid4()
    assert not await runs.set_fields(aconn, run.id, "in-progress", session_slug="s")
    assert await runs.set_fields(
        aconn,
        run.id,
        "new",
        session_slug="s",
        session_id=session_id,
        message_id="m",
        reuse="delivered",
        attempts=2,
    )
    got = await runs.get(aconn, run.id)
    assert (got.status, got.session_slug, got.session_id) == ("new", "s", session_id)
    assert (got.message_id, got.reuse, got.attempts) == ("m", "delivered", 2)


async def test_claim_next_caps_and_fifo(aconn):
    a, b = await add_schedule(aconn, "a"), await add_schedule(aconn, "b")
    r_b1 = await runs.insert_scheduled(aconn, b, at(1), "new")
    r_a1 = await runs.insert_scheduled(aconn, a, at(2), "new")
    r_a2 = await runs.insert_scheduled(aconn, a, at(3), "new")
    r_b2 = await runs.insert_scheduled(aconn, b, at(4), "new")

    first = await runs.claim_next(aconn, 3, 1, NOW)
    assert (first.id, first.status, first.started_at) == (r_b1.id, "in-progress", NOW)
    second = await runs.claim_next(aconn, 3, 1, NOW)
    assert second.id == r_a1.id
    # both schedules are at their per-schedule cap
    assert await runs.claim_next(aconn, 3, 1, NOW) is None
    assert await runs.transition(aconn, r_b1.id, "in-progress", "handled")
    third = await runs.claim_next(aconn, 3, 1, NOW)
    assert third.id == r_b2.id
    # global cap of 2 now reached (a1, b2 in progress)
    assert await runs.claim_next(aconn, 2, 5, NOW) is None
    assert await runs.transition(aconn, r_a1.id, "in-progress", "handled")
    assert (await runs.claim_next(aconn, 2, 5, NOW)).id == r_a2.id
    assert await runs.claim_next(aconn, 9, 9, NOW) is None


async def test_claim_next_per_schedule_order(aconn):
    a, b = await add_schedule(aconn, "a"), await add_schedule(aconn, "b")
    a1 = await runs.insert_scheduled(aconn, a, at(1), "new")
    a2 = await runs.insert_scheduled(aconn, a, at(2), "new")
    b1 = await runs.insert_scheduled(aconn, b, at(3), "new")
    # a1 is held by the per-schedule cap (a is full), a2 must not overtake it
    busy = await runs.insert_scheduled(aconn, a, at(0), "new")
    assert (await runs.claim_next(aconn, 9, 1, NOW)).id == busy.id
    assert (await runs.claim_next(aconn, 9, 1, NOW)).id == b1.id
    assert await runs.claim_next(aconn, 9, 1, NOW) is None
    assert await runs.transition(aconn, busy.id, "in-progress", "handled")
    assert (await runs.claim_next(aconn, 9, 1, NOW)).id == a1.id
    assert await runs.transition(aconn, a1.id, "in-progress", "handled")
    assert (await runs.claim_next(aconn, 9, 1, NOW)).id == a2.id


async def test_claim_skip_locked(aconn, db_url):
    sids = [await add_schedule(aconn, f"s{i}") for i in range(6)]
    created = {(await runs.insert_scheduled(aconn, s, at(i), "new")).id for i, s in enumerate(sids)}
    conns = [await psycopg.AsyncConnection.connect(db_url, autocommit=True) for _ in range(6)]
    try:
        got = await asyncio.gather(*(runs.claim_next(c, 99, 99, NOW) for c in conns))
    finally:
        for c in conns:
            await c.close()
    ids = [r.id for r in got]
    assert None not in got
    assert len(set(ids)) == 6
    assert set(ids) == created


async def test_last_scheduled_fire_ignores_manual(aconn):
    sid = await add_schedule(aconn)
    assert await runs.last_scheduled_fire(aconn, sid) is None
    await runs.insert_scheduled(aconn, sid, at(1), "new")
    await runs.insert_scheduled(aconn, sid, at(3), "ignored", "missed")
    await runs.insert_manual(aconn, sid, at(9), "new")
    assert await runs.last_scheduled_fire(aconn, sid) == at(3)


async def test_in_flight(aconn):
    sid = await add_schedule(aconn)
    new = await runs.insert_scheduled(aconn, sid, at(1), "new")
    prog = await runs.insert_scheduled(aconn, sid, at(2), "new")
    await runs.insert_scheduled(aconn, sid, at(3), "ignored", "x")
    done = await runs.insert_scheduled(aconn, sid, at(4), "new")
    assert await runs.in_flight(aconn, sid) == 3
    await runs.transition(aconn, prog.id, "new", "in-progress")
    await runs.transition(aconn, done.id, "new", "handled")
    assert await runs.in_flight(aconn, sid) == 2
    assert new.id


async def test_latest_with_session(aconn):
    sid, other = await add_schedule(aconn), await add_schedule(aconn, "other")
    assert await runs.latest_with_session(aconn, sid, uuid.uuid4()) is None
    r1 = await runs.insert_scheduled(aconn, sid, at(1), "new")
    r2 = await runs.insert_scheduled(aconn, sid, at(2), "new")
    await runs.insert_scheduled(aconn, sid, at(3), "ignored", "x")
    await runs.insert_scheduled(aconn, other, at(4), "new")
    r4 = await runs.insert_manual(aconn, sid, at(5), "new")
    assert (await runs.latest_with_session(aconn, sid, r4.id)).id == r2.id
    assert (await runs.latest_with_session(aconn, sid, r2.id)).id == r4.id
    assert (await runs.latest_with_session(aconn, sid, uuid.uuid4())).id == r4.id
    await runs.transition(aconn, r1.id, "new", "failed")
    assert r1.id != (await runs.latest_with_session(aconn, sid, r2.id)).id


async def test_listings(aconn):
    sid, other = await add_schedule(aconn), await add_schedule(aconn, "other")
    r1 = await runs.insert_scheduled(aconn, sid, at(1), "new")
    r2 = await runs.insert_scheduled(aconn, sid, at(2), "new")
    r3 = await runs.insert_scheduled(aconn, other, at(3), "new")
    await runs.transition(aconn, r2.id, "new", "in-progress")
    assert [r.id for r in await runs.list_for_schedule(aconn, sid, 10)] == [r2.id, r1.id]
    assert [r.id for r in await runs.list_for_schedule(aconn, sid, 1)] == [r2.id]
    assert [r.id for r in await runs.list_recent(aconn)] == [r3.id, r2.id, r1.id]
    assert [r.id for r in await runs.list_recent(aconn, "new", 1)] == [r3.id]
    assert [r.id for r in await runs.list_in_progress(aconn)] == [r2.id]
