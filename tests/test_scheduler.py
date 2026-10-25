import asyncio
import logging
import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta

import psycopg
import pytest
from agents_common.db import notify, open_pool

from agents_schedule import dispatcher, scheduler
from agents_schedule.db import runs, schedules
from agents_schedule.scheduler import TickResult
from agents_schedule.settings import Settings

T0 = datetime(2026, 3, 1, 0, 0, tzinfo=UTC)
HOUR = timedelta(hours=1)
HOURLY = "0 * * * *"


@pytest.fixture
async def aconn(db_url: str) -> AsyncIterator[psycopg.AsyncConnection]:
    async with await psycopg.AsyncConnection.connect(db_url, autocommit=True) as connection:
        yield connection


@pytest.fixture
async def pool(db_url: str):
    p = await open_pool(db_url, min_size=1, max_size=4)
    yield p
    await p.close()


def make_settings(db_url: str, **overrides) -> Settings:
    return Settings.model_validate(
        {
            "database_url": db_url,
            "orchestrator_url": "http://orchestrator",
            "orchestrator_token": "test-token",
            **overrides,
        }
    )


@pytest.fixture
def settings(db_url: str) -> Settings:
    return make_settings(db_url)


async def add_schedule(
    conn,
    name: str = "nightly",
    cron: str = HOURLY,
    timezone: str = "UTC",
    enabled: bool = True,
    armed_at: datetime | None = T0,
) -> schedules.Schedule:
    sched = await schedules.create(
        conn,
        name=name,
        cron=cron,
        timezone=timezone,
        preset_id=uuid.uuid4(),
        prompt="go",
        enabled=enabled,
    )
    if armed_at is not None:
        await conn.execute("UPDATE schedules SET armed_at = %s WHERE id = %s", (armed_at, sched.id))
    got = await schedules.get(conn, sched.id)
    assert got is not None
    return got


async def all_runs(conn, schedule_id: uuid.UUID) -> list[runs.Run]:
    return sorted(await runs.list_for_schedule(conn, schedule_id, 100), key=lambda r: r.fire_time)


async def test_due_schedule_creates_new_run(aconn, pool, settings):
    sched = await add_schedule(aconn)

    assert await scheduler.tick(pool, settings, T0 + HOUR / 2) == TickResult(0, 0, 0)
    assert await all_runs(aconn, sched.id) == []

    assert await scheduler.tick(pool, settings, T0 + HOUR) == TickResult(1, 0, 0)
    (run,) = await all_runs(aconn, sched.id)
    assert (run.trigger, run.status, run.reason) == ("scheduled", "new", None)
    assert run.fire_time == T0 + HOUR


async def test_new_run_notifies_dispatcher(aconn, pool, settings, db_url):
    sched = await add_schedule(aconn)
    async with await psycopg.AsyncConnection.connect(db_url, autocommit=True) as listener:
        await listener.execute(f"LISTEN {dispatcher.CHANNEL}")

        assert await scheduler.tick(pool, settings, T0 + HOUR) == TickResult(1, 0, 0)
        async with asyncio.timeout(5):
            note = await anext(listener.notifies())
        assert note.channel == dispatcher.CHANNEL

        # an ignored fire wakes nobody
        assert await scheduler.tick(pool, settings, T0 + 2 * HOUR) == TickResult(0, 1, 0)
        with pytest.raises(TimeoutError):
            async with asyncio.timeout(0.5):
                await anext(listener.notifies())
    assert len(await all_runs(aconn, sched.id)) == 2


async def test_tick_is_exactly_once(aconn, pool, settings, db_url):
    sched = await add_schedule(aconn)
    now = T0 + HOUR

    assert await scheduler.tick(pool, settings, now) == TickResult(1, 0, 0)
    assert await scheduler.tick(pool, settings, now) == TickResult(0, 0, 0)
    assert len(await all_runs(aconn, sched.id)) == 1

    # two processes at once, on their own connections, same and later fire times
    other = await open_pool(db_url, min_size=1, max_size=2)
    try:
        for when in (now + HOUR, now + 2 * HOUR):
            results = await asyncio.gather(
                scheduler.tick(pool, settings, when), scheduler.tick(other, settings, when)
            )
            assert sum(r.created + r.ignored for r in results) == 1
    finally:
        await other.close()
    got = await all_runs(aconn, sched.id)
    assert [r.fire_time for r in got] == [now, now + HOUR, now + 2 * HOUR]
    assert [r.status for r in got] == ["new", "ignored", "ignored"]


async def test_missed_runs_collapse(aconn, pool, settings, caplog):
    sched = await add_schedule(aconn)
    caplog.set_level(logging.INFO, logger="agents_schedule.scheduler")

    result = await scheduler.tick(pool, settings, T0 + 5 * HOUR + timedelta(minutes=30))

    assert result == TickResult(1, 0, 4)
    (run,) = await all_runs(aconn, sched.id)
    assert (run.fire_time, run.status) == (T0 + 5 * HOUR, "new")
    fire = (T0 + 5 * HOUR).isoformat()
    missed = [r.getMessage() for r in caplog.records if r.getMessage().startswith("missed")]
    assert missed == [f"missed 4 fire times of nightly before {fire}"]

    # a re-run of the same tick logs nothing more and creates nothing
    caplog.clear()
    assert await scheduler.tick(pool, settings, T0 + 5 * HOUR) == TickResult(0, 0, 0)
    assert not [r for r in caplog.records if r.getMessage().startswith("missed")]


async def test_overlap_recorded_ignored_and_stamp_advances(aconn, pool, settings):
    sched = await add_schedule(aconn)

    assert await scheduler.tick(pool, settings, T0 + HOUR) == TickResult(1, 0, 0)
    assert await scheduler.tick(pool, settings, T0 + 2 * HOUR) == TickResult(0, 1, 0)
    # the stamp moved past the ignored fire: nothing is due again
    assert await scheduler.tick(pool, settings, T0 + 2 * HOUR) == TickResult(0, 0, 0)

    first, ignored = await all_runs(aconn, sched.id)
    assert (ignored.fire_time, ignored.status, ignored.reason) == (
        T0 + 2 * HOUR,
        "ignored",
        "previous_running",
    )

    # still in flight when in progress
    assert await runs.transition(aconn, first.id, "new", "in-progress")
    assert await scheduler.tick(pool, settings, T0 + 3 * HOUR) == TickResult(0, 1, 0)

    # finished: the next fire runs again, for its own fire time only
    assert await runs.transition(aconn, first.id, "in-progress", "handled")
    assert await scheduler.tick(pool, settings, T0 + 4 * HOUR) == TickResult(1, 0, 0)
    got = await all_runs(aconn, sched.id)
    assert [(r.fire_time, r.status) for r in got] == [
        (T0 + HOUR, "handled"),
        (T0 + 2 * HOUR, "ignored"),
        (T0 + 3 * HOUR, "ignored"),
        (T0 + 4 * HOUR, "new"),
    ]


async def test_overlap_honours_max_per_schedule(aconn, pool, db_url):
    settings = make_settings(db_url, max_per_schedule=2)
    sched = await add_schedule(aconn)

    assert await scheduler.tick(pool, settings, T0 + HOUR) == TickResult(1, 0, 0)
    assert await scheduler.tick(pool, settings, T0 + 2 * HOUR) == TickResult(1, 0, 0)
    assert await scheduler.tick(pool, settings, T0 + 3 * HOUR) == TickResult(0, 1, 0)
    assert [r.status for r in await all_runs(aconn, sched.id)] == ["new", "new", "ignored"]


async def test_disabled_and_rearmed(aconn, pool, settings):
    sched = await add_schedule(aconn, enabled=False)

    # disabled: nothing fires however much time passes
    assert await scheduler.tick(pool, settings, T0 + 10 * HOUR) == TickResult(0, 0, 0)
    assert await all_runs(aconn, sched.id) == []

    # a run from before the schedule was disabled
    assert await runs.insert_scheduled(aconn, sched.id, T0, "handled") is not None

    # re-enabled: armed_at moves to now, so the disabled period never fires or counts as missed
    rearmed = await schedules.update(
        aconn,
        sched.id,
        name=sched.name,
        cron=sched.cron,
        timezone=sched.timezone,
        preset_id=sched.preset_id,
        prompt=sched.prompt,
        enabled=True,
    )
    armed = rearmed.armed_at
    assert armed > T0 + 10 * HOUR
    assert await scheduler.tick(pool, settings, armed) == TickResult(0, 0, 0)
    assert await scheduler.tick(pool, settings, armed + HOUR) == TickResult(1, 0, 0)
    got = await all_runs(aconn, sched.id)
    assert [r.fire_time for r in got][0] == T0
    assert len(got) == 2
    assert armed < got[1].fire_time <= armed + HOUR


async def test_bad_schedule_isolated(aconn, pool, settings, caplog):
    bad_cron = await add_schedule(aconn, name="a-bad-cron", cron="not a cron at all")
    bad_tz = await add_schedule(aconn, name="b-bad-tz", timezone="Nowhere/Atlantis")
    good = await add_schedule(aconn, name="c-good")

    assert await scheduler.tick(pool, settings, T0 + HOUR) == TickResult(1, 0, 0)

    assert await all_runs(aconn, bad_cron.id) == []
    assert await all_runs(aconn, bad_tz.id) == []
    assert len(await all_runs(aconn, good.id)) == 1
    errors = [r.getMessage() for r in caplog.records if r.levelno >= logging.ERROR]
    assert any("a-bad-cron" in m for m in errors), errors
    assert any("b-bad-tz" in m for m in errors), errors


async def test_next_fire_is_earliest_enabled(aconn, pool):
    await add_schedule(aconn, name="hourly")
    await add_schedule(aconn, name="quarter", cron="*/15 * * * *", enabled=False)
    await add_schedule(aconn, name="broken", cron="nope")
    await add_schedule(aconn, name="daily", cron="0 3 * * *")

    assert await scheduler.next_fire(pool, T0 + timedelta(minutes=5)) == T0 + HOUR


async def test_next_fire_none_without_schedules(pool):
    assert await scheduler.next_fire(pool, T0) is None


async def test_run_scheduler_wakes_on_schedules_changed(aconn, pool, db_url):
    settings = make_settings(db_url, tick_seconds=300)
    stop = asyncio.Event()
    task = asyncio.create_task(scheduler.run_scheduler(pool, settings, stop))
    try:
        # the first tick sees no schedules, then the loop sleeps the full tick interval
        await asyncio.sleep(0.5)
        sched = await add_schedule(aconn, cron="* * * * *", armed_at=datetime.now(UTC) - HOUR)
        async with asyncio.timeout(10):
            while not await all_runs(aconn, sched.id):
                await notify(aconn, scheduler.CHANNEL)
                await asyncio.sleep(0.1)
    finally:
        stop.set()
        async with asyncio.timeout(10):
            await task
    (run,) = await all_runs(aconn, sched.id)
    assert run.status == "new"
