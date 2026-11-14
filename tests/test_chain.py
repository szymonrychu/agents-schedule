import asyncio
import logging
import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta

import httpx
import psycopg
import pytest
from agents_common.db import open_pool
from agents_common.fake_orchestrator import FakeOrchestrator
from agents_common.orchestrator import OrchestratorClient

from agents_schedule import chain, dispatcher, tracker
from agents_schedule.db import runs, schedules
from agents_schedule.settings import Settings


@pytest.fixture
async def aconn(db_url: str) -> AsyncIterator[psycopg.AsyncConnection]:
    async with await psycopg.AsyncConnection.connect(db_url, autocommit=True) as connection:
        yield connection


@pytest.fixture
async def pool(db_url: str):
    p = await open_pool(db_url, min_size=1, max_size=6)
    yield p
    await p.close()


@pytest.fixture
def fake() -> FakeOrchestrator:
    return FakeOrchestrator()


@pytest.fixture
async def client(fake: FakeOrchestrator) -> AsyncIterator[OrchestratorClient]:
    async with OrchestratorClient(
        "http://orchestrator", fake.token, transport=httpx.ASGITransport(app=fake.app)
    ) as c:
        yield c


@pytest.fixture
def settings(db_url: str) -> Settings:
    return Settings.model_validate(
        {
            "database_url": db_url,
            "orchestrator_url": "http://orchestrator",
            "orchestrator_token": "test-token",
            "max_in_progress": 50,
        }
    )


@pytest.fixture
def preset(fake: FakeOrchestrator) -> uuid.UUID:
    return fake.add_preset("default").id


async def add_schedule(
    aconn, preset: uuid.UUID, name: str, *, enabled: bool = True, prompt: str = "go"
) -> schedules.Schedule:
    return await schedules.create(
        aconn,
        name=name,
        cron="0 3 * * *",
        timezone="UTC",
        preset_id=preset,
        prompt=prompt,
        enabled=enabled,
    )


async def link(aconn, source: schedules.Schedule, *targets: schedules.Schedule) -> None:
    await schedules.set_chain(aconn, source.id, [t.id for t in targets])


async def get(aconn, run: runs.Run) -> runs.Run:
    got = await runs.get(aconn, run.id)
    assert got is not None
    return got


def mid(run: runs.Run) -> int:
    assert run.message_id is not None
    return int(run.message_id)


async def dispatched(aconn, pool, client, settings, sched: schedules.Schedule) -> runs.Run:
    """A manual run of `sched`, dispatched: in-progress with a session and a message."""
    run = await runs.insert_manual(aconn, sched.id, datetime.now(UTC), "new")
    assert await dispatcher.dispatch_once(pool, client, settings)
    got = await get(aconn, run)
    assert got.status == "in-progress" and got.message_id is not None
    return got


async def handled(aconn, pool, client, fake, settings, sched, text="research-done") -> runs.Run:
    """A run of `sched` whose first turn completed with `text`: handled, session active."""
    run = await dispatched(aconn, pool, client, settings, sched)
    fake.complete_turn(mid(run), text)
    assert await tracker.track_once(pool, client, settings, datetime.now(UTC)) == 1
    got = await get(aconn, run)
    assert got.status == "handled"
    return got


async def chained_runs(aconn, target: schedules.Schedule) -> list[runs.Run]:
    return [
        r for r in await runs.list_for_schedule(aconn, target.id, 100) if r.trigger == "chained"
    ]


def session_reads(fake: FakeOrchestrator, session_id) -> int:
    path = f"/api/v1/sessions/{session_id}"
    return sum(1 for r in fake.requests if r.method == "GET" and r.url.path == path)


async def once(pool, client, settings, now: datetime | None = None) -> int:
    return await chain.chain_once(pool, client, settings, now or datetime.now(UTC))


async def test_no_fire_while_session_active_then_fires_on_close(
    aconn, pool, client, fake, settings, preset
):
    a = await add_schedule(aconn, preset, "research")
    b = await add_schedule(aconn, preset, "pill")
    await link(aconn, a, b)
    run = await handled(aconn, pool, client, fake, settings, a)

    assert await once(pool, client, settings) == 0
    assert (await get(aconn, run)).chain_status == "pending"
    assert await chained_runs(aconn, b) == []

    fake.request_close(str(run.session_id))
    assert await once(pool, client, settings) == 1

    assert (await get(aconn, run)).chain_status == "fired"
    (got,) = await chained_runs(aconn, b)
    assert (got.status, got.chain_source, got.chain_ref) == ("new", "schedule", str(run.id))
    assert (got.chained_from, got.chain_depth) == (run.id, 1)
    assert (got.previous_name, got.previous_status) == ("research", "handled")
    assert (got.previous_session_slug, got.previous_result) == (run.session_slug, "research-done")


async def test_fires_when_session_deleting(aconn, pool, client, fake, settings, preset):
    a = await add_schedule(aconn, preset, "a")
    b = await add_schedule(aconn, preset, "b")
    await link(aconn, a, b)
    run = await handled(aconn, pool, client, fake, settings, a)

    fake.discard_session(str(run.session_id))
    assert await once(pool, client, settings) == 1
    assert len(await chained_runs(aconn, b)) == 1


async def test_fires_when_session_gone_with_captured_result(
    aconn, pool, client, fake, settings, preset
):
    a = await add_schedule(aconn, preset, "a")
    b = await add_schedule(aconn, preset, "b")
    await link(aconn, a, b)
    run = await handled(aconn, pool, client, fake, settings, a)

    fake.remove_session(str(run.session_id))  # purged: 404, messages gone too
    assert await once(pool, client, settings) == 1
    (got,) = await chained_runs(aconn, b)
    assert got.previous_result == "research-done"


async def test_second_pass_is_noop(aconn, pool, client, fake, settings, preset):
    a = await add_schedule(aconn, preset, "a")
    b = await add_schedule(aconn, preset, "b")
    await link(aconn, a, b)
    run = await handled(aconn, pool, client, fake, settings, a)
    fake.request_close(str(run.session_id))

    assert await once(pool, client, settings) == 1
    assert await once(pool, client, settings) == 0
    assert len(await chained_runs(aconn, b)) == 1


async def test_concurrent_passes_fire_once(aconn, pool, client, fake, settings, preset):
    a = await add_schedule(aconn, preset, "a")
    b = await add_schedule(aconn, preset, "b")
    c = await add_schedule(aconn, preset, "c")
    await link(aconn, a, b, c)
    run = await handled(aconn, pool, client, fake, settings, a)
    fake.request_close(str(run.session_id))

    moved = await asyncio.gather(*(once(pool, client, settings) for _ in range(3)))

    assert sum(moved) == 1
    assert len(await chained_runs(aconn, b)) == 1
    assert len(await chained_runs(aconn, c)) == 1


async def test_partial_fan_out_failure_rolls_back_and_retries_once(
    aconn, pool, client, fake, settings, preset, monkeypatch
):
    a = await add_schedule(aconn, preset, "a")
    b = await add_schedule(aconn, preset, "b")
    c = await add_schedule(aconn, preset, "c")
    await link(aconn, a, b, c)
    run = await handled(aconn, pool, client, fake, settings, a)
    fake.request_close(str(run.session_id))
    second = max(b.id, c.id)
    real = runs.insert_chained
    broken = {"on": True}

    async def flaky(conn, target, *args, **kwargs):
        if target == second and broken["on"]:
            raise RuntimeError("database hiccup")
        return await real(conn, target, *args, **kwargs)

    monkeypatch.setattr(runs, "insert_chained", flaky)

    assert await once(pool, client, settings) == 0
    assert (await get(aconn, run)).chain_status == "pending"
    assert await chained_runs(aconn, b) == [] and await chained_runs(aconn, c) == []

    broken["on"] = False
    assert await once(pool, client, settings) == 1
    assert len(await chained_runs(aconn, b)) == 1
    assert len(await chained_runs(aconn, c)) == 1


async def test_self_close_inside_first_turn_fires_once_handled(
    aconn, pool, client, fake, settings, preset
):
    a = await add_schedule(aconn, preset, "a")
    b = await add_schedule(aconn, preset, "b")
    await link(aconn, a, b)
    run = await dispatched(aconn, pool, client, settings, a)
    fake.request_close(str(run.session_id))

    assert await once(pool, client, settings) == 0  # still in progress
    assert (await get(aconn, run)).chain_status == "pending"

    fake.complete_turn(mid(run), "early")
    assert await tracker.track_once(pool, client, settings, datetime.now(UTC)) == 1
    assert await once(pool, client, settings) == 1
    (got,) = await chained_runs(aconn, b)
    assert got.previous_result == "early"


async def test_failed_source_skipped(aconn, pool, client, fake, settings, preset):
    a = await add_schedule(aconn, preset, "a")
    b = await add_schedule(aconn, preset, "b")
    await link(aconn, a, b)
    run = await dispatched(aconn, pool, client, settings, a)
    late = datetime.now(UTC) + timedelta(seconds=settings.run_timeout_seconds + 1)
    assert await tracker.track_once(pool, client, settings, late) == 1
    assert (await get(aconn, run)).reason == "timeout"
    fake.request_close(str(run.session_id))

    assert await once(pool, client, settings) == 1
    assert (await get(aconn, run)).chain_status == "skipped"
    assert await chained_runs(aconn, b) == []


async def test_ignored_source_skipped(aconn, pool, client, fake, settings, preset):
    a = await add_schedule(aconn, preset, "a", enabled=False)
    b = await add_schedule(aconn, preset, "b")
    await link(aconn, a, b)
    run = await runs.insert_scheduled(aconn, a.id, datetime.now(UTC), "new")
    assert run is not None and run.chain_status == "pending"
    assert await dispatcher.dispatch_once(pool, client, settings)
    assert (await get(aconn, run)).status == "ignored"

    assert await once(pool, client, settings) == 1
    assert (await get(aconn, run)).chain_status == "skipped"
    assert await chained_runs(aconn, b) == []


async def test_delivered_source_fires_at_handled_without_polling(
    aconn, pool, client, fake, settings
):
    reuse = fake.add_preset("reuse", spec={"reuse_running_agent": True}).id
    a = await add_schedule(aconn, reuse, "a")
    b = await add_schedule(aconn, reuse, "b")
    first = await dispatched(aconn, pool, client, settings, a)
    assert await runs.transition(aconn, first.id, "in-progress", "handled")
    await link(aconn, a, b)  # only the delivered run snapshots the link
    run = await dispatched(aconn, pool, client, settings, a)
    assert run.reuse == "delivered"
    fake.mark_sent(mid(run))
    assert await tracker.track_once(pool, client, settings, datetime.now(UTC)) == 1
    reads = session_reads(fake, run.session_id)

    assert await once(pool, client, settings) == 1

    assert session_reads(fake, run.session_id) == reads
    (got,) = await chained_runs(aconn, b)
    assert got.previous_session_slug == run.session_slug


async def test_fan_out_each_target_once(aconn, pool, client, fake, settings, preset):
    a = await add_schedule(aconn, preset, "a")
    b = await add_schedule(aconn, preset, "b")
    c = await add_schedule(aconn, preset, "c")
    await link(aconn, a, b, c)
    run = await handled(aconn, pool, client, fake, settings, a)
    fake.request_close(str(run.session_id))

    assert await once(pool, client, settings) == 1
    for target in (b, c):
        (got,) = await chained_runs(aconn, target)
        assert (got.status, got.chained_from) == ("new", run.id)


async def test_target_in_flight_gets_ignored_run(aconn, pool, client, fake, settings, preset):
    a = await add_schedule(aconn, preset, "a")
    b = await add_schedule(aconn, preset, "b")
    await link(aconn, a, b)
    run = await handled(aconn, pool, client, fake, settings, a)
    await runs.insert_manual(aconn, b.id, datetime.now(UTC), "new")
    fake.request_close(str(run.session_id))

    assert await once(pool, client, settings) == 1
    (got,) = await chained_runs(aconn, b)
    assert (got.status, got.reason) == ("ignored", "previous_running")
    assert (await get(aconn, run)).chain_status == "fired"


async def test_deleted_target_skipped_other_fires(aconn, pool, client, fake, settings, preset):
    a = await add_schedule(aconn, preset, "a")
    b = await add_schedule(aconn, preset, "b")
    c = await add_schedule(aconn, preset, "c")
    await link(aconn, a, b, c)
    run = await handled(aconn, pool, client, fake, settings, a)
    await schedules.delete(aconn, b.id)
    fake.request_close(str(run.session_id))

    assert await once(pool, client, settings) == 1
    assert len(await chained_runs(aconn, c)) == 1
    assert await runs.list_for_schedule(aconn, b.id, 10) == []


async def test_depth_limit(aconn, pool, client, fake, settings, preset, caplog):
    a = await add_schedule(aconn, preset, "a")
    b = await add_schedule(aconn, preset, "b")
    await link(aconn, a, b)
    run = await handled(aconn, pool, client, fake, settings, a)
    await aconn.execute(
        "UPDATE runs SET chain_depth = %s WHERE id = %s", (runs.MAX_CHAIN_DEPTH, run.id)
    )
    fake.request_close(str(run.session_id))
    caplog.set_level(logging.INFO, logger="agents_schedule.chain")

    assert await once(pool, client, settings) == 1
    assert await chained_runs(aconn, b) == []
    assert "depth" in caplog.text

    # depth 9 still chains, to depth 10
    a2 = await add_schedule(aconn, preset, "a2")
    await link(aconn, a2, b)
    run2 = await handled(aconn, pool, client, fake, settings, a2)
    await aconn.execute(
        "UPDATE runs SET chain_depth = %s WHERE id = %s", (runs.MAX_CHAIN_DEPTH - 1, run2.id)
    )
    fake.request_close(str(run2.session_id))
    assert await once(pool, client, settings) == 1
    (got,) = await chained_runs(aconn, b)
    assert got.chain_depth == runs.MAX_CHAIN_DEPTH


async def test_pending_chain_expires(aconn, pool, client, fake, settings, preset, caplog):
    a = await add_schedule(aconn, preset, "a")
    b = await add_schedule(aconn, preset, "b")
    await link(aconn, a, b)
    run = await handled(aconn, pool, client, fake, settings, a)
    caplog.set_level(logging.INFO, logger="agents_schedule.chain")
    reads = session_reads(fake, run.session_id)

    late = run.created_at + timedelta(seconds=settings.chain_max_wait_seconds + 1)
    assert await once(pool, client, settings, late) == 1

    assert (await get(aconn, run)).chain_status == "expired"
    assert session_reads(fake, run.session_id) == reads
    assert sum("expired" in r.getMessage() for r in caplog.records) == 1
    fake.request_close(str(run.session_id))
    assert await once(pool, client, settings, late) == 0
    assert await chained_runs(aconn, b) == []


async def test_chain_of_three(aconn, pool, client, fake, settings, preset):
    a = await add_schedule(aconn, preset, "a")
    b = await add_schedule(aconn, preset, "b")
    c = await add_schedule(aconn, preset, "c")
    await link(aconn, a, b)
    await link(aconn, b, c)
    run = await handled(aconn, pool, client, fake, settings, a)
    fake.request_close(str(run.session_id))

    assert await once(pool, client, settings) == 1
    (got,) = await chained_runs(aconn, b)
    assert (got.then_schedule_ids, got.chain_status) == ([c.id], "pending")

    # B runs, finishes and closes: C gets a depth-2 run fed B's result
    assert await dispatcher.dispatch_once(pool, client, settings)
    got = await get(aconn, got)
    fake.complete_turn(mid(got), "pill-done")
    assert await tracker.track_once(pool, client, settings, datetime.now(UTC)) == 1
    fake.request_close(str(got.session_id))
    assert await once(pool, client, settings) == 1
    (last,) = await chained_runs(aconn, c)
    assert (last.chain_depth, last.previous_name, last.previous_result) == (2, "b", "pill-done")


async def test_orchestrator_unavailable_keeps_pending(
    aconn, pool, client, fake, settings, preset, caplog
):
    a = await add_schedule(aconn, preset, "a")
    b = await add_schedule(aconn, preset, "b")
    await link(aconn, a, b)
    run = await handled(aconn, pool, client, fake, settings, a)
    fake.request_close(str(run.session_id))
    caplog.set_level(logging.WARNING, logger="agents_schedule.chain")
    now = datetime.now(UTC)

    for offset, expected_logs in ((0, 1), (10, 1), (61, 2)):
        fake.fail_next("get_session", 503)
        assert await once(pool, client, settings, now + timedelta(seconds=offset)) == 0
        assert (await get(aconn, run)).chain_status == "pending"
        assert len(caplog.records) == expected_logs

    assert await once(pool, client, settings, now) == 1
    assert len(await chained_runs(aconn, b)) == 1


async def test_run_chain_stops(pool, client, settings):
    stop = asyncio.Event()
    stop.set()
    await asyncio.wait_for(chain.run_chain(pool, client, settings, stop), timeout=5)
