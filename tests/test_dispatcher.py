import asyncio
import re
import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

import httpx
import psycopg
import pytest
from agents_common.db import notify, open_pool
from agents_common.fake_orchestrator import FakeOrchestrator
from agents_common.orchestrator import OrchestratorClient

from agents_schedule import dispatcher
from agents_schedule.db import runs, schedules
from agents_schedule.prompt import render_prompt, session_slug
from agents_schedule.settings import Settings

FIRE = datetime(2026, 3, 1, 2, 0, tzinfo=UTC)
LATER = timedelta(hours=1)
LABEL = re.compile(r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$")


@pytest.fixture
async def aconn(db_url: str) -> AsyncIterator[psycopg.AsyncConnection]:
    async with await psycopg.AsyncConnection.connect(db_url, autocommit=True) as connection:
        yield connection


@pytest.fixture
async def pool(db_url: str):
    p = await open_pool(db_url, min_size=1, max_size=4)
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


def make_settings(db_url: str, **overrides) -> Settings:
    return Settings(
        database_url=db_url,
        orchestrator_url="http://orchestrator",
        orchestrator_token="test-token",
        **overrides,
    )


@pytest.fixture
def settings(db_url: str) -> Settings:
    return make_settings(db_url)


async def add_schedule(
    conn,
    preset_id: uuid.UUID,
    name: str = "Nightly Build",
    prompt: str = "Run {schedule_name} at {fired_at}; keep {braces} and {{this}}",
    timezone: str = "Europe/Warsaw",
    enabled: bool = True,
) -> schedules.Schedule:
    return await schedules.create(
        conn,
        name=name,
        cron="0 3 * * *",
        timezone=timezone,
        preset_id=preset_id,
        prompt=prompt,
        enabled=enabled,
    )


async def add_run(conn, schedule_id: uuid.UUID, fire_time: datetime = FIRE) -> runs.Run:
    run = await runs.insert_scheduled(conn, schedule_id, fire_time, "new")
    assert run is not None
    return run


def marker_of(run_id: uuid.UUID) -> str:
    return f"<!-- agents-schedule:{run_id} -->"


def creates(fake: FakeOrchestrator) -> int:
    return sum(1 for r in fake.requests if r.method == "POST" and r.url.path == "/api/v1/sessions")


def posts(fake: FakeOrchestrator) -> int:
    return sum(1 for r in fake.requests if r.method == "POST" and r.url.path.endswith("/messages"))


async def finish(conn, run_id: uuid.UUID) -> None:
    assert await runs.transition(conn, run_id, "in-progress", "handled")


async def dispatch_previous(aconn, pool, client, settings, sched) -> runs.Run:
    """Run one earlier run of `sched` to completion of dispatch and mark it handled."""
    prev = await add_run(aconn, sched.id, FIRE - timedelta(days=1))
    assert await dispatcher.dispatch_once(pool, client, settings)
    await finish(aconn, prev.id)
    got = await runs.get(aconn, prev.id)
    assert got.session_id is not None
    return got


def test_render_prompt():
    fired = datetime(2026, 3, 1, 3, 0, tzinfo=ZoneInfo("Europe/Warsaw"))
    template = "{schedule_name} fired at {fired_at}; {other} {} {{x}} {schedule_name"
    assert render_prompt(template, "nightly", fired) == (
        "nightly fired at 2026-03-01T03:00:00+01:00; {other} {} {{x}} {schedule_name"
    )
    # a replaced value is never expanded again
    assert render_prompt("{schedule_name}", "{fired_at}", fired) == "{fired_at}"
    assert render_prompt("no placeholders %s {0}", "n", fired) == "no placeholders %s {0}"


def test_slug_is_dns_label():
    names = [
        "Nightly Build",
        "a",
        "!!!",
        "---x---",
        "Zażółć gęślą jaźń",
        "x" * 100,
        "y" * 23 + "-z",
        "orchestrator",
        "Orchestrator-Main",
        "orchestratorx",
        "UPPER_case.with.dots",
        "1234567890" * 10,
    ]
    for name in names:
        run_id = uuid.uuid4()
        slug = session_slug(name, run_id)
        assert len(slug) <= 40, slug
        assert LABEL.fullmatch(slug), slug
        assert slug != "orchestrator" and not slug.startswith("orchestrator-"), slug
        assert slug.endswith("-" + run_id.hex[:8]), slug
        base = slug.removesuffix("-" + run_id.hex[:8])
        assert 1 <= len(base) <= 24, slug
        assert session_slug(name, run_id) == slug
    rid = uuid.UUID("0123456789abcdef0123456789abcdef")
    assert session_slug("Nightly Build", rid) == "nightly-build-01234567"
    assert session_slug("x" * 100, rid) == "x" * 24 + "-01234567"


async def test_dispatch_happy_path(aconn, pool, client, fake, settings):
    preset = fake.add_preset("default")
    sched = await add_schedule(aconn, preset.id)
    run = await add_run(aconn, sched.id)

    assert await dispatcher.dispatch_once(pool, client, settings) is True
    assert await dispatcher.dispatch_once(pool, client, settings) is False

    got = await runs.get(aconn, run.id)
    assert got.status == "in-progress"
    assert got.session_slug == session_slug("Nightly Build", run.id)
    (session,) = fake.sessions.values()
    assert session["slug"] == got.session_slug
    assert session["preset_id"] == str(preset.id)
    assert got.session_id == uuid.UUID(session["id"])
    (message,) = fake.messages.values()
    assert got.message_id == str(message["id"])
    assert message["session_id"] == session["id"]
    fired = FIRE.astimezone(ZoneInfo("Europe/Warsaw")).isoformat()
    assert message["text"] == (
        f"Run Nightly Build at {fired}; keep {{braces}} and {{{{this}}}}\n\n{marker_of(run.id)}"
    )
    assert got.reuse is None
    assert got.attempts == 0


async def test_resume_never_repeats(aconn, pool, client, fake, settings):
    preset = fake.add_preset("default")
    sched = await add_schedule(aconn, preset.id)

    # crash after create: the post fails transiently, the run keeps its session
    run = await add_run(aconn, sched.id)
    fake.fail_next("post_message", 503)
    assert await dispatcher.dispatch_once(pool, client, settings)
    got = await runs.get(aconn, run.id)
    assert got.status == "in-progress"
    assert got.session_id is not None and got.message_id is None
    assert await dispatcher.resume_pending(pool, client, settings, datetime.now(UTC) + LATER) == 1
    got = await runs.get(aconn, run.id)
    assert got.message_id is not None
    assert (creates(fake), posts(fake), len(fake.sessions), len(fake.messages)) == (1, 2, 1, 1)

    # crash after post, before the message id was recorded: the marker is adopted
    await aconn.execute("UPDATE runs SET message_id = NULL WHERE id = %s", (run.id,))
    assert await dispatcher.resume_pending(pool, client, settings, datetime.now(UTC) + LATER) == 1
    again = await runs.get(aconn, run.id)
    assert again.message_id == got.message_id
    assert again.session_id == got.session_id
    assert (creates(fake), posts(fake), len(fake.messages)) == (1, 2, 1)

    # crash after claim, before create: the resume creates exactly one session
    await finish(aconn, run.id)
    other = await add_run(aconn, sched.id, FIRE + timedelta(days=1))
    await runs.transition(aconn, other.id, "new", "in-progress", started_at=datetime.now(UTC))
    assert await dispatcher.resume_pending(pool, client, settings, datetime.now(UTC)) == 1
    assert await dispatcher.resume_pending(pool, client, settings, datetime.now(UTC)) == 0
    resumed = await runs.get(aconn, other.id)
    assert resumed.session_id is not None and resumed.message_id is not None
    assert (len(fake.sessions), len(fake.messages)) == (2, 2)


async def test_conflict_resolves_existing_session(aconn, pool, client, fake, settings):
    preset = fake.add_preset("default")
    sched = await add_schedule(aconn, preset.id)
    run = await add_run(aconn, sched.id)
    existing = str(uuid.uuid4())
    fake.sessions[existing] = {
        "id": existing,
        "slug": session_slug(sched.name, run.id),
        "preset_id": str(preset.id),
        "desired": "active",
    }

    assert await dispatcher.dispatch_once(pool, client, settings)

    got = await runs.get(aconn, run.id)
    assert got.session_id == uuid.UUID(existing)
    assert len(fake.sessions) == 1
    (message,) = fake.messages.values()
    assert message["session_id"] == existing
    assert got.message_id == str(message["id"])


async def test_global_cap_fifo(aconn, pool, client, fake, db_url):
    settings = make_settings(db_url, max_in_progress=2, max_per_schedule=1)
    preset = fake.add_preset("default")
    s1 = await add_schedule(aconn, preset.id, name="one")
    s2 = await add_schedule(aconn, preset.id, name="two")
    s3 = await add_schedule(aconn, preset.id, name="three")
    late = await add_run(aconn, s1.id, FIRE + timedelta(minutes=30))
    first = await add_run(aconn, s2.id, FIRE)
    second = await add_run(aconn, s3.id, FIRE + timedelta(minutes=10))
    queued = await add_run(aconn, s2.id, FIRE + timedelta(minutes=20))

    while await dispatcher.dispatch_once(pool, client, settings):
        pass

    status = {r: (await runs.get(aconn, r.id)).status for r in (first, second, late, queued)}
    assert status == {first: "in-progress", second: "in-progress", late: "new", queued: "new"}
    cur = await aconn.execute("SELECT count(*) FROM runs WHERE status = 'in-progress'")
    assert await cur.fetchone() == (2,)

    # one slot frees: the oldest waiting run whose schedule has room goes next
    await finish(aconn, second.id)
    assert await dispatcher.dispatch_once(pool, client, settings)
    assert not await dispatcher.dispatch_once(pool, client, settings)
    assert (await runs.get(aconn, late.id)).status == "in-progress"
    assert (await runs.get(aconn, queued.id)).status == "new"

    await finish(aconn, first.id)
    assert await dispatcher.dispatch_once(pool, client, settings)
    assert (await runs.get(aconn, queued.id)).status == "in-progress"
    assert len(fake.sessions) == 4


class _Refused(httpx.AsyncBaseTransport):
    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused", request=request)


async def test_error_mapping(aconn, pool, client, fake, db_url):
    settings = make_settings(db_url, dispatch_max_attempts=2, max_per_schedule=10)
    preset = fake.add_preset("default")

    # the preset is gone before dispatch
    gone = await add_schedule(aconn, uuid.uuid4(), name="gone")
    run = await add_run(aconn, gone.id)
    assert await dispatcher.dispatch_once(pool, client, settings)
    got = await runs.get(aconn, run.id)
    assert (got.status, got.reason) == ("failed", "preset_not_found")
    assert got.finished_at is not None

    # the preset is gone at create
    sched = await add_schedule(aconn, preset.id)
    run = await add_run(aconn, sched.id)
    fake.fail_next("create_session", 404)
    assert await dispatcher.dispatch_once(pool, client, settings)
    got = await runs.get(aconn, run.id)
    assert (got.status, got.reason) == ("failed", "preset_not_found")

    # the orchestrator refuses
    run = await add_run(aconn, sched.id, FIRE + timedelta(minutes=1))
    fake.fail_next("create_session", 422)
    assert await dispatcher.dispatch_once(pool, client, settings)
    got = await runs.get(aconn, run.id)
    assert (got.status, got.reason) == ("failed", "orchestrator_refused: internal")

    # transient: stays in progress, waits out its backoff, then fails at the cap
    run = await add_run(aconn, sched.id, FIRE + timedelta(minutes=2))
    fake.fail_next("create_session", 503)
    assert await dispatcher.dispatch_once(pool, client, settings)
    got = await runs.get(aconn, run.id)
    assert (got.status, got.attempts, got.reason) == ("in-progress", 1, None)
    assert await dispatcher.resume_pending(pool, client, settings, datetime.now(UTC)) == 0
    assert (await runs.get(aconn, run.id)).attempts == 1
    fake.fail_next("get_preset", 503)
    assert await dispatcher.resume_pending(pool, client, settings, datetime.now(UTC) + LATER) == 1
    got = await runs.get(aconn, run.id)
    assert (got.status, got.attempts, got.reason) == ("failed", 2, "orchestrator_unavailable")
    assert not fake.sessions

    # a connection error is transient too
    run = await add_run(aconn, sched.id, FIRE + timedelta(minutes=3))
    async with OrchestratorClient("http://orchestrator", "t", transport=_Refused()) as down:
        assert await dispatcher.dispatch_once(pool, down, settings)
        got = await runs.get(aconn, run.id)
        assert (got.status, got.attempts) == ("in-progress", 1)
        later = datetime.now(UTC) + LATER
        assert await dispatcher.resume_pending(pool, down, settings, later) == 1
    got = await runs.get(aconn, run.id)
    assert (got.status, got.reason) == ("failed", "orchestrator_unavailable")


async def test_disabled_schedule_ignored(aconn, pool, client, fake, settings):
    preset = fake.add_preset("default")
    sched = await add_schedule(aconn, preset.id, enabled=False)
    scheduled = await add_run(aconn, sched.id)

    assert await dispatcher.dispatch_once(pool, client, settings)
    got = await runs.get(aconn, scheduled.id)
    assert (got.status, got.reason) == ("ignored", "schedule_disabled")
    assert got.finished_at is not None

    # run-now is allowed on a disabled schedule (design D11)
    manual = await runs.insert_manual(aconn, sched.id, datetime.now(UTC), "new")
    assert await dispatcher.dispatch_once(pool, client, settings)
    got = await runs.get(aconn, manual.id)
    assert got.status == "in-progress" and got.message_id is not None
    assert len(fake.sessions) == 1


async def test_reuse_posts_to_running_session(aconn, pool, client, fake, settings):
    preset = fake.add_preset("reuse", spec={"reuse_running_agent": True})
    sched = await add_schedule(aconn, preset.id)
    prev = await dispatch_previous(aconn, pool, client, settings, sched)
    run = await runs.insert_manual(aconn, sched.id, datetime.now(UTC), "new")

    assert await dispatcher.dispatch_once(pool, client, settings)

    got = await runs.get(aconn, run.id)
    assert got.status == "in-progress"
    assert got.reuse == "delivered"
    assert got.session_id == prev.session_id
    assert got.session_slug == session_slug(sched.name, run.id)
    assert len(fake.sessions) == 1
    assert creates(fake) == 1
    message = fake.messages[int(got.message_id)]
    assert message["session_id"] == str(prev.session_id)
    assert message["text"].endswith(marker_of(run.id))
    assert len(fake.messages) == 2


async def test_no_reuse_when_preset_not_opted_in(aconn, pool, client, fake, settings):
    preset = fake.add_preset("plain")
    sched = await add_schedule(aconn, preset.id)
    prev = await dispatch_previous(aconn, pool, client, settings, sched)
    run = await runs.insert_manual(aconn, sched.id, datetime.now(UTC), "new")

    assert await dispatcher.dispatch_once(pool, client, settings)

    got = await runs.get(aconn, run.id)
    assert got.reuse is None
    assert got.session_id is not None and got.session_id != prev.session_id
    assert len(fake.sessions) == 2
    assert fake.messages[int(got.message_id)]["session_id"] == str(got.session_id)


async def test_reuse_falls_back_when_session_gone(aconn, pool, client, fake, settings):
    preset = fake.add_preset("reuse", spec={"reuse_running_agent": True})
    sched = await add_schedule(aconn, preset.id)

    # the previous session is gone at check time
    prev = await dispatch_previous(aconn, pool, client, settings, sched)
    fake.remove_session(str(prev.session_id))
    run = await runs.insert_manual(aconn, sched.id, datetime.now(UTC), "new")
    assert await dispatcher.dispatch_once(pool, client, settings)
    got = await runs.get(aconn, run.id)
    assert got.reuse == "fallback"
    assert got.session_id is not None and got.session_id != prev.session_id
    assert fake.sessions[str(got.session_id)]["slug"] == session_slug(sched.name, run.id)
    assert fake.messages[int(got.message_id)]["session_id"] == str(got.session_id)
    await finish(aconn, run.id)

    # the previous session is running, but deliver returns None (session being deleted)
    run2 = await runs.insert_manual(aconn, sched.id, datetime.now(UTC), "new")
    fake.fail_next("post_message", 409)
    assert await dispatcher.dispatch_once(pool, client, settings)
    got2 = await runs.get(aconn, run2.id)
    assert got2.reuse == "fallback"
    assert got2.session_id not in (None, got.session_id)
    assert fake.sessions[str(got2.session_id)]["slug"] == session_slug(sched.name, run2.id)
    message = fake.messages[int(got2.message_id)]
    assert message["session_id"] == str(got2.session_id)
    assert message["text"].endswith(marker_of(run2.id))
    assert sum(1 for m in fake.messages.values() if marker_of(run2.id) in m["text"]) == 1


async def test_reuse_resume_never_reposts(aconn, pool, client, fake, settings):
    preset = fake.add_preset("reuse", spec={"reuse_running_agent": True})
    sched = await add_schedule(aconn, preset.id)
    prev = await dispatch_previous(aconn, pool, client, settings, sched)
    run = await runs.insert_manual(aconn, sched.id, datetime.now(UTC), "new")

    # crash after the reuse decision, before the post
    fake.fail_next("post_message", 503)
    assert await dispatcher.dispatch_once(pool, client, settings)
    got = await runs.get(aconn, run.id)
    assert (got.reuse, got.session_id, got.message_id) == ("delivered", prev.session_id, None)
    assert await dispatcher.resume_pending(pool, client, settings, datetime.now(UTC) + LATER) == 1
    got = await runs.get(aconn, run.id)
    assert got.message_id is not None

    # crash after the post, before the message id was recorded
    await aconn.execute("UPDATE runs SET message_id = NULL WHERE id = %s", (run.id,))
    assert await dispatcher.resume_pending(pool, client, settings, datetime.now(UTC) + LATER) == 1
    again = await runs.get(aconn, run.id)
    assert (again.reuse, again.session_id, again.message_id) == (
        "delivered",
        prev.session_id,
        got.message_id,
    )
    assert sum(1 for m in fake.messages.values() if marker_of(run.id) in m["text"]) == 1
    assert len(fake.sessions) == 1
    assert creates(fake) == 1


async def test_run_dispatcher_resumes_then_wakes_on_new_run(aconn, pool, client, fake, db_url):
    settings = make_settings(db_url, poll_seconds=300)
    preset = fake.add_preset("default")
    s1 = await add_schedule(aconn, preset.id, name="one")
    s2 = await add_schedule(aconn, preset.id, name="two")
    stale = await add_run(aconn, s1.id)
    await runs.transition(aconn, stale.id, "new", "in-progress", started_at=datetime.now(UTC))

    stop = asyncio.Event()
    task = asyncio.create_task(dispatcher.run_dispatcher(pool, client, settings, stop))
    try:
        async with asyncio.timeout(10):
            while (await runs.get(aconn, stale.id)).message_id is None:
                await asyncio.sleep(0.05)
            fresh = await add_run(aconn, s2.id)
            while (await runs.get(aconn, fresh.id)).message_id is None:
                await notify(aconn, dispatcher.CHANNEL)
                await asyncio.sleep(0.1)
    finally:
        stop.set()
        async with asyncio.timeout(10):
            await task
    assert len(fake.sessions) == 2


async def test_unexpected_error_isolated_per_run(
    aconn, pool, client, fake, settings, monkeypatch, caplog
):
    preset = fake.add_preset("default")
    first = await add_schedule(aconn, preset.id, name="Bad")
    second = await add_schedule(aconn, preset.id, name="Good")
    bad = await add_run(aconn, first.id, FIRE)
    good = await add_run(aconn, second.id, FIRE + timedelta(minutes=1))

    real = dispatcher._send
    broken = {"on": True}

    async def flaky(pool, client, settings, run):
        if run.id == bad.id and broken["on"]:
            raise ValueError("malformed orchestrator payload")
        await real(pool, client, settings, run)

    monkeypatch.setattr(dispatcher, "_send", flaky)

    # dispatch_once: the bad run is claimed and fails unexpectedly, the next call sends the good one
    assert await dispatcher.dispatch_once(pool, client, settings)
    assert f"run {bad.id}" in caplog.text and "malformed orchestrator payload" in caplog.text
    assert await dispatcher.dispatch_once(pool, client, settings)
    assert (await runs.get(aconn, good.id)).message_id is not None
    stuck = await runs.get(aconn, bad.id)
    assert (stuck.status, stuck.message_id) == ("in-progress", None)

    # resume_pending: a failing run does not stop the others; the bad run is retried later
    third = await add_schedule(aconn, preset.id, name="Third")
    other = await add_run(aconn, third.id, FIRE + timedelta(minutes=2))
    await runs.transition(aconn, other.id, "new", "in-progress", started_at=datetime.now(UTC))
    assert await dispatcher.resume_pending(pool, client, settings) == 1
    assert (await runs.get(aconn, other.id)).message_id is not None
    assert (await runs.get(aconn, bad.id)).message_id is None

    broken["on"] = False
    assert await dispatcher.resume_pending(pool, client, settings) == 1
    assert (await runs.get(aconn, bad.id)).message_id is not None
    assert (creates(fake), posts(fake)) == (3, 3)
