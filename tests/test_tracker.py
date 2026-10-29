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

from agents_schedule import dispatcher, tracker
from agents_schedule.db import runs, schedules
from agents_schedule.settings import Settings

FIRE = datetime(2026, 3, 1, 2, 0, tzinfo=UTC)


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


async def start_run(
    aconn, pool, client, settings, preset_id: uuid.UUID, name: str, prompt: str = "go"
) -> runs.Run:
    """A run of a fresh schedule, dispatched: in-progress with a session and a message."""
    sched = await schedules.create(
        aconn,
        name=name,
        cron="0 3 * * *",
        timezone="Europe/Warsaw",
        preset_id=preset_id,
        prompt=prompt,
        enabled=True,
    )
    run = await runs.insert_manual(aconn, sched.id, datetime.now(UTC), "new")
    assert await dispatcher.dispatch_once(pool, client, settings)
    got = await runs.get(aconn, run.id)
    assert got is not None and got.message_id is not None
    return got


def mid(run: runs.Run) -> int:
    assert run.message_id is not None
    return int(run.message_id)


async def state(aconn, run: runs.Run) -> runs.Run:
    got = await runs.get(aconn, run.id)
    assert got is not None
    return got


async def test_completed_turn_handled(aconn, pool, client, fake, settings):
    preset = fake.add_preset("default")
    run = await start_run(aconn, pool, client, settings, preset.id, "done")
    fake.complete_turn(mid(run), "all good")

    assert await tracker.track_once(pool, client, settings, datetime.now(UTC)) == 1

    got = await state(aconn, run)
    assert (got.status, got.reason) == ("handled", None)
    completed = fake.messages[mid(run)]["turn"]["completed_at"]
    assert got.finished_at == datetime.fromisoformat(completed)
    assert await tracker.track_once(pool, client, settings, datetime.now(UTC)) == 0


async def test_command_prompt_run_handled(aconn, pool, client, fake, settings):
    preset = fake.add_preset("default")
    run = await start_run(aconn, pool, client, settings, preset.id, "slash", "/review-pr 12")
    now = datetime.now(UTC)

    # the command has no outcome yet: still running
    assert await tracker.track_once(pool, client, settings, now) == 0
    assert (await state(aconn, run)).status == "in-progress"

    fake.complete_command(mid(run), "ok")

    assert await tracker.track_once(pool, client, settings, now) == 1
    got = await state(aconn, run)
    assert (got.status, got.reason) == ("handled", None)
    assert got.finished_at is not None
    assert await tracker.track_once(pool, client, settings, now) == 0


async def test_command_prompt_run_failed_reason(aconn, pool, client, fake, settings):
    preset = fake.add_preset("default")
    unknown = await start_run(aconn, pool, client, settings, preset.id, "unknown", "/nope")
    cancelled = await start_run(aconn, pool, client, settings, preset.id, "cancel", "/review-pr 1")

    fake.fail_command(mid(unknown), "unknown_command")
    fake.fail_command(mid(cancelled), "cancelled", state="cancelled")

    assert await tracker.track_once(pool, client, settings, datetime.now(UTC)) == 2

    got = {r.id: await state(aconn, r) for r in (unknown, cancelled)}
    assert {rid: (g.status, g.reason) for rid, g in got.items()} == {
        unknown.id: ("failed", "command_unknown_command"),
        cancelled.id: ("failed", "command_cancelled"),
    }
    assert all(g.finished_at is not None for g in got.values())


async def test_failures_have_reasons(aconn, pool, client, fake, settings):
    preset = fake.add_preset("default")
    failed = await start_run(aconn, pool, client, settings, preset.id, "failed")
    cancelled = await start_run(aconn, pool, client, settings, preset.id, "cancelled")
    turn = await start_run(aconn, pool, client, settings, preset.id, "turn")
    gone = await start_run(aconn, pool, client, settings, preset.id, "gone")
    slow = await start_run(aconn, pool, client, settings, preset.id, "slow")

    fake.fail_turn(mid(failed))
    fake.messages[mid(cancelled)]["status"] = "cancelled"
    fake.messages[mid(turn)]["status"] = "sent"
    fake.messages[mid(turn)]["turn"] = {"status": "failed"}
    fake.remove_session(str(gone.session_id))
    now = datetime.now(UTC)
    for r in (failed, cancelled, turn, gone):
        await aconn.execute("UPDATE runs SET started_at = %s WHERE id = %s", (now, r.id))
    await aconn.execute(
        "UPDATE runs SET started_at = %s WHERE id = %s",
        (now - timedelta(seconds=settings.run_timeout_seconds + 1), slow.id),
    )

    assert await tracker.track_once(pool, client, settings, now) == 5

    reasons = {r.id: (await state(aconn, r)) for r in (failed, cancelled, turn, gone, slow)}
    assert {rid: (g.status, g.reason) for rid, g in reasons.items()} == {
        failed.id: ("failed", "message_failed"),
        cancelled.id: ("failed", "message_cancelled"),
        turn.id: ("failed", "turn_failed"),
        gone.id: ("failed", "session_not_found"),
        slow.id: ("failed", "timeout"),
    }
    assert all(g.finished_at is not None for g in reasons.values())


async def test_running_unchanged(aconn, pool, client, fake, settings, caplog):
    preset = fake.add_preset("default")
    run = await start_run(aconn, pool, client, settings, preset.id, "running")
    now = datetime.now(UTC)

    assert await tracker.track_once(pool, client, settings, now) == 0
    assert (await state(aconn, run)).status == "in-progress"

    caplog.set_level(logging.WARNING, logger="agents_schedule.tracker")
    for offset, expected_logs in ((0, 1), (10, 1), (61, 2)):
        fake.fail_next("get_message", 503)
        assert (
            await tracker.track_once(pool, client, settings, now + timedelta(seconds=offset)) == 0
        )
        assert (await state(aconn, run)).status == "in-progress"
        assert len(caplog.records) == expected_logs


async def test_never_deletes_sessions(aconn, pool, client, fake, settings):
    preset = fake.add_preset("default")
    done = await start_run(aconn, pool, client, settings, preset.id, "done")
    bad = await start_run(aconn, pool, client, settings, preset.id, "bad")
    fake.complete_turn(mid(done), "ok")
    fake.fail_turn(mid(bad))

    assert await tracker.track_once(pool, client, settings, datetime.now(UTC)) == 2

    assert not [r for r in fake.requests if r.method == "DELETE"]
    assert len(fake.sessions) == 2


async def test_reused_run_tracked_by_delivery(aconn, pool, client, fake, settings):
    preset = fake.add_preset("reuse", spec={"reuse_running_agent": True})
    sched = await schedules.create(
        aconn,
        name="reuse",
        cron="0 3 * * *",
        timezone="Europe/Warsaw",
        preset_id=preset.id,
        prompt="go",
        enabled=True,
    )
    first = await runs.insert_manual(aconn, sched.id, datetime.now(UTC), "new")
    assert await dispatcher.dispatch_once(pool, client, settings)
    assert await runs.transition(aconn, first.id, "in-progress", "handled")
    reused = await runs.insert_manual(aconn, sched.id, datetime.now(UTC), "new")
    assert await dispatcher.dispatch_once(pool, client, settings)
    run = await state(aconn, reused)
    assert run.reuse == "delivered" and run.message_id is not None
    now = datetime.now(UTC)

    # still queued: unchanged
    assert await tracker.track_once(pool, client, settings, now) == 0
    assert (await state(aconn, run)).status == "in-progress"

    # sent: handled
    fake.mark_sent(mid(run))
    assert await tracker.track_once(pool, client, settings, now) == 1
    got = await state(aconn, run)
    assert (got.status, got.reason) == ("handled", None)

    # gone: reset to fallback with session and message cleared, still in progress
    again = await runs.insert_manual(aconn, sched.id, datetime.now(UTC), "new")
    assert await dispatcher.dispatch_once(pool, client, settings)
    run = await state(aconn, again)
    assert run.reuse == "delivered" and run.message_id is not None
    fake.messages[mid(run)]["status"] = "cancelled"
    assert await tracker.track_once(pool, client, settings, now) == 1
    got = await state(aconn, run)
    assert (got.status, got.reuse, got.session_id, got.message_id) == (
        "in-progress",
        "fallback",
        None,
        None,
    )
    # the dispatcher resumes it and creates a new session
    sessions = len(fake.sessions)
    assert await dispatcher.resume_pending(pool, client, settings, now + timedelta(hours=1)) == 1
    resumed = await state(aconn, run)
    assert resumed.session_id is not None and resumed.message_id is not None
    assert len(fake.sessions) == sessions + 1


async def test_unexpected_error_isolated_per_run(
    aconn, pool, client, fake, settings, monkeypatch, caplog
):
    preset = fake.add_preset("default")
    bad = await start_run(aconn, pool, client, settings, preset.id, "bad")
    good = await start_run(aconn, pool, client, settings, preset.id, "good")
    fake.complete_turn(mid(bad), "ok")
    fake.complete_turn(mid(good), "ok")

    real = tracker._track
    broken = {"on": True}

    async def flaky(client, store, settings, run, now):
        if run.id == bad.id and broken["on"]:
            raise ValueError("malformed orchestrator payload")
        return await real(client, store, settings, run, now)

    monkeypatch.setattr(tracker, "_track", flaky)

    assert await tracker.track_once(pool, client, settings, datetime.now(UTC)) == 1
    assert f"run {bad.id}" in caplog.text and "malformed orchestrator payload" in caplog.text
    assert (await state(aconn, good)).status == "handled"
    assert (await state(aconn, bad)).status == "in-progress"

    broken["on"] = False
    assert await tracker.track_once(pool, client, settings, datetime.now(UTC)) == 1
    assert (await state(aconn, bad)).status == "handled"
