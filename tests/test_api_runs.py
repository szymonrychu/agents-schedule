import uuid
from collections.abc import Callable
from datetime import UTC, datetime, timedelta

import httpx
import psycopg
import pytest
from agents_common.db import open_pool
from agents_common.fake_orchestrator import FakeOrchestrator
from agents_common.orchestrator import OrchestratorClient
from psycopg import sql

from agents_schedule import dispatcher
from agents_schedule.api.app import create_app
from agents_schedule.db import runs, schedules
from agents_schedule.settings import Settings

BASE = "/schedules/api/v1"


@pytest.fixture
async def pool(db_url: str):
    p = await open_pool(db_url, min_size=1, max_size=4)
    yield p
    await p.close()


@pytest.fixture
async def http(pool, db_url: str, settings_for: Callable[[str], Settings]):
    fake = FakeOrchestrator()
    async with OrchestratorClient(
        "http://orchestrator", fake.token, transport=httpx.ASGITransport(app=fake.app)
    ) as orch:
        app = create_app(settings_for(db_url), pool, orch)
        transport = httpx.ASGITransport(app=app, client=("127.0.0.1", 50000))
        async with httpx.AsyncClient(transport=transport, base_url="http://sched") as c:
            yield c


async def make_schedule(pool, name: str = "nightly", enabled: bool = True) -> schedules.Schedule:
    async with pool.connection() as conn:
        return await schedules.create(
            conn,
            name=name,
            cron="0 9 * * *",
            timezone="UTC",
            preset_id=uuid.uuid4(),
            prompt="go",
            enabled=enabled,
        )


async def listen_on(db_url: str, channel: str) -> psycopg.AsyncConnection:
    conn = await psycopg.AsyncConnection.connect(db_url, autocommit=True)
    await conn.execute(sql.SQL("LISTEN {}").format(sql.Identifier(channel)))
    return conn


async def got_notify(conn: psycopg.AsyncConnection) -> bool:
    async for _ in conn.notifies(timeout=2, stop_after=1):
        return True
    return False


async def test_run_now_creates_manual_run(http, pool, db_url):
    sched = await make_schedule(pool, enabled=False)  # run now works on a disabled schedule
    async with await listen_on(db_url, dispatcher.CHANNEL) as listener:
        r = await http.post(f"{BASE}/schedules/{sched.id}/run")
        assert r.status_code == 202, r.text
        assert await got_notify(listener)
    run = r.json()
    assert run["trigger"] == "manual"
    assert run["status"] == "new"
    assert run["reason"] is None
    assert run["schedule_id"] == str(sched.id)
    assert datetime.fromisoformat(run["fire_time"]) > datetime.now(UTC) - timedelta(minutes=1)
    got = await http.get(f"{BASE}/runs/{run['id']}")
    assert got.status_code == 200
    assert got.json() == run


async def test_run_now_overlap_ignored(http, pool, db_url):
    sched = await make_schedule(pool)
    first = await http.post(f"{BASE}/schedules/{sched.id}/run")
    assert first.json()["status"] == "new"
    async with await listen_on(db_url, dispatcher.CHANNEL) as listener:
        r = await http.post(f"{BASE}/schedules/{sched.id}/run")
        assert r.status_code == 202, r.text
        assert not await got_notify(listener)  # an ignored run needs no dispatch
    run = r.json()
    assert run["status"] == "ignored"
    assert run["reason"] == "previous_running"
    assert run["trigger"] == "manual"
    assert run["finished_at"] is not None


async def test_run_lists(http, pool):
    a = await make_schedule(pool, "a")
    b = await make_schedule(pool, "b")
    base = datetime(2026, 1, 1, tzinfo=UTC)
    ids = []
    async with pool.connection() as conn:
        for i in range(3):
            run = await runs.insert_scheduled(conn, a.id, base + timedelta(hours=i), "new")
            assert run is not None
            ids.append(str(run.id))
        other = await runs.insert_scheduled(conn, b.id, base + timedelta(hours=9), "new")
        assert other is not None
        assert await runs.transition(conn, other.id, "new", "in-progress")

    r = await http.get(f"{BASE}/schedules/{a.id}/runs")
    assert r.status_code == 200
    assert [x["id"] for x in r.json()] == ids[::-1]
    r = await http.get(f"{BASE}/schedules/{a.id}/runs", params={"limit": 2})
    assert [x["id"] for x in r.json()] == ids[:0:-1]

    r = await http.get(f"{BASE}/runs")
    assert [x["id"] for x in r.json()] == [str(other.id), *ids[::-1]]
    r = await http.get(f"{BASE}/runs", params={"limit": 1})
    assert [x["id"] for x in r.json()] == [str(other.id)]
    r = await http.get(f"{BASE}/runs", params={"status": "in-progress"})
    assert [x["id"] for x in r.json()] == [str(other.id)]
    r = await http.get(f"{BASE}/runs", params={"status": "handled"})
    assert r.json() == []


async def test_run_errors(http, pool):
    sched = await make_schedule(pool)
    missing = uuid.uuid4()
    for method, path in [
        ("POST", f"/schedules/{missing}/run"),
        ("GET", f"/schedules/{missing}/runs"),
        ("GET", f"/runs/{missing}"),
        ("GET", "/runs/not-a-uuid"),
        ("POST", "/schedules/not-a-uuid/run"),
    ]:
        r = await http.request(method, BASE + path)
        assert r.status_code == 404, path
        assert r.json()["error"]["code"] == "not_found", path

    for path, params in [
        (f"/schedules/{sched.id}/runs", {"limit": 0}),
        (f"/schedules/{sched.id}/runs", {"limit": 101}),
        (f"/schedules/{sched.id}/runs", {"limit": "x"}),
        ("/runs", {"limit": 0}),
        ("/runs", {"limit": 201}),
        ("/runs", {"status": "bogus"}),
    ]:
        r = await http.get(BASE + path, params=params)
        assert r.status_code == 422, (path, params)
        assert r.json()["error"]["code"] == "invalid_request", (path, params)
