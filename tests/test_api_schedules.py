import uuid
from collections.abc import AsyncIterator, Callable
from datetime import datetime, timedelta

import httpx
import psycopg
import pytest
from agents_common.db import open_pool
from agents_common.fake_orchestrator import FakeOrchestrator
from agents_common.orchestrator import OrchestratorClient
from psycopg import sql

from agents_schedule import scheduler
from agents_schedule.api.app import create_app
from agents_schedule.db import runs
from agents_schedule.settings import Settings

BASE = "/schedules/api/v1"


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
def settings(db_url: str, settings_for: Callable[[str], Settings]) -> Settings:
    return settings_for(db_url)


@pytest.fixture
def make_http(pool, client, settings):
    def make(peer: str = "127.0.0.1", orch: OrchestratorClient | None = None):
        app = create_app(settings, pool, orch or client)
        transport = httpx.ASGITransport(app=app, client=(peer, 50000))
        return httpx.AsyncClient(transport=transport, base_url="http://sched")

    return make


@pytest.fixture
async def http(make_http) -> AsyncIterator[httpx.AsyncClient]:
    async with make_http() as c:
        yield c


def body(preset, **over):
    data = {
        "name": "nightly",
        "cron": "0 9 * * *",
        "timezone": "Asia/Tokyo",
        "preset_id": str(preset.id),
        "prompt": "do the thing",
    }
    data.update(over)
    return data


@pytest.fixture
def preset(fake: FakeOrchestrator):
    return fake.add_preset("p1")


async def listen_on(db_url: str, channel: str) -> psycopg.AsyncConnection:
    conn = await psycopg.AsyncConnection.connect(db_url, autocommit=True)
    await conn.execute(sql.SQL("LISTEN {}").format(sql.Identifier(channel)))
    return conn


async def got_notify(conn: psycopg.AsyncConnection) -> bool:
    async for _ in conn.notifies(timeout=2, stop_after=1):
        return True
    return False


async def test_crud(http, preset, db_url):
    listener = await listen_on(db_url, scheduler.CHANNEL)
    async with listener:
        r = await http.post(f"{BASE}/schedules", json=body(preset))
        assert r.status_code == 201, r.text
        created = r.json()
        assert created["enabled"] is True
        assert created["last_run"] is None
        assert len(created["next_fires"]) == 3
        assert created["timezone"] == "Asia/Tokyo"
        assert await got_notify(listener)
        sid = created["id"]

        await http.post(f"{BASE}/schedules", json=body(preset, name="a-first"))
        listed = (await http.get(f"{BASE}/schedules")).json()
        assert [s["name"] for s in listed] == ["a-first", "nightly"]
        assert (await http.get(f"{BASE}/schedules/{sid}")).json()["id"] == sid

        while await got_notify(listener):  # drain the second create
            break
        r = await http.put(
            f"{BASE}/schedules/{sid}",
            json=body(preset, name="renamed", prompt="new prompt", enabled=False),
        )
        assert r.status_code == 200, r.text
        updated = r.json()
        assert updated["name"] == "renamed"
        assert updated["prompt"] == "new prompt"
        assert updated["enabled"] is False
        assert updated["next_fires"] == []
        assert await got_notify(listener)

        r = await http.delete(f"{BASE}/schedules/{sid}")
        assert r.status_code == 204
        assert await got_notify(listener)
        assert (await http.get(f"{BASE}/schedules/{sid}")).status_code == 404


async def test_default_timezone_and_last_run(http, preset, pool, settings):
    r = await http.post(
        f"{BASE}/schedules",
        json={k: v for k, v in body(preset).items() if k != "timezone"},
    )
    assert r.status_code == 201
    view = r.json()
    assert view["timezone"] == settings.default_timezone
    async with pool.connection() as conn:
        run = await runs.insert_manual(
            conn, uuid.UUID(view["id"]), datetime.now().astimezone(), "new"
        )
    got = (await http.get(f"{BASE}/schedules/{view['id']}")).json()
    assert got["last_run"]["id"] == str(run.id)
    assert got["last_run"]["status"] == "new"
    assert got["last_run"]["trigger"] == "manual"
    assert {
        "schedule_id",
        "reason",
        "session_slug",
        "session_id",
        "message_id",
        "reuse",
        "started_at",
        "finished_at",
        "fire_time",
    } <= got["last_run"].keys()


async def test_validation_422(http, preset):
    r = await http.post(f"{BASE}/schedules", json=body(preset, cron="not a cron"))
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "invalid_cron"
    assert r.json()["error"]["message"]

    r = await http.post(f"{BASE}/schedules", json=body(preset, cron="* * * *"))
    assert r.json()["error"]["code"] == "invalid_cron"

    r = await http.post(f"{BASE}/schedules", json=body(preset, timezone="Mars/Base"))
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "invalid_timezone"
    assert "Mars/Base" in r.json()["error"]["message"]

    for bad in (
        body(preset, prompt=""),
        body(preset, prompt="x" * 16385),
        body(preset, name=""),
        body(preset, name="n" * 101),
        body(preset, preset_id="nope"),
    ):
        r = await http.post(f"{BASE}/schedules", json=bad)
        assert r.status_code == 422
        assert r.json()["error"]["code"] == "invalid_request"
    ok = await http.post(f"{BASE}/schedules", json=body(preset, prompt="x" * 16384, name="n" * 100))
    assert ok.status_code == 201

    created = (await http.post(f"{BASE}/schedules", json=body(preset, name="v"))).json()
    r = await http.put(f"{BASE}/schedules/{created['id']}", json=body(preset, cron="bad"))
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "invalid_cron"

    r = await http.post(f"{BASE}/schedules/preview", json={"cron": "bad", "timezone": "UTC"})
    assert r.json()["error"]["code"] == "invalid_cron"
    r = await http.post(f"{BASE}/schedules/preview", json={"cron": "* * * * *", "timezone": "No"})
    assert r.json()["error"]["code"] == "invalid_timezone"


async def test_preset_checked(make_http, preset, fake):
    async with make_http() as http:
        r = await http.post(f"{BASE}/schedules", json=body(preset, preset_id=str(uuid.uuid4())))
        assert r.status_code == 422
        assert r.json()["error"]["code"] == "unknown_preset"
        created = (await http.post(f"{BASE}/schedules", json=body(preset))).json()
        r = await http.put(
            f"{BASE}/schedules/{created['id']}", json=body(preset, preset_id=str(uuid.uuid4()))
        )
        assert r.json()["error"]["code"] == "unknown_preset"

    def refuse(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("down", request=request)

    async with (
        OrchestratorClient(
            "http://orchestrator", "t", transport=httpx.MockTransport(refuse)
        ) as down,
        make_http(orch=down) as http,
    ):
        r = await http.post(f"{BASE}/schedules", json=body(preset, name="other"))
        assert r.status_code == 503
        assert r.json()["error"]["code"] == "orchestrator_unavailable"

    fake.fail_next("get_preset", 500)
    async with make_http() as http:
        r = await http.post(f"{BASE}/schedules", json=body(preset, name="other2"))
        assert r.status_code == 503


async def test_preview(http):
    r = await http.post(
        f"{BASE}/schedules/preview", json={"cron": "0 9 * * *", "timezone": "Asia/Tokyo"}
    )
    assert r.status_code == 200
    fires = [datetime.fromisoformat(x) for x in r.json()["next_fires"]]
    assert len(fires) == 3
    assert all(f.hour == 9 and f.minute == 0 for f in fires)
    assert all(f.utcoffset() == timedelta(hours=9) for f in fires)
    assert fires == sorted(fires)
    assert (fires[1] - fires[0]).days == 1


async def test_preview_prompt(http):
    r = await http.post(
        f"{BASE}/schedules/preview",
        json={
            "cron": "0 9 * * *",
            "timezone": "UTC",
            "prompt": "{{ schedule_name }} at {{ fired_at }} {{ preset }}",
            "schedule_name": "nightly",
            "preset_id": "p-1",
        },
    )
    assert r.status_code == 200
    data = r.json()
    first = datetime.fromisoformat(data["next_fires"][0]).isoformat()
    assert data["rendered"] == f"nightly at {first} p-1"
    assert data["variables"] == ["fired_at", "preset", "schedule_name"]

    r = await http.post(f"{BASE}/schedules/preview", json={"cron": "0 9 * * *"})
    assert r.json()["rendered"] is None
    assert r.json()["variables"] == []


async def test_preview_prompt_errors(http):
    base = {"cron": "0 9 * * *", "timezone": "UTC"}
    r = await http.post(f"{BASE}/schedules/preview", json=base | {"prompt": "{{ nope }}"})
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "invalid_prompt"
    assert "nope" in r.json()["error"]["message"]
    r = await http.post(f"{BASE}/schedules/preview", json=base | {"prompt": "a\n{% if %}"})
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "invalid_prompt"


async def test_invalid_prompt_refused_on_save(http, preset):
    for bad in ("{% if %}", "{{ a"):
        r = await http.post(f"{BASE}/schedules", json=body(preset, prompt=bad))
        assert r.status_code == 422
        assert r.json()["error"]["code"] == "invalid_prompt"
    created = (await http.post(f"{BASE}/schedules", json=body(preset))).json()
    r = await http.put(
        f"{BASE}/schedules/{created['id']}", json=body(preset, prompt="ok\n{% for %}")
    )
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "invalid_prompt"
    assert "line 2" in r.json()["error"]["message"]
    # an undefined variable is only known at fire time, so it saves
    r = await http.put(f"{BASE}/schedules/{created['id']}", json=body(preset, prompt="{{ later }}"))
    assert r.status_code == 200


async def test_conflict_and_not_found(http, preset):
    first = (await http.post(f"{BASE}/schedules", json=body(preset))).json()
    r = await http.post(f"{BASE}/schedules", json=body(preset))
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "conflict"

    other = (await http.post(f"{BASE}/schedules", json=body(preset, name="other"))).json()
    r = await http.put(f"{BASE}/schedules/{other['id']}", json=body(preset))
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "conflict"

    missing = uuid.uuid4()
    assert (await http.get(f"{BASE}/schedules/{missing}")).status_code == 404
    r = await http.put(f"{BASE}/schedules/{missing}", json=body(preset, name="zz"))
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "not_found"
    assert (await http.delete(f"{BASE}/schedules/{missing}")).status_code == 404
    assert (await http.get(f"{BASE}/schedules/not-a-uuid")).status_code == 404
    assert first["id"]


async def test_source_allowlist(make_http, preset):
    async with make_http(peer="10.9.9.9") as http:
        r = await http.get(f"{BASE}/schedules", headers={"X-Forwarded-For": "127.0.0.1"})
        assert r.status_code == 403
        assert r.json()["error"]["code"] == "forbidden"
        r = await http.get(
            f"{BASE}/schedules", headers={"Forwarded": "for=127.0.0.1", "X-Real-IP": "127.0.0.1"}
        )
        assert r.status_code == 403
        assert (await http.get("/healthz")).status_code == 200
    async with make_http(peer="127.0.0.1") as http:
        r = await http.get(f"{BASE}/schedules", headers={"X-Forwarded-For": "8.8.8.8"})
        assert r.status_code == 200


async def test_probes(make_http, pool):
    async with make_http() as http:
        assert (await http.get("/healthz")).json() == {"status": "ok"}
        assert (await http.get("/readyz")).status_code == 200
    await pool.close()
    async with make_http() as http:
        r = await http.get("/readyz")
        assert r.status_code == 503
        assert r.json()["error"]["code"] == "not_ready"


@pytest.mark.parametrize("status", [401, 403])
async def test_orchestrator_auth_failure_is_distinct(make_http, preset, caplog, status):
    def deny(request: httpx.Request) -> httpx.Response:
        err = {"error": {"code": "unauthorized", "message": "bad token"}}
        return httpx.Response(status, json=err)

    async with (
        OrchestratorClient("http://orchestrator", "t", transport=httpx.MockTransport(deny)) as bad,
        make_http(orch=bad) as http,
    ):
        with caplog.at_level("ERROR"):
            r = await http.post(f"{BASE}/schedules", json=body(preset))
    assert r.status_code == 503
    assert r.json()["error"]["code"] == "orchestrator_auth_failed"
    assert "token" in r.json()["error"]["message"]
    assert any(
        str(status) in rec.getMessage() and "unauthorized" in rec.getMessage()
        for rec in caplog.records
    )
