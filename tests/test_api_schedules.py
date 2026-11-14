import asyncio
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


async def test_if_match_matching_replaces(http, preset):
    created = (await http.post(f"{BASE}/schedules", json=body(preset))).json()
    r = await http.put(
        f"{BASE}/schedules/{created['id']}",
        json=body(preset, prompt="changed"),
        headers={"If-Match": f'"{created["updated_at"]}"'},
    )
    assert r.status_code == 200, r.text
    assert r.json()["prompt"] == "changed"
    assert r.json()["updated_at"] != created["updated_at"]


async def test_if_match_mismatch_is_412_and_writes_nothing(http, preset, db_url):
    created = (await http.post(f"{BASE}/schedules", json=body(preset))).json()
    sid = created["id"]
    moved = (await http.put(f"{BASE}/schedules/{sid}", json=body(preset, prompt="v2"))).json()
    listener = await listen_on(db_url, scheduler.CHANNEL)
    async with listener:
        r = await http.put(
            f"{BASE}/schedules/{sid}",
            json=body(preset, prompt="lost update"),
            headers={"If-Match": f'"{created["updated_at"]}"'},
        )
        assert r.status_code == 412, r.text
        assert r.json()["error"]["code"] == "precondition_failed"
        assert not await got_notify(listener)
    assert (await http.get(f"{BASE}/schedules/{sid}")).json() == moved


async def test_if_match_absent_keeps_last_write_wins(http, preset):
    created = (await http.post(f"{BASE}/schedules", json=body(preset))).json()
    sid = created["id"]
    await http.put(f"{BASE}/schedules/{sid}", json=body(preset, prompt="v2"))
    r = await http.put(f"{BASE}/schedules/{sid}", json=body(preset, prompt="v3"))
    assert r.status_code == 200, r.text
    assert r.json()["prompt"] == "v3"


@pytest.mark.parametrize(
    "value",
    [
        "garbage",
        "2026-11-08T04:15:16.123456Z",  # not quoted
        '"not a timestamp"',
        '"2026-11-08T04:15:16"',  # no offset
        'W/"2026-11-08T04:15:16.123456Z"',
        "*",
    ],
)
async def test_if_match_malformed_is_400(http, preset, value):
    created = (await http.post(f"{BASE}/schedules", json=body(preset))).json()
    r = await http.put(
        f"{BASE}/schedules/{created['id']}",
        json=body(preset, prompt="changed"),
        headers={"If-Match": value},
    )
    assert r.status_code == 400, r.text
    assert r.json()["error"]["code"] == "bad_request"
    assert (await http.get(f"{BASE}/schedules/{created['id']}")).json() == created


async def test_if_match_on_missing_schedule_is_404(http, preset):
    r = await http.put(
        f"{BASE}/schedules/{uuid.uuid4()}",
        json=body(preset),
        headers={"If-Match": '"2026-11-08T04:15:16.123456Z"'},
    )
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "not_found"


async def test_if_match_concurrent_puts_one_wins(http, preset, db_url):
    created = (await http.post(f"{BASE}/schedules", json=body(preset))).json()
    sid = created["id"]
    tag = {"If-Match": f'"{created["updated_at"]}"'}
    async with await psycopg.AsyncConnection.connect(db_url) as holder:
        # A third connection holds the row lock so both PUTs queue behind it.
        await holder.execute("SELECT 1 FROM schedules WHERE id = %s FOR UPDATE", (sid,))
        puts = [
            asyncio.create_task(
                http.put(f"{BASE}/schedules/{sid}", json=body(preset, prompt=p), headers=tag)
            )
            for p in ("first", "second")
        ]
        async with await psycopg.AsyncConnection.connect(db_url, autocommit=True) as probe:
            for _ in range(200):
                cur = await probe.execute(
                    "SELECT count(*) FROM pg_stat_activity "
                    "WHERE datname = current_database() AND wait_event_type = 'Lock'"
                )
                row = await cur.fetchone()
                assert row is not None
                if row[0] == 2:
                    break
                await asyncio.sleep(0.02)
            else:
                pytest.fail("the PUTs never queued on the row lock")
        await holder.commit()
        results = await asyncio.gather(*puts)
    assert sorted(r.status_code for r in results) == [200, 412]
    winner = next(r for r in results if r.status_code == 200).json()
    loser = next(r for r in results if r.status_code == 412).json()
    assert loser["error"]["code"] == "precondition_failed"
    assert (await http.get(f"{BASE}/schedules/{sid}")).json()["prompt"] == winner["prompt"]


async def make(http, preset, name, **over):
    r = await http.post(f"{BASE}/schedules", json=body(preset, name=name, **over))
    assert r.status_code == 201, r.text
    return r.json()


async def test_chain_create_keeps_order_and_put_clears(http, preset):
    a, b = await make(http, preset, "a"), await make(http, preset, "b")
    assert a["then_schedule_ids"] == []
    r = await http.post(
        f"{BASE}/schedules", json=body(preset, name="src", then_schedule_ids=[b["id"], a["id"]])
    )
    assert r.status_code == 201, r.text
    src = r.json()
    assert src["then_schedule_ids"] == [b["id"], a["id"]]
    got = (await http.get(f"{BASE}/schedules/{src['id']}")).json()
    assert got["then_schedule_ids"] == [b["id"], a["id"]]
    r = await http.put(f"{BASE}/schedules/{src['id']}", json=body(preset, name="src"))
    assert r.status_code == 200, r.text
    assert r.json()["then_schedule_ids"] == []


async def test_chain_errors(http, preset):
    a = await make(http, preset, "a")
    r = await http.put(
        f"{BASE}/schedules/{a['id']}", json=body(preset, name="a", then_schedule_ids=[a["id"]])
    )
    assert (r.status_code, r.json()["error"]["code"]) == (422, "chain_self")
    ghost = str(uuid.uuid4())
    r = await http.post(f"{BASE}/schedules", json=body(preset, name="x", then_schedule_ids=[ghost]))
    assert (r.status_code, r.json()["error"]["code"]) == (422, "chain_target_unknown")
    r = await http.post(
        f"{BASE}/schedules", json=body(preset, name="x", then_schedule_ids=[a["id"], a["id"]])
    )
    assert r.status_code == 422
    r = await http.post(
        f"{BASE}/schedules",
        json=body(preset, name="x", then_schedule_ids=[str(uuid.uuid4()) for _ in range(11)]),
    )
    assert r.status_code == 422
    assert (await http.get(f"{BASE}/schedules")).json()[0]["name"] == "a"
    assert len((await http.get(f"{BASE}/schedules")).json()) == 1


async def test_chain_cycle_409_and_diamond_ok(http, preset):
    a, b, c = [await make(http, preset, n) for n in "abc"]
    r = await http.put(
        f"{BASE}/schedules/{a['id']}", json=body(preset, name="a", then_schedule_ids=[b["id"]])
    )
    assert r.status_code == 200, r.text
    r = await http.put(
        f"{BASE}/schedules/{b['id']}", json=body(preset, name="b", then_schedule_ids=[c["id"]])
    )
    assert r.status_code == 200, r.text
    r = await http.put(
        f"{BASE}/schedules/{c['id']}", json=body(preset, name="c", then_schedule_ids=[a["id"]])
    )
    assert (r.status_code, r.json()["error"]["code"]) == (409, "chain_cycle")
    assert (await http.get(f"{BASE}/schedules/{c['id']}")).json()["then_schedule_ids"] == []
    r = await http.put(
        f"{BASE}/schedules/{a['id']}",
        json=body(preset, name="a", then_schedule_ids=[b["id"], c["id"]]),
    )
    assert r.status_code == 200, r.text


async def test_chain_target_delete_drops_link(http, preset):
    a, b = await make(http, preset, "a"), await make(http, preset, "b")
    r = await http.put(
        f"{BASE}/schedules/{a['id']}", json=body(preset, name="a", then_schedule_ids=[b["id"]])
    )
    assert r.status_code == 200
    assert (await http.delete(f"{BASE}/schedules/{b['id']}")).status_code == 204
    assert (await http.get(f"{BASE}/schedules/{a['id']}")).json()["then_schedule_ids"] == []
