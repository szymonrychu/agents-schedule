"""The token-gated admin listener: the same CRUD and run routes behind ServiceTokenGate."""

import secrets

import httpx
import pytest
from agents_common.db import open_pool
from agents_common.fake_orchestrator import FakeOrchestrator
from agents_common.orchestrator import OrchestratorClient

from agents_schedule.api.app import create_admin_app, create_app

BASE = "/schedules/api/v1"


@pytest.fixture
def token() -> str:
    return secrets.token_urlsafe(24)


@pytest.fixture
async def pool(db_url: str):
    p = await open_pool(db_url, min_size=1, max_size=4)
    yield p
    await p.close()


@pytest.fixture
async def fake():
    return FakeOrchestrator()


@pytest.fixture
async def clients(pool, db_url, settings_for, fake, token):
    """(admin client, ui client); the admin peer is outside the ui allowlist."""
    async with OrchestratorClient(
        "http://orchestrator", fake.token, transport=httpx.ASGITransport(app=fake.app)
    ) as orch:
        settings = settings_for(db_url)
        admin = create_admin_app(settings, pool, orch, [token])
        ui = create_app(settings, pool, orch)
        async with (
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=admin, client=("10.9.8.7", 5000)),
                base_url="http://admin",
            ) as a,
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=ui, client=("127.0.0.1", 5000)),
                base_url="http://ui",
            ) as u,
        ):
            yield a, u


def body(fake: FakeOrchestrator, name: str = "nightly") -> dict:
    return {
        "name": name,
        "cron": "0 9 * * *",
        "timezone": "UTC",
        "preset_id": str(fake.add_preset("p").id),
        "prompt": "go",
    }


async def test_admin_requires_token(clients, token):
    admin, _ = clients
    for headers in ({}, {"Authorization": "Bearer " + secrets.token_urlsafe(24)}):
        r = await admin.get(f"{BASE}/schedules", headers=headers)
        assert r.status_code == 401, r.text
        assert r.json()["error"]["code"] == "unauthorized"
    assert (await admin.get(f"{BASE}/schedules", headers=auth(token))).status_code == 200


def auth(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


async def test_admin_health_is_exempt(clients):
    admin, _ = clients
    assert (await admin.get("/healthz")).status_code == 200
    assert (await admin.get("/readyz")).status_code == 200


async def test_admin_ignores_source_allowlist(clients, token):
    admin, _ = clients  # peer 10.9.8.7 is outside SCHED_ALLOWED_CIDRS
    assert (await admin.get(f"{BASE}/schedules", headers=auth(token))).status_code == 200


async def test_admin_schedule_crud_and_run(clients, token, fake):
    admin, _ = clients
    h = auth(token)
    payload = body(fake)
    created = await admin.post(f"{BASE}/schedules", json=payload, headers=h)
    assert created.status_code == 201, created.text
    sid = created.json()["id"]
    assert (await admin.get(f"{BASE}/schedules/{sid}", headers=h)).status_code == 200
    updated = await admin.put(
        f"{BASE}/schedules/{sid}", json={**payload, "prompt": "changed"}, headers=h
    )
    assert updated.status_code == 200, updated.text
    assert updated.json()["prompt"] == "changed"
    run = await admin.post(f"{BASE}/schedules/{sid}/run", headers=h)
    assert run.status_code == 202, run.text
    assert run.json()["trigger"] == "manual"
    assert (await admin.get(f"{BASE}/runs/{run.json()['id']}", headers=h)).status_code == 200
    assert (await admin.get(f"{BASE}/schedules/{sid}/runs", headers=h)).status_code == 200
    assert (await admin.delete(f"{BASE}/schedules/{sid}", headers=h)).status_code == 204
    assert (await admin.get(f"{BASE}/schedules/{sid}", headers=h)).status_code == 404


async def test_ui_port_unchanged_and_needs_no_token(clients):
    _, ui = clients
    assert (await ui.get(f"{BASE}/schedules")).status_code == 200


def test_admin_app_has_no_openapi_or_docs(settings_for):
    app = create_admin_app(settings_for("postgresql://x/y"), None, None, ["t"])  # type: ignore[arg-type]
    assert app.openapi_url is None and app.docs_url is None


CHAIN = {
    "source": "webhook",
    "source_ref": "evt-1",
    "previous_name": "pr-review",
    "previous_status": "handled",
    "previous_session_slug": "pr-review-1",
    "previous_result": "final text",
}


async def make_schedule(admin, token, fake, name: str = "target") -> str:
    r = await admin.post(f"{BASE}/schedules", json=body(fake, name), headers=auth(token))
    assert r.status_code == 201, r.text
    return r.json()["id"]


async def test_chain_route_creates_chained_run(clients, token, fake, db_url):
    from test_api_runs import got_notify, listen_on

    from agents_schedule import dispatcher

    admin, _ = clients
    sid = await make_schedule(admin, token, fake)
    async with await listen_on(db_url, dispatcher.CHANNEL) as listener:
        r = await admin.post(f"{BASE}/schedules/{sid}/chain", json=CHAIN, headers=auth(token))
        assert r.status_code == 202, r.text
        assert await got_notify(listener)
    run = r.json()
    assert run["trigger"] == "chained" and run["status"] == "new"
    assert (run["chain_source"], run["chain_ref"]) == ("webhook", "evt-1")
    assert (run["chained_from"], run["chain_depth"]) == (None, 1)
    assert (run["previous_name"], run["previous_status"]) == ("pr-review", "handled")
    stored = await admin.get(f"{BASE}/runs/{run['id']}", headers=auth(token))
    assert stored.json()["id"] == run["id"]


async def test_chain_route_idempotent(clients, token, fake):
    admin, _ = clients
    h = auth(token)
    sid = await make_schedule(admin, token, fake)
    first = await admin.post(f"{BASE}/schedules/{sid}/chain", json=CHAIN, headers=h)
    again = await admin.post(f"{BASE}/schedules/{sid}/chain", json=CHAIN, headers=h)
    assert (first.status_code, again.status_code) == (202, 200), again.text
    assert again.json()["id"] == first.json()["id"]
    listed = await admin.get(f"{BASE}/schedules/{sid}/runs", headers=h)
    assert len(listed.json()) == 1


async def test_chain_route_target_in_flight(clients, token, fake):
    admin, _ = clients
    h = auth(token)
    sid = await make_schedule(admin, token, fake)
    assert (await admin.post(f"{BASE}/schedules/{sid}/run", headers=h)).status_code == 202
    r = await admin.post(f"{BASE}/schedules/{sid}/chain", json=CHAIN, headers=h)
    assert r.status_code == 202, r.text
    assert (r.json()["status"], r.json()["reason"]) == ("ignored", "previous_running")
    again = await admin.post(f"{BASE}/schedules/{sid}/chain", json=CHAIN, headers=h)
    assert again.status_code == 200 and again.json()["id"] == r.json()["id"]


async def test_chain_route_unknown_schedule_404(clients, token):
    admin, _ = clients
    for sid in ("00000000-0000-0000-0000-000000000001", "nope"):
        r = await admin.post(f"{BASE}/schedules/{sid}/chain", json=CHAIN, headers=auth(token))
        assert r.status_code == 404, r.text
        assert r.json()["error"]["code"] == "not_found"


async def test_chain_route_not_on_ui_app(clients, token, fake):
    admin, ui = clients
    sid = await make_schedule(admin, token, fake)
    r = await ui.post(f"{BASE}/schedules/{sid}/chain", json=CHAIN)
    assert r.status_code in (404, 405)


async def test_chain_route_requires_admin_token(clients, token, fake):
    admin, _ = clients
    sid = await make_schedule(admin, token, fake)
    bad = {"Authorization": "Bearer " + secrets.token_urlsafe(24)}
    for headers in ({}, bad):
        r = await admin.post(f"{BASE}/schedules/{sid}/chain", json=CHAIN, headers=headers)
        assert r.status_code == 401, r.text


async def test_chain_route_rejects_extra_fields(clients, token, fake):
    admin, _ = clients
    sid = await make_schedule(admin, token, fake)
    h = auth(token)
    extra = await admin.post(f"{BASE}/schedules/{sid}/chain", json={**CHAIN, "x": 1}, headers=h)
    assert extra.status_code == 422, extra.text
    bad = await admin.post(
        f"{BASE}/schedules/{sid}/chain", json={**CHAIN, "previous_status": "failed"}, headers=h
    )
    assert bad.status_code == 422, bad.text
