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
