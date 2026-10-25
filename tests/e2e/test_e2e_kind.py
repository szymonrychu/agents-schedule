"""API-only kind e2e (design D11, D15): every capability over HTTP through a port-forward to the
api Service of a real deployment (2 worker replicas); the orchestrator is the agents-common fake,
steered and inspected through its /_ control routes (e2e/fake-orchestrator.yaml).

scripts/e2e.sh (mise run test:e2e) brings the cluster up and sets SCHED_E2E_API_URL and
SCHED_E2E_FAKE_URL. test_every_route_exercised must stay last: it compares the routes the whole
module hit with openapi.json.
"""

import json
import os
import re
import time
import uuid
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import httpx
import pytest

pytestmark = pytest.mark.e2e

BASE = "/schedules/api/v1"
OPENAPI = Path(__file__).resolve().parents[2] / "openapi.json"
NEVER = "0 0 1 1 *"
"""A cron that does not fire during a test."""
EVERY_MINUTE = "* * * * *"
FIRE_WAIT = 150.0
"""Longest wait for a scheduled fire: up to a minute to the boundary, a tick, a dispatch."""


def wait_for[T](probe: Callable[[], T | None], what: str, timeout: float = 60.0) -> T:
    """Poll `probe` until it returns something truthy; fail after `timeout` seconds."""
    deadline = time.monotonic() + timeout
    while True:
        value = probe()
        if value:
            return value
        if time.monotonic() > deadline:
            pytest.fail(f"timed out after {timeout:.0f}s waiting for {what}")
        time.sleep(0.5)


class Api:
    """The api client; records the (method, openapi path) of every request it sends."""

    def __init__(self, url: str) -> None:
        self.http = httpx.Client(base_url=url, timeout=30)
        spec = json.loads(OPENAPI.read_text())
        self.templates: list[tuple[str, re.Pattern[str], set[str]]] = [
            (path, re.compile("^" + re.sub(r"\{[^/]+\}", "[^/]+", path) + "$"), set(ops))
            for path, ops in spec["paths"].items()
        ]
        self.hit: set[tuple[str, str]] = set()

    def request(self, method: str, path: str, **kw: Any) -> httpx.Response:
        found = [
            (template.count("{"), template)
            for template, pattern, ops in self.templates
            if pattern.match(path) and method.lower() in ops
        ]
        if found:
            self.hit.add((method.upper(), min(found)[1]))
        return self.http.request(method, path, **kw)

    def get(self, path: str, **kw: Any) -> httpx.Response:
        return self.request("GET", path, **kw)

    def post(self, path: str, **kw: Any) -> httpx.Response:
        return self.request("POST", path, **kw)

    def put(self, path: str, **kw: Any) -> httpx.Response:
        return self.request("PUT", path, **kw)

    def delete(self, path: str, **kw: Any) -> httpx.Response:
        return self.request("DELETE", path, **kw)

    def spec_routes(self) -> set[tuple[str, str]]:
        return {(op.upper(), path) for path, _, ops in self.templates for op in ops}


class Fake:
    """The fake orchestrator's control routes."""

    def __init__(self, url: str) -> None:
        self.http = httpx.Client(base_url=url, timeout=30)

    def preset(self, **spec: Any) -> str:
        r = self.http.post("/_presets", json={"name": f"p-{uuid.uuid4().hex[:8]}", "spec": spec})
        assert r.status_code == 201, r.text
        return r.json()["id"]

    def calls(self) -> list[dict[str, Any]]:
        return self.http.get("/_calls").json()

    def creates(self, slug: str | None = None) -> list[dict[str, Any]]:
        """Session creates the fake accepted (201), optionally for one slug."""
        return [
            c
            for c in self.calls()
            if (c["method"], c["path"], c["status"]) == ("POST", "/api/v1/sessions", 201)
            and (slug is None or c["body"]["slug"] == slug)
        ]

    def posts(self, session_id: str) -> list[dict[str, Any]]:
        """Prompts posted to a session."""
        return [
            c
            for c in self.calls()
            if c["method"] == "POST" and c["path"] == f"/api/v1/sessions/{session_id}/messages"
        ]

    def complete(self, message_id: str) -> None:
        r = self.http.post(f"/_messages/{message_id}/complete", json={"text": "done"})
        assert r.status_code == 200, r.text

    def fail_turn(self, message_id: str) -> None:
        r = self.http.post(f"/_messages/{message_id}/fail_turn")
        assert r.status_code == 200, r.text


@pytest.fixture(scope="module")
def api() -> Iterator[Api]:
    url = os.environ.get("SCHED_E2E_API_URL")
    assert url, "SCHED_E2E_API_URL is not set; run the e2e through `mise run test:e2e`"
    client = Api(url)
    yield client
    client.http.close()


@pytest.fixture(scope="module")
def fake() -> Iterator[Fake]:
    url = os.environ.get("SCHED_E2E_FAKE_URL")
    assert url, "SCHED_E2E_FAKE_URL is not set; run the e2e through `mise run test:e2e`"
    client = Fake(url)
    yield client
    client.http.close()


@pytest.fixture
def make_schedule(api: Api) -> Iterator[Callable[..., dict[str, Any]]]:
    """Create schedules (`preset_id` required); every one is deleted when the test ends."""
    created: list[str] = []

    def make(preset_id: str, **overrides: Any) -> dict[str, Any]:
        body = {
            "name": f"e2e-{uuid.uuid4().hex[:10]}",
            "cron": NEVER,
            "timezone": "UTC",
            "preset_id": preset_id,
            "prompt": "e2e prompt for {schedule_name} at {fired_at}",
            **overrides,
        }
        r = api.post(f"{BASE}/schedules", json=body)
        assert r.status_code == 201, r.text
        created.append(r.json()["id"])
        return r.json()

    yield make
    for schedule_id in created:
        api.delete(f"{BASE}/schedules/{schedule_id}")


# --- helpers over runs ----------------------------------------------------------------------


def run_now(api: Api, schedule_id: str) -> dict[str, Any]:
    r = api.post(f"{BASE}/schedules/{schedule_id}/run")
    assert r.status_code == 202, r.text
    return r.json()


def get_run(api: Api, run_id: str) -> dict[str, Any]:
    r = api.get(f"{BASE}/runs/{run_id}")
    assert r.status_code == 200, r.text
    return r.json()


def schedule_runs(api: Api, schedule_id: str) -> list[dict[str, Any]]:
    r = api.get(f"{BASE}/schedules/{schedule_id}/runs", params={"limit": 100})
    assert r.status_code == 200, r.text
    return r.json()


def wait_run(api: Api, run_id: str, status: str, what: str = "") -> dict[str, Any]:
    """Wait for the run to reach `status`; an `in-progress` run is waited for until its prompt
    is posted (session and message recorded)."""

    def probe() -> dict[str, Any] | None:
        run = get_run(api, run_id)
        if run["status"] == status and (status != "in-progress" or run["message_id"]):
            return run
        if run["status"] in ("handled", "failed", "ignored") and run["status"] != status:
            pytest.fail(f"run {run_id} ended {run['status']} ({run['reason']}), expected {status}")
        return None

    return wait_for(probe, what or f"run {run_id} to be {status}")


def handled(api: Api, fake: Fake, run_id: str) -> dict[str, Any]:
    """Wait for the run to be in progress, complete its turn, wait for `handled`."""
    run = wait_run(api, run_id, "in-progress")
    fake.complete(run["message_id"])
    return wait_run(api, run_id, "handled")


# --- create, validate, list, get, replace, preview, enable and disable ------------------------


def test_create_validation(api: Api, fake: Fake, make_schedule):
    preset = fake.preset()
    good = {
        "name": f"e2e-{uuid.uuid4().hex[:10]}",
        "cron": NEVER,
        "timezone": "UTC",
        "preset_id": preset,
        "prompt": "p",
    }

    def code(r: httpx.Response, status: int) -> str:
        assert r.status_code == status, r.text
        error = r.json()["error"]
        assert error["message"]
        return error["code"]

    assert code(api.post(f"{BASE}/schedules", json={**good, "cron": "not a cron"}), 422) == (
        "invalid_cron"
    )
    assert code(api.post(f"{BASE}/schedules", json={**good, "cron": "@daily"}), 422) == (
        "invalid_cron"
    )
    assert code(api.post(f"{BASE}/schedules", json={**good, "timezone": "Mars/Base"}), 422) == (
        "invalid_timezone"
    )
    assert code(
        api.post(f"{BASE}/schedules", json={**good, "preset_id": str(uuid.uuid4())}), 422
    ) == ("unknown_preset")
    assert code(api.post(f"{BASE}/schedules", json={**good, "name": ""}), 422) == (
        "invalid_request"
    )
    assert code(api.post(f"{BASE}/schedules", json={**good, "surprise": 1}), 422) == (
        "invalid_request"
    )
    assert code(api.post(f"{BASE}/schedules", json={"name": "x"}), 422) == "invalid_request"
    # none of the refused bodies created a schedule
    names = {s["name"] for s in api.get(f"{BASE}/schedules").json()}
    assert good["name"] not in names

    schedule = make_schedule(preset, name=good["name"])
    assert code(api.post(f"{BASE}/schedules", json=good), 409) == "conflict"
    assert schedule["name"] == good["name"]


def test_list_get_replace(api: Api, fake: Fake, make_schedule):
    preset = fake.preset()
    first = make_schedule(preset, prompt="first")
    other = make_schedule(preset)
    assert first["enabled"] is True and first["last_run"] is None
    assert first["timezone"] == "UTC" and first["preset_id"] == preset
    assert first["created_at"] and first["updated_at"]

    listed = {s["id"]: s for s in api.get(f"{BASE}/schedules").json()}
    assert listed[first["id"]] == first and other["id"] in listed
    assert api.get(f"{BASE}/schedules/{first['id']}").json() == first

    body = {
        "name": first["name"],
        "cron": "30 8 * * 1-5",
        "timezone": "Europe/Warsaw",
        "preset_id": preset,
        "prompt": "second",
        "enabled": True,
    }
    r = api.put(f"{BASE}/schedules/{first['id']}", json=body)
    assert r.status_code == 200, r.text
    replaced = r.json()
    assert replaced["id"] == first["id"] and replaced["created_at"] == first["created_at"]
    assert (replaced["cron"], replaced["timezone"], replaced["prompt"]) == (
        "30 8 * * 1-5",
        "Europe/Warsaw",
        "second",
    )
    assert len(replaced["next_fires"]) == 3
    assert api.get(f"{BASE}/schedules/{first['id']}").json() == replaced

    clash = api.put(f"{BASE}/schedules/{first['id']}", json={**body, "name": other["name"]})
    assert (clash.status_code, clash.json()["error"]["code"]) == (409, "conflict")
    bad = api.put(f"{BASE}/schedules/{first['id']}", json={**body, "cron": "nope"})
    assert (bad.status_code, bad.json()["error"]["code"]) == (422, "invalid_cron")
    unknown = api.put(
        f"{BASE}/schedules/{first['id']}", json={**body, "preset_id": str(uuid.uuid4())}
    )
    assert (unknown.status_code, unknown.json()["error"]["code"]) == (422, "unknown_preset")
    assert api.get(f"{BASE}/schedules/{first['id']}").json() == replaced

    for missing in (str(uuid.uuid4()), "not-a-uuid"):
        r = api.get(f"{BASE}/schedules/{missing}")
        assert (r.status_code, r.json()["error"]["code"]) == (404, "not_found")
        r = api.put(f"{BASE}/schedules/{missing}", json=body)
        assert (r.status_code, r.json()["error"]["code"]) == (404, "not_found")


def test_preview(api: Api):
    r = api.post(f"{BASE}/schedules/preview", json={"cron": "*/15 * * * *", "timezone": "UTC"})
    assert r.status_code == 200, r.text
    fires = r.json()["next_fires"]
    assert len(fires) == 3 and fires == sorted(fires)
    assert all(re.search(r":(00|15|30|45):00", f) for f in fires)

    default_zone = api.post(f"{BASE}/schedules/preview", json={"cron": "0 12 * * *"})
    assert default_zone.status_code == 200 and len(default_zone.json()["next_fires"]) == 3

    bad_cron = api.post(f"{BASE}/schedules/preview", json={"cron": "61 * * * *"})
    assert (bad_cron.status_code, bad_cron.json()["error"]["code"]) == (422, "invalid_cron")
    bad_zone = api.post(f"{BASE}/schedules/preview", json={"cron": NEVER, "timezone": "Nope/Zone"})
    assert (bad_zone.status_code, bad_zone.json()["error"]["code"]) == (422, "invalid_timezone")
    no_cron = api.post(f"{BASE}/schedules/preview", json={})
    assert (no_cron.status_code, no_cron.json()["error"]["code"]) == (422, "invalid_request")


def test_disable_and_enable(api: Api, fake: Fake, make_schedule):
    schedule = make_schedule(fake.preset())
    body = {k: schedule[k] for k in ("name", "cron", "timezone", "preset_id", "prompt")}
    assert len(schedule["next_fires"]) == 3

    off = api.put(f"{BASE}/schedules/{schedule['id']}", json={**body, "enabled": False}).json()
    assert off["enabled"] is False and off["next_fires"] == []
    assert api.get(f"{BASE}/schedules/{schedule['id']}").json()["enabled"] is False

    on = api.put(f"{BASE}/schedules/{schedule['id']}", json={**body, "enabled": True}).json()
    assert on["enabled"] is True and len(on["next_fires"]) == 3


# --- run now, history --------------------------------------------------------------------------


def test_run_now_handled(api: Api, fake: Fake, make_schedule):
    schedule = make_schedule(fake.preset(), name=f"e2e-now-{uuid.uuid4().hex[:6]}")
    run = run_now(api, schedule["id"])
    assert run["trigger"] == "manual" and run["schedule_id"] == schedule["id"]
    assert run["status"] in ("new", "in-progress")

    running = wait_run(api, run["id"], "in-progress")
    assert running["session_id"] and running["session_slug"]
    assert re.fullmatch(r"e2e-now-[0-9a-f]{6}-[0-9a-f]{8}", running["session_slug"])
    assert running["session_slug"].endswith(run["id"].replace("-", "")[:8])
    assert running["started_at"] and running["finished_at"] is None
    assert [c["body"]["slug"] for c in fake.creates(running["session_slug"])] == [
        running["session_slug"]
    ]
    prompts = fake.posts(running["session_id"])
    assert len(prompts) == 1
    assert schedule["name"] in prompts[0]["body"]["text"]
    assert "{schedule_name}" not in prompts[0]["body"]["text"]

    fake.complete(running["message_id"])
    done = wait_run(api, run["id"], "handled")
    assert done["reason"] is None and done["finished_at"] and done["reuse"] is None

    assert [r["id"] for r in schedule_runs(api, schedule["id"])] == [run["id"]]
    assert get_run(api, run["id"]) == done
    last = api.get(f"{BASE}/schedules/{schedule['id']}").json()["last_run"]
    assert last["id"] == run["id"] and last["status"] == "handled"

    everywhere = api.get(f"{BASE}/runs", params={"status": "handled", "limit": 200})
    assert everywhere.status_code == 200 and run["id"] in [r["id"] for r in everywhere.json()]
    other = api.get(f"{BASE}/runs", params={"status": "failed", "limit": 200}).json()
    assert run["id"] not in [r["id"] for r in other]
    assert len(api.get(f"{BASE}/runs", params={"limit": 1}).json()) == 1
    for params in ({"status": "bogus"}, {"limit": 0}, {"limit": 201}):
        r = api.get(f"{BASE}/runs", params=params)
        assert (r.status_code, r.json()["error"]["code"]) == (422, "invalid_request")
    r = api.get(f"{BASE}/schedules/{schedule['id']}/runs", params={"limit": 101})
    assert (r.status_code, r.json()["error"]["code"]) == (422, "invalid_request")

    for missing in (str(uuid.uuid4()), "not-a-uuid"):
        r = api.get(f"{BASE}/runs/{missing}")
        assert (r.status_code, r.json()["error"]["code"]) == (404, "not_found")
    r = api.get(f"{BASE}/schedules/{uuid.uuid4()}/runs")
    assert (r.status_code, r.json()["error"]["code"]) == (404, "not_found")
    r = api.post(f"{BASE}/schedules/{uuid.uuid4()}/run")
    assert (r.status_code, r.json()["error"]["code"]) == (404, "not_found")


def test_run_now_on_disabled_schedule_is_handled(api: Api, fake: Fake, make_schedule):
    schedule = make_schedule(fake.preset(), enabled=False)
    run = run_now(api, schedule["id"])
    assert handled(api, fake, run["id"])["trigger"] == "manual"


def test_run_now_overlap_is_ignored(api: Api, fake: Fake, make_schedule):
    schedule = make_schedule(fake.preset())
    first = run_now(api, schedule["id"])
    running = wait_run(api, first["id"], "in-progress")

    second = run_now(api, schedule["id"])
    assert (second["status"], second["reason"]) == ("ignored", "previous_running")
    assert second["session_id"] is None and second["session_slug"] is None
    assert len(fake.creates(running["session_slug"])) == 1

    fake.complete(running["message_id"])
    wait_run(api, first["id"], "handled")


def test_failed_turn_fails_the_run(api: Api, fake: Fake, make_schedule):
    schedule = make_schedule(fake.preset())
    run = run_now(api, schedule["id"])
    running = wait_run(api, run["id"], "in-progress")

    fake.fail_turn(running["message_id"])
    failed = wait_run(api, run["id"], "failed")
    assert failed["reason"] == "turn_failed" and failed["finished_at"]


# --- reuse (design D15) ------------------------------------------------------------------------


def test_reuse_on_posts_to_the_running_session(api: Api, fake: Fake, make_schedule):
    schedule = make_schedule(fake.preset(reuse_running_agent=True))
    first = run_now(api, schedule["id"])
    done = handled(api, fake, first["id"])
    assert done["reuse"] is None  # nothing to reuse yet

    second = run_now(api, schedule["id"])
    running = wait_run(api, second["id"], "in-progress")
    assert running["reuse"] == "delivered"
    assert running["session_id"] == done["session_id"]

    # the second run created no session; both prompts went to the first run's session
    assert len(fake.creates(done["session_slug"])) == 1
    assert fake.creates(running["session_slug"]) == []
    assert [c["status"] for c in fake.posts(done["session_id"])] == [202, 202]

    fake.complete(running["message_id"])
    final = wait_run(api, second["id"], "handled")
    assert final["reuse"] == "delivered" and final["message_id"] != done["message_id"]


def test_reuse_off_creates_a_session_per_run(api: Api, fake: Fake, make_schedule):
    schedule = make_schedule(fake.preset())
    first = handled(api, fake, run_now(api, schedule["id"])["id"])
    second = handled(api, fake, run_now(api, schedule["id"])["id"])

    assert first["reuse"] is None and second["reuse"] is None
    assert first["session_id"] != second["session_id"]
    assert first["session_slug"] != second["session_slug"]
    assert len(fake.creates(first["session_slug"])) == 1
    assert len(fake.creates(second["session_slug"])) == 1
    assert len(fake.posts(first["session_id"])) == 1
    assert len(fake.posts(second["session_id"])) == 1


# --- scheduled fires, two workers --------------------------------------------------------------


def test_scheduled_fire_is_handled(api: Api, fake: Fake, make_schedule):
    schedule = make_schedule(fake.preset(), cron=EVERY_MINUTE)
    assert len(schedule["next_fires"]) == 3

    first = wait_for(
        lambda: next(iter(schedule_runs(api, schedule["id"])), None),
        "the first scheduled run",
        FIRE_WAIT,
    )
    assert first["trigger"] == "scheduled"
    done = handled(api, fake, first["id"])
    assert done["reason"] is None
    last = api.get(f"{BASE}/schedules/{schedule['id']}").json()["last_run"]
    assert last["id"] == first["id"]


def test_scheduled_overlap_is_ignored(api: Api, fake: Fake, make_schedule):
    schedule = make_schedule(fake.preset(), cron=EVERY_MINUTE)

    first = wait_for(
        lambda: next(iter(schedule_runs(api, schedule["id"])), None),
        "the first scheduled run",
        FIRE_WAIT,
    )
    running = wait_run(api, first["id"], "in-progress")  # held: its turn is never completed

    def ignored() -> dict[str, Any] | None:
        return next(
            (r for r in schedule_runs(api, schedule["id"]) if r["status"] == "ignored"), None
        )

    skipped = wait_for(ignored, "the overlapping fire to be ignored", FIRE_WAIT)
    assert (skipped["trigger"], skipped["reason"]) == ("scheduled", "previous_running")
    assert skipped["fire_time"] != running["fire_time"] and skipped["session_id"] is None
    assert get_run(api, running["id"])["status"] == "in-progress"
    assert len(fake.creates(running["session_slug"])) == 1

    fake.complete(running["message_id"])
    wait_run(api, running["id"], "handled")


def test_two_workers_one_run_and_one_session_per_fire(api: Api, fake: Fake, make_schedule):
    preset = fake.preset()
    schedules = [make_schedule(preset, cron=EVERY_MINUTE) for _ in range(3)]
    completed: set[str] = set()

    def fires_done() -> bool:
        """Complete every in-progress turn; true once each schedule has two handled runs."""
        done = True
        for s in schedules:
            runs = schedule_runs(api, s["id"])
            for run in runs:
                if run["status"] == "in-progress" and run["message_id"] not in completed:
                    fake.complete(run["message_id"])
                    completed.add(run["message_id"])
            done = done and sum(r["status"] == "handled" for r in runs) >= 2
        return done

    wait_for(fires_done, "two handled fires of every schedule", 2 * FIRE_WAIT)

    creates = fake.creates()
    for s in schedules:
        runs = schedule_runs(api, s["id"])
        # exactly one run per (schedule, fire_time) ...
        fire_times = [r["fire_time"] for r in runs if r["trigger"] == "scheduled"]
        assert len(fire_times) == len(set(fire_times)) >= 2
        # ... and exactly one session create per sent run
        for run in runs:
            if run["session_slug"]:
                assert len([c for c in creates if c["body"]["slug"] == run["session_slug"]]) == 1
                assert len(fake.posts(run["session_id"])) == 1
            else:
                assert run["status"] == "ignored"
        slugs = {r["session_slug"] for r in runs if r["session_slug"]}
        assert len(slugs) == sum(1 for r in runs if r["session_slug"])


# --- delete, health, coverage ------------------------------------------------------------------


def test_delete_removes_the_schedule_and_its_runs(api: Api, fake: Fake, make_schedule):
    schedule = make_schedule(fake.preset())
    run = handled(api, fake, run_now(api, schedule["id"])["id"])

    r = api.delete(f"{BASE}/schedules/{schedule['id']}")
    assert r.status_code == 204 and r.content == b""
    for path in (
        f"{BASE}/schedules/{schedule['id']}",
        f"{BASE}/schedules/{schedule['id']}/runs",
        f"{BASE}/runs/{run['id']}",
    ):
        got = api.get(path)
        assert (got.status_code, got.json()["error"]["code"]) == (404, "not_found")
    again = api.delete(f"{BASE}/schedules/{schedule['id']}")
    assert (again.status_code, again.json()["error"]["code"]) == (404, "not_found")
    assert schedule["id"] not in [s["id"] for s in api.get(f"{BASE}/schedules").json()]


def test_health_routes(api: Api):
    assert api.get("/healthz").status_code == 200
    assert api.get("/readyz").status_code == 200


def test_every_route_exercised(api: Api):
    """Every (method, path) of openapi.json was hit by this module's tests (AC3)."""
    missing = sorted(api.spec_routes() - api.hit)
    assert not missing, f"routes in openapi.json the e2e never hit: {missing}"
