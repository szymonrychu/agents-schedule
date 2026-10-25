"""The service commands run as real processes against a real database."""

import asyncio
import os
import signal
import socket
import subprocess
import sys
import time
import uuid
from collections.abc import Callable, Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
import psycopg
import pytest
from agents_common.db import open_pool
from agents_common.orchestrator import OrchestratorClient
from agents_common.runtime import lock_key

from agents_schedule import runtime
from agents_schedule.api.app import create_app
from agents_schedule.settings import Settings

LOCK = "agents-schedule-worker"
STARTED = "worker holds the lock"
STANDBY = "worker waiting for the lock"


def env_for(url: str, **extra: str) -> dict[str, str]:
    env = {k: v for k, v in os.environ.items() if not k.startswith("SCHED_")}
    env.update(
        SCHED_DATABASE_URL=url,
        SCHED_ORCHESTRATOR_URL="http://127.0.0.1:1",
        SCHED_ORCHESTRATOR_TOKEN="test-token",
        SCHED_TICK_SECONDS="1",
        SCHED_POLL_SECONDS="1",
        SCHED_SHUTDOWN_GRACE_SECONDS="10",
        PYTHONUNBUFFERED="1",
        **extra,
    )
    return env


def sched(*args: str, env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "agents_schedule.cli", *args],
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
    )


class Worker:
    def __init__(self, url: str, log: Path) -> None:
        self.log = log
        self.out = log.open("w")
        self.proc = subprocess.Popen(
            [sys.executable, "-m", "agents_schedule.cli", "worker"],
            env=env_for(url),
            stdout=self.out,
            stderr=subprocess.STDOUT,
        )

    def text(self) -> str:
        return self.log.read_text()

    def wait_for(self, needle: str, timeout: float = 30) -> None:
        wait(lambda: needle in self.text(), timeout, f"{needle!r} in {self.text()!r}")

    def stop(self) -> int:
        self.proc.send_signal(signal.SIGTERM)
        return self.proc.wait(timeout=30)


def wait(check: Callable[[], object], timeout: float = 30, what: str = "condition") -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if check():
            return
        time.sleep(0.1)
    raise AssertionError(f"timed out waiting for {what}")


@pytest.fixture
def workers(db_url: str, tmp_path: Path) -> Iterator[Callable[[str], Worker]]:
    started: list[Worker] = []

    def start(name: str) -> Worker:
        worker = Worker(db_url, tmp_path / f"{name}.log")
        started.append(worker)
        return worker

    yield start
    for worker in started:
        if worker.proc.poll() is None:
            worker.proc.kill()
            worker.proc.wait()
        worker.out.close()


def add_due_schedule(url: str, name: str, cron: str = "* * * * *") -> uuid.UUID:
    sid = uuid.uuid4()
    with psycopg.connect(url, autocommit=True) as conn:
        conn.execute(
            "INSERT INTO schedules (id, name, cron, timezone, preset_id, prompt, armed_at) "
            "VALUES (%s, %s, %s, 'UTC', %s, 'go', %s)",
            (sid, name, cron, uuid.uuid4(), datetime.now(UTC) - timedelta(hours=2)),
        )
    return sid


def run_count(url: str, sid: uuid.UUID) -> int:
    with psycopg.connect(url) as conn:
        row = conn.execute("SELECT count(*) FROM runs WHERE schedule_id = %s", (sid,)).fetchone()
    assert row is not None
    return row[0]


def lock_holders(url: str) -> int:
    key = lock_key(LOCK)
    with psycopg.connect(url) as conn:
        row = conn.execute(
            "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND granted "
            "AND ((classid::bigint << 32) | objid::bigint) = %s",
            (key,),
        ).fetchone()
    assert row is not None
    return row[0]


def test_migrate_command(empty_db_url: str):
    result = sched("migrate", env=env_for(empty_db_url))
    assert result.returncode == 0, result.stderr
    assert "0001_schedules.sql" in result.stdout
    with psycopg.connect(empty_db_url) as conn:
        row = conn.execute("SELECT to_regclass('public.runs')").fetchone()
    assert row is not None and row[0] == "runs"
    again = sched("migrate", env=env_for(empty_db_url))
    assert again.returncode == 0
    assert "0001_schedules.sql" not in again.stdout


def test_single_worker_fires(db_url: str, workers):
    first = workers("first")
    first.wait_for(STARTED)
    second = workers("second")
    second.wait_for(STANDBY)
    # Daily at the wall-clock time one hour ago: exactly one fire time lies in
    # (armed_at, now] and the next is a day away, whatever minute the run crosses.
    due = datetime.now(UTC) - timedelta(hours=1)
    sid = add_due_schedule(db_url, "daily", cron=f"{due.minute} {due.hour} * * *")
    wait(lambda: run_count(db_url, sid) == 1, what="the due schedule to fire")
    time.sleep(3)  # several ticks of both workers
    assert run_count(db_url, sid) == 1
    assert lock_holders(db_url) == 1
    assert STARTED in first.text()
    assert STARTED not in second.text()


def test_standby_takes_over(db_url: str, workers):
    first = workers("first")
    first.wait_for(STARTED)
    second = workers("second")
    second.wait_for(STANDBY)
    assert first.stop() == 0
    second.wait_for(STARTED)
    sid = add_due_schedule(db_url, "after-failover")
    wait(lambda: run_count(db_url, sid) == 1, what="the standby to fire the schedule")
    assert lock_holders(db_url) == 1
    assert second.stop() == 0
    wait(lambda: lock_holders(db_url) == 0, what="the lock to be released")


def test_bad_config_exits_78(db_url: str):
    env = env_for(db_url, SCHED_MAX_IN_PROGRESS="0")
    del env["SCHED_DATABASE_URL"]
    for command in ("migrate", "api", "worker"):
        result = sched(command, env=env)
        assert result.returncode == 78, (command, result.stderr)
        assert "SCHED_DATABASE_URL" in result.stderr
        assert "SCHED_MAX_IN_PROGRESS" in result.stderr


def test_help_lists_commands():
    result = sched("--help", env=env_for("postgresql://x@127.0.0.1/x"))
    assert result.returncode == 0
    for command in ("migrate", "api", "worker"):
        assert command in result.stdout


def test_api_serves_on_http_port_and_stops_on_sigterm(db_url: str):
    import socket
    import urllib.request

    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    env = env_for(db_url, SCHED_HTTP_PORT=str(port), SCHED_HTTP_HOST="127.0.0.1")
    proc = subprocess.Popen(
        [sys.executable, "-m", "agents_schedule.cli", "api"],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:

        def healthy() -> bool:
            try:
                with urllib.request.urlopen(f"http://127.0.0.1:{port}/healthz", timeout=2) as r:
                    return r.status == 200
            except OSError:
                return False

        wait(healthy, what="the api to answer /healthz")
        proc.send_signal(signal.SIGTERM)
        assert proc.wait(timeout=30) == 0
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait()


async def test_api_ignores_forwarded_headers(db_url: str):
    """Behind the oauth2-proxy sidecar the allowlist sees the socket peer, not X-Forwarded-For."""
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    settings = Settings.model_validate(
        {
            "database_url": db_url,
            "orchestrator_url": "http://127.0.0.1:1",
            "orchestrator_token": "test-token",
            "http_host": "127.0.0.1",
            "http_port": port,
        }
    )
    pool = await open_pool(db_url, min_size=1, max_size=2)
    client = OrchestratorClient(settings.orchestrator_url, "test-token")
    server = runtime._Server(runtime._api_config(settings, create_app(settings, pool, client)))
    serving = asyncio.create_task(server.serve())
    try:
        async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{port}") as http:
            for _ in range(100):
                if server.started:
                    break
                await asyncio.sleep(0.05)
            assert server.started
            url = "/schedules/api/v1/schedules"
            plain = await http.get(url)
            forwarded = await http.get(url, headers={"X-Forwarded-For": "203.0.113.9"})
        assert plain.status_code == 200
        assert forwarded.status_code == 200, forwarded.text
    finally:
        server.should_exit = True
        await serving
        await client.aclose()
        await pool.close()
