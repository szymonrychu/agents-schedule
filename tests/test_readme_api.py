"""README API coverage and the committed OpenAPI document (design D11, D15)."""

from __future__ import annotations

import re
from pathlib import Path

from fastapi.routing import APIRoute

from agents_schedule.api import openapi
from agents_schedule.api.app import PREFIX, create_app
from agents_schedule.settings import Settings

ROOT = Path(__file__).resolve().parent.parent
README = (ROOT / "README.md").read_text(encoding="utf-8")


def section(title: str) -> str:
    """The body of the `## title` section (up to the next H2)."""
    match = re.search(rf"^## {re.escape(title)}\s*$", README, re.MULTILINE)
    assert match, f"README has no '## {title}' section"
    rest = README[match.end() :]
    nxt = re.search(r"^## ", rest, re.MULTILINE)
    return rest[: nxt.start()] if nxt else rest


def _walk(router, prefix: str = ""):
    """(path, methods) of every route; FastAPI includes routers lazily, so descend into them."""
    for route in router.routes:
        if isinstance(route, APIRoute):
            yield prefix + route.path, route.methods
        elif hasattr(route, "original_router"):
            yield from _walk(route.original_router, prefix + route.include_context.prefix)


def app_routes() -> set[str]:
    settings = Settings(
        database_url="postgresql://unused/db",
        orchestrator_url="http://orchestrator.invalid",
        orchestrator_token="t",
    )
    app = create_app(settings, pool=None, client=None)  # type: ignore[arg-type]
    out = set()
    for path, methods in _walk(app):
        if path.startswith(PREFIX):
            out.update(f"{m} {path}" for m in methods - {"HEAD"})
    return out


def readme_routes() -> set[str]:
    found = re.findall(
        r"^### `(GET|POST|PATCH|PUT|DELETE) (/schedules/api/v1\S*)`\s*$",
        section("API"),
        re.MULTILINE,
    )
    return {f"{m} {p}" for m, p in found}


def test_openapi_matches_app() -> None:
    committed = (ROOT / "openapi.json").read_text(encoding="utf-8")
    assert committed == openapi.export(), (
        "run: uv run python -m agents_schedule.api.openapi > openapi.json"
    )
    assert committed.endswith("\n")


def test_every_route_documented() -> None:
    expected = app_routes()
    assert len(expected) == 10
    missing = expected - readme_routes()
    assert not missing, f"undocumented routes: {sorted(missing)}"
    stale = readme_routes() - expected
    assert not stale, f"README documents routes the app lacks: {sorted(stale)}"


def test_readme_documents_runs_reuse_and_versioning() -> None:
    api = section("API")
    for needle in (
        "/schedules/api/v1",
        "previous_running",
        "schedule_disabled",
        "preset_not_found",
        "session_not_found",
        "orchestrator_refused",
        "orchestrator_unavailable",
        "message_failed",
        "message_cancelled",
        "turn_failed",
        "timeout",
        "reuse_running_agent",
        "delivered",
        "fallback",
        "{{ schedule_name }}",
        "{{ fired_at }}",
        "invalid_prompt",
        "orchestrator_auth_failed",
        "unknown_preset",
        "invalid_cron",
        "invalid_timezone",
        "forbidden",
        "not_found",
        "conflict",
        "invalid_request",
        "SCHED_ALLOWED_CIDRS",
    ):
        assert needle in api, needle
    for status in ("new", "in-progress", "handled", "failed", "ignored"):
        assert f"`{status}`" in api, status
    assert re.search(r"^### Versioning\s*$", api, re.MULTILINE)
    assert "additive" in api.split("### Versioning", 1)[1]


def test_readme_names_commands() -> None:
    commands = section("Commands")
    for name in ("sched migrate", "sched api", "sched worker"):
        assert name in commands, name
