"""The committed OpenAPI document matches the app (design D15)."""

from __future__ import annotations

from pathlib import Path

from agents_schedule.api import openapi

ROOT = Path(__file__).resolve().parent.parent


def test_openapi_matches_app() -> None:
    committed = (ROOT / "openapi.json").read_text(encoding="utf-8")
    assert committed == openapi.export(), (
        "run: uv run python -m agents_schedule.api.openapi > openapi.json"
    )
    assert committed.endswith("\n")
