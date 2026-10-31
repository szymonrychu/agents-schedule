"""Prompt rendering and session slugs of a run (design D7, D9)."""

from __future__ import annotations

import re
import uuid
from datetime import datetime

from agents_common import templating

SLUG_MAX = 40
_BASE_MAX = 24
_NOT_LABEL = re.compile(r"[^a-z0-9]+")


def prompt_context(
    schedule_name: str, fired_at: datetime, preset: str, now: datetime
) -> dict[str, str]:
    """The variables a prompt template can read; datetimes are ISO 8601."""
    return {
        "schedule_name": schedule_name,
        "fired_at": fired_at.isoformat(),
        "preset": preset,
        "now": now.isoformat(),
    }


def render_prompt(
    template: str, schedule_name: str, fired_at: datetime, preset: str, now: datetime
) -> str:
    """Render the prompt Jinja template; raises `TemplateError` when it does not render."""
    return templating.render(template, prompt_context(schedule_name, fired_at, preset, now))


def _slugify(text: str, max_len: int) -> str:
    return _NOT_LABEL.sub("-", text.lower()).strip("-")[:max_len].rstrip("-")


def session_slug(schedule_name: str, run_id: uuid.UUID) -> str:
    """`<name slug, at most 24>-<first 8 hex of run id>`: a DNS-1123 label of at most 40.

    A name with nothing usable becomes `schedule`; the orchestrator reserves `orchestrator`
    and `orchestrator-*`, so such a name is prefixed `s-`.
    """
    base = _slugify(schedule_name, _BASE_MAX) or "schedule"
    if base == "orchestrator" or base.startswith("orchestrator-"):
        base = _slugify(f"s-{base}", _BASE_MAX)
    slug = f"{base}-{run_id.hex[:8]}"
    assert len(slug) <= SLUG_MAX
    return slug
