"""The admin-only chain route: agents-webhook starts a chained run for a handled event.

Registered by `create_admin_app` only, so it is behind the bearer token gate, never on the ui
app and never in the orchestrator relay (design: webhook-sourced chains).
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Literal

from fastapi import APIRouter, Request, Response
from pydantic import BaseModel, ConfigDict, Field

from agents_schedule import chain
from agents_schedule.api.errors import ApiError
from agents_schedule.api.runs import _parse_id, _view
from agents_schedule.api.schemas import RunView
from agents_schedule.db import runs, schedules

router = APIRouter()


class ChainIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source: Literal["webhook"]
    source_ref: str = Field(min_length=1, max_length=100)
    previous_name: str = Field(min_length=1, max_length=200)
    previous_status: Literal["handled"]
    previous_session_slug: str | None = None
    previous_result: str | None = Field(default=None, max_length=8000)


@router.post("/schedules/{schedule_id}/chain", status_code=202, response_model=RunView)
async def chain_run(
    request: Request, response: Response, schedule_id: str, payload: ChainIn
) -> RunView:
    sid = _parse_id(schedule_id, "schedule")
    source = chain.ChainSource(
        chain_source=payload.source,
        chain_ref=payload.source_ref,
        chained_from=None,
        depth=0,
        previous_name=payload.previous_name,
        previous_status=payload.previous_status,
        previous_session_slug=payload.previous_session_slug,
        previous_result=payload.previous_result,
    )
    async with request.app.state.pool.connection() as conn, conn.transaction():
        if await schedules.get(conn, sid) is None:
            raise ApiError(404, "not_found", "schedule not found")
        inserted = await chain.fire_chain(
            conn,
            targets=[sid],
            source=source,
            max_per_schedule=request.app.state.settings.max_per_schedule,
            now=datetime.now(UTC),
        )
        if inserted:
            return _view(inserted[0])
        # fire_chain skips a target this source already chained into: an idempotent retry.
        existing = await runs.get_chained(conn, sid, payload.source, payload.source_ref)
        if existing is None:
            raise ApiError(404, "not_found", "schedule not found")
        response.status_code = 200
        return _view(existing)
