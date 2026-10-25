"""Run routes under /schedules/api/v1: history and run now (design D6, D11)."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Annotated, Literal

from agents_common.db import notify
from fastapi import APIRouter, Query, Request

from agents_schedule import dispatcher, scheduler
from agents_schedule.api.errors import ApiError
from agents_schedule.api.schemas import RunView
from agents_schedule.db import runs, schedules

router = APIRouter()

Status = Literal["new", "in-progress", "handled", "failed", "ignored"]


def _parse_id(raw: str, what: str) -> uuid.UUID:
    try:
        return uuid.UUID(raw)
    except ValueError:
        raise ApiError(404, "not_found", f"{what} not found") from None


def _view(run: runs.Run) -> RunView:
    return RunView(**vars(run))


@router.post("/schedules/{schedule_id}/run", status_code=202, response_model=RunView)
async def run_now(request: Request, schedule_id: str) -> RunView:
    sid = _parse_id(schedule_id, "schedule")
    max_per_schedule = request.app.state.settings.max_per_schedule
    async with request.app.state.pool.connection() as conn, conn.transaction():
        # The scheduler's per-schedule lock keeps the in-flight count and the insert atomic.
        await conn.execute(
            "SELECT pg_advisory_xact_lock(%s, hashtext(%s::text))", (scheduler._FIRE_LOCK, sid)
        )
        if await schedules.get(conn, sid) is None:
            raise ApiError(404, "not_found", "schedule not found")
        now = datetime.now(UTC)
        if await runs.in_flight(conn, sid) >= max_per_schedule:
            run = await runs.insert_manual(conn, sid, now, "ignored", "previous_running")
        else:
            run = await runs.insert_manual(conn, sid, now, "new")
            await notify(conn, dispatcher.CHANNEL)
        return _view(run)


@router.get("/schedules/{schedule_id}/runs", response_model=list[RunView])
async def schedule_runs(
    request: Request, schedule_id: str, limit: Annotated[int, Query(ge=1, le=100)] = 20
) -> list[RunView]:
    sid = _parse_id(schedule_id, "schedule")
    async with request.app.state.pool.connection() as conn:
        if await schedules.get(conn, sid) is None:
            raise ApiError(404, "not_found", "schedule not found")
        return [_view(r) for r in await runs.list_for_schedule(conn, sid, limit)]


@router.get("/runs", response_model=list[RunView])
async def list_runs(
    request: Request,
    status: Status | None = None,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
) -> list[RunView]:
    async with request.app.state.pool.connection() as conn:
        return [_view(r) for r in await runs.list_recent(conn, status, limit)]


@router.get("/runs/{run_id}", response_model=RunView)
async def get_run(request: Request, run_id: str) -> RunView:
    rid = _parse_id(run_id, "run")
    async with request.app.state.pool.connection() as conn:
        run = await runs.get(conn, rid)
    if run is None:
        raise ApiError(404, "not_found", "run not found")
    return _view(run)
