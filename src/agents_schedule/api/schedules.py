"""Schedule routes under /schedules/api/v1. The api is the only writer of `schedules`."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from zoneinfo import ZoneInfo

import psycopg
from agents_common.db import notify
from agents_common.orchestrator import (
    NotFound,
    OrchestratorClient,
    OrchestratorError,
    OrchestratorUnavailable,
)
from fastapi import APIRouter, Request, Response

from agents_schedule import cron, scheduler
from agents_schedule.api.errors import ApiError
from agents_schedule.api.schemas import (
    PreviewIn,
    PreviewView,
    RunView,
    ScheduleIn,
    ScheduleView,
)
from agents_schedule.db import runs, schedules
from agents_schedule.settings import Settings

router = APIRouter()


def _parse_id(raw: str) -> uuid.UUID:
    try:
        return uuid.UUID(raw)
    except ValueError:
        raise ApiError(404, "not_found", "schedule not found") from None


def _validate_cron(
    expr: str, timezone: str | None, settings: Settings
) -> tuple[str, str, ZoneInfo]:
    try:
        normalised = cron.validate_cron(expr)
        name = timezone if timezone is not None else settings.default_timezone
        tz = cron.validate_timezone(name)
    except cron.CronError as exc:
        raise ApiError(422, exc.code, exc.message) from exc
    return normalised, name, tz


async def _check_preset(client: OrchestratorClient, preset_id: uuid.UUID) -> None:
    try:
        await client.get_preset(preset_id)
    except NotFound as exc:
        raise ApiError(422, "unknown_preset", f"preset {preset_id} does not exist") from exc
    except (OrchestratorUnavailable, OrchestratorError) as exc:
        raise ApiError(
            503, "orchestrator_unavailable", "the orchestrator could not be reached"
        ) from exc


def _view(sched: schedules.Schedule, last: runs.Run | None, now: datetime) -> ScheduleView:
    fires = cron.next_fires(sched.cron, ZoneInfo(sched.timezone), now, 3) if sched.enabled else []
    return ScheduleView(
        id=sched.id,
        name=sched.name,
        cron=sched.cron,
        timezone=sched.timezone,
        preset_id=sched.preset_id,
        prompt=sched.prompt,
        enabled=sched.enabled,
        next_fires=fires,
        last_run=RunView(**vars(last).copy()) if last else None,
        created_at=sched.created_at,
        updated_at=sched.updated_at,
    )


async def _view_of(conn: psycopg.AsyncConnection, sched: schedules.Schedule) -> ScheduleView:
    last = await runs.list_for_schedule(conn, sched.id, 1)
    return _view(sched, last[0] if last else None, datetime.now(UTC))


async def _write(request: Request, data: ScheduleIn, schedule_id: uuid.UUID | None) -> ScheduleView:
    settings: Settings = request.app.state.settings
    expr, tz_name, _ = _validate_cron(data.cron, data.timezone, settings)
    await _check_preset(request.app.state.client, data.preset_id)
    fields = {
        "name": data.name,
        "cron": expr,
        "timezone": tz_name,
        "preset_id": data.preset_id,
        "prompt": data.prompt,
        "enabled": data.enabled,
    }
    try:
        async with request.app.state.pool.connection() as conn, conn.transaction():
            if schedule_id is None:
                sched = await schedules.create(conn, **fields)
            else:
                sched = await schedules.update(conn, schedule_id, **fields)
            await notify(conn, scheduler.CHANNEL)
            return await _view_of(conn, sched)
    except schedules.ScheduleConflict as exc:
        raise ApiError(409, "conflict", "a schedule with this name already exists") from exc
    except schedules.ScheduleNotFound as exc:
        raise ApiError(404, "not_found", "schedule not found") from exc


@router.get("/schedules", response_model=list[ScheduleView])
async def list_schedules(request: Request) -> list[ScheduleView]:
    async with request.app.state.pool.connection() as conn:
        return [await _view_of(conn, s) for s in await schedules.list_all(conn)]


@router.post("/schedules", status_code=201, response_model=ScheduleView)
async def create_schedule(request: Request, data: ScheduleIn) -> ScheduleView:
    return await _write(request, data, None)


@router.post("/schedules/preview", response_model=PreviewView)
async def preview(request: Request, data: PreviewIn) -> PreviewView:
    expr, _, tz = _validate_cron(data.cron, data.timezone, request.app.state.settings)
    return PreviewView(next_fires=cron.next_fires(expr, tz, datetime.now(UTC), 3))


@router.get("/schedules/{schedule_id}", response_model=ScheduleView)
async def get_schedule(request: Request, schedule_id: str) -> ScheduleView:
    async with request.app.state.pool.connection() as conn:
        sched = await schedules.get(conn, _parse_id(schedule_id))
        if sched is None:
            raise ApiError(404, "not_found", "schedule not found")
        return await _view_of(conn, sched)


@router.put("/schedules/{schedule_id}", response_model=ScheduleView)
async def replace_schedule(request: Request, schedule_id: str, data: ScheduleIn) -> ScheduleView:
    return await _write(request, data, _parse_id(schedule_id))


@router.delete("/schedules/{schedule_id}", status_code=204)
async def delete_schedule(request: Request, schedule_id: str) -> Response:
    try:
        async with request.app.state.pool.connection() as conn, conn.transaction():
            await schedules.delete(conn, _parse_id(schedule_id))
            await notify(conn, scheduler.CHANNEL)
    except schedules.ScheduleNotFound as exc:
        raise ApiError(404, "not_found", "schedule not found") from exc
    return Response(status_code=204)
