"""The schedules table (design D3; approval A2).

Only the api process calls the write functions (one writer per table). `armed_at` is the moment
a schedule started counting fires: it is set on create and moved on update only when the schedule
is enabled again or its cron or timezone changes, so editing the prompt never causes a catch-up.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any, LiteralString, cast

import psycopg
from psycopg import errors
from psycopg.rows import class_row

_COLUMNS = "id, name, cron, timezone, preset_id, prompt, enabled, armed_at, created_at, updated_at"
_UPDATABLE = frozenset({"name", "cron", "timezone", "preset_id", "prompt", "enabled"})


class ScheduleConflict(Exception):
    """A schedule with this name already exists."""


class ScheduleNotFound(Exception):
    """No schedule has this id."""


@dataclass(frozen=True)
class Schedule:
    id: uuid.UUID
    name: str
    cron: str
    timezone: str
    preset_id: uuid.UUID
    prompt: str
    enabled: bool
    armed_at: datetime
    created_at: datetime
    updated_at: datetime


async def _fetch(conn: psycopg.AsyncConnection, query: str, params: Any = None) -> list[Schedule]:
    async with conn.cursor(row_factory=class_row(Schedule)) as cur:
        try:
            await cur.execute(cast(LiteralString, query), params)  # built from constants
        except errors.UniqueViolation as exc:
            raise ScheduleConflict("a schedule with this name already exists") from exc
        return await cur.fetchall()


async def create(
    conn: psycopg.AsyncConnection,
    *,
    name: str,
    cron: str,
    timezone: str,
    preset_id: uuid.UUID,
    prompt: str,
    enabled: bool,
) -> Schedule:
    rows = await _fetch(
        conn,
        "INSERT INTO schedules (id, name, cron, timezone, preset_id, prompt, enabled, armed_at) "
        f"VALUES (%s, %s, %s, %s, %s, %s, %s, now()) RETURNING {_COLUMNS}",
        (uuid.uuid4(), name, cron, timezone, preset_id, prompt, enabled),
    )
    return rows[0]


async def get(conn: psycopg.AsyncConnection, schedule_id: uuid.UUID) -> Schedule | None:
    rows = await _fetch(conn, f"SELECT {_COLUMNS} FROM schedules WHERE id = %s", (schedule_id,))
    return rows[0] if rows else None


async def list_all(conn: psycopg.AsyncConnection) -> list[Schedule]:
    return await _fetch(conn, f"SELECT {_COLUMNS} FROM schedules ORDER BY name")


async def list_enabled(conn: psycopg.AsyncConnection) -> list[Schedule]:
    return await _fetch(conn, f"SELECT {_COLUMNS} FROM schedules WHERE enabled ORDER BY name")


async def update(conn: psycopg.AsyncConnection, schedule_id: uuid.UUID, **fields: Any) -> Schedule:
    """Full replace of name, cron, timezone, preset_id, prompt and enabled."""
    if fields.keys() != _UPDATABLE:
        raise TypeError(f"update needs exactly {sorted(_UPDATABLE)}, got {sorted(fields)}")
    rows = await _fetch(
        conn,
        """
        UPDATE schedules SET
            name = %(name)s, cron = %(cron)s, timezone = %(timezone)s,
            preset_id = %(preset_id)s, prompt = %(prompt)s, enabled = %(enabled)s,
            armed_at = CASE
                WHEN (%(enabled)s AND NOT enabled)
                  OR %(cron)s <> cron OR %(timezone)s <> timezone THEN now()
                ELSE armed_at END,
            updated_at = now()
        WHERE id = %(id)s
        RETURNING """
        + _COLUMNS,
        {**fields, "id": schedule_id},
    )
    if not rows:
        raise ScheduleNotFound(str(schedule_id))
    return rows[0]


async def delete(conn: psycopg.AsyncConnection, schedule_id: uuid.UUID) -> None:
    cur = await conn.execute("DELETE FROM schedules WHERE id = %s", (schedule_id,))
    if cur.rowcount == 0:
        raise ScheduleNotFound(str(schedule_id))
