"""The runs table (design D3, D6, D8, D9, D15).

`runs` is the only multi-writer table: the api inserts manual runs, the worker the rest. Every
UPDATE carries `AND status=<expected>` through `agents_common.db.transition`, scheduled inserts
are exactly-once on `(schedule_id, fire_time)`, and claims use `FOR UPDATE SKIP LOCKED`.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import psycopg
from agents_common.db import claim
from agents_common.db import transition as guarded_transition
from psycopg.rows import class_row

# Serialises claims, so the count-based caps hold with more than one claimer.
_CLAIM_LOCK = 0x73636864  # "schd"

_COLUMNS = (
    "id, schedule_id, trigger, fire_time, status, reason, attempts, session_slug, session_id, "
    "message_id, reuse, created_at, updated_at, started_at, finished_at"
)
_ORDER = "ORDER BY fire_time DESC, created_at DESC"

# The oldest `new` run that fits both caps and has no older `new` run in its schedule.
_PICK = f"""
SELECT {_COLUMNS} FROM runs r
WHERE r.status = 'new'
  AND (SELECT count(*) FROM runs WHERE status = 'in-progress') < %(cap)s
  AND (SELECT count(*) FROM runs p
       WHERE p.schedule_id = r.schedule_id AND p.status = 'in-progress') < %(per)s
  AND NOT EXISTS (
      SELECT 1 FROM runs o
      WHERE o.schedule_id = r.schedule_id AND o.status = 'new'
        AND (o.fire_time, o.created_at) < (r.fire_time, r.created_at))
ORDER BY r.fire_time, r.created_at
LIMIT 1
FOR UPDATE OF r SKIP LOCKED
"""


@dataclass(frozen=True)
class Run:
    id: uuid.UUID
    schedule_id: uuid.UUID
    trigger: str
    fire_time: datetime
    status: str
    reason: str | None
    attempts: int
    session_slug: str | None
    session_id: uuid.UUID | None
    message_id: str | None
    reuse: str | None
    created_at: datetime
    updated_at: datetime
    started_at: datetime | None
    finished_at: datetime | None


async def _fetch(conn: psycopg.AsyncConnection, query: str, params: Any = None) -> list[Run]:
    async with conn.cursor(row_factory=class_row(Run)) as cur:
        await cur.execute(query, params)
        return await cur.fetchall()


async def _fetch_one(conn: psycopg.AsyncConnection, query: str, params: Any = None) -> Run | None:
    rows = await _fetch(conn, query, params)
    return rows[0] if rows else None


async def _insert(
    conn: psycopg.AsyncConnection,
    schedule_id: uuid.UUID,
    trigger: str,
    fire_time: datetime,
    status: str,
    reason: str | None,
    tail: str = "",
) -> Run | None:
    return await _fetch_one(
        conn,
        "INSERT INTO runs (id, schedule_id, trigger, fire_time, status, reason, finished_at) "
        f"VALUES (%s, %s, %s, %s, %s, %s, %s) {tail} RETURNING {_COLUMNS}",
        (
            uuid.uuid4(),
            schedule_id,
            trigger,
            fire_time,
            status,
            reason,
            datetime.now(UTC) if status == "ignored" else None,
        ),
    )


async def insert_scheduled(
    conn: psycopg.AsyncConnection,
    schedule_id: uuid.UUID,
    fire_time: datetime,
    status: str,
    reason: str | None = None,
) -> Run | None:
    """Record one fire; None when `(schedule_id, fire_time)` was already recorded."""
    return await _insert(
        conn,
        schedule_id,
        "scheduled",
        fire_time,
        status,
        reason,
        "ON CONFLICT (schedule_id, fire_time) WHERE trigger = 'scheduled' DO NOTHING",
    )


async def insert_manual(
    conn: psycopg.AsyncConnection,
    schedule_id: uuid.UUID,
    now: datetime,
    status: str,
    reason: str | None = None,
) -> Run:
    run = await _insert(conn, schedule_id, "manual", now, status, reason)
    assert run is not None
    return run


async def last_scheduled_fire(
    conn: psycopg.AsyncConnection, schedule_id: uuid.UUID
) -> datetime | None:
    cur = await conn.execute(
        "SELECT max(fire_time) FROM runs WHERE schedule_id = %s AND trigger = 'scheduled'",
        (schedule_id,),
    )
    row = await cur.fetchone()
    return row[0] if row else None


async def in_flight(conn: psycopg.AsyncConnection, schedule_id: uuid.UUID) -> int:
    """Runs of the schedule in `new` or `in-progress`."""
    cur = await conn.execute(
        "SELECT count(*) FROM runs WHERE schedule_id = %s AND status IN ('new', 'in-progress')",
        (schedule_id,),
    )
    row = await cur.fetchone()
    assert row is not None
    return row[0]


async def claim_next(
    conn: psycopg.AsyncConnection,
    max_in_progress: int,
    max_per_schedule: int,
    now: datetime,
) -> Run | None:
    """Move the oldest eligible `new` run to in-progress; None when nothing is claimable.

    A schedule's runs are claimed strictly in `(fire_time, created_at)` order: a run is never
    eligible while an older run of its schedule is still `new`.
    """
    params = {"cap": max_in_progress, "per": max_per_schedule}
    async with conn.transaction():
        await conn.execute("SELECT pg_advisory_xact_lock(%s)", (_CLAIM_LOCK,))
        row = await claim(conn, _PICK, params)
        if row is None:
            return None
        if not await transition(conn, row["id"], "new", "in-progress", started_at=now):
            return None
        return await get(conn, row["id"])


async def transition(
    conn: psycopg.AsyncConnection,
    run_id: uuid.UUID,
    expected: str,
    new: str,
    **fields: Any,
) -> bool:
    """Status-guarded move; False when the run was not in `expected`."""
    fields.setdefault("updated_at", datetime.now(UTC))
    return await guarded_transition(conn, "runs", run_id, expected, new, **fields)


async def set_fields(
    conn: psycopg.AsyncConnection, run_id: uuid.UUID, expected: str, **fields: Any
) -> bool:
    """Set session_slug, session_id, message_id, reuse or attempts without a status change."""
    return await transition(conn, run_id, expected, expected, **fields)


async def get(conn: psycopg.AsyncConnection, run_id: uuid.UUID) -> Run | None:
    return await _fetch_one(conn, f"SELECT {_COLUMNS} FROM runs WHERE id = %s", (run_id,))


async def latest_with_session(
    conn: psycopg.AsyncConnection, schedule_id: uuid.UUID, exclude_run_id: uuid.UUID
) -> Run | None:
    """The schedule's newest non-ignored run other than `exclude_run_id` (design D15)."""
    return await _fetch_one(
        conn,
        f"SELECT {_COLUMNS} FROM runs WHERE schedule_id = %s AND id <> %s "
        f"AND status <> 'ignored' {_ORDER} LIMIT 1",
        (schedule_id, exclude_run_id),
    )


async def list_for_schedule(
    conn: psycopg.AsyncConnection, schedule_id: uuid.UUID, limit: int
) -> list[Run]:
    return await _fetch(
        conn,
        f"SELECT {_COLUMNS} FROM runs WHERE schedule_id = %s {_ORDER} LIMIT %s",
        (schedule_id, limit),
    )


async def list_recent(
    conn: psycopg.AsyncConnection, status: str | None = None, limit: int = 50
) -> list[Run]:
    return await _fetch(
        conn,
        f"SELECT {_COLUMNS} FROM runs WHERE (%(status)s::text IS NULL OR status = %(status)s) "
        f"{_ORDER} LIMIT %(limit)s",
        {"status": status, "limit": limit},
    )


async def list_in_progress(conn: psycopg.AsyncConnection) -> list[Run]:
    return await _fetch(conn, f"SELECT {_COLUMNS} FROM runs WHERE status = 'in-progress' {_ORDER}")
