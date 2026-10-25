import uuid
from collections.abc import AsyncIterator

import psycopg
import pytest

from agents_schedule.db import schedules


@pytest.fixture
async def aconn(db_url: str) -> AsyncIterator[psycopg.AsyncConnection]:
    async with await psycopg.AsyncConnection.connect(db_url, autocommit=True) as connection:
        yield connection


def fields(**over):
    base = {
        "name": "nightly",
        "cron": "0 3 * * *",
        "timezone": "UTC",
        "preset_id": uuid.uuid4(),
        "prompt": "go",
        "enabled": True,
    }
    return base | over


async def test_crud_round_trip(aconn):
    b = await schedules.create(aconn, **fields(name="b"))
    a = await schedules.create(aconn, **fields(name="a", enabled=False))
    assert (b.name, b.cron, b.timezone, b.prompt, b.enabled) == (
        "b",
        "0 3 * * *",
        "UTC",
        "go",
        True,
    )
    assert b.armed_at is not None
    assert await schedules.get(aconn, b.id) == b
    assert await schedules.get(aconn, uuid.uuid4()) is None
    assert [s.name for s in await schedules.list_all(aconn)] == ["a", "b"]
    assert [s.name for s in await schedules.list_enabled(aconn)] == ["b"]
    await schedules.delete(aconn, a.id)
    assert await schedules.get(aconn, a.id) is None


async def test_duplicate_name_conflicts(aconn):
    first = await schedules.create(aconn, **fields(name="x"))
    with pytest.raises(schedules.ScheduleConflict):
        await schedules.create(aconn, **fields(name="x"))
    other = await schedules.create(aconn, **fields(name="y"))
    with pytest.raises(schedules.ScheduleConflict):
        await schedules.update(aconn, other.id, **fields(name="x"))
    assert (await schedules.get(aconn, first.id)) == first


async def test_armed_at_rules(aconn):
    s = await schedules.create(aconn, **fields())
    base = fields(preset_id=s.preset_id)

    edited = await schedules.update(aconn, s.id, **(base | {"prompt": "new prompt", "name": "n2"}))
    assert edited.armed_at == s.armed_at
    assert (edited.prompt, edited.name) == ("new prompt", "n2")
    assert edited.updated_at > s.updated_at

    base = base | {"prompt": "new prompt", "name": "n2"}
    cron = await schedules.update(aconn, s.id, **(base | {"cron": "0 4 * * *"}))
    assert cron.armed_at > edited.armed_at

    tz = await schedules.update(
        aconn, s.id, **(base | {"cron": "0 4 * * *", "timezone": "Europe/Warsaw"})
    )
    assert tz.armed_at > cron.armed_at

    base = base | {"cron": "0 4 * * *", "timezone": "Europe/Warsaw"}
    off = await schedules.update(aconn, s.id, **(base | {"enabled": False}))
    assert off.armed_at == tz.armed_at
    again = await schedules.update(aconn, s.id, **(base | {"enabled": False, "prompt": "p"}))
    assert again.armed_at == tz.armed_at
    on = await schedules.update(aconn, s.id, **(base | {"enabled": True}))
    assert on.armed_at > tz.armed_at
    same = await schedules.update(aconn, s.id, **(base | {"enabled": True}))
    assert same.armed_at == on.armed_at


async def test_missing_id(aconn):
    with pytest.raises(schedules.ScheduleNotFound):
        await schedules.update(aconn, uuid.uuid4(), **fields())
    with pytest.raises(schedules.ScheduleNotFound):
        await schedules.delete(aconn, uuid.uuid4())
