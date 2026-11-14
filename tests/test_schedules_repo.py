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


async def three(aconn):
    return [(await schedules.create(aconn, **fields(name=n))).id for n in ("a", "b", "c")]


async def test_set_chain_keeps_order_and_replaces(aconn):
    a, b, c = await three(aconn)
    assert (await schedules.get(aconn, a)).then_schedule_ids == []
    await schedules.set_chain(aconn, a, [c, b])
    assert (await schedules.get(aconn, a)).then_schedule_ids == [c, b]
    assert [s.then_schedule_ids for s in await schedules.list_all(aconn)] == [[c, b], [], []]
    await schedules.set_chain(aconn, a, [b])
    assert (await schedules.get(aconn, a)).then_schedule_ids == [b]
    await schedules.set_chain(aconn, a, [])
    assert (await schedules.get(aconn, a)).then_schedule_ids == []


async def test_chain_cycle(aconn):
    a, b, c = await three(aconn)
    await schedules.set_chain(aconn, a, [b])
    assert await schedules.chain_cycle(aconn, b, [a]) is True
    await schedules.set_chain(aconn, b, [c])
    assert await schedules.chain_cycle(aconn, c, [a]) is True
    assert await schedules.chain_cycle(aconn, c, [b]) is True
    assert await schedules.chain_cycle(aconn, c, []) is False


async def test_chain_diamond_is_no_cycle(aconn):
    a, b, c = await three(aconn)
    d = (await schedules.create(aconn, **fields(name="d"))).id
    await schedules.set_chain(aconn, b, [d])
    await schedules.set_chain(aconn, c, [d])
    assert await schedules.chain_cycle(aconn, a, [b, c]) is False


async def test_chain_cycle_ignores_own_current_links(aconn):
    a, b, c = await three(aconn)
    await schedules.set_chain(aconn, a, [b])
    # replacing a's links with [c] must not see the old a to b link
    await schedules.set_chain(aconn, b, [c])
    assert await schedules.chain_cycle(aconn, c, [a]) is True
    assert await schedules.chain_cycle(aconn, a, [c]) is False
