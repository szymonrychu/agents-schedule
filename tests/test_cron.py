from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from agents_schedule.cron import (
    CronError,
    Due,
    due,
    next_fires,
    validate_cron,
    validate_timezone,
)

WARSAW = ZoneInfo("Europe/Warsaw")


def test_validate_normalises():
    assert validate_cron("  */5   9-17  * *\t1-5 ") == "*/5 9-17 * * 1-5"


@pytest.mark.parametrize(
    "expr",
    ["* * * *", "* * * * * *", "0 0 * * * 2026", "@daily", "nonsense", "", "61 * * * *"],
)
def test_validate_refuses(expr):
    with pytest.raises(CronError) as exc:
        validate_cron(expr)
    assert exc.value.code == "invalid_cron"
    assert exc.value.message


def test_timezone_refused():
    assert validate_timezone("Europe/Warsaw") == WARSAW
    for name in ("Mars/Olympus", "", "../etc/passwd"):
        with pytest.raises(CronError) as exc:
            validate_timezone(name)
        assert exc.value.code == "invalid_timezone"


def test_next_fires():
    after = datetime(2026, 5, 1, 10, 0, tzinfo=UTC)  # 12:00 in Warsaw (CEST)
    fires = next_fires("0 * * * *", WARSAW, after)
    assert len(fires) == 3
    assert all(f.tzinfo is not None and f > after for f in fires)
    assert [f.astimezone(WARSAW).hour for f in fires] == [13, 14, 15]
    assert all(f.utcoffset() == timedelta(hours=2) for f in fires)
    assert len(next_fires("0 * * * *", WARSAW, after, n=5)) == 5


def test_due_collapses_missed_runs():
    stamp = datetime(2026, 5, 1, 10, 0, tzinfo=UTC)
    now = datetime(2026, 5, 1, 14, 30, tzinfo=UTC)
    result = due("0 * * * *", UTC, stamp, now)
    assert result == Due(fire_time=datetime(2026, 5, 1, 14, 0, tzinfo=UTC), skipped=3)


def test_due_single_fire_has_no_skips():
    stamp = datetime(2026, 5, 1, 10, 0, tzinfo=UTC)
    now = datetime(2026, 5, 1, 11, 0, tzinfo=UTC)
    assert due("0 * * * *", UTC, stamp, now) == Due(
        fire_time=datetime(2026, 5, 1, 11, 0, tzinfo=UTC), skipped=0
    )


def test_not_due_before_next():
    stamp = datetime(2026, 5, 1, 10, 0, tzinfo=UTC)
    now = datetime(2026, 5, 1, 10, 59, tzinfo=UTC)
    assert due("0 * * * *", UTC, stamp, now) == Due(fire_time=None, skipped=0)


def _walk(expr, start, end):
    fires = []
    cursor = start
    while True:
        batch = next_fires(expr, WARSAW, cursor, n=1)
        if batch[0] > end:
            return fires
        fires.append(batch[0])
        cursor = batch[0]


def test_dst_transitions_fire_once():
    # fall-back 2026-10-25: 02:30 happens twice in Warsaw
    fall = _walk(
        "30 2 * * *",
        datetime(2026, 10, 24, 12, 0, tzinfo=UTC),
        datetime(2026, 10, 26, 12, 0, tzinfo=UTC),
    )
    on_day = [f for f in fall if f.astimezone(WARSAW).date().isoformat() == "2026-10-25"]
    assert len(on_day) == 1
    # spring-forward 2026-03-29: 02:30 does not exist in Warsaw
    spring = _walk(
        "30 2 * * *",
        datetime(2026, 3, 28, 12, 0, tzinfo=UTC),
        datetime(2026, 3, 30, 12, 0, tzinfo=UTC),
    )
    on_day = [f for f in spring if f.astimezone(WARSAW).date().isoformat() == "2026-03-29"]
    assert len(on_day) == 1
    assert len({f for f in spring}) == len(spring)
    # due() agrees: one catch-up run, nothing double counted across the gap
    result = due(
        "30 2 * * *",
        WARSAW,
        datetime(2026, 3, 28, 12, 0, tzinfo=UTC),
        datetime(2026, 3, 29, 12, 0, tzinfo=UTC),
    )
    assert result.fire_time is not None and result.skipped == 0
