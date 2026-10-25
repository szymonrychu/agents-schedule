"""Cron expression and timezone handling shared by the api and the scheduler."""

import re
from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from croniter import CroniterBadCronError, CroniterError, croniter

_FIVE_FIELDS = re.compile(r"^(\S+\s+){4}\S+$")


class CronError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


@dataclass(frozen=True)
class Due:
    fire_time: datetime | None
    skipped: int


def validate_cron(expr: str) -> str:
    normalised = " ".join(expr.split())
    if not _FIVE_FIELDS.match(normalised):
        raise CronError(
            "invalid_cron", "cron must have exactly 5 fields: minute hour day month weekday"
        )
    if "@" in normalised:
        raise CronError("invalid_cron", "cron macros are not supported")
    try:
        if not croniter.is_valid(normalised):
            raise CronError("invalid_cron", f"not a valid cron expression: {normalised}")
        croniter(normalised, datetime(2026, 1, 1))
    except (CroniterError, CroniterBadCronError, ValueError) as exc:
        raise CronError("invalid_cron", f"not a valid cron expression: {exc}") from exc
    return normalised


def validate_timezone(name: str) -> ZoneInfo:
    try:
        return ZoneInfo(name)
    except (ZoneInfoNotFoundError, ValueError, OSError) as exc:
        raise CronError("invalid_timezone", f"unknown IANA timezone: {name!r}") from exc


def _fires_after(expr: str, tz: ZoneInfo, after: datetime):
    """Yield strictly increasing fire times after `after`, tz-aware in `tz`."""
    it = croniter(expr, after.astimezone(tz))
    last = after
    while True:
        fire = it.get_next(datetime).astimezone(tz)
        if fire <= last:
            continue
        last = fire
        yield fire


def next_fires(expr: str, tz: ZoneInfo, after: datetime, n: int = 3) -> list[datetime]:
    gen = _fires_after(expr, tz, after)
    return [next(gen) for _ in range(n)]


def due(expr: str, tz: ZoneInfo, stamp: datetime, now: datetime) -> Due:
    latest: datetime | None = None
    count = 0
    for fire in _fires_after(expr, tz, stamp):
        if fire > now:
            break
        latest = fire
        count += 1
    if latest is None:
        return Due(fire_time=None, skipped=0)
    return Due(fire_time=latest, skipped=count - 1)
