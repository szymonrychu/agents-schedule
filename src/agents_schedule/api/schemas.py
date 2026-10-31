"""Request and response models of the schedules api."""

from __future__ import annotations

import uuid
from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


class ScheduleIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1, max_length=100)
    cron: str
    timezone: str | None = None  # None means SCHED_DEFAULT_TIMEZONE
    preset_id: uuid.UUID
    prompt: str = Field(min_length=1, max_length=16384)
    enabled: bool = True


class PreviewIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    cron: str
    timezone: str | None = None
    prompt: str | None = Field(default=None, max_length=16384)
    schedule_name: str | None = None
    preset_id: str | None = None


class PreviewView(BaseModel):
    next_fires: list[datetime]
    rendered: str | None = None
    variables: list[str] = []


class RunView(BaseModel):
    id: uuid.UUID
    schedule_id: uuid.UUID
    trigger: str
    fire_time: datetime
    status: str
    reason: str | None
    session_id: uuid.UUID | None
    session_slug: str | None
    message_id: str | None
    reuse: str | None
    created_at: datetime
    updated_at: datetime
    started_at: datetime | None
    finished_at: datetime | None


class ScheduleView(BaseModel):
    id: uuid.UUID
    name: str
    cron: str
    timezone: str
    preset_id: uuid.UUID
    prompt: str
    enabled: bool
    next_fires: list[datetime]
    last_run: RunView | None
    created_at: datetime
    updated_at: datetime
