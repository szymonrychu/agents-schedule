"""Request and response models of the schedules api."""

from __future__ import annotations

import uuid
from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field, field_validator


class ScheduleIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1, max_length=100)
    cron: str
    timezone: str | None = None  # None means SCHED_DEFAULT_TIMEZONE
    preset_id: uuid.UUID
    prompt: str = Field(min_length=1, max_length=16384)
    enabled: bool = True
    then_schedule_ids: list[uuid.UUID] = Field(default_factory=list, max_length=10)

    @field_validator("then_schedule_ids")
    @classmethod
    def _no_duplicates(cls, value: list[uuid.UUID]) -> list[uuid.UUID]:
        if len(set(value)) != len(value):
            raise ValueError("then_schedule_ids must not contain duplicates")
        return value


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
    then_schedule_ids: list[uuid.UUID] | None
    chain_status: str | None
    chained_from: uuid.UUID | None
    chain_source: str | None
    chain_ref: str | None
    chain_depth: int
    previous_name: str | None
    previous_status: str | None


class ScheduleView(BaseModel):
    id: uuid.UUID
    name: str
    cron: str
    timezone: str
    preset_id: uuid.UUID
    prompt: str
    enabled: bool
    then_schedule_ids: list[uuid.UUID]
    next_fires: list[datetime]
    last_run: RunView | None
    created_at: datetime
    updated_at: datetime
