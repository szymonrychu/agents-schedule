"""SCHED_* settings of agents-schedule."""

import ipaddress
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from agents_common.settings import BaseServiceSettings, CsvList, SettingsError
from agents_common.settings import load_settings as _load
from pydantic import Field, ValidationInfo, field_validator, model_validator
from pydantic_settings import SettingsConfigDict

__all__ = ["Settings", "SettingsError", "load_settings"]


class Settings(BaseServiceSettings):
    """Environment-driven settings shared by every process (api, scheduler, migrate)."""

    model_config = SettingsConfigDict(env_prefix="SCHED_", extra="ignore")

    allowed_cidrs: CsvList = ["127.0.0.1/32"]
    max_in_progress: int = Field(3, ge=1, le=50)
    max_per_schedule: int = Field(1, ge=1, le=10)
    default_timezone: str = "Europe/Warsaw"
    tick_seconds: float = Field(15, ge=1, le=300)
    poll_seconds: float = Field(10, ge=1, le=300)
    dispatch_max_attempts: int = Field(5, ge=1, le=20)
    run_timeout_seconds: int = Field(3600, ge=60, le=86400)
    admin_port: int = Field(8001, ge=1, le=65535)
    admin_tokens_file: str | None = None

    @model_validator(mode="before")
    @classmethod
    def _one_token_source(cls, data: object) -> object:
        if (
            isinstance(data, dict)
            and data.get("orchestrator_token")
            and data.get("orchestrator_token_file")
        ):
            raise ValueError(
                "SCHED_ORCHESTRATOR_TOKEN::set only one of SCHED_ORCHESTRATOR_TOKEN"
                " and SCHED_ORCHESTRATOR_TOKEN_FILE"
            )
        return data

    @model_validator(mode="after")
    def _distinct_admin_port(self) -> "Settings":
        if self.admin_tokens_file and self.admin_port == self.http_port:
            raise ValueError("SCHED_ADMIN_PORT::must differ from SCHED_HTTP_PORT")
        return self

    @field_validator("allowed_cidrs")
    @classmethod
    def _cidrs(cls, value: list[str], info: ValidationInfo) -> list[str]:
        for item in value:
            try:
                ipaddress.ip_network(item, strict=False)
            except ValueError:
                raise ValueError(
                    f"SCHED_{str(info.field_name).upper()}::not a CIDR: {item!r}"
                ) from None
        return value

    @field_validator("default_timezone")
    @classmethod
    def _iana(cls, value: str) -> str:
        try:
            ZoneInfo(value)
        except (ZoneInfoNotFoundError, ValueError, OSError):
            raise ValueError(f"SCHED_DEFAULT_TIMEZONE::unknown IANA zone: {value!r}") from None
        return value

    def api_networks(self) -> list[ipaddress.IPv4Network | ipaddress.IPv6Network]:
        return [ipaddress.ip_network(c, strict=False) for c in self.allowed_cidrs]


def load_settings() -> Settings:
    """Load Settings from the environment or raise SettingsError naming every bad variable."""
    return _load(Settings)
