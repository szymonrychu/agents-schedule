import pytest
from agents_common.settings import SettingsError

from agents_schedule.settings import Settings, load_settings

REQUIRED = {
    "SCHED_DATABASE_URL": "postgresql://u:p@db/sched",
    "SCHED_ORCHESTRATOR_URL": "http://agents-orchestrator-services:8002",
    "SCHED_ORCHESTRATOR_TOKEN": "s3cr3t-value",
}


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    import os

    for key in list(os.environ):
        if key.startswith("SCHED_"):
            monkeypatch.delenv(key)


def set_env(monkeypatch, **extra):
    for key, value in {**REQUIRED, **extra}.items():
        monkeypatch.setenv(key, value)


def test_defaults(monkeypatch):
    set_env(monkeypatch)
    s = load_settings()
    assert isinstance(s, Settings)
    assert s.allowed_cidrs == ["127.0.0.1/32"]
    assert s.http_port == 8000
    assert s.max_in_progress == 3
    assert s.max_per_schedule == 1
    assert s.default_timezone == "Europe/Warsaw"
    assert s.tick_seconds == 15
    assert s.poll_seconds == 10
    assert s.dispatch_max_attempts == 5
    assert s.run_timeout_seconds == 3600
    assert s.orchestrator_token.get_secret_value() == "s3cr3t-value"
    assert "s3cr3t-value" not in repr(s)
    assert "postgresql://u:p" not in repr(s)


def test_allowed_cidrs_csv(monkeypatch):
    set_env(monkeypatch, SCHED_ALLOWED_CIDRS="127.0.0.1/32, 10.0.0.0/8")
    assert load_settings().allowed_cidrs == ["127.0.0.1/32", "10.0.0.0/8"]


def test_errors_name_each_variable(monkeypatch):
    monkeypatch.setenv("SCHED_MAX_IN_PROGRESS", "0")
    monkeypatch.setenv("SCHED_MAX_PER_SCHEDULE", "11")
    monkeypatch.setenv("SCHED_TICK_SECONDS", "301")
    monkeypatch.setenv("SCHED_POLL_SECONDS", "0")
    monkeypatch.setenv("SCHED_DISPATCH_MAX_ATTEMPTS", "21")
    monkeypatch.setenv("SCHED_RUN_TIMEOUT_SECONDS", "59")
    monkeypatch.setenv("SCHED_ALLOWED_CIDRS", "not-a-cidr")
    with pytest.raises(SettingsError) as err:
        load_settings()
    assert set(err.value.problems) >= {
        "SCHED_DATABASE_URL",
        "SCHED_ORCHESTRATOR_URL",
        "SCHED_ORCHESTRATOR_TOKEN",
        "SCHED_MAX_IN_PROGRESS",
        "SCHED_MAX_PER_SCHEDULE",
        "SCHED_TICK_SECONDS",
        "SCHED_POLL_SECONDS",
        "SCHED_DISPATCH_MAX_ATTEMPTS",
        "SCHED_RUN_TIMEOUT_SECONDS",
        "SCHED_ALLOWED_CIDRS",
    }


def test_token_sources(monkeypatch, tmp_path):
    set_env(monkeypatch)
    monkeypatch.delenv("SCHED_ORCHESTRATOR_TOKEN")
    path = tmp_path / "token"
    path.write_text("from-file\n")
    monkeypatch.setenv("SCHED_ORCHESTRATOR_TOKEN_FILE", str(path))
    assert load_settings().orchestrator_token.get_secret_value() == "from-file"

    monkeypatch.setenv("SCHED_ORCHESTRATOR_TOKEN", "s3cr3t-value")
    with pytest.raises(SettingsError) as both:
        load_settings()
    assert "SCHED_ORCHESTRATOR_TOKEN" in both.value.problems

    monkeypatch.delenv("SCHED_ORCHESTRATOR_TOKEN")
    monkeypatch.delenv("SCHED_ORCHESTRATOR_TOKEN_FILE")
    with pytest.raises(SettingsError) as none:
        load_settings()
    assert "SCHED_ORCHESTRATOR_TOKEN" in none.value.problems

    monkeypatch.setenv("SCHED_ORCHESTRATOR_TOKEN_FILE", str(tmp_path / "missing"))
    with pytest.raises(SettingsError) as missing:
        load_settings()
    assert "SCHED_ORCHESTRATOR_TOKEN_FILE" in missing.value.problems


def test_default_timezone_must_be_iana(monkeypatch):
    set_env(monkeypatch, SCHED_DEFAULT_TIMEZONE="Mars/Olympus")
    with pytest.raises(SettingsError) as err:
        load_settings()
    assert "SCHED_DEFAULT_TIMEZONE" in err.value.problems
    monkeypatch.setenv("SCHED_DEFAULT_TIMEZONE", "UTC")
    assert load_settings().default_timezone == "UTC"
