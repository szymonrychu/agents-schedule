import pytest

from agents_schedule.settings import load_settings

pytestmark = pytest.mark.critical


def test_secrets_never_appear_in_settings_repr(monkeypatch):
    monkeypatch.setenv("SCHED_DATABASE_URL", "postgresql://u:p@db/sched")
    monkeypatch.setenv("SCHED_ORCHESTRATOR_URL", "http://agents-orchestrator-services:8002")
    monkeypatch.setenv("SCHED_ORCHESTRATOR_TOKEN", "s3cr3t-value")
    s = load_settings()
    assert "s3cr3t-value" not in repr(s)
    assert "postgresql://u:p" not in repr(s)
