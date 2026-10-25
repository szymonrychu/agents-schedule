"""The CI workflow lints, tests and publishes the image and chart; chart-version.sh names main builds."""

import re
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parent.parent
TEXT = (ROOT / ".github/workflows/ci.yaml").read_text()
WF = yaml.safe_load(TEXT)
JOBS = WF["jobs"]
MAIN = "github.event_name == 'push' && github.ref == 'refs/heads/main'"
TAG = "startsWith(github.ref, 'refs/tags/v')"


def steps_text(job: str) -> str:
    return yaml.safe_dump(JOBS[job]["steps"])


def test_workflow_jobs_and_targets():
    for job in ("lint", "test", "images", "chart", "e2e", "chart-publish", "chart-publish-tag"):
        assert job in JOBS
    on = WF.get("on", WF.get(True))
    assert "pull_request" in on
    assert on["push"]["branches"] == ["main"]
    assert on["push"]["tags"] == ["v*"]
    assert "mise run lint" in steps_text("lint")
    assert "mise run test" in steps_text("test")
    chart = steps_text("chart")
    assert "mise run lint:chart" in chart and "mise run test:chart" in chart
    assert "mise run test:e2e" in steps_text("e2e")
    images = steps_text("images")
    assert "docker build" in images and "docker run --rm agents-schedule:ci --help" in images
    assert "harbor.szymonrichert.pl/containers/agents-schedule" in TEXT
    assert "$IMAGE:$SHORTSHA" in images and "--blob-chunk" in images
    for step in JOBS["images"]["steps"]:
        if "regctl" in str(step) or "HARBOR" in str(step):
            assert step.get("if") == MAIN, step
    assert JOBS["chart-publish"]["if"] == MAIN
    assert JOBS["chart-publish-tag"]["if"] == TAG
    for job in ("chart-publish", "chart-publish-tag"):
        assert JOBS[job]["env"]["CHART_REPO"] == "oci://harbor.szymonrichert.pl/charts"
        assert "$CHART_REPO/agents-schedule" in steps_text(job)
        assert "chart/agents-schedule" in steps_text(job)
    assert "chart-version.sh branch" in steps_text("chart-publish")
    assert "chart-version.sh tag" in steps_text("chart-publish-tag")
    assert "secrets." not in str({k: v for k, v in WF.items() if k != "jobs"})
    for name in ("lint", "test", "chart", "e2e"):
        assert "secrets." not in steps_text(name)


def test_regctl_installer_pinned_by_sha():
    uses = re.findall(r"regclient/actions/regctl-installer@(\S+)", TEXT)
    assert uses
    assert all(re.fullmatch(r"[0-9a-f]{40}", u) for u in uses)


def test_dockerfile_shape():
    text = (ROOT / "Dockerfile").read_text()
    assert re.search(r"^FROM python:3\.12-slim", text, re.M)
    assert "uv sync --frozen --no-dev" in text
    assert "USER 1000" in text
    assert 'ENTRYPOINT ["sched"]' in text
    assert "node" not in text


@pytest.fixture
def script_tree(tmp_path: Path) -> Path:
    (tmp_path / "scripts").mkdir()
    shutil.copy(ROOT / "scripts/chart-version.sh", tmp_path / "scripts/chart-version.sh")
    chart = tmp_path / "chart/agents-schedule"
    chart.mkdir(parents=True)
    (chart / "Chart.yaml").write_text("apiVersion: v2\nname: agents-schedule\nversion: 0.4.2\n")
    return tmp_path


@pytest.mark.parametrize(
    ("args", "code", "out"),
    [
        (["branch", "main", "abcdef1234567"], 0, "0.4.2-main.abcdef1"),
        (["tag", "v0.4.2", "abcdef1234567"], 0, "0.4.2"),
        (["tag", "v9.9.9", "abcdef1234567"], 1, ""),
        (["branch", "feature", "abcdef1234567"], 2, ""),
    ],
)
def test_chart_version_script(script_tree: Path, args: list[str], code: int, out: str):
    r = subprocess.run(
        ["bash", str(script_tree / "scripts/chart-version.sh"), *args],
        capture_output=True,
        text=True,
    )
    assert r.returncode == code, r.stderr
    assert r.stdout.strip() == out
