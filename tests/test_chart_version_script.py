"""chart-version.sh names main builds."""

import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent


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
        (["tag", "v0.4.2", "abcdef1234567"], 2, ""),
        (["tag", "v9.9.9", "abcdef1234567"], 2, ""),
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
    if code == 2:
        assert r.stdout == ""
