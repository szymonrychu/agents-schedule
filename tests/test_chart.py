"""Chart render: api and worker Deployments, the secrets split and the required values."""

import shutil
import subprocess
import tempfile
from pathlib import Path

import pytest
import yaml

CHART = Path(__file__).resolve().parent.parent / "chart" / "agents-schedule"

DB_PASS = "".join(["db", "-", "fixture", "-", "value"])
TOKEN = "".join(["orch", "-", "fixture", "-", "value"])

BASE = {
    "image": {"tag": "1.2.3"},
    "postgres": {"url": "postgresql://" + "u:" + DB_PASS + "@db:5432/s"},
    "orchestrator": {"url": "http://orch.agents.svc", "token": TOKEN},
}


def _merge(a: dict, b: dict) -> dict:
    out = dict(a)
    for k, v in b.items():
        out[k] = _merge(out[k], v) if isinstance(v, dict) and isinstance(out.get(k), dict) else v
    return out


def _run(values: dict) -> subprocess.CompletedProcess:
    helm = shutil.which("helm")
    assert helm, "helm binary not found on PATH; the chart tests require it"
    with tempfile.TemporaryDirectory() as tmp:
        vf = Path(tmp) / "values.yaml"
        vf.write_text(yaml.safe_dump(values))
        return subprocess.run(
            [helm, "template", "sc", str(CHART), "-n", "agents", "-f", str(vf)],
            capture_output=True,
            text=True,
        )


def render(overrides: dict | None = None) -> list[dict]:
    proc = _run(_merge(BASE, overrides or {}))
    assert proc.returncode == 0, proc.stderr
    return [d for d in yaml.safe_load_all(proc.stdout) if d]


def deployments(docs: list[dict]) -> dict[str, dict]:
    return {
        d["metadata"]["labels"]["app.kubernetes.io/component"]: d
        for d in docs
        if d["kind"] == "Deployment"
    }


def _env(container: dict) -> dict[str, dict]:
    return {e["name"]: e for e in container.get("env", [])}


def test_components_render():
    docs = render()
    deps = deployments(docs)
    assert set(deps) == {"api", "worker"}
    for component, dep in deps.items():
        spec = dep["spec"]["template"]["spec"]
        (main,) = spec["containers"]
        assert main["command"] == ["sched"]
        assert main["args"] == [component]
        assert main["image"].endswith("/agents-schedule:1.2.3")
        (init,) = spec["initContainers"]
        assert init["name"] == "migrate"
        assert init["command"] == ["sched"]
        assert init["args"] == ["migrate"]
    api = deps["api"]["spec"]["template"]["spec"]["containers"][0]
    assert api["readinessProbe"]["httpGet"] == {"path": "/readyz", "port": 8000}
    assert api["livenessProbe"]["httpGet"] == {"path": "/healthz", "port": 8000}
    assert api["ports"][0]["name"] == "http"
    worker = deps["worker"]["spec"]["template"]["spec"]["containers"][0]
    assert "ports" not in worker
    assert "readinessProbe" not in worker
    services = [d for d in docs if d["kind"] == "Service"]
    assert [s["metadata"]["name"] for s in services] == ["sc-agents-schedule-api"]
    assert services[0]["spec"]["ports"][0]["name"] == "http"
    assert deps["worker"]["spec"]["replicas"] == 1


def test_secrets_wired():
    docs = render()
    config = next(d for d in docs if d["kind"] == "ConfigMap")
    blob = yaml.safe_dump(config)
    assert TOKEN not in blob
    assert DB_PASS not in blob
    assert "SCHED_DATABASE_URL" not in config["data"]
    assert "SCHED_ORCHESTRATOR_TOKEN" not in config["data"]
    assert config["data"]["SCHED_ORCHESTRATOR_URL"] == "http://orch.agents.svc"
    secrets = {d["metadata"]["name"]: d for d in docs if d["kind"] == "Secret"}
    assert {s["stringData"][k] for s in secrets.values() for k in s["stringData"]} >= {TOKEN}
    for dep in deployments(docs).values():
        spec = dep["spec"]["template"]["spec"]
        for container in [*spec["initContainers"], *spec["containers"]]:
            env = _env(container)
            ref = env["SCHED_DATABASE_URL"]["valueFrom"]["secretKeyRef"]
            assert ref["name"] in secrets and ref["key"] == "uri"
            assert "value" not in env["SCHED_DATABASE_URL"]
            assert "SCHED_ORCHESTRATOR_TOKEN" not in env
            path = config["data"]["SCHED_ORCHESTRATOR_TOKEN_FILE"]
            mount = next(m for m in container["volumeMounts"] if path.startswith(m["mountPath"]))
            volume = next(v for v in spec["volumes"] if v["name"] == mount["name"])
            assert volume["secret"]["secretName"] in secrets
            assert TOKEN not in yaml.safe_dump(dep)


def test_existing_secrets_are_referenced_not_rendered():
    docs = render(
        {
            "postgres": {"url": "", "existingSecret": "cnpg-app", "existingSecretKey": "dsn"},
            "orchestrator": {"token": "", "tokenSecret": {"name": "orch-tok", "key": "t"}},
        }
    )
    assert not [d for d in docs if d["kind"] == "Secret"]
    spec = deployments(docs)["api"]["spec"]["template"]["spec"]
    ref = _env(spec["containers"][0])["SCHED_DATABASE_URL"]["valueFrom"]["secretKeyRef"]
    assert ref == {"name": "cnpg-app", "key": "dsn"}
    vol = next(v for v in spec["volumes"] if v.get("secret", {}).get("secretName") == "orch-tok")
    assert vol["secret"]["items"] == [{"key": "t", "path": "token"}]


@pytest.mark.parametrize(
    "overrides, needle",
    [
        ({"postgres": {"url": ""}}, "postgres"),
        ({"postgres": {"url": "x", "existingSecret": "y"}}, "only one of postgres"),
        ({"orchestrator": {"url": ""}}, "orchestrator.url"),
        ({"orchestrator": {"token": ""}}, "orchestrator token"),
        ({"env": {"OTHER": "1"}}, "SCHED_"),
    ],
)
def test_required_values(overrides, needle):
    proc = _run(_merge(BASE, overrides))
    assert proc.returncode != 0
    assert needle in proc.stderr


def test_image_tag_defaults_to_app_version():
    docs = render({"image": {"tag": ""}})
    main = deployments(docs)["api"]["spec"]["template"]["spec"]["containers"][0]
    assert main["image"].endswith("/agents-schedule:0.1.0")


def test_env_and_port_reach_the_configmap():
    docs = render({"env": {"SCHED_MAX_IN_PROGRESS": 5}, "components": {"api": {"port": 9000}}})
    config = next(d for d in docs if d["kind"] == "ConfigMap")
    assert config["data"]["SCHED_MAX_IN_PROGRESS"] == "5"
    assert config["data"]["SCHED_HTTP_PORT"] == "9000"
    api = deployments(docs)["api"]["spec"]["template"]["spec"]["containers"][0]
    assert api["readinessProbe"]["httpGet"]["port"] == 9000
    assert api["ports"][0]["containerPort"] == 9000
