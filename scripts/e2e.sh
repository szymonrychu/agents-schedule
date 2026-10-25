#!/usr/bin/env bash
# kind end-to-end (mise run test:e2e): a kind cluster, the image built and loaded, a plain
# Postgres, the agents-common fake orchestrator (e2e/fake-orchestrator.yaml), the chart installed
# into namespace agents with e2e/values-kind.yaml (2 worker replicas), helm test, then
# tests/e2e against port-forwards of the api Service and the fake orchestrator. Only the API is
# exercised; no UI. The kind cluster is deleted on exit (E2E_KEEP=1 keeps it for debugging).
#
# Environment:
#   E2E_KEEP=1     keep the kind cluster (and its kubeconfig) after the run
#   E2E_PORT       local port of the api port-forward (default 18090)
#   E2E_FAKE_PORT  local port of the fake orchestrator port-forward (default 18091)
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

CLUSTER=agents-schedule-e2e
NS=agents
RELEASE=agents-schedule
IMAGE=agents-schedule:e2e
PORT="${E2E_PORT:-18090}"
FAKE_PORT="${E2E_FAKE_PORT:-18091}"
WORK="$(mktemp -d "${TMPDIR:-/tmp}/sched-e2e.XXXXXX")"
export KUBECONFIG="$WORK/kubeconfig"
PF_PIDS=()

log() { printf '\n==> %s\n' "$*"; }

diagnostics() {
  log "diagnostics"
  kubectl get nodes -o wide || true
  kubectl -n "$NS" get all,configmap -o wide || true
  kubectl -n "$NS" get events --sort-by=.lastTimestamp | tail -60 || true
  for c in api worker; do
    echo "--- $RELEASE-$c"
    kubectl -n "$NS" logs "deployment/$RELEASE-$c" --all-containers --tail=120 || true
  done
  echo "--- fake-orchestrator"
  kubectl -n "$NS" logs deployment/fake-orchestrator --tail=120 || true
  [ -f "$WORK/port-forward.log" ] && cat "$WORK/port-forward.log" || true
}

cleanup() {
  local rc=$?
  set +e
  for pid in "${PF_PIDS[@]}"; do kill "$pid" 2>/dev/null; done
  if [ "$rc" -ne 0 ] && [ -f "$KUBECONFIG" ]; then diagnostics; fi
  if [ "${E2E_KEEP:-}" = 1 ]; then
    log "keeping kind cluster $CLUSTER (KUBECONFIG=$KUBECONFIG)"
  else
    log "deleting kind cluster $CLUSTER"
    kind delete cluster --name "$CLUSTER" >/dev/null 2>&1
    rm -rf "$WORK"
  fi
  exit "$rc"
}
trap cleanup EXIT
# a signal exits through cleanup too, so the kind cluster never outlives the run
trap 'exit 143' TERM INT HUP

ARCH="$(docker version --format '{{.Server.Arch}}')"
PLATFORM="linux/$ARCH"

load_image() {
  # docker save --platform keeps multi-platform image stores (containerd snapshotter) from
  # exporting an index whose other platforms are missing, which kind cannot import.
  local image="$1" tar
  tar="$WORK/$(echo "$image" | tr '/:' '__').tar"
  # Older daemons have no --platform; their single-platform images save as they are.
  docker save --platform "$PLATFORM" -o "$tar" "$image" 2>/dev/null \
    || docker save -o "$tar" "$image"
  kind load image-archive --name "$CLUSTER" "$tar"
  rm -f "$tar"
}

forward() {
  # forward <svc> <local port> <remote port> <probe path>
  kubectl -n "$NS" port-forward "svc/$1" "$2:$3" >>"$WORK/port-forward.log" 2>&1 &
  PF_PIDS+=("$!")
  for _ in $(seq 1 60); do
    if curl -fsS "http://127.0.0.1:$2$4" >/dev/null 2>&1; then return; fi
    sleep 1
  done
  curl -fsS "http://127.0.0.1:$2$4" >/dev/null
}

log "creating kind cluster $CLUSTER"
kind delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true
kind create cluster --name "$CLUSTER" --kubeconfig "$KUBECONFIG" --wait 180s

log "building $IMAGE"
docker build -t "$IMAGE" .

log "loading $IMAGE into kind"
load_image "$IMAGE"

log "namespace $NS and the e2e Postgres"
kubectl create namespace "$NS"
# credentials are generated per run; nothing secret-shaped is committed
gen_secret() { openssl rand -hex 16; }
PG_PASSWORD="$(gen_secret)"
ORCH_TOKEN="$(gen_secret)"
kubectl -n "$NS" create secret generic postgres-credentials --from-literal="password=$PG_PASSWORD"
kubectl -n "$NS" apply -f - <<'EOF'
apiVersion: apps/v1
kind: Deployment
metadata: {name: postgres}
spec:
  replicas: 1
  selector:
    matchLabels: {app: postgres}
  template:
    metadata:
      labels: {app: postgres}
    spec:
      containers:
        - name: postgres
          image: postgres:16-alpine
          env:
            - {name: POSTGRES_USER, value: sched}
            - {name: POSTGRES_DB, value: sched}
            - name: POSTGRES_PASSWORD
              valueFrom:
                secretKeyRef: {name: postgres-credentials, key: password}
          ports:
            - {containerPort: 5432}
          readinessProbe:
            exec:
              command: [pg_isready, -U, sched, -d, sched]
            periodSeconds: 2
---
apiVersion: v1
kind: Service
metadata: {name: postgres}
spec:
  selector: {app: postgres}
  ports:
    - {port: 5432, targetPort: 5432}
EOF
kubectl -n "$NS" rollout status deployment/postgres --timeout=300s

log "fake orchestrator"
kubectl -n "$NS" create secret generic fake-orchestrator-token --from-literal="token=$ORCH_TOKEN"
kubectl -n "$NS" apply -f e2e/fake-orchestrator.yaml
kubectl -n "$NS" rollout status deployment/fake-orchestrator --timeout=300s

log "helm install $RELEASE"
helm install "$RELEASE" chart/agents-schedule -n "$NS" -f e2e/values-kind.yaml \
  --set-string "postgres.url=postgresql://sched:$PG_PASSWORD@postgres:5432/sched" \
  --set-string "orchestrator.token=$ORCH_TOKEN" --wait --timeout 10m

log "two worker replicas are ready"
READY="$(kubectl -n "$NS" get "deployment/$RELEASE-worker" -o jsonpath='{.status.readyReplicas}')"
[ "$READY" = 2 ] || { echo "worker readyReplicas is '$READY', expected 2" >&2; exit 1; }

log "helm test $RELEASE"
helm test "$RELEASE" -n "$NS" --timeout 5m

log "port-forwarding svc/$RELEASE-api to 127.0.0.1:$PORT and svc/fake-orchestrator to $FAKE_PORT"
forward "$RELEASE-api" "$PORT" 80 /readyz
forward fake-orchestrator "$FAKE_PORT" 8000 /_calls

log "pytest -m e2e tests/e2e"
# -rA: a "PASSED <node id>" line per test whatever the verbosity (pyproject's addopts -q
# cancels a -v), so the output names every test
env "SCHED_E2E_API_URL=http://127.0.0.1:$PORT" "SCHED_E2E_FAKE_URL=http://127.0.0.1:$FAKE_PORT" \
  uv run pytest -m e2e tests/e2e -p no:cacheprovider -rA
